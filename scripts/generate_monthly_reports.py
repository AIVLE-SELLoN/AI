"""월간 리포트 일괄 생성 — 매월 1일 배치 진입점.

## 스케줄 (2026-08-03 확정)

    대상 데이터   전월 1일 00:00:00.00 ~ 말일 23:59:59.99
    시작         당월 1일 **00:00** — 집계부터 시작한다
    집계 종료 즉시 보고서 생성으로 이어지고, **오전 8시까지 S3 적재까지 끝낸다**

단계를 나눠 두되(`--stage`) 운영은 `--stage all` 한 번으로 돈다. 나눠 둔 이유는
성격이 다른 부하라 리허설·재실행을 따로 하기 위해서다:

    [1단계] 집계   순열검정 B=10,000 이 상품×채널쌍마다 돌아 **CPU** 를 쓴다. LLM 미사용.
                   산출물: data/monthly_inputs/{YYYY-MM}.json
    [2단계] 생성   상품별 LLM 문장 생성·검증 → **전 상품 합본 PDF 1개** → S3 → 콜백 1건.

## 산출물이 월 1개인 이유

PDF 는 상품별이 아니라 **전 상품을 합친 1개**다(2026-08-03 확정). UI 는 첫 페이지(표지)만
띄우고 전체는 presigned URL 로 내려받는다. 따라서
  - S3 객체도, 콜백도 **월 1건**이다.
  - 상품 하나가 실패해도 합본은 나간다. 빠진 상품은 표지에 보류/실패로 표기된다.

## 부하 제어 세 가지

1. **동시성 상한** — 세마포어로 동시에 도는 상품 수를 묶는다. 높이면 빨라지지만
   LLM rate limit(429)에 걸려 재시도가 돌면서 오히려 느려진다. 기본 4.
2. **상품 단위 실패 격리** — 한 상품이 실패해도 나머지는 계속 간다. 전체를 한
   트랜잭션으로 묶지 않는다.
3. **보류 게이트 = 감속기** — VOC 10건 미만 상품은 세마포어를 **획득하기 전에** 걸러
   HOLD 로 처리한다. LLM 슬롯을 점유하지 않으므로, 표본이 적은 상품이 많을수록
   실제 호출량이 자동으로 줄어든다.

실행:
    # 운영: 매월 1일 00:00 크론 — 08:00 까지 완료 목표
    python scripts/generate_monthly_reports.py --stage all --month 2026-07 --deadline 08:00

    # 단계별 재실행
    python scripts/generate_monthly_reports.py --stage aggregate --month 2026-07
    python scripts/generate_monthly_reports.py --stage generate  --month 2026-07

    # 리허설 (LLM 미호출)
    python scripts/generate_monthly_reports.py --stage aggregate --month 2026-07 --permutations 200
    python scripts/generate_monthly_reports.py --stage generate --month 2026-07 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import constants
from app.core.mq import close_mq, new_trace_id, publish_report_generated
from app.core.schemas import CallbackStatus, MonthlyReportInput
from app.reporting.monthly_aggregator import aggregate_monthly_inputs
from app.reporting.monthly_report_service import (
    build_report_id,
    compile_and_upload_monthly_book,
    generate_monthly_report_output,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("MonthlyBatch")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = os.getenv("RAW_DB_PATH", "./data/raw.db")
INPUTS_DIR = ROOT / "data" / "monthly_inputs"
RESULTS_DIR = ROOT / "data" / "monthly_results"

DEFAULT_CONCURRENCY = 4


# ── 공통 ────────────────────────────────────────────────────────────────


class DeadlineTracker:
    """마감 시각 대비 진행 상황을 알린다.

    초과해도 **중단하지 않는다** — 8시/10시는 목표이지 데이터 유실 사유가 아니다.
    늦어지는 것을 모르고 지나가는 게 문제라, 경고만 올리고 끝까지 처리한다.
    """

    def __init__(self, deadline: str | None) -> None:
        self.deadline: datetime | None = None
        if deadline:
            hour, minute = (int(x) for x in deadline.split(":"))
            now = datetime.now().astimezone()
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            # 마감이 이미 지난 시각이면 다음 날로 본다(자정 넘겨 도는 집계 단계 대응)
            self.deadline = target if target > now else target + timedelta(days=1)
        self.started = time.monotonic()

    def report(self, done: int, total: int) -> None:
        elapsed = time.monotonic() - self.started
        if not total:
            return
        eta = elapsed / done * (total - done) if done else 0.0
        message = f"[PROGRESS] {done}/{total} · 경과 {elapsed / 60:.1f}분 · 남은 예상 {eta / 60:.1f}분"

        if self.deadline is None:
            logger.info(message)
            return

        remaining = (self.deadline - datetime.now().astimezone()).total_seconds()
        if eta > remaining:
            logger.warning(
                f"{message} · ⚠️ 마감({self.deadline:%H:%M}) 초과 예상 "
                f"— 동시성을 올리거나 다음 달 스케줄을 앞당기세요"
            )
        else:
            logger.info(f"{message} · 마감까지 {remaining / 60:.1f}분")


def _display_path(path: Path) -> str:
    """로그용 경로. 저장소 밖(테스트 등)을 가리켜도 터지지 않게 절대경로로 떨어뜨린다."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _month_path(directory: Path, report_month: str, suffix: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{report_month}{suffix}"


# ── 1단계: 집계 ─────────────────────────────────────────────────────────


def run_aggregate(args: argparse.Namespace) -> int:
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        logger.error(f"[DB ERROR] raw DB 가 없습니다: {db_path}")
        return 1

    tracker = DeadlineTracker(args.deadline)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000;")

    try:
        inputs = aggregate_monthly_inputs(
            conn,
            args.month,
            product_group_ids=args.products.split(",") if args.products else None,
            n_permutations=args.permutations,
        )
    finally:
        conn.close()

    out = _month_path(INPUTS_DIR, args.month, ".json")
    out.write_text(
        json.dumps([i.model_dump(mode="json") for i in inputs], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    tracker.report(len(inputs), len(inputs))
    hold = sum(1 for i in inputs if i.total_voc_count < constants.MIN_VOC_COUNT_FOR_REPORT)
    logger.info(
        f"[AGGREGATE DONE] {len(inputs)}개 상품 → {_display_path(out)} "
        f"(보류 예정 {hold}개 · 2단계 LLM 대상 {len(inputs) - hold}개)"
    )
    return 0


# ── 2단계: 생성 ─────────────────────────────────────────────────────────


def load_inputs(report_month: str) -> list[MonthlyReportInput]:
    path = _month_path(INPUTS_DIR, report_month, ".json")
    if not path.exists():
        raise FileNotFoundError(f"집계 결과가 없습니다: {path} (--stage aggregate 를 먼저 실행)")
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [MonthlyReportInput.model_validate(item) for item in raw]


async def _generate_one(
    input_data: MonthlyReportInput,
    semaphore: asyncio.Semaphore,
    dry_run: bool,
) -> dict[str, Any]:
    """상품 1건의 문장 생성. 예외를 여기서 흡수해 **다른 상품에 전파되지 않게** 한다.

    PDF 는 만들지 않는다 — 합본은 전 상품이 끝난 뒤 한 번만 만든다.
    """
    report_id = build_report_id(input_data)

    # [감속기] 보류 대상은 세마포어를 잡지 않는다 — LLM 슬롯을 점유할 이유가 없다.
    if input_data.total_voc_count < constants.MIN_VOC_COUNT_FOR_REPORT:
        logger.info(f"[HOLD] {report_id} (VOC {input_data.total_voc_count}건)")
        return {
            "report_id": report_id,
            "product_group_id": input_data.product_group_id,
            "status": CallbackStatus.HOLD_INSUFFICIENT_DATA.value,
            # 합본이 보류 페이지에 상품명·VOC 건수를 찍어야 해서 입력을 통째로 넘긴다
            "input": input_data,
        }

    if dry_run:
        return {
            "report_id": report_id,
            "product_group_id": input_data.product_group_id,
            "status": "DRY_RUN",
        }

    async with semaphore:
        started = time.monotonic()
        try:
            output, status, errors = await generate_monthly_report_output(input_data)
        except Exception as exc:  # 상품 단위 격리 — 어떤 예외도 배치를 멈추지 않는다
            logger.exception(f"[ISOLATED FAILURE] {report_id}")
            return {
                "report_id": report_id,
                "product_group_id": input_data.product_group_id,
                "status": CallbackStatus.FAILED_ERROR.value,
                "error": f"{type(exc).__name__}: {exc}",
            }

        logger.info(f"[{status.value}] {report_id} ({time.monotonic() - started:.1f}초)")
        return {
            "report_id": report_id,
            "product_group_id": input_data.product_group_id,
            "status": status.value,
            "errors": errors,
            "input": input_data,
            "output": output,
        }


async def run_generate(args: argparse.Namespace) -> int:
    inputs = load_inputs(args.month)
    if args.products:
        wanted = set(args.products.split(","))
        inputs = [i for i in inputs if i.product_group_id in wanted]
    if args.limit:
        inputs = inputs[: args.limit]

    tracker = DeadlineTracker(args.deadline)
    semaphore = asyncio.Semaphore(args.concurrency)
    logger.info(
        f"[GENERATE] {args.month} · 상품 {len(inputs)}개 · 동시성 {args.concurrency}"
        f"{' · DRY-RUN' if args.dry_run else ''}"
    )

    tasks = [asyncio.create_task(_generate_one(i, semaphore, args.dry_run)) for i in inputs]
    results: list[dict[str, Any]] = []
    for done, task in enumerate(asyncio.as_completed(tasks), start=1):
        results.append(await task)
        if done % max(1, len(tasks) // 10) == 0 or done == len(tasks):
            tracker.report(done, len(tasks))

    counts = Counter(r["status"] for r in results)
    book: dict[str, Any] = {"status": "SKIPPED"}

    if not args.dry_run:
        # 성공분만 모아 **월 1개 합본**을 만든다. 보류·실패 상품은 표지에 표기된다.
        items = [
            {"input": r["input"], "output": r["output"]}
            for r in results
            if r["status"] == CallbackStatus.SUCCESS.value and r.get("output")
        ]
        items.sort(key=lambda i: i["input"].product_group_id)
        # 보류는 **입력 객체**로 넘긴다 — 합본이 보류 페이지에 상품명·VOC 건수를 찍는다.
        held = [
            r["input"] for r in results
            if r["status"] == CallbackStatus.HOLD_INSUFFICIENT_DATA.value and r.get("input")
        ]
        held.sort(key=lambda i: i.product_group_id)
        failed = [r["product_group_id"] for r in results if r["status"].startswith("FAILED")]

        logger.info(f"[BOOK] 합본 생성 시작 — 수록 {len(items)}개 / 보류 {len(held)} / 실패 {len(failed)}")
        result = await compile_and_upload_monthly_book(
            args.month,
            [{"input": i["input"], "report": i["output"]} for i in items],
            held_inputs=held,
            failed_products=failed,
        )
        book = {
            "report_id": result.callback.report_id,
            "status": result.callback.status.value,
            "included_products": len(items),
            "s3_key": result.callback.pdf_s3_meta.s3_full_key if result.callback.pdf_s3_meta else None,
            "file_size_bytes": (
                result.callback.pdf_s3_meta.file_size_bytes if result.callback.pdf_s3_meta else None
            ),
            "notice_message": result.callback.notice_message,
            "published": False,
        }

        # 발행은 **실패해도 배치를 죽이지 않는다.** PDF 는 이미 S3 에 올라갔고, 이벤트만
        # 다시 쏘면 되는 상태다(멱등 키가 report_id 라 메인이 upsert 한다). 여기서 예외를
        # 올리면 요약 파일조차 안 남아 무엇이 성공했는지 알 수 없게 된다.
        trace_id = new_trace_id()
        try:
            await publish_report_generated(result.callback, args.month, trace_id)
            book["published"] = True
            logger.info(f"[PUBLISH] {result.callback.report_id} 발행 완료 (trace={trace_id})")
        except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
            book["publish_error"] = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"[PUBLISH FAILED] {result.callback.report_id} — {exc}. "
                f"PDF 는 S3 에 있으므로 이벤트만 재발행하면 된다 (trace={trace_id})"
            )
        finally:
            await close_mq()

    summary = {
        "report_month": args.month,
        "generated_at": datetime.now(UTC).astimezone().isoformat(),
        "concurrency": args.concurrency,
        "total": len(results),
        "status_counts": dict(counts),
        "book": book,
        "results": sorted(
            [{k: v for k, v in r.items() if k not in ("input", "output")} for r in results],
            key=lambda r: r["report_id"],
        ),
    }
    out = _month_path(RESULTS_DIR, args.month, ".json")
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n========== [월간 리포트 생성 요약] ==========")
    for status, count in counts.most_common():
        print(f" - 상품 {status}: {count}건")
    if book.get("status") != "SKIPPED":
        size = book.get("file_size_bytes") or 0
        print(f" 합본 {book['status']}: {book.get('included_products')}개 수록 · {size / 1024:.0f}KB")
        print(f"       {book.get('s3_key')}")
    print(f" 총 {len(results)}건 → {_display_path(out)}")
    print("=============================================")

    # 합본이 실패했으면 그게 곧 배치 실패다(산출물이 없다).
    if book.get("status", "SKIPPED").startswith("FAILED"):
        return 1
    failed_products = sum(v for k, v in counts.items() if k.startswith("FAILED"))
    return 1 if failed_products else 0


# ── 진입점 ──────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="월간 리포트 일괄 생성 (집계/생성 2단계)")
    parser.add_argument(
        "--stage", choices=["aggregate", "generate", "all"], default="all", help="실행 단계"
    )
    parser.add_argument("--month", required=True, help="보고서 연월 (YYYY-MM)")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="raw DB 경로(집계 단계)")
    parser.add_argument("--products", default=None, help="쉼표 구분 상품 그룹 필터")
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help=f"동시에 생성할 상품 수 (기본 {DEFAULT_CONCURRENCY}). 올리면 rate limit 위험",
    )
    parser.add_argument("--limit", type=int, default=None, help="생성 상품 수 상한(시험 실행)")
    parser.add_argument(
        "--permutations", type=int, default=None,
        help="순열검정 반복 횟수 override — 리허설용. 낮추면 p값 해상도가 떨어진다",
    )
    parser.add_argument("--deadline", default=None, help="마감 시각 HH:MM (초과 시 경고만)")
    parser.add_argument("--dry-run", action="store_true", help="생성 단계에서 LLM 미호출")
    args = parser.parse_args()

    exit_code = 0
    if args.stage in ("aggregate", "all"):
        exit_code = run_aggregate(args)
        if exit_code:
            sys.exit(exit_code)
    if args.stage in ("generate", "all"):
        exit_code = asyncio.run(run_generate(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    # ⚠️ 출력보다 먼저. 이 스크립트의 로그·도움말에 `—`(U+2014)가 들어 있는데 cp949 에는
    #    없어서, 한국어 윈도우 콘솔에서는 `--help` 조차 UnicodeEncodeError 로 죽는다.
    #    발행 실패 로그도 같은 문자를 쓰므로, 인코딩 때문에 **실패 사유가 안 보이는**
    #    상황이 된다. (app/core/console.py — PR #36 이 배치 요약에서 겪은 것과 같은 문제)
    from app.core.console import force_utf8_output

    force_utf8_output()
    main()
