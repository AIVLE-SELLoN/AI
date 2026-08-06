"""담당: 서영 (Agent2) — 일 1회 탐지 배치. **운영 진입점.**

왜 여기 있나
------------
백엔드 확정 구조상 탐지의 시작점은 **AI 노드 안에서 매일 도는 배치**다. 메인→AI 방향
큐에 '탐지 요청'이 없고(셀러 피드백 2종뿐), `/detect` 는 body 로 items(분류 결과 전량)
를 받는 API 인데 그걸 손에 든 건 분류를 수행하는 AI 노드 자신뿐이다 — 백엔드는 분류를
하지 않는다. 그래서 `POST /detect` 는 운영 경로가 아니라 **재현·디버깅 창구**로 남는다.
(2026-08-05 지인님 결선 정리)

이 모듈이 `detection/service.py` 밖에 있는 이유
    탐지 → 개선안 → CS 가이드라인 → 발행 루프를 detection 안에 넣으면 detection 이
    recommendation 을 import 하게 되어 **"각 모듈은 core 에서만 가져다 쓴다"**는 팀
    규칙이 깨진다. 그래서 양쪽 바깥에 있는 이 모듈이 둘 다 부른다.

실행 (스케줄링은 아직 안 붙인다 — 주체가 백엔드인지 AI 노드인지 미정)::

    python -m app.batch.daily --dry-run          # LLM 0회, 호출 횟수만 실측
    python -m app.batch.daily --max-alerts 3     # 비용 상한
    python -m app.batch.daily --window-end 2026-08-28
    python -m app.batch.daily --input-source golden --dry-run   # 평가·재현 (oracle)

⚠️ 인프라가 아직 없다 (RabbitMQ · `classified_item` 테이블). 그래서 입력은 **주입**
   받는 형태로 뒀다 — `run_batch(load_inputs=...)`. 기본값 `load_inputs_from_db()` 는
   rawDB 스키마 §5 확정 전까지 NotImplementedError 다.
   발행·개선안·가이드라인 함수도 아직 없어서 import 폴백으로 뒀다. 담당자가 만들면
   **이 파일 수정 없이** 자동으로 연결된다.

🔴 **이 모듈은 `data/golden/` 을 읽지 않는다.** `eval/README.md` §232("`data/golden/`
   은 `eval/` 만 읽는다 — `app/` 이 import 하면 컨닝이다")를 지키기 위해 골든 로더를
   `scripts/golden_inputs.py` 로 뺐다(2026-08-06). 평가·재현으로 배치를 돌릴 때만
   `--input-source golden` 으로 주입하고, 그때는 요약이 oracle 경고를 함께 낸다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS
from app.core.schemas import ClassifiedItem, DetectionAlert
from app.detection.loader import check_coverage, unreliable_slots
from app.detection.service import detect_anomaly

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]

STATE_PATH = ROOT / "data" / "batch_state" / "published_alerts.json"
"""발행 기록 캐시. **`prior_alerts` 의 출처.**

alert 저장의 정본은 서비스 DB(백엔드 단독 소유)이고 AI 는 직접 접근할 수 없다. 하지만
재알림 억제(`filter_suppressed`)와 기준선 오염 방지(`_alert_days`)가 `prior_alerts` 를
필요로 하는데, 그건 **AI 가 자기가 발행한 것이라 이미 안다.** 그래서 서비스 DB 쓰기가
아니라 '자기 발행 기록 캐시'로 둔다. 백엔드 조회 API 가 생기면 이 자리만 교체한다.

⚠️ 이게 없으면 매일 배치가 첫 실행처럼 굴러서 ① 같은 알림이 억제 기간 내내 매일 나가고
   ② 더 나쁘게는 `_alert_days` 가 비어 **지속되는 이상이 과거 윈도우에 섞여 '새로운
   평소'로 굳고 알림이 스스로 꺼진다.** 매일 도는 배치에서는 며칠 안에 실제로 난다.

🔴 **컨테이너로 올릴 때 이 경로에 볼륨을 붙일 것** (지인님 지적, 2026-08-05).
   이미지 안 임시 파일로 두면 재시작마다 날아가서 **캐시가 없는 것과 같아진다** —
   위 ①② 가 그대로 재현된다. compose 예시::

       volumes:
         - ./data/batch_state:/app/data/batch_state

   `data/**` 는 .gitignore 대상이라 저장소에도 안 올라간다. 배포 시 호스트 경로를
   반드시 확보할 것.
"""

STATE_RETENTION_DAYS = CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS
"""캐시 보관 기간(일). 임의값이 아니라 두 소비처가 요구하는 범위의 합이다.

`_alert_days` 가 과거 윈도우(28일) 안의 알림 구간을 제외하고, 억제 판정은 현재
윈도우(7일) 기준으로 경과일을 센다. 그보다 짧게 자르면 그 경계에서 조용히 억제가
풀리고 기준선이 오염된다.
"""


# ── 담당자 미완성 함수 — import 폴백 ────────────────────────────
# 시그니처는 지인님 결선 정리(2026-08-05)의 예시 코드를 그대로 따른다.
# 실물이 생기면 아래 except 가 안 타므로 이 파일은 손댈 필요가 없다.

try:  # pragma: no cover - 실물이 생기면 이쪽
    from app.core.mq import (  # type: ignore[attr-defined]
        new_trace_id,
        publish_anomaly_analyzed,
        publish_guideline_generated,
    )

    MQ_AVAILABLE = True
except ImportError:  # pragma: no cover - 인프라 도입 전
    MQ_AVAILABLE = False

    def new_trace_id() -> str:
        return f"trace-{uuid.uuid4().hex[:16]}"

    async def publish_anomaly_analyzed(alert: Any, rec: Any, trace_id: str) -> None:
        logger.info(
            "[MQ 미구현] ai.anomaly.analyzed 발행 생략 alert=%s", alert.alert_id
        )

    async def publish_guideline_generated(guideline: Any, trace_id: str) -> None:
        logger.info("[MQ 미구현] ai.guideline.generated 발행 생략")


try:  # pragma: no cover
    from app.recommendation.pipeline import (
        generate_for_alert,  # type: ignore[attr-defined]
    )

    RECOMMENDATION_AVAILABLE = True
except ImportError:  # pragma: no cover
    RECOMMENDATION_AVAILABLE = False

    async def generate_for_alert(alert: Any) -> Any:
        logger.info("[Agent3 미연결] 개선안 생성 생략 alert=%s", alert.alert_id)
        return None


try:  # pragma: no cover
    from app.reporting.cs_reply_service import (
        generate_guideline,  # type: ignore[attr-defined]
    )

    GUIDELINE_AVAILABLE = True
except ImportError:  # pragma: no cover
    GUIDELINE_AVAILABLE = False

    async def generate_guideline(alert: Any, rec: Any) -> Any:
        logger.info("[가이드라인 미연결] 생성 생략 alert=%s", alert.alert_id)
        return None


# ── dry-run 스텁 ────────────────────────────────────────────────

STUB_CAUSE = "사진_색감_오차"
"""스텁이 돌려줄 원인 라벨. `SCOPE_LIMIT_LABELS`(실물_염색_편차·실제_원단_문제)를
피한다 — 그 라벨이면 Agent3 가 라우팅·생성을 통째로 건너뛰어 호출 수 추정이 어긋난다.
(crosscheck 스크립트와 같은 이유·같은 값)"""


class _CountingClient:
    """LLM 을 부르지 않고 호출 횟수만 세는 스텁. `--dry-run` 전용.

    `detect_anomaly(client=...)` 에 주입하면 [6] 원인분류가 **실제로 몇 번 불릴지**를
    돈 한 푼 안 쓰고 실측한다. 후보 수를 눈으로 세는 추정이 아니다.

    프롬프트에 박힌 cs_id 를 그대로 되돌려준다 — 개수를 맞춰야 `root_cause.total` 이
    현실적인 크기로 나오고, 그래야 `recommended_action` 이 실제와 비슷하게 산출된다.
    비우면 원인이 '미특정'으로 빠져 게이트 통과 수를 실제보다 적게 잡는다.
    """

    def __init__(self) -> None:
        self.calls = 0
        self.empty_extractions = 0

    async def complete_json(
        self, prompt: str, *, trace_key: str = "-", **_: object
    ) -> dict:
        import re

        self.calls += 1
        cs_ids = re.findall(r'"cs_id"\s*:\s*"([^"]+)"', prompt)
        if not cs_ids:
            self.empty_extractions += 1
        return {
            "results": [
                {
                    "cs_id": cs_id,
                    "cause": STUB_CAUSE,
                    "confidence": 0.9,
                    "evidence": "",
                    "aspect_match": True,
                }
                for cs_id in cs_ids
            ]
        }


# ── 입력 ────────────────────────────────────────────────────────


def load_inputs_from_db() -> tuple[list[ClassifiedItem], list[dict]]:
    """(items, documents) 를 원본 DB 에서 읽는다. **기본 입력원.**

    items 는 워커가 분류해 `classified_item` 에 넣은 결과, documents 는 `cs`·`reviews`
    원문이다. 둘을 따로 읽는 이유는 **분모가 documents 에서 나와야 하기 때문**이다 —
    리뷰는 aspect 0개면 `classified_item` 에 행이 아예 없어서(`explode_to_rows` 가
    aspect 마다 1행), items 로 분모를 세면 그 문서가 통째로 빠지고 부정률이 부풀려진다
    (탐지 분모 산출 방식 §1).

    ⚠️ **아직 구현할 수 없다.** rawDB 스키마 §5(`item_id` 연결 방식, `mapped_data`
       컬럼명)가 미확정이고 `classified_item` 테이블이 실재하지 않는다. 확정되면
       여기만 채우면 되고, `run_batch(load_inputs=...)` 시그니처는 그대로다.

    평가·재현으로 배치를 돌리려면 `scripts/golden_inputs.load_golden_inputs` 를
    주입한다 — 골든을 `app/` 안에서 읽지 않기 위해 밖으로 뺐다(eval/README §232).
    """
    raise NotImplementedError(
        "classified_item 테이블 미구현 (rawDB 스키마 §5 확정 대기). "
        "평가·재현은 run_batch(load_inputs=scripts.golden_inputs.load_golden_inputs) "
        "로 주입할 것 — 그 입력은 oracle 이라 결과가 탐지 성능이 아니다."
    )


# ── 발행 기록 캐시 ──────────────────────────────────────────────


def load_prior_alerts(
    window_end: date, path: Path = STATE_PATH
) -> list[DetectionAlert]:
    """캐시에서 `prior_alerts` 를 읽는다. 없으면 빈 리스트(첫 실행)."""
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    alerts = [DetectionAlert.model_validate(a) for a in raw]
    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    return [a for a in alerts if a.window_end >= cutoff]


def save_published(
    published: list[DetectionAlert], window_end: date, path: Path = STATE_PATH
) -> int:
    """발행된 알림을 캐시에 누적한다. 반환값은 저장 후 총 건수.

    ⚠️ **`published` 만 넣는다. `suppressed` 는 넣으면 안 된다.** `prior_alerts` 의
       정의가 "과거 **발행된** 알림"이라, 억제된 걸 넣으면 다음 배치가 그걸 기준으로
       또 억제해 이중 억제가 된다.

    같은 `alert_id` 는 덮어쓴다(같은 날 재실행 시 중복 누적 방지).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, dict] = {}
    if path.exists():
        for a in json.loads(path.read_text(encoding="utf-8")):
            existing[a["alert_id"]] = a
    for alert in published:
        existing[alert.alert_id] = alert.model_dump(mode="json")

    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    kept = [
        a for a in existing.values() if date.fromisoformat(a["window_end"]) >= cutoff
    ]
    path.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(kept)


# ── 배치 본체 ───────────────────────────────────────────────────


async def run_batch(
    *,
    window_end: date | None = None,
    max_alerts: int | None = None,
    dry_run: bool = False,
    state_path: Path = STATE_PATH,
    load_inputs: Callable[[], tuple[list[ClassifiedItem], list[dict]]] | None = None,
) -> dict:
    """탐지 → 개선안 → 가이드라인 → 발행. 배치 1회.

    Args:
        window_end: 현재 윈도우 마지막 날. 없으면 입력의 최신 날짜.
        max_alerts: Agent3·가이드라인에 태울 alert 수 상한 (비용 통제).
        dry_run: LLM 을 한 번도 부르지 않고 **몇 번 부를지만 실측**한다.
        state_path: 발행 기록 캐시 경로 (테스트 주입용).
        load_inputs: 입력원. 기본은 원본 DB(`load_inputs_from_db`).
            평가·재현은 `scripts.golden_inputs.load_golden_inputs` 를 주입한다.
            **골든을 주입하면 분류 오차가 0 이라 결과가 탐지 성능이 아니다** —
            요약에 그 경고가 함께 출력된다.

    Returns:
        배치 요약 dict. 실패는 모아서 담고 **중간에 던지지 않는다.**
    """
    loader = load_inputs or load_inputs_from_db
    trace_id = new_trace_id()
    started = datetime.now()
    logger.info("배치 시작 trace_id=%s dry_run=%s", trace_id, dry_run)

    items, documents = loader()
    gaps = unreliable_slots(check_coverage(documents, items))
    if gaps:
        logger.warning("분류 커버리지 미달 %d슬롯 — 검정에서 제외됩니다", len(gaps))

    prior = load_prior_alerts(window_end or date.today(), state_path)
    logger.info(
        "입력 items=%d documents=%d prior_alerts=%d",
        len(items),
        len(documents),
        len(prior),
    )

    # ⚠️ dry-run 이어도 [6] 원인분류는 detect_anomaly 안에서 돈다. 스텁을 안 주면
    #    "LLM 0회"라고 해놓고 실제로 과금된다.
    stub = _CountingClient() if dry_run else None

    alerts, suppressed = await detect_anomaly(
        items,
        documents=documents,
        window_end=window_end,
        prior_alerts=prior,
        # 백엔드가 어디서 줄지 미정 — 정해질 때까지 빈 집합 (지인님 결선 §8).
        resolved_alert_ids=set(),
        client=stub,
    )

    targets = alerts if max_alerts is None else alerts[:max_alerts]
    failures: list[dict] = []
    counts = Counter()

    for alert in targets:
        if dry_run:
            # 실제로 몇 번 부를지만 센다. 추정이 아니라 발행 대상 실측이다.
            counts["개선안"] += 1
            counts["가이드라인"] += 1
            counts["발행"] += 2
            continue

        # ⚠️ alert 1건이 터져도 배치는 계속한다. 여기서 던지면 **이미 LLM 비용을 쓴
        #    앞쪽 알림들까지 발행되지 않고 날아간다.** 실패는 모아서 끝에 요약한다.
        rec = guideline = None
        try:
            rec = await generate_for_alert(alert)
            counts["개선안"] += 1
        except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
            failures.append(
                {"alert_id": alert.alert_id, "stage": "개선안", "error": repr(exc)}
            )

        try:
            guideline = await generate_guideline(alert, rec)
            counts["가이드라인"] += 1
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {"alert_id": alert.alert_id, "stage": "가이드라인", "error": repr(exc)}
            )

        try:
            await publish_anomaly_analyzed(alert, rec, trace_id)
            counts["발행"] += 1
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {"alert_id": alert.alert_id, "stage": "발행:이상", "error": repr(exc)}
            )

        if guideline is not None:
            try:
                await publish_guideline_generated(guideline, trace_id)
                counts["발행"] += 1
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "alert_id": alert.alert_id,
                        "stage": "발행:가이드",
                        "error": repr(exc),
                    }
                )

    # 캐시는 dry-run 에서 건드리지 않는다 — 안 보낸 걸 보냈다고 기록하지 않는다.
    cached = (
        0
        if dry_run
        else save_published(alerts, alerts[0].window_end, state_path)
        if alerts
        else 0
    )

    return {
        "trace_id": trace_id,
        "dry_run": dry_run,
        # 입력원을 결과에 박아둔다 — 골든(oracle)으로 돌린 숫자를 "탐지 성능"으로
        # 인용하는 사고를 막는 게 목적이다. eval/README §68 이 실험① 에 붙인 경고와
        # 같은 이유이고, 거기서는 사람이 문서에 적었지만 여기서는 코드가 매번 낸다.
        "input_source": getattr(loader, "__name__", str(loader)),
        "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
        "items": len(items),
        "documents": len(documents),
        "prior_alerts": len(prior),
        "published": len(alerts),
        # suppressed 도 정상 alert_id 를 갖고 있다. 구분 없이 세면 나중에 "발행된 건가
        # 억제된 건가"를 알 수 없으므로 따로 센다 (지인님 결선 §6-②).
        "suppressed": len(suppressed),
        "processed": len(targets),
        "llm_calls": dict(counts),
        "cause_calls": stub.calls if stub else None,
        "failures": failures,
        "state_cached": cached,
    }


def print_summary(summary: dict) -> None:
    print("\n" + "=" * 62)
    print(f"배치 요약  trace_id={summary['trace_id']}  {summary['elapsed_sec']}초")
    print("=" * 62)
    print(
        f"  입력          items {summary['items']} / documents {summary['documents']}"
        f"  [{summary['input_source']}]"
    )
    if summary["input_source"] == "load_golden_inputs":
        print("  ⚠️ 골든 라벨(oracle) 입력 — 분류 오차 0%. 이 숫자는 탐지 성능이 아니다.")
    print(f"  prior_alerts  {summary['prior_alerts']}건")
    print(f"  발행          {summary['published']}건")
    print(f"  억제          {summary['suppressed']}건  ← 발행 아님")
    print(f"  후속 처리     {summary['processed']}건")
    if summary["dry_run"]:
        print("\n  [dry-run] LLM 호출 0회. 실제로 돌리면:")
        print(
            f"     Agent2 [6] 원인분류 : {summary['cause_calls']}회  ← 스텁이 가로채 실측"
        )
        print(f"     후속 단계           : {summary['llm_calls']}")
        print("     개선안 1건당 LLM 2~4회. 가이드라인은 별도.")
    if summary["failures"]:
        print(f"\n  ⚠️ 실패 {len(summary['failures'])}건 (배치는 계속 진행됨)")
        for f in summary["failures"][:10]:
            print(f"     {f['alert_id']} [{f['stage']}] {f['error'][:80]}")
    missing = [
        name
        for name, ok in [
            ("RabbitMQ(app.core.mq)", MQ_AVAILABLE),
            ("Agent3(generate_for_alert)", RECOMMENDATION_AVAILABLE),
            ("가이드라인(generate_guideline)", GUIDELINE_AVAILABLE),
        ]
        if not ok
    ]
    if missing:
        print(f"\n  ℹ️ 미연결: {', '.join(missing)} — 해당 단계는 no-op 입니다.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--window-end", default=None, help="현재 윈도우 마지막 날 (YYYY-MM-DD)"
    )
    ap.add_argument(
        "--max-alerts",
        type=int,
        default=None,
        help="후속 처리할 alert 수 상한 (비용 통제)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="LLM 0회 — 몇 번 부를지만 실측한다"
    )
    ap.add_argument(
        "--input-source",
        choices=["db", "golden"],
        default="db",
        help="db(기본, 운영) / golden(평가·재현 전용 — scripts/golden_inputs.py)."
        " golden 은 분류 오차가 0 이라 결과가 탐지 성능이 아니다. 요약이 경고를 낸다.",
    )
    args = ap.parse_args()

    loader = None
    if args.input_source == "golden":
        # app/ 안에서 골든을 읽지 않기 위해 **여기서만** 늦게 import 한다
        # (eval/README §232). 모듈 최상단에 두면 운영 import 경로에 골든이 딸려온다.
        from scripts.golden_inputs import load_golden_inputs

        loader = load_golden_inputs

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    summary = asyncio.run(
        run_batch(
            window_end=date.fromisoformat(args.window_end) if args.window_end else None,
            max_alerts=args.max_alerts,
            dry_run=args.dry_run,
            load_inputs=loader,
        )
    )
    print_summary(summary)


if __name__ == "__main__":
    main()
