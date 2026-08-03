"""실험② 분류 오류 전파 — 실제 LLM 분류가 탐지 성능을 얼마나 깎는가.

무엇을 재나: 실험① 대비 하락폭 (①−②)
어떻게:     ①과 **딱 한 가지만** 다르게 돌린다 — 케이스 슬롯의 현재 윈도우
            카운트를 config 값(=golden 라벨) 대신 **Agent1 이 실제로 분류한
            결과**로 채운다. 배치 구성·과거 기준·채점 로직은 ①과 완전히 동일하다.
            그래야 차이가 오직 분류 오차에서만 온다.

왜 케이스만 실제 분류하나
-------------------------
배경 슬롯은 ① 에서도 BASELINE_RATE 공식으로 만든 합성값이라 대응하는 원문이 없다.
거기까지 바꾸면 비교 조건이 둘이 되어 (①−②)가 '분류 오차'를 뜻하지 않게 된다.
비용도 96,514건으로 자릿수가 뛴다.

과거 윈도우는 config 값(oracle)을 쓴다. ②가 묻는 것은 "현재 윈도우의 분류 오차가
판정을 흔드는가"이고, 과거는 28일 누적이라 오차가 상쇄되는 안정된 기준선이다.

분모는 원본에서 센다
--------------------
`loader.build_rows()` 경유. 분류 결과에서 분모를 세면 aspect 가 0개로 나온 문서가
통째로 빠져 부정률이 부풀려진다(탐지 분모 산출 방식 §1). CS 는 '기타' 가 있어
실무상 안전하지만, 운영과 같은 경로를 쓰는 편이 회귀에 강하다.

비용
----
프롬프트1 은 현재 **문의 1건당 LLM 1회**다(service.py:69 — 배치 전환은 재검증 대기).
프롬프트가 4,586 토큰인데 본문은 평균 14 토큰이라 호출의 99.7% 가 프롬프트 재전송이다.

    건당 호출   현재 윈도우 12,274건 × 3회 ≈ $26
    20건 배치   같은 규모                  ≈ $2

`classify_aspect(list) -> list` 는 이미 배치를 받는 모양이라, 내부가 배치로 바뀌어도
이 스크립트는 그대로 동작한다.

**분류 결과는 회차별로 캐싱한다.** 한 번 태운 문의는 다시 부르지 않으므로 재실행·
채점 로직 수정에는 과금이 없다. 회차를 나누는 이유는 LLM 이 temperature=0 에서도
실행마다 흔들리기 때문이다(실험⑥에서 같은 입력에 89.0% / 84.0%).

실행:
    python eval/run_pipeline_eval.py --dry-run     # 비용 0, 대상만 확인
    python eval/run_pipeline_eval.py --limit 50    # 파일럿
    python eval/run_pipeline_eval.py --runs 3      # 본실행 (3회 평균)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "eval"))

from run_detection_eval import (  # ①과 배치·채점을 공유해야 비교가 성립한다
    CONFIG_ANOMALY,
    CONFIG_PRODUCTS,
    GOLDEN_ANOMALY,
    build_combinations,
    cause_consistency,
    change_log,
    read,
    score,
)

from app.classification.service import ClassifyRequestItem, classify_aspect
from app.core.constants import CURRENT_WINDOW_DAYS
from app.core.schemas import AspectSentiment, ClassifiedItem
from app.detection.aggregate import count_window
from app.detection.loader import build_rows, check_coverage
from app.detection.service import _build_candidates
from app.detection.statistics import run_detection
from app.detection.verdict import run_verdict

INPUT_INQUIRIES = ROOT / "data" / "input" / "input_cs_inquiries.csv"
INPUT_CHANNEL_PRODUCTS = ROOT / "data" / "input" / "input_channel_products.csv"
GOLDEN_MAPPING = ROOT / "data" / "golden" / "golden_mapping.csv"
GOLDEN_CS_LABELS = ROOT / "data" / "golden" / "golden_cs_labels.csv"
CACHE_DIR = ROOT / "data" / "eval_cache"

DAY1 = date(2026, 6, 30)  # Day 1 = 문의 데이터 첫날
SOURCE_CS = "cs"
SOURCES = ("cs", "review")


# ── 대상 문의 수집 ───────────────────────────────────────────────


def _product_of() -> dict[tuple[str, str], str]:
    """(channel, channel_product_id) → 상품 그룹 ID. 매핑 2단 조인."""
    group_of = {r["variant_row_id"]: r["golden_group_id"] for r in read(GOLDEN_MAPPING)}
    out: dict[tuple[str, str], str] = {}
    for r in read(INPUT_CHANNEL_PRODUCTS):
        group = group_of.get(r["variant_row_id"])
        if group:
            out.setdefault((r["channel"], r["channel_product_id"]), group)
    return out


def collect_documents(config_rows: list[dict]) -> tuple[list[dict], dict[str, tuple]]:
    """케이스 상품의 **현재 윈도우** CS 문의를 모은다 (분모의 출처).

    Returns:
        (documents, windows)  — windows 는 product → (cur_start, cur_end) 실제 날짜
    """
    windows: dict[str, tuple] = {}
    for r in config_rows:
        if r["source"] != SOURCE_CS:
            continue
        end = DAY1 + timedelta(days=int(r["window_end_day"]) - 1)
        windows[r["golden_group_id"]] = (
            end - timedelta(days=CURRENT_WINDOW_DAYS - 1),
            end,
        )

    product_of = _product_of()
    documents: list[dict] = []
    for r in read(INPUT_INQUIRIES):
        product = product_of.get((r["channel"], r["channel_product_id"]))
        if product not in windows:
            continue
        created = datetime.fromisoformat(r["inquired_at"])
        cur_start, cur_end = windows[product]
        if not (cur_start <= created.date() <= cur_end):
            continue
        documents.append(
            {
                "id": r["inquiry_id"],
                "product": product,
                "channel": r["channel"],
                "source": SOURCE_CS,
                "created_at": created,
                "text": r["content"],
            }
        )
    return documents, windows


def take_whole_products(documents: list[dict], limit: int) -> list[dict]:
    """대략 limit 건까지 자르되 **상품 경계에서 끊는다.**

    문서를 그냥 documents[:limit] 로 자르면 한 상품이 중간에 잘려 그 슬롯의
    **분모가 실제보다 작아진다.** measure() 가 총문의를 실제 행에서 세기 때문이다.
    분모가 깎이면 부정률이 부풀려져 판정이 왜곡되고, 그 상품은 ①과 비교 자체가
    성립하지 않는다. 파일럿이라도 슬롯은 통째로 넣어야 한다.
    """
    by_product: dict[str, list[dict]] = {}
    for doc in documents:
        by_product.setdefault(doc["product"], []).append(doc)

    taken: list[dict] = []
    for docs in by_product.values():
        if taken and len(taken) + len(docs) > limit:
            break
        taken.extend(docs)
    return taken


# ── 분류 ─────────────────────────────────────────────────────────


def _to_items(
    documents: list[dict], aspects_of: dict[str, list]
) -> list[ClassifiedItem]:
    return [
        ClassifiedItem(
            item_id=d["id"],
            source=d["source"],
            channel=d["channel"],
            product_group_id=d["product"],
            raw_text=d["text"],
            aspects=[
                AspectSentiment(aspect=a["aspect"], sentiment=a["sentiment"])
                for a in aspects_of.get(d["id"], [])
            ],
            created_at=d["created_at"],
        )
        for d in documents
    ]


async def classify_cached(
    documents: list[dict], run: int, tag: str = "full"
) -> list[ClassifiedItem]:
    """Agent1 분류. 회차별 캐시를 먼저 보고 없는 것만 태운다.

    tag 로 캐시를 갈라두는 이유: 파일럿(--limit)과 본실행이 같은 캐시를 쓰면,
    나중에 호출 방식이 바뀌었을 때(건당 → 배치) 한 회차 안에 두 방식의 결과가
    섞인다. 지금 파일럿을 돌려도 본실행 숫자가 오염되지 않게 분리한다.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"pipeline_{tag}_run{run}.json"
    cache: dict = (
        json.loads(cache_path.read_text(encoding="utf-8"))
        if cache_path.exists()
        else {}
    )

    todo = [d for d in documents if d["id"] not in cache]
    print(
        f"  회차 {run}: 캐시 {len(documents) - len(todo):,}건 / 신규 호출 {len(todo):,}건"
    )

    if todo:
        results = await classify_aspect(
            [
                ClassifyRequestItem(
                    item_id=d["id"],
                    source=d["source"],
                    channel=d["channel"],
                    product_group_id=d["product"],
                    raw_text=d["text"],
                    created_at=d["created_at"],
                )
                for d in todo
            ]
        )
        for item in results:
            cache[item.item_id] = [
                {"aspect": a.aspect.value, "sentiment": int(a.sentiment)}
                for a in item.aspects
            ]
        cache_path.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    return _to_items(documents, cache)


def oracle_classified(documents: list[dict]) -> list[ClassifiedItem]:
    """golden 라벨로 만든 ClassifiedItem — ①과 같은 입력. LLM 0회."""
    labels = {r["inquiry_id"]: r for r in read(GOLDEN_CS_LABELS)}
    aspects_of: dict[str, list] = {}
    for d in documents:
        label = labels.get(d["id"], {})
        if label.get("true_aspect"):
            aspects_of[d["id"]] = [
                {
                    "aspect": label["true_aspect"],
                    "sentiment": int(label["true_sentiment"]),
                }
            ]
    return _to_items(documents, aspects_of)


# ── 예측 (①의 배치에 실측 카운트만 덮어쓴다) ─────────────────────


def measure(rows: list[dict], config_rows: list[dict]) -> dict:
    """정규화 행 → {(product, aspect, channel, source): (cur_neg, cur_total)}.

    케이스가 정의된 슬롯만 낸다 — 배경을 건드리면 (①−②)가 분류 오차만 뜻하지 않게 된다.
    """
    slots = {
        (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
        for r in config_rows
        if r["source"] == SOURCE_CS
    }
    days = sorted({r["day"] for r in rows})
    if not days:
        return {}
    totals, negs = count_window(rows, days[0], days[-1])

    out: dict[tuple, tuple[int, int]] = {}
    for slot in slots:
        product, _aspect, channel, source = slot
        total = totals.get((product, channel, source), 0)
        if total:
            out[slot] = (negs.get(slot, 0), total)
    return out


def predict_with_counts(
    config_rows: list[dict], products: list[str], measured: dict
) -> dict:
    """①의 배치 구성을 그대로 쓰되, 케이스 슬롯의 현재 윈도우 카운트만 교체한다."""
    combos = []
    for product, aspect, channel, source, counts in build_combinations(
        config_rows, products
    ):
        cur_neg, cur_total, past_neg, past_total = counts
        hit = measured.get((product, aspect, channel, source))
        if hit is not None:
            cur_neg, cur_total = hit
        combos.append(
            (
                product,
                aspect,
                channel,
                source,
                (cur_neg, cur_total, past_neg, past_total),
            )
        )

    batch, held = run_detection(combos)
    verdicts = run_verdict(batch, held)
    tests = {t["key"]: t for t in batch}
    counts_map = {(p, a, c, s): cnt for p, a, c, s, cnt in combos}

    candidates: dict[tuple, dict] = {}
    for source in SOURCES:
        for key, cand in _build_candidates(verdicts, source, tests, counts_map).items():
            candidates[(*key, source)] = cand

    return {
        "tests": tests,
        "candidates": candidates,
        "consistent": cause_consistency(config_rows),
        "changes": change_log(config_rows),
        "n_batch": len(batch),
        "n_held": len(held),
    }


# ── 리포트 ───────────────────────────────────────────────────────


def _rate(metric) -> float:
    ok, n = metric if isinstance(metric, tuple) else (metric.ok, metric.n)
    return ok / n if n else 0.0


METRICS = [
    ("탐지율(recall)", "recall"),
    ("오탐률(FPR)", "fpr"),
    ("verdict 정확도", "verdict"),
    ("is_biased 정확도", "biased"),
    ("main_aspect 정확도", "aspect"),
    ("편중 채널 정확도", "channel"),
]


def report(runs: list[dict], oracle: dict) -> None:
    print(f"\n{'=' * 72}")
    print("실험② 분류 오류 전파 — ①(oracle) vs ②(실제 분류)")
    print(f"{'=' * 72}")
    print(f"{'지표':24s} {'① oracle':>10s} {'② 실제분류':>13s} {'하락폭':>9s}")
    print("-" * 72)
    for label, key in METRICS:
        a = _rate(oracle[key])
        values = [_rate(r[key]) for r in runs]
        b = statistics.mean(values)
        spread = f" ±{(max(values) - min(values)) / 2:.1%}" if len(values) > 1 else ""
        print(f"{label:24s} {a:>10.1%} {b:>8.1%}{spread:<5s} {a - b:>+9.1%}")

    per_run = " / ".join(f"{_rate(r['recall']):.1%}" for r in runs)
    print(f"\n회차별 탐지율: {per_run}")

    only_in_pipeline = sorted({m for r in runs for m in r["misses"]})
    if only_in_pipeline:
        print(f"  ②에서 놓친 케이스: {', '.join(only_in_pipeline)}")


# ── 진입점 ───────────────────────────────────────────────────────


async def main_async(args) -> None:
    config_rows = read(CONFIG_ANOMALY)
    products = [r["golden_group_id"] for r in read(CONFIG_PRODUCTS)]
    golden = read(GOLDEN_ANOMALY)

    documents, windows = collect_documents(config_rows)
    if args.limit > 0:
        documents = take_whole_products(documents, args.limit)

    print(f"케이스 상품 {len(windows)}개 · 현재 윈도우 CS 문의 {len(documents):,}건")
    print("과거 윈도우·배경 슬롯은 ①과 동일(oracle) — 차이는 현재 윈도우 분류뿐")
    print(f"→ LLM 호출 예상 {len(documents):,}건 × {args.runs}회 (캐시 적중분 제외)")

    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    # ① 기준선 — 같은 코드에 oracle 입력을 태운다. 채점 버그면 여기서 먼저 드러난다.
    oracle_rows = build_rows(documents, oracle_classified(documents))
    oracle_pred = predict_with_counts(
        config_rows, products, measure(oracle_rows, config_rows)
    )
    oracle_score = score(golden, oracle_pred)

    tag = "full" if args.limit <= 0 else f"limit{args.limit}"
    runs = []
    for run in range(1, args.runs + 1):
        classified = await classify_cached(documents, run, tag)

        gaps = check_coverage(documents, classified)
        if gaps:
            print(
                f"  ⚠️ 분류 커버리지 미달 {len(gaps)}슬롯 — 부정률이 과소추정될 수 있음"
            )

        rows = build_rows(documents, classified)
        pred = predict_with_counts(config_rows, products, measure(rows, config_rows))
        runs.append(score(golden, pred))

    report(runs, oracle_score)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", type=int, default=3, help="LLM 실행 횟수 (평균용)")
    ap.add_argument("--limit", type=int, default=0, help="문의 수 제한 (0=전량)")
    ap.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 대상만 확인")
    args = ap.parse_args()

    if not GOLDEN_ANOMALY.exists():
        raise SystemExit(
            f"{GOLDEN_ANOMALY} 없음 — scripts/build_golden_anomaly.py 를 먼저 실행할 것"
        )
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
