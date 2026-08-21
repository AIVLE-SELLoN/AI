"""실험② 를 **일별 배치 조건**(데모·운영)에서 다시 잰다. LLM 0회 — 캐시만 읽는다.

지금까지 갈라져 있던 두 축을 처음으로 겹친다.

                        케이스별 윈도우      일별 슬라이딩
    oracle                   100%              15.2%      persist_sim.py
    실제 분류                 52.0%             ???        ← 이 스크립트

바꾸는 것은 **분류 라벨의 출처 하나뿐**이다. 슬라이딩·family·채점은 persist_sim.py 를
그대로 따르고, 라벨은 실험② 캐시(진짜 LLM 분류 결과)로 덮는다. 케이스 상품의 CS 만
덮이므로 배경·리뷰는 oracle 로 남는다 — 실험②의 계약과 같다.

캐시가 덮는 날짜만 유효하다. extend_cache.py 로 [we-8, we] 9일치를 태워두면 연속 1·2·3일을
전부 볼 수 있다. 안 태운 날은 골든이 남아 결과가 낙관적으로 나오므로, 아래 커버리지 검사가
부족하면 경고하고 그 조합을 건너뛴다.
"""
import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from statsmodels.stats.multitest import multipletests

from app.core.console import force_utf8_output
from app.core.constants import (
    BH_FDR_Q,
    CURRENT_WINDOW_DAYS,
    PAST_WINDOW_DAYS,
)
from app.core.schemas import AspectSentiment
from app.detection.aggregate import build_combinations
from app.detection.loader import build_rows
from app.detection.statistics import build_batch
from scripts.golden_inputs import load_golden_inputs as load_inputs

ASPECTS = ["색상", "사이즈", "소재", "파손", "오배송", "기타"]
ANCHOR = date(2026, 8, 28)  # day 60
CACHE = ROOT / "data/eval_cache/pipeline_full_batch_classify_aspect_v5-15290041_run1.json"

FAMILIES = {
    "현재(전체)": None,
    "상품별": lambda k: k[0],
    "상품×source": lambda k: (k[0], k[3]),
}


def day_ordinal(n: int) -> int:
    """day n (1~60) → ordinal. persist_sim.d2 와 동일 기준."""
    return (ANCHOR - timedelta(days=60 - n)).toordinal()


def apply_bh(batch, keyfn, q=BH_FDR_Q) -> None:
    groups = defaultdict(list)
    for t in batch:
        groups["_" if keyfn is None else keyfn(t["key"])].append(t)
    for g in groups.values():
        rej, _, _, _ = multipletests(
            [t["p_value"] for t in g], alpha=q, method="fdr_bh"
        )
        for t, sig in zip(g, rej):
            t["bh_significant"] = bool(sig)
            t["fired"] = t["bh_significant"] and t["meaningful"]


def fired_by_day(rows, keyfn, days) -> dict:
    out = {}
    for d in days:
        end = day_ordinal(d)
        combos, _ = build_combinations(
            rows,
            end - CURRENT_WINDOW_DAYS + 1,
            end,
            aspects=ASPECTS,
            past_days=PAST_WINDOW_DAYS,
            alert_days=set(),
        )
        batch, _ = build_batch(combos)
        apply_bh(batch, keyfn)
        out[d] = {t["key"] for t in batch if t["fired"]}
    return out


def with_persistence(fired, days, need=2) -> dict:
    out = {}
    for i, d in enumerate(days):
        if i < need - 1:
            out[d] = set()
            continue
        keep = set(fired[d])
        for back in range(1, need):
            keep &= fired[days[i - back]]
        out[d] = keep
    return out


def swap_in_real_classification(items, cache) -> tuple[list, int]:
    """캐시에 있는 문서만 실제 분류 결과로 덮는다. 나머지는 골든 유지."""
    swapped = 0
    out = []
    for it in items:
        preds = cache.get(it.item_id)
        if preds is None:
            out.append(it)
            continue
        swapped += 1
        out.append(
            it.model_copy(
                update={
                    "aspects": [
                        AspectSentiment(aspect=p["aspect"], sentiment=p["sentiment"])
                        for p in preds
                    ]
                }
            )
        )
    return out, swapped


def load_truth() -> dict:
    with (ROOT / "data/config/config_anomaly.csv").open(encoding="utf-8-sig") as f:
        cfg = list(csv.DictReader(f))
    truth = {}
    for r in cfg:
        if r["intended_answer"].strip().upper() == "TRUE":
            truth[
                (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
            ] = (int(r["window_start_day"]), int(r["window_end_day"]))
    return truth


def coverage_by_need(cache, docs, truth) -> dict:
    """연속 n일을 실제 분류로 볼 수 있는지 — 필요한 날짜가 캐시에 덮였는지 검사.

    연속 n 일은 종료일 we 에서 거슬러 (n-1) 일 전 윈도우까지 본다. 그 윈도우의
    시작일은 we-6-(n-1) 이다. 그 날의 케이스 상품 문서가 캐시에 없으면 골든이
    남아 **탐지가 실제보다 좋게** 나온다.
    """
    cached = set(cache)
    by_day: dict[tuple[str, int], list] = defaultdict(list)
    for d in docs:
        if d["source"] != "cs":
            continue
        # load_inputs() 는 created_at 을 CSV 원문(str)으로 둔다 — 여기서 정규화.
        raw = d["created_at"]
        when = raw if isinstance(raw, date) else datetime.fromisoformat(str(raw)).date()
        by_day[(d["product"], when.toordinal())].append(d["id"])

    out = {}
    for need in (1, 2, 3):
        missing = 0
        total = 0
        for (product, _aspect, _ch, source), (_ws, we) in truth.items():
            if source != "cs":
                continue
            first = we - CURRENT_WINDOW_DAYS + 1 - (need - 1)
            for n in range(first, we + 1):
                ids = by_day.get((product, day_ordinal(n)), [])
                total += len(ids)
                missing += sum(1 for i in ids if i not in cached)
        out[need] = (missing, total)
    return out


def main() -> None:
    # 첫 문장이어야 한다 — 이 파일 요약 출력이 `—`·`⚠️` 를 쓰는데 cp949 에 없다.
    # 형제 파일들과 달리 여기는 `def main()`(동기)이라 가드가 `main()` 을 진입 지점으로 본다.
    # 호출을 `__main__` 블록으로 옮기면 가드가 실패한다.
    force_utf8_output()

    items, docs = load_inputs()
    cache = json.loads(CACHE.read_text(encoding="utf-8"))
    truth = load_truth()

    print(f"골든 문서 {len(docs):,}건 · 분류 캐시 {len(cache):,}건")

    cov = coverage_by_need(cache, docs, truth)
    print("\n■ 캐시 커버리지 (케이스 CS 슬롯 · 부족하면 그만큼 골든이 남아 낙관 편향)")
    for need, (missing, total) in cov.items():
        mark = "OK" if missing == 0 else f"⚠️ 미분류 {missing:,}건"
        print(f"    연속{need}일  필요 {total:,}건 중 {mark}")

    real_items, swapped = swap_in_real_classification(items, cache)
    print(f"\n실제 분류로 덮은 문서 {swapped:,}건 (나머지는 oracle)")

    days = list(range(29, 61))
    rows_oracle = build_rows(docs, items)
    rows_real = build_rows(docs, real_items)

    print(f"\n{'=' * 74}")
    print("일별 슬라이딩 탐지율 — oracle vs 실제 분류 (케이스 종료일에 발화했는가)")
    print(f"분모 {len(truth)}건 (config intended_answer=TRUE) · day 29~60")
    print(f"{'=' * 74}")
    print(f"{'family':14s} {'라벨':10s} {'연속1일':>9s} {'연속2일':>9s} {'연속3일':>9s}")
    print("-" * 74)

    for name, keyfn in FAMILIES.items():
        for label, rows in (("oracle", rows_oracle), ("실제분류", rows_real)):
            fired = fired_by_day(rows, keyfn, days)
            cells = []
            for need in (1, 2, 3):
                f = fired if need == 1 else with_persistence(fired, days, need)
                tp = sum(
                    1
                    for k, (_ws, we) in truth.items()
                    if we in days and k in f.get(we, set())
                )
                cells.append(f"{100 * tp / len(truth):8.1f}%")
            print(f"{name:14s} {label:10s} {cells[0]} {cells[1]} {cells[2]}")
        print()

    # ── 알림 품질 종합표 ────────────────────────────────────────
    #
    # 발화 1건 = (슬롯, 날짜) 1개. 같은 슬롯이 사흘 연속 뜨면 3건으로 센다 —
    # 셀러가 실제로 받는 알림 수가 그것이기 때문이다(억제 로직은 여기 미적용).
    #
    #   창 안 발화 = 그 슬롯이 진짜 이상이고, 이상 기간 [ws, we] 안에서 뜸  → 참
    #   창 밖 발화 = 그 외 전부                                            → 헛알림
    #   헛알림 비율 = 창밖 / (창안 + 창밖)
    print(f"{'=' * 92}")
    print("알림 품질 종합 (day 29~60, 32일 누적) — 발화 1건 = (슬롯 × 날짜)")
    print(f"{'=' * 92}")
    print(
        f"{'family':14s} {'관문':8s} {'라벨':9s} {'탐지율':>8s} "
        f"{'창안':>7s} {'창밖':>7s} {'헛알림비율':>10s} {'알림/일':>9s}"
    )
    print("-" * 92)

    windows = dict(truth)
    for name, keyfn in FAMILIES.items():
        for label, rows in (("oracle", rows_oracle), ("실제분류", rows_real)):
            fired = fired_by_day(rows, keyfn, days)
            for need in (1, 2, 3):
                f = fired if need == 1 else with_persistence(fired, days, need)
                inside = outside = 0
                for d, keys in f.items():
                    for k in keys:
                        span = windows.get(k)
                        if span is not None and span[0] <= d <= span[1]:
                            inside += 1
                        else:
                            outside += 1
                total = inside + outside
                fa_ratio = outside / total if total else 0.0
                tp = sum(
                    1
                    for k, (_ws, we) in truth.items()
                    if we in days and k in f.get(we, set())
                )
                recall = 100 * tp / len(truth)
                print(
                    f"{name:14s} {'연속' + str(need) + '일':8s} {label:9s} "
                    f"{recall:7.1f}% {inside:6d}건 {outside:6d}건 "
                    f"{fa_ratio:9.1%} {total / len(days):8.2f}건"
                )
        print()


if __name__ == "__main__":
    main()
