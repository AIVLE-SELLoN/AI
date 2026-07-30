"""실험① 이상탐지 주력 지표 — 발표용 대표 숫자.

무엇을 재나: 탐지율 · 오탐률 · 편중/전역 판정 정확도
어떻게:     golden 카운트를 검정에 **직접 입력**(oracle)하고 golden_anomaly 와 대조
비용:       **$0** — 통계 계산이고 앞단 분류를 안 태운다

⚠️ oracle 인 이유: 저사건 케이스는 분류 오차 1건에 판정이 뒤집혀서, 탐지 로직이
   정상인데도 오답 처리된다. 분류 성능은 실험②·③·⑥에서 따로 잰다.

배치 구성 (validate_anomaly.py 와 동일 — 로직 V3 §[2-B])
--------------------------------------------------------
① 42상품 × 6aspect × 3채널 × 2source = 1,512슬롯을 한 배치로 구성.
   케이스 슬롯은 config_anomaly 값, 나머지는 baseline 으로 채운다.
② BH-FDR 은 **배치 전체**에 적용한다. 케이스별로 쪼개 돌리면 family 가 달라져
   컷오프가 바뀐다 — 골든이 한 배치 기준으로 만들어졌으므로 반드시 맞춰야 한다.
   (윈도우가 25개로 흩어져 있으나 Fisher 는 4개 숫자만 쓰므로 날짜는 무관 —
    validate_anomaly.py 헤더 "window 선택은 통계 결과에 전혀 영향을 주지 않는다")

⚠️ validate_anomaly.py 는 scipy 를 직접 호출해 **config 가 옳은지** 검산한다.
   이 스크립트는 같은 배치를 **app.detection 에 먹여 우리 코드가 옳은지** 잰다.
   둘은 목적이 다르다. 배치 구성 규약만 공유한다.

채점 단위 (스키마 §6.1)
-----------------------
- 편중형  → (case_id × channel). 비유의 채널의 '안 울림'까지 정답
- 전역형  → case_id 수준 (예측은 channel=ALL 1건이라 채널별 대조 면제)
- 정상    → (case_id × channel) alert 부재 확인
- 구분불가/잠정전역 → 채점 제외 (scoring_included=N)

실행:
    python eval/run_detection_eval.py
    python eval/run_detection_eval.py --verbose    # 케이스별 상세
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from validate_anomaly import (
    ASPECTS,
    BASELINE_RATE,
    BG_VOLUME,
    CHANNELS,
    SOURCES,
    round_counts,
)

from app.core.constants import CONSISTENT_COUNT, CONSISTENT_RATIO
from app.core.schemas import Verdict
from app.detection.confidence import decide_confidence
from app.detection.statistics import run_detection
from app.detection.verdict import run_verdict

CONFIG_ANOMALY = ROOT / "data" / "config" / "config_anomaly.csv"
CONFIG_PRODUCTS = ROOT / "data" / "config" / "config_products.csv"
GOLDEN_ANOMALY = ROOT / "data" / "golden" / "golden_anomaly.csv"

# ⚠️ str(Verdict.NORMAL) 은 "Verdict.NORMAL" 을 낸다(3.11+ StrEnum 아님). 반드시 .value.
NORMAL = Verdict.NORMAL.value
BIASED = Verdict.BIASED.value
SCOPE_IN = {"색상", "사이즈", "소재"}


def read(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ── 배치 구성 ────────────────────────────────────────────────────


def build_combinations(config_rows: list[dict], products: list[str]) -> list[tuple]:
    """1,512슬롯 → run_detection 이 먹는 [(product, aspect, channel, source, counts)].

    관문①(최소표본) 은 여기서 거르지 않는다 — 그 판단도 우리 코드가 하는 일이라
    app.detection.build_batch 에 맡긴다.
    """
    explicit = {
        (r["golden_group_id"], r["channel"], r["aspect"], r["source"]): r
        for r in config_rows
    }
    # 분모는 aspect 무관 — 같은 (상품,채널,source) 에 케이스가 있으면 그 총문의를 물려받는다
    totals: dict[tuple[str, str, str], tuple[int, int]] = {}
    for r in config_rows:
        totals[(r["golden_group_id"], r["channel"], r["source"])] = (
            int(r["cur_total"]),
            int(r["past_total"]),
        )

    combos: list[tuple] = []
    for product in products:
        for channel in CHANNELS:
            for source in SOURCES:
                cur_total, past_total = totals.get(
                    (product, channel, source),
                    (BG_VOLUME[source]["cur_total"], BG_VOLUME[source]["past_total"]),
                )
                for aspect in ASPECTS:
                    row = explicit.get((product, channel, aspect, source))
                    if row:
                        counts = (
                            int(row["cur_neg"]),
                            int(row["cur_total"]),
                            int(row["past_neg"]),
                            int(row["past_total"]),
                        )
                    else:
                        rate = BASELINE_RATE[aspect][channel]
                        counts = (
                            round_counts(rate, cur_total),
                            cur_total,
                            round_counts(rate, past_total),
                            past_total,
                        )
                    combos.append((product, aspect, channel, source, counts))
    return combos


# ── 오라클 재료 ──────────────────────────────────────────────────


def cause_consistency(config_rows: list[dict]) -> dict[tuple[str, str], bool]:
    """(product, source) → 원인 일관 여부. [6] 대신 config 의 설계 의도를 쓴다(oracle).

    LLM 을 안 태우려고 cause_distribution 의 최다 비중을 [6] 의 판정 기준
    (CONSISTENT_RATIO·CONSISTENT_COUNT) 에 그대로 대입한다.
    """
    out: dict[tuple[str, str], bool] = {}
    for r in config_rows:
        dist = r["cause_distribution"].strip()
        if not dist:
            continue
        weights = []
        for part in dist.split(","):
            _, _, w = part.partition(":")
            try:
                weights.append(float(w))
            except ValueError:
                pass
        if not weights:
            continue
        top = max(weights)
        consistent = (
            top >= CONSISTENT_RATIO and top * int(r["cur_neg"]) >= CONSISTENT_COUNT
        )
        key = (r["golden_group_id"], r["source"])
        out[key] = out.get(key, False) or consistent
    return out


def change_log(config_rows: list[dict]) -> set[tuple[str, str]]:
    """(product, aspect) 집합 — 상세페이지 수정 이력이 이상 시점과 맞는 슬롯.

    input_detail_changes.csv 는 아직 미생성이라, 설계 의도인
    config_anomaly.create_change_event 에서 직접 만든다(①은 oracle 이므로 정당).
    """
    return {
        (r["golden_group_id"], r["aspect"])
        for r in config_rows
        if r["create_change_event"] == "Y"
    }


# ── 예측 산출 ────────────────────────────────────────────────────


def predict(config_rows: list[dict], products: list[str]) -> dict:
    combos = build_combinations(config_rows, products)
    batch, held = run_detection(combos)
    verdicts = run_verdict(batch, held)

    by_key = {t["key"]: t for t in batch}
    verdict_of = {(v["product"], v["aspect"], v["source"]): v for v in verdicts}

    # main_aspect: 그 (상품, source) 에서 발화한 aspect 중 delta 최대 (service.py 와 동일)
    best: dict[tuple[str, str], tuple[str, float]] = {}
    for (product, aspect, source), v in verdict_of.items():
        if v["verdict"].value == NORMAL:
            continue
        deltas = [
            by_key[(product, aspect, ch, source)]["delta"]
            for ch in CHANNELS
            if (product, aspect, ch, source) in by_key
            and by_key[(product, aspect, ch, source)]["fired"]
        ]
        if not deltas:
            continue
        top = max(deltas)
        key = (product, source)
        if key not in best or top > best[key][1]:
            best[key] = (aspect, top)

    return {
        "batch": by_key,
        "verdict_of": verdict_of,
        "main_of": {k: v[0] for k, v in best.items()},
        "consistent": cause_consistency(config_rows),
        "changes": change_log(config_rows),
        "n_batch": len(batch),
        "n_held": len(held),
    }


def predicted_row(pred: dict, product: str, source: str, channel: str) -> dict:
    """골든 1행에 대응하는 예측값."""
    main = pred["main_of"].get((product, source))
    if main is None:
        return {
            "verdict": NORMAL,
            "is_anomaly": False,
            "is_biased": False,
            "main_aspect": "",
            "channel_significant": False,
            "detection_confidence": "",
        }
    v = pred["verdict_of"][(product, main, source)]
    verdict = v["verdict"].value
    test = pred["batch"].get((product, main, channel, source))
    return {
        "verdict": verdict,
        "is_anomaly": verdict != NORMAL,
        "is_biased": verdict == BIASED,
        "main_aspect": main,
        "channel_significant": bool(test and test["fired"]),
        "detection_confidence": decide_confidence(
            v["verdict"],
            is_cause_consistent=pred["consistent"].get((product, source)),
            timestamp_matched=(product, main) in pred["changes"],
        ).value,
    }


# ── 채점 ─────────────────────────────────────────────────────────


class Tally:
    """맞은 수/전체를 같이 들고 다니는 카운터. 분모가 0이면 N/A 로 표시된다."""

    def __init__(self) -> None:
        self.ok = 0
        self.n = 0

    def add(self, correct: bool) -> None:
        self.ok += bool(correct)
        self.n += 1

    def __str__(self) -> str:
        return f"{self.ok / self.n:.1%}   ({self.ok}/{self.n})" if self.n else "N/A"


def score(golden: list[dict], pred: dict) -> dict:
    """골든 ↔ 예측을 대조해 지표만 낸다. 출력은 report() 담당."""
    scored = [g for g in golden if g["scoring_included"] == "Y"]

    # 케이스 수준 지표는 (case_id, source) 단위로 한 번만 센다
    cases: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for g in scored:
        cases[(g["case_id"], g["source"])].append(g)

    verdict, biased, aspect, confidence, channel = (Tally() for _ in range(5))
    tp = fn = fp = tn = 0
    cause_skipped = 0
    misses: list[str] = []
    false_alarms: list[str] = []
    detail: list[str] = []

    for (case_id, source), rows in sorted(cases.items()):
        g = rows[0]
        p = predicted_row(pred, g["golden_group_id"], source, g["channel"])
        g_anom = g["verdict"] != NORMAL

        if g_anom and p["is_anomaly"]:
            tp += 1
        elif g_anom:
            fn += 1
            misses.append(f"{case_id}/{source}(골든 {g['verdict']})")
        elif p["is_anomaly"]:
            fp += 1
            false_alarms.append(f"{case_id}/{source}(예측 {p['verdict']})")
        else:
            tn += 1

        verdict.add(g["verdict"] == p["verdict"])

        if g["is_biased"]:  # 정상·구분불가는 null 이라 채점 대상 아님
            biased.add((g["is_biased"] == "true") == p["is_biased"])

        if g_anom:
            aspect.add(g["main_aspect"] == p["main_aspect"])
            # 골든이 원인을 주장하지 않는 스코프 내 케이스(SC-032/035)는 root_cause 대조 불가
            if not g["root_cause"] and g["main_aspect"] in SCOPE_IN:
                cause_skipped += 1

        if g["detection_confidence"]:  # 골든이 채운 칸만 (시나리오 §4 채점 범위)
            confidence.add(g["detection_confidence"] == p["detection_confidence"])

        mark = "✅" if g["verdict"] == p["verdict"] else "❌"
        detail.append(
            f"  {mark} {case_id}/{source:6s} 골든={g['verdict']:6s} 예측={p['verdict']:6s} "
            f"main={p['main_aspect'] or '-':4s} 확신도={p['detection_confidence'] or '-'}"
        )

    # 편중형은 '다른 채널이 안 울리는 것'까지 정답 — 채널 단위로 따로 센다
    for g in scored:
        if g["verdict"] != BIASED:
            continue
        p = predicted_row(pred, g["golden_group_id"], g["source"], g["channel"])
        channel.add((g["channel_significant"] == "Y") == p["channel_significant"])

    return {
        "recall": (tp, tp + fn),
        "fpr": (fp, fp + tn),
        "verdict": verdict,
        "biased": biased,
        "aspect": aspect,
        "channel": channel,
        "confidence": confidence,
        "n_scored": tp + fn + fp + tn,
        "cause_skipped": cause_skipped,
        "misses": misses,
        "false_alarms": false_alarms,
        "detail": detail,
    }


def report(m: dict, pred: dict, golden: list[dict], verbose: bool) -> None:
    def ratio(ok: int, n: int) -> str:
        return f"{ok / n:.1%}   ({ok}/{n})" if n else "N/A"

    if verbose:
        print("── 케이스별 ──")
        print("\n".join(m["detail"]))

    print(f"\n{'=' * 62}")
    print("실험① 이상탐지 — oracle (LLM 호출 0회)")
    print(f"{'=' * 62}")
    print(f"검정 배치 {pred['n_batch']}건 / 보류 {pred['n_held']}건")
    print(f"채점 {m['n_scored']}건 (케이스×소스 단위)\n")

    print(f"■ 탐지율(recall)     {ratio(*m['recall'])}")
    print(f"■ 오탐률(FPR)        {ratio(*m['fpr'])}")
    print(f"■ verdict 정확도     {m['verdict']}")
    print(f"■ is_biased 정확도   {m['biased']}")
    print(f"■ main_aspect 정확도 {m['aspect']}")
    print(f"■ 편중 채널 정확도   {m['channel']}  ← 비유의 채널 '안 울림' 포함")
    print(f"■ 확신도 정확도      {m['confidence']}  ← 골든이 확신도를 채운 케이스만")

    if m["misses"]:
        print(f"\n  미탐 {len(m['misses'])}건: {', '.join(m['misses'])}")
    if m["false_alarms"]:
        print(f"  오탐 {len(m['false_alarms'])}건: {', '.join(m['false_alarms'])}")
    if m["cause_skipped"]:
        print(
            f"\n  ⚠️ root_cause 대조 제외 {m['cause_skipped']}건 — 골든에 원인 정답 없음"
        )

    excluded = [g for g in golden if g["scoring_included"] != "Y"]
    if excluded:
        print("\n── 채점 제외 케이스의 발화 성향 (참고용, 지표 미반영) ──")
        seen: set[tuple[str, str]] = set()
        for g in excluded:
            key = (g["case_id"], g["source"])
            if key in seen:
                continue
            seen.add(key)
            p = predicted_row(pred, g["golden_group_id"], g["source"], g["channel"])
            print(f"    {g['case_id']}/{g['source']:6s} → 예측 {p['verdict']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verbose", action="store_true", help="케이스별 상세 출력")
    args = ap.parse_args()

    if not GOLDEN_ANOMALY.exists():
        raise SystemExit(
            f"{GOLDEN_ANOMALY} 없음 — scripts/build_golden_anomaly.py 를 먼저 실행할 것"
        )

    pred = predict(
        read(CONFIG_ANOMALY),
        [r["golden_group_id"] for r in read(CONFIG_PRODUCTS)],
    )
    golden = read(GOLDEN_ANOMALY)
    report(score(golden, pred), pred, golden, args.verbose)


if __name__ == "__main__":
    main()
