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
② BH-FDR 은 **상품별 family**에 적용한다. 먼저 상품별 36슬롯 그리드를 구성하고,
   최소표본·과거표본·분류 커버리지 보류를 제외한 12~36개 판정 가능 검정을 함께 보정한다.
   임의로 일부 슬롯만 쪼개 실행하면 family가 달라져 컷오프가 바뀌므로 금지한다.
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

# 리포트에 한글·'—'·'←' 가 들어간다. Windows 콘솔 기본이 cp949 라 재설정하지 않으면
# 채점을 다 끝낸 **마지막 출력 단계에서** UnicodeEncodeError 로 죽는다 (지인님
# run_recommendation_eval.py 와 같은 처리).
sys.stdout.reconfigure(encoding="utf-8")

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
from app.detection.service import _build_candidates
from app.detection.statistics import run_detection
from app.detection.verdict import run_verdict

CONFIG_ANOMALY = ROOT / "data" / "config" / "config_anomaly.csv"
CONFIG_PRODUCTS = ROOT / "data" / "config" / "config_products.csv"
GOLDEN_ANOMALY = ROOT / "data" / "golden" / "golden_anomaly.csv"

DOC_TOTAL_TESTS = 1464
DOC_MAX_FAMILY_SIZE = 36
"""정본 데이터의 전체 검정 수와 상품 하나의 최대 family 크기.

둘 다 판정 임계가 아니라 실행 구성을 감시하는 대조값이다. 보류 채널이 있는 상품은
family가 36보다 작을 수 있고, 상품 수가 바뀌면 전체 검정 수도 달라진다.
"""

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
    """[2]→[3]→[4] 를 운영 코드 그대로 태운다.

    main_aspect 선택은 service._build_candidates 에 맡긴다. 여기서 따로 구현하면
    운영과 다른 답이 나올 수 있다 — 운영은 (상품, 채널) 버킷으로 묶고
    _representative_delta 로 비교하는데, (상품, source) 단위로 고르면 색상이 NAVER,
    사이즈가 COUPANG 에서 발화할 때 서로 갈린다.
    """
    combos = build_combinations(config_rows, products)
    batch, held = run_detection(combos)
    verdicts = run_verdict(batch, held)

    tests = {t["key"]: t for t in batch}
    counts = {(p, a, c, s): cnt for p, a, c, s, cnt in combos}

    candidates: dict[tuple[str, str], dict] = {}
    for source in SOURCES:
        for key, candidate in _build_candidates(
            verdicts, source, tests, counts
        ).items():
            candidates[(*key, source)] = candidate

    return {
        "tests": tests,
        "candidates": candidates,
        "consistent": cause_consistency(config_rows),
        "changes": change_log(config_rows),
        "n_batch": len(batch),
        "n_held": len(held),
        "n_grid": len(combos),
    }


def _report_family(pred: dict) -> None:
    """전체 검정 수와 상품별 BH family 크기 분포를 함께 출력한다.

    BH는 family 크기와 발견 순위로 컷오프를 정하므로 구성이 조용히 달라지면 모든
    판정이 함께 움직인다. 실제 변동 경로는 ① 최소표본 보류 범위 오축소와 ② 분류
    커버리지 미달 슬롯 제외다.

    상품별 전환 후에는 보류가 자기 상품의 m을 직접 줄인다. 정본에서 정상 상품은
    m=36이지만 P036/P042는 m=24, P041은 m=12라 순위 1 임계가 각각 1.5배, 3배
    완화된다. 현재 정본 판정은 고정 m=36 반사실과 같지만, 데이터가 부족한 상품의
    임계가 느슨해지는 방향이므로 분포를 계속 출력해 감시한다.
    """
    excluded = pred["n_grid"] - pred["n_batch"]
    family_sizes: dict[str, int] = {}
    for product, _aspect, _channel, _source in pred["tests"]:
        family_sizes[product] = family_sizes.get(product, 0) + 1
    sizes = list(family_sizes.values())
    span = f"{min(sizes)}~{max(sizes)}" if sizes else "0"
    line = (
        f"  전체 검정 {pred['n_batch']} = 그리드 {pred['n_grid']} − 미검정 {excluded}"
        f" / 상품별 family {len(sizes)}개 (크기 {span}, 최대 {DOC_MAX_FAMILY_SIZE})"
    )
    if pred["n_batch"] != DOC_TOTAL_TESTS:
        line += f"  ⚠️ 정본 전체 검정 {DOC_TOTAL_TESTS} 과 다름 — 입력 구성 재확인"
    print(line)


_NO_ALERT = {
    "verdict": NORMAL,
    "is_anomaly": False,
    "is_biased": False,
    "main_aspect": "",
    "channel_significant": False,
    "detection_confidence": "",
}


def predicted_row(pred: dict, product: str, source: str, channel: str) -> dict:
    """골든 1행에 대응하는 예측값.

    후보 조회는 **케이스 수준**이다. 골든의 verdict·main_aspect 는 케이스 전체에
    같은 값이 들어가고(3채널 행 모두 '편중형'), 채널별 차이는 channel_significant
    한 칸으로만 표현되기 때문이다(mock 정의서 §7). 발화 채널에만 후보가 생기므로
    골든 행의 채널로 조회하면 비유의 채널 행이 전부 '정상'으로 잡혀 미탐이 된다.

    channel_significant 만 인자로 받은 채널의 검정 결과를 본다.
    """
    candidate = next(
        (
            c
            for (p, _ch, s), c in pred["candidates"].items()
            if p == product and s == source
        ),
        None,
    )
    if candidate is None:
        return dict(_NO_ALERT)

    verdict = candidate["verdict"].value
    main = candidate["aspect"]
    test = pred["tests"].get((product, main, channel, source))
    return {
        "verdict": verdict,
        "is_anomaly": verdict != NORMAL,
        "is_biased": verdict == BIASED,
        "main_aspect": main,
        "channel_significant": bool(test and test["fired"]),
        "detection_confidence": decide_confidence(
            candidate["verdict"],
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
    _report_family(pred)
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
