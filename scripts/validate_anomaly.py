"""
이상탐지 검산 모듈 (validator)
================================
config_anomaly.csv에 심은 수치가, 실제 이상탐지 로직(Fisher 단측 → BH-FDR → min_delta)을
돌렸을 때 intended_answer(의도한 정답)와 일치하는지 자동 검산한다.

핵심 순서 — 절대 바뀌면 안 됨 (이상탐지 로직 V3 §[2-B])
--------------------------------------------------
① 풀 배치 구성(케이스 123행 + 배경 검정 전부, 총 1,464개)
② Fisher 단측 검정 → p값만 산출(발화 여부는 아직 결정 안 함)
③ BH-FDR(q=0.05)을 **상품별 family**에 적용 → 통계적 유의 확정
④ min_delta(3%p) AND로 겹침 → 최종 발화(fired) 확정
⑤ intended_answer와 대조. 단, 공백인 16행(G 9 + SC-036 보류 1 + SC-037/038 6)은
   assert 스킵 — 정답이 없는 관찰용 케이스이므로 "발화 경로만" 확인하고 정오 비교는 안 함

배경(background) 검정 window 라벨에 대한 메모
--------------------------------------------
같은 상품에 케이스가 있으면 그 상품 케이스의 window을, 순수 배경 상품(P037~040)은
아무 기준(여기선 마지막 7일)으로 라벨만 붙인다. Fisher 계산은 4개 숫자(cur_neg/cur_total/
past_neg/past_total)만 쓰고 날짜는 입력값에 없으므로, window 선택은 통계 결과(p값·BH
컷오프·발화여부)에 전혀 영향을 주지 않는다 — 순수 리포트 표시용.

사용법
------
    python validate_anomaly.py \
        --anomaly-config config_anomaly.csv \
        --products-config config_products.csv \
        --alpha 0.05 --min-delta 0.03
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

# scripts/ 는 저장소 루트의 형제 폴더 — app 패키지를 절대경로로 import하려면
# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output

ASPECTS = ["색상", "사이즈", "소재", "파손", "오배송", "기타"]
CHANNELS = ["COUPANG", "NAVER", "ZIGZAG"]
SOURCES = ["cs", "review"]

# 배경 검정용 baseline (이상탐지 시나리오 정의서 §1, "기타"는 팀 확정값 3%)
BASELINE_RATE = {
    "색상":   {"COUPANG": 0.05, "NAVER": 0.06, "ZIGZAG": 0.07},
    "사이즈": {"COUPANG": 0.08, "NAVER": 0.09, "ZIGZAG": 0.07},
    "소재":   {"COUPANG": 0.04, "NAVER": 0.05, "ZIGZAG": 0.06},
    "파손":   {"COUPANG": 0.02, "NAVER": 0.02, "ZIGZAG": 0.02},
    "오배송": {"COUPANG": 0.01, "NAVER": 0.01, "ZIGZAG": 0.01},
    "기타":   {"COUPANG": 0.03, "NAVER": 0.03, "ZIGZAG": 0.03},
}
# 배경 볼륨(회신 확정값): CS 현재42/과거168(일6건), 리뷰 현재14/과거56(일2건)
BG_VOLUME = {
    "cs":     {"cur_total": 42, "past_total": 168},
    "review": {"cur_total": 14, "past_total": 56},
}

MIN_SAMPLE = 10  # (상품,채널) 총문의 < 10 → 그 채널 전체 보류(family 제외)


# ────────────────────────────────────────────────────────────────
# 1. config 읽기
# ────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_products(path: str) -> list[str]:
    return [r["golden_group_id"] for r in load_csv(path)]


# ────────────────────────────────────────────────────────────────
# 2. 풀 배치 구성 — 42상품 × 6aspect × 3채널 × 2source = 1,512슬롯
#    케이스가 심어진 슬롯은 config 값 그대로, 나머지는 baseline으로 채움
# ────────────────────────────────────────────────────────────────

def round_counts(rate: float, total: int) -> int:
    return round(rate * total)


def build_full_batch(anomaly_rows: list[dict], all_products: list[str]) -> tuple[list[dict], list[tuple]]:
    """전체 1,512슬롯을 구성하고, 최소표본 미달 채널을 찾아 제외한다.
    반환: (배치 리스트, 보류된 (상품,채널) 목록)
    """
    # config에 명시된 슬롯: (상품,채널,aspect,source) -> 행
    explicit = {}
    for r in anomaly_rows:
        key = (r["golden_group_id"], r["channel"], r["aspect"], r["source"])
        explicit[key] = r

    # 케이스가 있는 (상품,채널)의 window은 해당 케이스 window을 재사용(표시용).
    # 순수 배경 상품은 window 정보가 아예 없으므로 "마지막 7일" 등 임의값을 붙인다.
    window_label = {}
    for r in anomaly_rows:
        window_label[(r["golden_group_id"], r["channel"])] = (
            int(r["window_start_day"]), int(r["window_end_day"])
        )

    # ── 1단계: (상품,채널) 단위로 최소표본 체크 ──
    # ⚠️ 문서 §9 명시: "보류는 채널 단위다 — 한 채널이 보류되면 그 (상품,채널)의
    #    모든 aspect·source 12검정이 통째로 family에서 빠진다."
    # CS가 표본 부족이면, 그 (상품,채널)은 리뷰까지 같이 보류한다(소스별 개별 판정 아님).
    channel_cur_total = {}   # (상품,채널,source) -> cur_total (있는 것만)
    channel_past_total = {}  # (상품,채널,source) -> past_total (있는 것만)
    for r in anomaly_rows:
        key = (r["golden_group_id"], r["channel"], r["source"])
        channel_cur_total[key] = int(r["cur_total"])
        channel_past_total[key] = int(r["past_total"])

    held_channel_pairs = set()  # (상품,채널) — 어느 한쪽 source라도 미달이면 여기 들어감
    for gid in all_products:
        for ch in CHANNELS:
            for src in SOURCES:
                key = (gid, ch, src)
                total = channel_cur_total.get(key, BG_VOLUME[src]["cur_total"])
                if total < MIN_SAMPLE:
                    held_channel_pairs.add((gid, ch))

    held_channels = [(gid, ch, src) for (gid, ch) in held_channel_pairs for src in SOURCES]

    # ── 2단계: 전체 1,512슬롯 구성, 보류된 채널의 슬롯은 배치에서 제외 ──
    batch = []
    for gid in all_products:
        for ch in CHANNELS:
            for src in SOURCES:
                if (gid, ch) in held_channel_pairs:
                    continue  # 보류 채널 → 그 채널의 6aspect(=12검정 중 이 source분)를 통째로 제외
                for asp in ASPECTS:
                    key = (gid, ch, asp, src)
                    if key in explicit:
                        r = explicit[key]
                        cur_neg, cur_total = int(r["cur_neg"]), int(r["cur_total"])
                        past_neg, past_total = int(r["past_neg"]), int(r["past_total"])
                        intended = r.get("intended_answer", "").strip()
                        case_id = r.get("case_id", "")
                    else:
                        # ⚠️ 배경 슬롯도 "분모는 aspect 무관" 원칙을 따라야 함 — 이 (상품,채널,source)에
                        # 이미 명시된 aspect(케이스)가 있으면 그 총문의를 그대로 물려받는다.
                        # 없으면(순수 배경 상품) baseline 볼륨을 쓴다.
                        rate = BASELINE_RATE[asp][ch]
                        cur_total = channel_cur_total.get((gid, ch, src), BG_VOLUME[src]["cur_total"])
                        past_total = channel_past_total.get((gid, ch, src), BG_VOLUME[src]["past_total"])
                        cur_neg = round_counts(rate, cur_total)
                        past_neg = round_counts(rate, past_total)
                        intended = ""  # 배경은 애초에 정답 대조 대상 아님(무조건 미발화 기대이지만 비공식)
                        case_id = ""

                    w = window_label.get((gid, ch), (None, None))
                    batch.append({
                        "product": gid, "channel": ch, "aspect": asp, "source": src,
                        "cur_neg": cur_neg, "cur_total": cur_total,
                        "past_neg": past_neg, "past_total": past_total,
                        "window_start_day": w[0], "window_end_day": w[1],
                        "intended_answer": intended, "case_id": case_id,
                    })
    return batch, held_channels


# ────────────────────────────────────────────────────────────────
# 3. Fisher 단측(②) → BH-FDR(③) → min_delta AND(④)
# ────────────────────────────────────────────────────────────────

def run_fisher(cur_neg: int, cur_total: int, past_neg: int, past_total: int) -> tuple[float, float]:
    """(p값, delta) 반환. 발화 여부는 여기서 결정하지 않는다(BH가 배치로 결정)."""
    table = [[cur_neg, cur_total - cur_neg], [past_neg, past_total - past_neg]]
    _, p_value = fisher_exact(table, alternative="greater")
    delta = (cur_neg / cur_total) - (past_neg / past_total)
    return p_value, delta


def apply_pipeline(batch: list[dict], alpha: float = 0.05, min_delta: float = 0.03) -> list[dict]:
    # ② Fisher — 전 슬롯 p값만 먼저 산출
    for item in batch:
        p, delta = run_fisher(item["cur_neg"], item["cur_total"], item["past_neg"], item["past_total"])
        item["p_value"] = p
        item["delta"] = delta
        item["meaningful"] = delta >= min_delta

    # ③ BH-FDR — 상품별 family의 모든 검정에 적용 (min_delta 선필터 금지)
    groups: dict[str, list[dict]] = defaultdict(list)
    for item in batch:
        groups[item["product"]].append(item)

    for group in groups.values():
        rejected, corrected_p, _, _ = multipletests(
            [item["p_value"] for item in group], alpha=alpha, method="fdr_bh"
        )
        for item, is_sig, cp in zip(group, rejected, corrected_p):
            item["bh_significant"] = bool(is_sig)
            item["bh_corrected_p"] = cp
            # ④ min_delta AND — 이중 잠금
            item["fired"] = bool(is_sig) and item["meaningful"]

    return batch


# ────────────────────────────────────────────────────────────────
# 4. intended_answer 대조 (공백 16행은 스킵)
# ────────────────────────────────────────────────────────────────

def score(batch: list[dict]) -> dict:
    checked, skipped, mismatches = 0, 0, []
    for item in batch:
        intended = item["intended_answer"]
        if intended == "":
            skipped += 1
            continue
        checked += 1
        expected = intended.upper() == "TRUE"
        actual = item["fired"]
        if expected != actual:
            mismatches.append(item)

    return {
        "총_배치_크기": len(batch),
        "정답_대조_대상": checked,
        "공백_스킵": skipped,
        "불일치_건수": len(mismatches),
        "불일치_상세": mismatches,
    }


# ────────────────────────────────────────────────────────────────
# 4-2. ±1건 강건성 검사 (§5-5, "원칙 18")
#      G(SC-026~028)·D(SC-019~021)는 예외 — 저사건이 케이스의 본질이라
#      ±1건에 흔들리는 게 정상 동작이므로 강건성 검사 대상에서 뺀다.
# ────────────────────────────────────────────────────────────────

ROBUSTNESS_EXEMPT = {"SC-019", "SC-020", "SC-021", "SC-026", "SC-027", "SC-028"}


def robustness_check(batch: list[dict], alpha: float = 0.05, min_delta: float = 0.03) -> dict:
    """cur_neg를 ±1한 뒤 해당 상품 family의 BH를 다시 계산해 판정 강건성을 본다."""
    families: dict[str, list[dict]] = defaultdict(list)
    for item in batch:
        families[item["product"]].append(item)

    results = []
    for item in batch:
        if item["intended_answer"] == "" or item["case_id"] in ROBUSTNESS_EXEMPT:
            continue

        row_result = {"case_id": item["case_id"], "product": item["product"],
                       "channel": item["channel"], "aspect": item["aspect"], "source": item["source"],
                       "intended": item["intended_answer"].upper() == "TRUE", "flips": []}

        for delta_n, label in [(+1, "+1건"), (-1, "-1건")]:
            new_neg = item["cur_neg"] + delta_n
            if new_neg < 0 or new_neg > item["cur_total"]:
                continue
            p, d = run_fisher(new_neg, item["cur_total"], item["past_neg"], item["past_total"])
            family = families[item["product"]]
            item_index = next(i for i, sibling in enumerate(family) if sibling is item)
            p_values = [p if sibling is item else sibling["p_value"] for sibling in family]
            rejected, _, _, _ = multipletests(p_values, alpha=alpha, method="fdr_bh")
            fired = bool(rejected[item_index]) and (d >= min_delta)
            if fired != row_result["intended"]:
                row_result["flips"].append({"변화": label, "p": p, "delta": d, "새_fired": fired})

        if row_result["flips"]:
            results.append(row_result)

    return {
        "방법": "각 ±1 변경마다 해당 상품 family의 BH 재계산",
        "검사_대상_제외": sorted(ROBUSTNESS_EXEMPT),
        "흔들린_케이스": results,
    }


# ────────────────────────────────────────────────────────────────
# 5. 메인
# ────────────────────────────────────────────────────────────────

def main():
    # 🔴 첫 문장이어야 한다 — 아래 `parse_args()` 가 `--help` 를 먼저 찍고, 그 도움말
    #    (`description=__doc__`)에 `—` 가 있다. `app/core/console.py`.
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--anomaly-config", default="config_anomaly.csv")
    ap.add_argument("--products-config", default="config_products.csv")
    ap.add_argument("--alpha", type=float, default=0.05, help="BH-FDR q값")
    ap.add_argument("--min-delta", type=float, default=0.03)
    ap.add_argument("--robustness", action="store_true", help="±1건 강건성 검사도 같이 수행")
    args = ap.parse_args()

    anomaly_rows = load_csv(args.anomaly_config)
    all_products = load_products(args.products_config)

    batch, held_channels = build_full_batch(anomaly_rows, all_products)
    print(f"전체 검정 수: {len(batch)}  (보류 채널 {len(held_channels)}개 제외됨)")
    if held_channels:
        print("  보류된 (상품,채널,source):", held_channels)

    batch = apply_pipeline(batch, alpha=args.alpha, min_delta=args.min_delta)

    fired_p = [i["p_value"] for i in batch if i["fired"]]
    cutoff = max(fired_p) if fired_p else None
    print(f"BH 기각(유의) 건수: {sum(i['bh_significant'] for i in batch)}")
    print(f"최종 발화(fired) 건수: {sum(i['fired'] for i in batch)}")
    print(f"발화 raw p 최대값(상품별 family 전체): {cutoff}")

    result = score(batch)
    print()
    print("=== 검산 결과 ===")
    print(f"총 배치 크기: {result['총_배치_크기']}")
    print(f"정답 대조 대상: {result['정답_대조_대상']}")
    print(f"공백 스킵(G·보류·관찰케이스): {result['공백_스킵']}")
    print(f"불일치: {result['불일치_건수']}건")

    if result["불일치_건수"] > 0:
        print("\n--- 불일치 상세 ---")
        for m in result["불일치_상세"]:
            print(f"  {m['case_id']} {m['product']}/{m['channel']}/{m['aspect']}/{m['source']}: "
                  f"기대={m['intended_answer']} 실제fired={m['fired']} "
                  f"(p={m['p_value']:.6f}, bh_p={m['bh_corrected_p']:.6f}, delta={m['delta']:.4f})")
        print("\n검산 실패 — 불일치 존재")
    else:
        print("\n검산 통과 - 전체 일치")

    if args.robustness:
        print("\n=== ±1건 강건성 검사 ===")
        rc = robustness_check(batch, alpha=args.alpha, min_delta=args.min_delta)
        print(f"방법: {rc['방법']}")
        print(f"검사 제외(G·D, {len(rc['검사_대상_제외'])}개): {rc['검사_대상_제외']}")
        if rc["흔들린_케이스"]:
            print(f"\n⚠️ ±1건에 판정이 흔들리는 케이스 {len(rc['흔들린_케이스'])}건:")
            for r in rc["흔들린_케이스"]:
                print(f"  {r['case_id']} {r['product']}/{r['channel']}/{r['aspect']}/{r['source']} "
                      f"(정답={r['intended']}):")
                for f in r["flips"]:
                    print(f"    {f['변화']} → p={f['p']:.6f}, delta={f['delta']:.4f}, fired={f['새_fired']}")
            print("\n강건성 검사 실패 — 위 케이스들은 ±1건에 취약함")
        else:
            print("\n강건성 검사 통과 — 전 케이스가 ±1건에도 판정 유지")


if __name__ == "__main__":
    main()
