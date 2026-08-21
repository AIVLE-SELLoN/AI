"""이상적 배경에서 데모를 다시 돌린다 — 헛알림·탐지율이 얼마나 달라지나. LLM 0회.

지금 mock 배경은 config 가정과 어긋난다(실측: P037/COUPANG/색상 골든 1.4% vs 가정 5.0%).
과거 기준선이 낮게 깔려 평범한 주간이 '상승'으로 읽히고, 그래서 헛알림이 부풀려진다.

여기서는 **케이스 신호만 남기고 배경을 설계대로 다시 뽑는다.**

    config_anomaly 의 intended_answer=TRUE 구간 [ws, we] 안의 문서  → 그대로 (실제 분류)
    그 외 전부(다른 날·다른 슬롯·배경 상품)                        → BASELINE_RATE 로 재추첨

분모(documents)는 손대지 않는다 — 상품·채널·source·날짜별 건수가 실제와 완전히 같은
채로 부정만 설계값이 된다. sweep_v2.py 가 쓴 방법과 같고, 다른 점은 케이스를 0으로
지우지 않고 **남겨둔다**는 것이다(귀무가 아니라 '깨끗한 배경 + 진짜 이상').

이건 mock 데이터 결함을 제거한 **가상 조건**이다. 실서비스 예측이 아니라 "배경이 설계대로
였다면" 의 상한이다. demo_sim.py 의 실측과 나란히 읽을 것.
"""
import asyncio
import csv
import random
import statistics
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from demo_sim import (
    ANCHOR,
    FAMILIES,
    load_truth_sets,
    require_full_real_cache,
    run_family,
)

from app.core.console import force_utf8_output
from app.core.schemas import AspectSentiment
from scripts.golden_inputs import load_golden_inputs as load_inputs

BASELINE_RATE = {
    "색상": {"COUPANG": 0.05, "NAVER": 0.06, "ZIGZAG": 0.07},
    "사이즈": {"COUPANG": 0.08, "NAVER": 0.09, "ZIGZAG": 0.07},
    "소재": {"COUPANG": 0.04, "NAVER": 0.05, "ZIGZAG": 0.06},
    "파손": {"COUPANG": 0.02, "NAVER": 0.02, "ZIGZAG": 0.02},
    "오배송": {"COUPANG": 0.01, "NAVER": 0.01, "ZIGZAG": 0.01},
    "기타": {"COUPANG": 0.03, "NAVER": 0.03, "ZIGZAG": 0.03},
}
CS_POOL = list(BASELINE_RATE)
REVIEW_POOL = ["색상", "사이즈", "소재"]
SEEDS = [11, 22, 33]


def day_number(when) -> int:
    d = when if isinstance(when, date) else datetime.fromisoformat(str(when)).date()
    return 60 - (ANCHOR.toordinal() - d.toordinal())


def case_regions() -> dict:
    """(product, channel, source) → [(ws, we), ...]. intended_answer=TRUE 만.

    **aspect 를 키에 넣으면 안 된다.** 케이스 창 안에서 '다른 aspect 문서'를 재추첨하면 그
    문서가 케이스 aspect 의 부정을 배경률로 새로 만들어내서 cur_neg 가 부풀려진다(실측:
    config 34/200 → 49/200). config 의 cur_neg 는 이미 배경분을 포함한 총량이므로, 창 안은
    **통째로 보존**해야 설계값이 유지된다.
    """
    with (ROOT / "data/config/config_anomaly.csv").open(encoding="utf-8-sig") as f:
        cfg = list(csv.DictReader(f))
    out: dict = {}
    for r in cfg:
        if r["intended_answer"].strip().upper() == "TRUE":
            out.setdefault(
                (r["golden_group_id"], r["channel"], r["source"]), []
            ).append((int(r["window_start_day"]), int(r["window_end_day"])))
    return out


def idealize(gold_items, real_items, regions, rng, labels: str = "real"):
    """케이스 구간의 라벨은 그대로 두고, 나머지는 배경률로 재추첨.

    labels: "real" = 케이스 구간에 실제 분류 결과 / "oracle" = 골든 라벨.
        두 조건을 같은 시드로 돌리면 배경이 동일하므로 차이가 분류 오차뿐이다.
    """
    out, kept, redrawn = [], 0, 0
    for gold, real in zip(gold_items, real_items):
        n = day_number(gold.created_at)
        product = gold.product_group_id
        channel = gold.channel.value
        source = gold.source.value

        in_case = any(
            ws <= n <= we for ws, we in regions.get((product, channel, source), ())
        )

        if in_case:
            kept += 1
            # 케이스 구간의 라벨만 골라 끼운다. oracle 이면 골든, 아니면 실제 분류.
            # 배경은 어느 쪽이든 아래에서 설계값으로 재생성되므로, 두 조건의 차이가
            # **분류 오차 하나**로 고정된다(실험②의 계약과 같은 구조).
            out.append(gold if labels == "oracle" else real)
            continue

        redrawn += 1
        pool = CS_POOL if source == "cs" else REVIEW_POOL
        drawn = [
            AspectSentiment(aspect=a, sentiment=-1)
            for a in pool
            if rng.random() < BASELINE_RATE[a][channel]
        ]
        # 부정만 뽑고 끝내면 CS 문서의 ~80% 가 aspect 0개가 된다. detect_anomaly 는
        # documents 를 받으면 check_coverage 를 스스로 돌리는데, 거기서 "aspect 0개 = 미분류"
        # 로 세어 그 슬롯을 통째로 family 에서 뺀다. 실제 CS 는 _cs_empty_fallback 이
        # aspect >= 1 을 보장하므로 그걸 맞춘다. 중립은 분자에 안 들어가니 부정률에는 영향이 없다.
        if source == "cs" and not drawn:
            drawn = [
                AspectSentiment(aspect=a.aspect, sentiment=0) for a in real.aspects
            ] or [AspectSentiment(aspect="기타", sentiment=0)]
        out.append(real.model_copy(update={"aspects": drawn}))
    return out, kept, redrawn


async def main() -> None:
    gold_items, documents = load_inputs()
    _path, _cache, real_items, _swapped, _coverage = require_full_real_cache(
        gold_items
    )
    truth, ignored = load_truth_sets()
    regions = case_regions()

    acc: dict = {(name, lab): [] for name in FAMILIES for lab in ("oracle", "real")}
    for seed in SEEDS:
        for labels in ("oracle", "real"):
            # 같은 시드 → 배경 재추첨이 동일 → 두 조건 차이가 분류 오차뿐이다.
            rng = random.Random(seed)
            ideal, kept, redrawn = idealize(
                gold_items, real_items, regions, rng, labels
            )
            if seed == SEEDS[0] and labels == "oracle":
                print(f"케이스 구간 유지 {kept:,}건 / 배경 재추첨 {redrawn:,}건")
            for name, keyfn in FAMILIES.items():
                r = await run_family(name, keyfn, ideal, documents, truth, ignored, labels)
                acc[(name, labels)].append(r)
                print(f"  seed={seed} [{labels:6s}] {name}  발행 {r['published']}건")

    print("\n" + "=" * 104)
    print("이상적 배경 (배경만 config BASELINE_RATE 로 재생성)")
    print(f"시드 {SEEDS} 평균 · 32일 · 억제·combine_sources 포함")
    print("=" * 104)
    print(
        f"{'family':14s} {'라벨':8s} {'발행':>7s} {'참':>7s} {'헛알림':>8s} "
        f"{'헛알림비율':>11s} {'알림/일':>9s} {'케이스도달':>11s}"
    )
    print("-" * 104)
    for name in FAMILIES:
        for labels in ("oracle", "real"):
            rs = acc[(name, labels)]
            pub = statistics.mean(r["published"] for r in rs)
            tru = statistics.mean(r["true"] for r in rs)
            fal = statistics.mean(r["false"] for r in rs)
            ratio = statistics.mean(r["fa_ratio"] for r in rs)
            hit = statistics.mean(r["cases_hit"] for r in rs)
            total_cases = rs[0]["cases_total"]
            print(
                f"{name:14s} {labels:8s} {pub:6.1f}건 {tru:6.1f}건 {fal:7.1f}건 "
                f"{ratio:10.1%} {pub / 32:8.2f}건 {hit:6.1f}/{total_cases:<4d}"
            )
        print()
    print("  oracle = 케이스 구간에 골든 라벨(분류 오차 0). real = 실제 분류 결과.")
    print("  같은 시드에서 배경이 동일하므로 두 행의 차이는 **분류 오차뿐**이다.")
    print("  ⚠️ 가상 조건이다 — mock 배경 결함을 제거했을 때의 값이지 실서비스 예측이 아니다.")


if __name__ == "__main__":
    # 첫 문장이어야 한다. `async def main()` 이라 `main()` 안이 아니라 **여기**다 — 가드가
    # `AsyncFunctionDef` 를 못 찾아 `__main__` 블록을 진입 지점으로 삼는다.
    force_utf8_output()

    asyncio.run(main())
