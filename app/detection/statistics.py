"""담당: 서영 (Agent2) — 통계 검정 로직 (이상탐지 로직 V3 §[2]).

여기는 순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.
그래야 fixture 없이도 숫자만 넣어 단위테스트할 수 있다 (tests/test_detection.py).

발화 판정은 3관문 AND:
    ① 표본 가드   (상품,채널) 총문의 >= MIN_SAMPLE_SIZE           ← 아니면 보류
    ② BH-FDR      윈도우 전체 p값에 BH 보정 후 유의               ← run_batch
    ③ min_delta   상승폭 >= MIN_DELTA                             ← run_one_test

주의 — 적용 순서: BH(②)를 전체 검정에 먼저 적용한 뒤 min_delta(③)를 AND 로 겹친다.
min_delta 로 먼저 거르면 '관측된 delta 데이터로 검정 집합을 고르는 것'이 되어 FDR
보장이 깨지고 컷오프가 흔들린다. 그래서 run_one_test 는 delta 정보만 담고 발화는
안 정하며, 발화 확정은 run_batch 가 배치 전체를 보고 한다.
"""

from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from app.core.constants import ALPHA, BH_FDR_Q, MIN_DELTA, MIN_SAMPLE_SIZE


def run_one_test(
    cur_neg: int, cur_total: int, past_neg: int, past_total: int
) -> dict:
    """검정 1건 = (한 상품, 한 aspect, 한 채널, 한 source).

    '현재 윈도우 vs 자기 과거 윈도우'를 Fisher 단측 검정한다. 채널간 비교가 아니라
    각 채널이 자기 평소와만 싸우므로 채널간 baseline 차이가 판정에 안 끼어든다.

    반환값에 '발화 여부'는 없다. 발화는 배치 전체를 봐야 정해지므로 run_batch 에서.

    Returns:
        {"p_value", "delta", "meaningful"}
          - p_value:    Fisher 단측 p (현재가 과거보다 높은가만 봄)
          - delta:      현재율 - 과거율 (예: 0.08 이면 +8%p)
          - meaningful: delta >= MIN_DELTA (관문③ 통과 여부)
    """
    # 2x2 분할표         부정        부정 아님
    #   현재      cur_neg     cur_total  - cur_neg
    #   과거      past_neg    past_total - past_neg
    table = [
        [cur_neg, cur_total - cur_neg],
        [past_neg, past_total - past_neg],
    ]

    # alternative='greater' — "현재가 과거보다 높은가"만. 낮아진 건 이상 아님.
    # Fisher 를 쓰는 이유: 표본이 크든 작든 정확하다 (저사건 함정도 안전).
    _, p_value = fisher_exact(table, alternative="greater")

    delta = cur_neg / cur_total - past_neg / past_total

    return {
        "p_value": p_value,
        "delta": delta,
        "meaningful": delta >= MIN_DELTA,
    }


def build_batch(all_combinations: list) -> tuple[list, list]:
    """윈도우 하나에 대해 '판정 가능한' 검정을 전부 모은다 (관문① 적용).

    Args:
        all_combinations: [(product, aspect, channel, source, counts), ...]
            counts = (cur_neg, cur_total, past_neg, past_total)

    Returns:
        (batch, held)
          - batch: 판정 가능한 검정 결과 리스트 (각 dict 에 "key" 부착)
          - held:  표본 부족으로 보류된 (product, channel) 리스트 (채널 단위)
    """
    batch: list = []
    held: list = []

    for product, aspect, channel, source, counts in all_combinations:
        cur_neg, cur_total, past_neg, past_total = counts

        # ── 관문① 최소표본 가드 ──────────────────────────
        # 기준은 분모인 (상품,채널) 총문의. aspect 무관. 보류는 채널 단위라
        # 그 채널의 모든 aspect 가 함께 빠진다.
        if cur_total < MIN_SAMPLE_SIZE:
            held.append((product, channel))
            continue

        result = run_one_test(cur_neg, cur_total, past_neg, past_total)
        result["key"] = (product, aspect, channel, source)
        batch.append(result)

    return batch, held


def decide_fires(batch: list, q: float = BH_FDR_Q) -> list:
    """윈도우 전체 검정에 BH-FDR(관문②)을 적용하고 min_delta(관문③)와 AND → 발화 확정.

    왜 배치로 하나:
        검정을 조합마다 매일 돌리면 하루 약 1,464건. 보정 없이 각각 α=0.05 로 보면
        정상 상품 하나도 하루 54.8% 확률로 오탐한다(부록 A). BH 는 step-up 이라
        발견이 많으면 기준이 완화돼 참양성은 살리고 가짜만 죽인다.

    각 test dict 에 "fired"(bool) 를 넣어 반환한다.
    """
    if not batch:
        return batch

    p_values = [t["p_value"] for t in batch]
    # rejected[i] = i번째 검정이 BH 기준으로 유의한가
    rejected, _, _, _ = multipletests(p_values, alpha=q, method="fdr_bh")

    for test, is_significant in zip(batch, rejected):
        # 이중 잠금: ② BH 보정 후 유의 AND ③ 상승폭이 실질적
        test["fired"] = bool(is_significant) and test["meaningful"]

    return batch


def run_detection(all_combinations: list, q: float = BH_FDR_Q) -> tuple[list, list]:
    """[2] 전체 진입점 — 집계 결과를 받아 발화 판정까지.

    build_batch(관문①) → decide_fires(관문②③) 를 엮은 것.
    반환: (발화 판정이 담긴 batch, 보류된 (상품,채널) 리스트)
    """
    batch, held = build_batch(all_combinations)
    decide_fires(batch, q=q)
    return batch, held


# ALPHA 는 raw Fisher 기준 문서화용. 실제 발화 임계는 BH 가 배치에서 정하므로
# 개별 검정에서 ALPHA 로 직접 컷하지 않는다 (BH_FDR_Q 가 그 역할).
_ = ALPHA
