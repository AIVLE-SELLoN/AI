"""담당: 서영 (Agent2) — 통계 검정 로직.

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다 — 그래야 fixture 없이 숫자만으로
단위테스트가 된다.

발화 판정은 3관문 AND:
    ① 표본 가드   (상품,채널) 총문의 >= MIN_SAMPLE_SIZE           ← 아니면 보류
    ② BH-FDR      상품별 family 안의 p값에 BH 보정 후 유의         ← decide_fires
    ③ min_delta   상승폭 >= MIN_DELTA                             ← run_one_test

적용 순서 주의: 보류를 뺀 상품별 판정 가능 검정 전체에 BH(②)를 먼저 적용한 뒤
min_delta(③)를 AND 로 겹친다. min_delta 로 먼저 거르면 '관측된 delta 로 검정 집합을
고르는 것'이 되어 FDR 제어 전제가 깨지고 컷오프가 흔들린다. 그래서 run_one_test 는
delta 정보만 담고 발화는 안 정하며, 발화 확정은 decide_fires 가 각 상품의 판정 가능
family 를 보고 한다.
"""

from collections import defaultdict

from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests

from app.core.constants import ALPHA, BH_FDR_Q, MIN_DELTA, MIN_SAMPLE_SIZE


def run_one_test(cur_neg: int, cur_total: int, past_neg: int, past_total: int) -> dict:
    """검정 1건 = (한 상품, 한 aspect, 한 채널, 한 source).

    '현재 윈도우 vs 자기 과거 윈도우'를 Fisher 단측 검정한다. 채널간 비교가 아니라
    각 채널이 자기 평소와만 싸우므로 채널간 baseline 차이가 판정에 안 끼어든다.

    반환값에 '발화 여부'는 없다. 발화는 같은 상품 family 전체를 봐야 정해지므로
    decide_fires 에서 확정한다.

    Returns:
        {"p_value", "delta", "meaningful"}. delta 는 현재율 - 과거율이고 meaningful 이
        관문③ 통과 여부다.
    """
    # 2x2 분할표         부정        부정 아님
    #   현재      cur_neg     cur_total  - cur_neg
    #   과거      past_neg    past_total - past_neg
    table = [
        [cur_neg, cur_total - cur_neg],
        [past_neg, past_total - past_neg],
    ]

    # 'greater' — 낮아진 건 이상이 아니다. Fisher 는 표본이 작아도 정확하다.
    _, p_value = fisher_exact(table, alternative="greater")

    delta = cur_neg / cur_total - past_neg / past_total

    return {
        "p_value": p_value,
        "delta": delta,
        "meaningful": delta >= MIN_DELTA,
    }


def build_batch(
    all_combinations: list, *, unreliable_denominators: set | None = None
) -> tuple[list, list]:
    """윈도우 하나에 대해 '판정 가능한' 검정을 전부 모은다 (관문① 적용).

    Args:
        all_combinations: [(product, aspect, channel, source, counts), ...]
            counts = (cur_neg, cur_total, past_neg, past_total)
        unreliable_denominators: 분모를 믿을 수 없는 (product, channel, source) 집합.
            분류 커버리지 미달 슬롯을 로더가 넘긴다 (loader.check_coverage).

    Returns:
        (batch, held). batch 는 판정 가능한 검정 결과(각 dict 에 "key" 부착), held 는
        판정하지 않은 (product, channel, source) 리스트로 중복이 없다.

    보류 사유가 셋이고 적용 범위가 다르다 — 묶지 말 것:
      ① 최소표본 미달 → (상품, 채널) 전체. 분모가 작으면 비율이 널뛴다는 규칙이라 기준
         단위도 분모와 같다. source 별로 좁히면 family 크기(m)가 달라져 컷오프가 어긋난다.
      ② 과거 표본 0 → 그 aspect 슬롯 하나만. past_total 을 aspect 마다 따로 세기
         때문이다. 채널 단위로 넓히면 색상 기준선이 비었다고 같은 채널의 사이즈·소재
         판정까지 접혀 그 이상이 조용히 사라진다.
      ③ 커버리지 미달 → 그 (상품, 채널, source). 분모가 source 마다 따로 집계된다.

    ③을 검정 전에 빼는 이유: 분모가 깎인 슬롯은 부정률이 부풀려져 p값이 실제보다 작게
    나오고, BH 는 step-up 이라 그 하나가 같은 상품 family 의 기각 개수를 늘려 다른 검정
    임계까지 완화시킨다.

    보류는 자기 상품 family 크기를 줄여 남은 검정의 BH 임계를 완화한다(m=36 대비 1채널
    보류 1.5배, 2채널 3배). 그래도 각 상품의 실제 family 에 BH 를 적용하므로 통계적으로
    유효하다. p=1 패딩은 결함 수정이 아니라 상품 간 임계를 균일하게 만드는 제품 선택이고
    보류 상품의 검정력을 낮추는 대가가 있다 — 사전 합의와 재평가 없이 적용하지 않는다.
    """
    unreliable = unreliable_denominators or set()

    held_pairs: set = set()  # ① (product, channel)
    held_slots: set = set()  # ② (product, aspect, channel, source)
    sources_of: dict = {}  # (product, channel) → 입력에 실제로 있던 source 들

    for product, aspect, channel, source, counts in all_combinations:
        _cur_neg, cur_total, _past_neg, past_total = counts
        sources_of.setdefault((product, channel), set()).add(source)

        if cur_total < MIN_SAMPLE_SIZE:
            held_pairs.add((product, channel))
        elif past_total == 0:
            # 정상 경로면 [1] 의 aggregate._apply_baseline_fallback 이 설정값
            # baseline 으로 채워 보낸다. 0 으로 왔다면 그 설정값이 주입되지 않은 것이라
            # 비교 기준이 없어 판정할 수 없다.
            held_slots.add((product, aspect, channel, source))

    # [3] 이 source 별로 독립 수행하므로 (상품, 채널, source) 로 눌러 내보낸다.
    # ② 는 aspect 단위지만 run_verdict 는 채널 상태만 보므로 계약은 그대로다.
    held: list = sorted(
        {(product, channel, source) for product, _a, channel, source in held_slots}
        | {
            (product, channel, source)
            for (product, channel) in held_pairs
            for source in sources_of[(product, channel)]
        }
        | {
            slot
            for slot in unreliable
            if slot[:2] in sources_of  # 이번 배치에 실제로 있는 슬롯만
        }
    )

    batch: list = []
    for product, aspect, channel, source, counts in all_combinations:
        if (
            (product, channel) in held_pairs
            or (product, aspect, channel, source) in held_slots
            or (product, channel, source) in unreliable
        ):
            continue
        cur_neg, cur_total, past_neg, past_total = counts
        result = run_one_test(cur_neg, cur_total, past_neg, past_total)
        result["key"] = (product, aspect, channel, source)
        batch.append(result)

    return batch, held


def decide_fires(batch: list, q: float = BH_FDR_Q) -> list:
    """상품별로 BH-FDR(관문②)을 적용하고 min_delta(관문③)와 AND → 발화 확정.

    보정이 필요한 이유: 조합마다 매일 돌리면 하루 약 1,464건이라, 보정 없이 각각
    alpha=0.05 로 보면 정상 상품 하나도 하루 54.8% 확률로 오탐한다.

    왜 배치 전체가 아니라 상품별인가 — BH 는 step-up 이라 임계가 그 family 안의 발견 수에
    비례해 오른다. 배치 전체를 한 family 로 두면 평가 배치와 일별 운영 배치의 실효 임계가
    36배 벌어진다(m=1,464 기각 41 → 0.00122 vs m=1,476 기각 1 → 3.39e-05). 그러면 채점은
    통과인데 운영·데모에선 안 뜬다 — 실측으로 일별 탐지가 3/25 였고 살아남은 3건이 전부
    파손·오배송이라, 개선안 스코프는 0/13 으로 Agent3 가 한 번도 못 돌았다.

    상품은 검정 결과를 보기 전에 정해지는 분할 단위라 BH 가정 아래에서 상품별 기각 집합의
    기대 FDR 을 q 로 제어한다. 전체를 합친 발행 알림의 실현 헛알림률 5% 를 보장하지는
    않으며, 그 값은 데모 시뮬레이션에서 따로 잰다.

    (상품, source) 로 더 쪼개지 말 것 — 32일 데모 시뮬 실측에서 헛알림률 35.0% / 케이스
    도달 25·26 으로 양쪽 다 나빠진다(상품별은 15.6% / 26·26).

    각 test dict 에 "fired" 를 넣어 반환한다. key 는 build_batch 가 붙이고 여기서는
    key[0](상품)만 쓴다.
    """
    if not batch:
        return batch

    # key 가 없으면 KeyError 로 세운다 — 조용히 전체 family 로 폴백하면 "key 를 안
    # 넘기면 옛 동작"이라는 함정이 생기고, 그건 미탐이라 조용하다.
    groups: dict[str, list] = defaultdict(list)
    for test in batch:
        groups[test["key"][0]].append(test)

    for group in groups.values():
        rejected, _, _, _ = multipletests(
            [t["p_value"] for t in group], alpha=q, method="fdr_bh"
        )
        for test, is_significant in zip(group, rejected):
            # bh_significant 는 BH 보정 결과 그 자체다 — 스키마가 이 필드를 "보정 후에도
            # 유의했는지"로 정의하고 대시보드 배지 근거로 쓴다. min_delta 를 섞으면
            # 통계적 유의성과 실무적 크기가 한 칸에 뭉개진다.
            test["bh_significant"] = bool(is_significant)
            test["fired"] = test["bh_significant"] and test["meaningful"]

    return batch


def run_detection(
    all_combinations: list,
    q: float = BH_FDR_Q,
    *,
    unreliable_denominators: set | None = None,
) -> tuple[list, list]:
    """[2] 전체 진입점 — build_batch(관문①) → decide_fires(관문②③).

    반환: (발화 판정이 담긴 batch, 보류된 (상품,채널,source) 리스트)
    """
    batch, held = build_batch(
        all_combinations, unreliable_denominators=unreliable_denominators
    )
    decide_fires(batch, q=q)
    return batch, held


# ALPHA 는 raw Fisher 기준 문서화용. 실제 발화 임계는 BH 가 배치에서 정하므로
# 개별 검정에서 ALPHA 로 직접 컷하지 않는다 (BH_FDR_Q 가 그 역할).
_ = ALPHA
