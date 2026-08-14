"""담당: 서영 (Agent2) — 통계 검정 로직 (이상탐지 로직 V3 §[2]).

여기는 순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.
그래야 fixture 없이도 숫자만 넣어 단위테스트할 수 있다 (tests/test_detection.py).

발화 판정은 3관문 AND:
    ① 표본 가드   (상품,채널) 총문의 >= MIN_SAMPLE_SIZE           ← 아니면 보류
    ② BH-FDR      상품별 family 안의 p값에 BH 보정 후 유의         ← run_batch
    ③ min_delta   상승폭 >= MIN_DELTA                             ← run_one_test

주의 — 적용 순서: 보류를 제외한 상품별 **판정 가능 검정 전체**에 BH(②)를 먼저 적용한 뒤
min_delta(③)를 AND 로 겹친다. min_delta 로 먼저 거르면 '관측된 delta 데이터로 검정
집합을 고르는 것'이 되어 FDR 제어 전제가 깨지고 컷오프가 흔들린다. 그래서
run_one_test 는 delta 정보만 담고 발화는 안 정하며, 발화 확정은 run_batch 가 각 상품의
판정 가능 family를 보고 한다. 보류 채널이 있으면 family 크기는 최대 36보다 작아진다.
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
    run_batch 에서 확정한다.

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
        (batch, held)
          - batch: 판정 가능한 검정 결과 리스트 (각 dict 에 "key" 부착)
          - held:  판정하지 않은 (product, channel, source) 리스트. 중복 없음.

    보류 사유가 셋이고 **적용 범위가 다르다** — 묶지 말 것:
      ① 최소표본 미달 → (상품, 채널) 전체. 그 채널의 6 aspect × 2 source 12검정이
         통째로 빠진다 (로직 §5·§215, config 보고 §304). CS 가 부족하면 리뷰도 함께.
         분모가 작으면 비율이 널뛴다는 규칙이라, 기준 단위도 분모와 같은 (상품, 채널)이다.
      ② 과거 표본 0 → **그 aspect 슬롯 하나만**. past_total 은 aspect 마다 따로 센다
         (aggregate §97·§181 — 색상 알림이 나갔던 날을 뺄 때 그 날의 사이즈 문의까지
         빠지면 사이즈의 과거 기준이 이유 없이 깎이므로). 색상의 기준선이 비었다고
         같은 채널의 사이즈·소재까지 판정을 접으면 그 이상이 조용히 사라진다.
      ③ 분류 커버리지 미달 → 그 (상품, 채널, source). 분모가 source 마다 따로 집계되니
         CS 가 빠졌다고 리뷰 분모까지 틀린 건 아니다.

    ③을 **검정 전에** 빼는 이유: 분모가 깎인 슬롯은 부정률이 부풀려져 p값이 실제보다
    작게 나온다. BH 는 step-up 이라 가짜로 작은 p값 하나가 그 상품 family의 기각
    개수(k)를 늘려 **같은 상품의 다른 검정 임계까지 완화**시킨다. 상품별 family이므로
    다른 상품의 컷오프에는 전파되지 않는다.

    ⚠️ 보류는 자기 상품 family 크기(m)를 줄여 남은 검정의 BH 임계를 완화한다. 정본에서
    m=36인 상품의 순위 1 임계는 q/36인데, 1채널 보류 상품(m=24)은 1.5배, 2채널 보류
    상품(m=12)은 3배가 된다. 현재 config의 판정은 고정 m=36 반사실과 같지만 상품 간
    임계가 달라지는 운영 특성이므로 평가기의 family 크기 출력으로 감시한다. **현재 방식도
    각 상품의 실제 검정 family에 BH를 적용하므로 통계적으로 유효하다.** 보류를 p=1로
    패딩하는 것은 결함 수정이 아니라 상품 간 임계를 균일하게 만드는 제품 선택이며,
    보류 상품의 검정력을 낮추는 대가가 있다. 별도 사전 합의와 재평가 없이 적용하지 않는다.

    ①을 source 별로 좁히면 그 상품 family 크기(m)가 달라져 BH 컷오프가 어긋난다.
    반대로 ②를 채널 단위로 넓히면 aspect 격리(로직 §150)가 깨진다. 범위를 섞지 말 것.
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
            # 정상 경로라면 [1] 에서 설정값 baseline 으로 채워져 들어온다
            # (로직 §153, aggregate._apply_baseline_fallback ①). 여기까지 0 으로 왔다면
            # 그 설정값이 주입되지 않은 것이므로, 비교 기준이 없어 판정할 수 없다.
            held_slots.add((product, aspect, channel, source))

    # [3] 은 source 별로 독립 수행하므로(로직 §116) 호출부가 쓰기 좋게 (상품, 채널,
    # source) 로 눌러서 내보낸다. ② 는 aspect 단위지만 run_verdict 는 채널 상태만
    # 보므로 계약은 그대로다.
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
    """**상품별로** BH-FDR(관문②)을 적용하고 min_delta(관문③)와 AND → 발화 확정.

    왜 보정이 필요한가:
        검정을 조합마다 매일 돌리면 하루 약 1,464건. 보정 없이 각각 α=0.05 로 보면
        정상 상품 하나도 하루 54.8% 확률로 오탐한다(부록 A).

    🔴 **왜 배치 전체가 아니라 상품별인가** (2026-08-13 변경):
        BH 는 step-up 이라 **임계가 그 family 안의 발견 수에 비례해 올라간다.** 배치
        전체를 한 family 로 두면, 평가 배치(전 케이스가 한 배치에 모여 참양성 41개)와
        일별 운영 배치(하루 이상 1~3개)의 실효 임계가 **36배** 벌어진다:

            평가 배치   m=1,464  기각 41  실효 컷오프 0.00122
            일별 배치   m=1,476  기각  1  실효 컷오프 3.39e-05 (= q/m)

        그래서 **채점은 통과인데 운영·데모에선 안 뜨는** 상태가 된다. 실측(oracle,
        케이스별 자기 윈도우 끝날 기준)으로 일별 배치 탐지가 3/25 였고, 살아남은 3건은
        전부 파손·오배송이었다 — 평소 부정률이 1~2%라 p 가 가장 작기 때문이다. 즉
        **개선안 스코프(색상·사이즈·소재)는 0/13 으로 Agent3 가 한 번도 못 돈다.**

        상품은 **검정 결과를 보기 전에 정해지는 분할 단위**다(관측된 delta 로 family를
        고르지 않는다). 따라서 BH 가정 아래에서 상품별 기각 집합의 기대 FDR을 q로
        제어한다. 전체 상품을 합친 최종 발행 알림의 실현 헛알림률 5%를 보장하는 것은
        아니며, 그 값은 데모 시뮬레이션에서 별도로 잰다.

    ⚠️ **`(상품, source)` 로 더 쪼개지 말 것.** 32일 데모 시뮬(oracle) 실측에서 양쪽
       축이 다 나쁘다 — 헛알림률 35.0% / 케이스 도달 25·26 (상품별은 15.6% / 26·26).
       근거: `eval/results/detection_review_followup_20260813.md` §3.1

    각 test dict 에 "fired"(bool) 를 넣어 반환한다. `t["key"]` 는 `build_batch` 가 붙이는
    `(product, aspect, channel, source)` 이고, **여기서는 `key[0]`(상품)만 쓴다.**
    """
    if not batch:
        return batch

    # 상품별 family. key 가 없으면 KeyError 로 세운다 — 조용히 전체 family 로 폴백하면
    # "key 를 안 넘기면 옛 동작"이라는 함정이 생기고, 그건 미탐이라 조용하다.
    groups: dict[str, list] = defaultdict(list)
    for test in batch:
        groups[test["key"][0]].append(test)

    for group in groups.values():
        # rejected[i] = 그 family 안에서 i번째 검정이 BH 기준으로 유의한가
        rejected, _, _, _ = multipletests(
            [t["p_value"] for t in group], alpha=q, method="fdr_bh"
        )
        for test, is_significant in zip(group, rejected):
            # bh_significant 는 BH 보정 결과 **그 자체**로 남긴다 — 스키마 §3 이 이 필드를
            # "BH-FDR 보정 후에도 유의했는지"로 정의하고 대시보드 '유의 ✓' 배지의 근거로
            # 쓰기 때문이다. min_delta 를 섞으면 통계적 유의성과 실무적 크기가 한 칸에
            # 뭉개진다. 발화(fired)는 그 둘의 AND.
            test["bh_significant"] = bool(is_significant)
            # 이중 잠금: ② BH 보정 후 유의 AND ③ 상승폭이 실질적
            test["fired"] = test["bh_significant"] and test["meaningful"]

    return batch


def run_detection(
    all_combinations: list,
    q: float = BH_FDR_Q,
    *,
    unreliable_denominators: set | None = None,
) -> tuple[list, list]:
    """[2] 전체 진입점 — 집계 결과를 받아 발화 판정까지.

    build_batch(관문①) → decide_fires(관문②③) 를 엮은 것.
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
