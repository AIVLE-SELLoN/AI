"""담당: 서영 (Agent2) — [8] CS·리뷰 종합 (이상탐지 로직 V3 §5, 탐지 결과 스키마 §3.1).

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.

CS 와 리뷰는 분모가 달라서([0] 각자 총건수) [0]~[6]과 [7]의 확신도 산출까지 **각각
독립 수행**한 뒤, 여기서 종합해 **알림 1건**을 만든다.

    CS 편중 O / 리뷰 편중 O → 강한 신호 (양 소스에서 동일 신호 확인)
    CS 편중 O / 리뷰 편중 X → CS 선행 신호 (리뷰는 배송 후 작성이라 2~5일 시차)
    CS 편중 X / 리뷰 편중 O → 리뷰 지연 반영 또는 CS 미표출

주 소스는 CS. CS 가 리뷰보다 빠른 신호라서, 둘 다 발화하면 verdict·main_aspect·
root_cause·stats 를 전부 CS 값으로 채택하고 확신도만 1단계 올린다.

⚠️ 상향은 **편중형에만.** 구분불가도 확신도가 '중간'이라 confidence 만으로는 편중형과
   구분되지 않는다 (문서 §5 주의). 그래서 verdict 를 함께 받는다.
"""

from app.core.schemas import DetectionConfidence, Source, Verdict

# 1단계 상향용 사다리. '해당없음'은 여기 없다 — 전역·잠정전역은 상향 대상이 아니다.
LEVELS: list[DetectionConfidence] = [
    DetectionConfidence.LOW,
    DetectionConfidence.MEDIUM,
    DetectionConfidence.HIGH,
]

# source_signals.interpretation 값. 문서 §5 문자열과 **완전일치**해야 한다
# (탐지 결과 스키마 §3: "combine_sources 반환값과 문자열 완전일치").
INTERPRETATION_BOTH = "강한 신호(양 소스)"
INTERPRETATION_CS_ONLY = "CS 선행 신호 — 리뷰는 시차로 미반영 가능"
INTERPRETATION_REVIEW_ONLY = "리뷰 지연 반영 또는 CS 미표출"


def combine_sources(
    cs_result: dict | None, review_result: dict | None
) -> tuple[DetectionConfidence | None, str | None]:
    """[8] 소스별 [7] 결과를 받아 최종 탐지 확신도와 해석 라벨을 낸다. (로직 §5)

    Args:
        cs_result / review_result: 각각 아래 형태. 그 소스가 아예 평가되지 않았으면 None.
            {"fired": bool, "verdict": str, "confidence": DetectionConfidence}

    Returns:
        (최종 탐지 확신도, 해석 라벨). 양쪽 미발화면 (None, None) — 알림 없음.
    """
    cs_fired = bool(cs_result and cs_result["fired"])
    rv_fired = bool(review_result and review_result["fired"])

    # ── 양 소스 발화 = 강한 신호 ─────────────────────
    if cs_fired and rv_fired:
        base_conf = cs_result["confidence"]      # 주 소스 = CS
        base_verdict = cs_result["verdict"]

        # 편중형만 상향. 전역·잠정전역은 확신도가 '해당없음'이라 사다리에 없다.
        if base_verdict != Verdict.BIASED:
            return base_conf, INTERPRETATION_BOTH

        index = min(LEVELS.index(base_conf) + 1, len(LEVELS) - 1)
        return LEVELS[index], INTERPRETATION_BOTH

    # ── 한쪽만 발화 ────────────────────────────────
    if cs_fired:
        return cs_result["confidence"], INTERPRETATION_CS_ONLY

    if rv_fired:
        return review_result["confidence"], INTERPRETATION_REVIEW_ONLY

    # ── 양쪽 미발화 = 정상 ─────────────────────────
    return None, None


def pick_primary_source(
    cs_result: dict | None, review_result: dict | None
) -> Source | None:
    """alert 필드를 어느 소스 값으로 채울지 결정한다. (탐지 결과 스키마 §3.1)

    verdict·main_aspect·root_cause·stats 는 전부 이 소스 값을 그대로 쓴다.
    둘 다 발화하면 CS 우선 — 종합의 단일 진실은 CS 다. 리뷰가 다른 verdict 를 냈어도
    alert verdict 에는 반영하지 않고, source_signals 에 발화 사실만 남는다.

    Returns:
        Source.CS / Source.REVIEW / None(양쪽 미발화)
    """
    if cs_result and cs_result["fired"]:
        return Source.CS
    if review_result and review_result["fired"]:
        return Source.REVIEW
    return None


def source_signal(result: dict | None) -> bool | None:
    """source_signals.cs / .review 값. (탐지 결과 스키마 §3)

    None = 그 소스가 **보류**(표본<10 등으로 판정 자체를 못 함). 미발화(False)와 다르다.
    "리뷰가 조용하다"와 "리뷰를 볼 수 없었다"를 구분해야 인사이트가 왜곡되지 않는다.
    """
    if result is None:
        return None
    return bool(result["fired"])
