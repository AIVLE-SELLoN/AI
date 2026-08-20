"""담당: 서영 (Agent2) — [8] CS·리뷰 종합.

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.

CS 와 리뷰는 분모가 달라 각자 독립 수행한 뒤 여기서 종합해 알림 1건을 만든다.

    CS O / 리뷰 O → 강한 신호
    CS O / 리뷰 X → CS 선행 신호 (리뷰는 배송 후 작성이라 2~5일 시차)
    CS X / 리뷰 O → 리뷰 지연 반영 또는 CS 미표출

주 소스는 CS — 더 빠른 신호라서 둘 다 발화하면 verdict·main_aspect·root_cause·stats 를
전부 CS 값으로 채택하고 확신도만 1단계 올린다.

상향은 편중형에만 한다. 구분불가도 확신도가 '중간'이라 confidence 만으로는 편중형과
구분되지 않아서 verdict 를 함께 받는다.
"""

from app.core.schemas import DetectionConfidence, Source, Verdict

# 1단계 상향용 사다리. '해당없음'은 여기 없다 — 전역·잠정전역은 상향 대상이 아니다.
LEVELS: list[DetectionConfidence] = [
    DetectionConfidence.LOW,
    DetectionConfidence.MEDIUM,
    DetectionConfidence.HIGH,
]

# source_signals.interpretation 값. 스키마가 이 문자열과 완전일치를 요구한다.
INTERPRETATION_BOTH = "강한 신호(양 소스)"
INTERPRETATION_CS_ONLY = "CS 선행 신호 — 리뷰는 시차로 미반영 가능"
INTERPRETATION_REVIEW_ONLY = "리뷰 지연 반영 또는 CS 미표출"


def combine_sources(
    cs_result: dict | None, review_result: dict | None
) -> tuple[DetectionConfidence | None, str | None]:
    """[8] 소스별 [7] 결과를 받아 최종 탐지 확신도와 해석 라벨을 낸다.

    Args:
        cs_result / review_result: {"fired", "verdict", "confidence"}. 그 소스가 아예
            평가되지 않았으면 None.

    Returns:
        (탐지 확신도, 해석 라벨). 양쪽 미발화면 (None, None) — 알림 없음.
    """
    cs_fired = bool(cs_result and cs_result["fired"])
    rv_fired = bool(review_result and review_result["fired"])

    if cs_fired and rv_fired:
        base_conf = cs_result["confidence"]      # 주 소스 = CS
        base_verdict = cs_result["verdict"]

        # 편중형만 상향. 전역·잠정전역은 확신도가 '해당없음'이라 사다리에 없다.
        if base_verdict != Verdict.BIASED:
            return base_conf, INTERPRETATION_BOTH

        index = min(LEVELS.index(base_conf) + 1, len(LEVELS) - 1)
        return LEVELS[index], INTERPRETATION_BOTH

    if cs_fired:
        return cs_result["confidence"], INTERPRETATION_CS_ONLY

    if rv_fired:
        return review_result["confidence"], INTERPRETATION_REVIEW_ONLY

    return None, None


def pick_primary_source(
    cs_result: dict | None, review_result: dict | None
) -> Source | None:
    """alert 필드를 어느 소스 값으로 채울지 결정한다.

    verdict·main_aspect·root_cause·stats 가 전부 이 소스 값이 된다. 둘 다 발화하면 CS 우선 —
    리뷰가 다른 verdict 를 냈어도 alert verdict 에는 반영하지 않고 source_signals 에 발화
    사실만 남는다.
    """
    if cs_result and cs_result["fired"]:
        return Source.CS
    if review_result and review_result["fired"]:
        return Source.REVIEW
    return None


def source_signal(result: dict | None) -> bool | None:
    """source_signals.cs / .review 값.

    None 은 그 소스가 보류(표본 부족으로 판정 자체를 못 함)라는 뜻이고 미발화(False)와 다르다.
    "리뷰가 조용하다"와 "리뷰를 볼 수 없었다"를 구분해야 인사이트가 왜곡되지 않는다.
    """
    if result is None:
        return None
    return bool(result["fired"])
