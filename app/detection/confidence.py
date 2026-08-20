"""담당: 서영 (Agent2) — [7] 결과 발행: 탐지 확신도 · 권장 조치.

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.

이 모듈에 도달한 후보는 통계 3관문을 이미 통과했다. 여기서는 원인 일관성이나 수정 이력이
부족하다고 후보를 추가 기각하지 않고 확신도와 권장 조치만 조정한다. 그래서 이 모듈에
"알림 안 함" 경로가 없으며, 통계 관문에서 탈락한 후보와 정상 판정은 애초에 들어오지 않는다.

상세페이지 수정 이력(timestamp_matched)은 확신도 보강용이지 판정 권한이 없다. 없다고
기각하지 않으며 확신도가 '높음'까지 못 갈 뿐이다.
"""

from app.core.schemas import Aspect, DetectionConfidence, RecommendedAction, Verdict
from app.detection.scope import SCOPE_ASPECTS

# [6] 미수행 판정 중 확신도가 해당없음인 전역형. 구분불가는 별도 MEDIUM 분기다.
_CAUSE_SKIPPED_VERDICTS = frozenset({Verdict.GLOBAL, Verdict.TENTATIVE_GLOBAL})

# 편중형 + 스코프 밖 aspect 의 조치.
_OUT_OF_SCOPE_ACTIONS = {
    Aspect.DAMAGE: RecommendedAction.LOGISTICS_CHECK,        # 파손 → 물류 점검
    Aspect.MISDELIVERY: RecommendedAction.OPERATION_CHECK,   # 오배송 → 운영 점검
    Aspect.ETC: RecommendedAction.OTHER_TYPE_CHECK,          # 기타 → 잔여 버킷
}


def decide_confidence(
    verdict: str,
    is_cause_consistent: bool | None = None,
    timestamp_matched: bool = False,
) -> DetectionConfidence:
    """[7] 탐지 확신도를 정한다. 기각 없음 — 확신도만 달라진다.

    이 값은 탐지 확신도다. 셀러 화면의 개선안 확신도(Agent3)와 다른 값이고, Agent3 의 자체
    캡핑 규칙에 입력으로 들어간다.

    Args:
        is_cause_consistent: [6] 원인 일관 여부. [6]을 수행한 경우에만 값이 있고, 건너뛴
            판정(전역·구분불가·스코프 밖)에서는 None.
        timestamp_matched: 상세페이지 수정 이력이 이상 시작일과 일치하는가. 없으면 False.
    """
    if verdict in _CAUSE_SKIPPED_VERDICTS:
        return DetectionConfidence.NOT_APPLICABLE  # 원인 진단 대상이 아님

    if verdict == Verdict.INDETERMINATE:
        # 원인 정보가 없어 편중이라 단정할 수 없다.
        return DetectionConfidence.MEDIUM

    # 여기서부터는 편중형.
    if not is_cause_consistent:
        return DetectionConfidence.LOW  # 편중은 확실하나 원인이 흩어짐

    if timestamp_matched:
        return DetectionConfidence.HIGH  # 편중 + 원인 일관 + 시점까지 일치

    return DetectionConfidence.MEDIUM  # 편중 + 원인 일관, 시점 미확인


def decide_recommended_action(
    verdict: str,
    main_aspect: str,
    is_cause_consistent: bool | None = None,
) -> RecommendedAction:
    """[7] 권장 조치 7종. verdict + main_aspect + 원인 상태로 자동 결정.

    대시보드가 이 값을 그대로 셀러 화면에 노출하고, Agent3 는 '개선안 생성'일 때만 작동한다.
    alert 1건당 main_aspect 기준 1개이고, 동시 발화한 부가 aspect 의 조치는 sub_aspects 에
    따로 담긴다.
    """
    if verdict in _CAUSE_SKIPPED_VERDICTS:
        return RecommendedAction.PRODUCT_CHECK

    if verdict == Verdict.INDETERMINATE:
        return RecommendedAction.SCOPE_UNDETERMINED

    if main_aspect not in SCOPE_ASPECTS:
        # 파손·오배송·기타는 원인 후보 자체가 없다 → aspect 별 고정 조치.
        return _OUT_OF_SCOPE_ACTIONS[Aspect(main_aspect)]

    if is_cause_consistent:
        return RecommendedAction.GENERATE_RECOMMENDATION

    # 편중은 맞으나 원인이 분산 → 개선안을 만들 근거가 없다.
    return RecommendedAction.CHANNEL_OPERATION_CHECK
