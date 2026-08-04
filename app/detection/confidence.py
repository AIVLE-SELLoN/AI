"""담당: 서영 (Agent2) — [7] 결과 발행: 탐지 확신도 · 권장 조치 (이상탐지 로직 V3 §[7]).

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다 ([2]·[3]·[5] 와 동일 원칙).
core/schemas.py 의 enum 만 가져온다 — 7종 매핑에서 문자열 오타를 막기 위해서다
(str Enum 이라 "편중형" 같은 평문 문자열과도 그대로 비교된다).

핵심 원칙 (로직 §[7]): **어떤 경우에도 '기각'은 없다.** 관문에서 걸러진 것도 알림은
나가고, 확신도와 권장 조치만 달라진다. 그래서 이 모듈에 "알림 안 함" 경로가 없다
(정상 = 애초에 발화가 없어서 여기 오지 않음).

상세페이지 수정 이력(timestamp_matched)은 **확신도 보강용이지 판정 권한이 없다.**
이력이 없다고 기각하지 않으며, 없으면 확신도가 '높음'까지 못 갈 뿐이다.
"""

from app.core.schemas import Aspect, DetectionConfidence, RecommendedAction, Verdict
from app.detection.scope import SCOPE_ASPECTS

# [6]을 수행하지 않는 판정들 — 원인 진단 대상이 아니거나(전역) 근거가 없다(구분불가).
_CAUSE_SKIPPED_VERDICTS = frozenset({Verdict.GLOBAL, Verdict.TENTATIVE_GLOBAL})

# 편중형 + 스코프 밖 aspect 의 조치. 로직 §3.2 표.
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
    """[7] 탐지 확신도를 정한다. 기각 없음 — 확신도만 달라진다. (로직 §[7])

    ⚠️ 여기서의 확신도는 **탐지 확신도**다. 셀러 화면(B4/B5)의 개선안 확신도(Agent3)와
       다른 값이다. 이 값은 Agent3 에 전달돼 자체 캡핑 규칙의 입력으로 쓰인다.

    Args:
        verdict: [3] 판정 결과 (편중형 / 전역형 / 잠정 전역형 / 구분불가).
        is_cause_consistent: [6] 원인 일관 여부. **[6]을 수행한 경우에만 값이 있다.**
            [6]을 건너뛴 판정(전역·구분불가·스코프 밖)에서는 None.
        timestamp_matched: 상세페이지 수정 이력이 이상 시작일과 일치하는가.
            보강 재료일 뿐 판정 권한은 없다. 이력 데이터가 없으면 False.

    Returns:
        높음 / 중간 / 낮음 / 해당없음
    """
    # ── [6]을 수행하지 않는 판정들 ─────────────────────
    if verdict in _CAUSE_SKIPPED_VERDICTS:
        return DetectionConfidence.NOT_APPLICABLE  # 원인 진단 대상이 아님

    if verdict == Verdict.INDETERMINATE:
        # [6]을 생략했으므로 원인 정보가 없다. 편중이라 단정할 수 없으니 '중간' 고정.
        return DetectionConfidence.MEDIUM

    # ── 여기서부터는 편중형 ──────────────────────────
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
    """[7] 권장 조치 7종. verdict + main_aspect + 원인 상태의 조합으로 자동 결정. (§3.2)

    대시보드는 이 값을 그대로 셀러 화면에 노출하고, **Agent3 는 '개선안 생성'일 때만
    작동한다.** alert 1건의 이 값은 main_aspect 기준 1개뿐이며, 동시 발화한 부가
    aspect 의 조치는 sub_aspects 배열에 따로 담긴다(§3.2 SC-029 예).

    Args:
        verdict: [3] 판정 결과.
        main_aspect: [4] 로 뽑힌 주 aspect.
        is_cause_consistent: [6] 원인 일관 여부. [6] 미수행이면 None.
    """
    if verdict in _CAUSE_SKIPPED_VERDICTS:
        return RecommendedAction.PRODUCT_CHECK  # 전역·잠정전역 → 상품 자체 점검

    if verdict == Verdict.INDETERMINATE:
        return RecommendedAction.SCOPE_UNDETERMINED  # 편중·전역 구분 불가

    # ── 편중형 ────────────────────────────────────
    if main_aspect not in SCOPE_ASPECTS:
        # 파손·오배송·기타는 원인 후보 자체가 없다 → aspect 별 고정 조치.
        return _OUT_OF_SCOPE_ACTIONS[Aspect(main_aspect)]

    if is_cause_consistent:
        return RecommendedAction.GENERATE_RECOMMENDATION  # 편중 + 스코프 내 + 원인 명확

    # 편중은 맞으나 원인이 분산 → 개선안을 만들 근거가 없다.
    return RecommendedAction.CHANNEL_OPERATION_CHECK
