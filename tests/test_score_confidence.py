"""담당: 지인 — pipeline.score_confidence() 테스트 (§4-4 확신도 규칙 + §5-1 탐지 캡핑).

Agent1/2 코드 없이 conftest.py 픽스처(DetectionAlert)만으로 완결되는 로직이라
mock 데이터만으로 전부 검증 가능하다.

§4-4 개정(2026-08-04) — 근거 축을 evaluator.checks.grounding 으로 교체했다.
이전엔 proposal.detailpage_grounded 를 썼는데 그건 copy_draft 전용이라
image_guide 가 구조적으로 항상 '낮음'이었다(실연동 크로스체크에서 발견).
"""

from app.core.schemas import (
    DetectionConfidence,
    Evaluator,
    EvaluatorChecks,
    Proposal,
    ProposalType,
    RecommendationConfidence,
    RootCause,
)
from app.recommendation.pipeline import score_confidence


def _proposal(proposal_type: ProposalType = ProposalType.COPY_DRAFT) -> Proposal:
    return Proposal(
        type=proposal_type,
        target_field="색상",
        current_text="아이보리 컬러",
        proposed_text="테스트",
        rationale="테스트",
        # 확신도는 더 이상 이 값을 보지 않는다(§4-4 개정) — 근거는 evaluator 가 판정한다.
        detailpage_grounded=False,
    )


def _evaluator(grounding: bool) -> Evaluator:
    return Evaluator(
        passed=grounding,
        attempts=1,
        checks=EvaluatorChecks(grounding=grounding, consistency=True, actionability=True),
    )


def _no_cause(alert):
    """보강 축(원인 일관)을 끈 alert — 근거 축만 보고 싶을 때 쓴다."""
    return alert.model_copy(update={"root_cause": None})


# ── 베이스 라벨 (근거 필수 + 보강 2축) ──────────────────────────────


def test_high_when_grounded_and_cause_consistent(biased_alert):
    """근거 통과 + 원인 일관 → 높음. 컬렉션2가 비어 있어도 '높음'이 나올 수 있어야 한다."""
    assert biased_alert.detection_confidence == DetectionConfidence.HIGH
    assert biased_alert.root_cause.consistent is True

    confidence, reason, capped = score_confidence(
        _proposal(), {"similar_case": None}, biased_alert, _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.HIGH
    assert capped is False
    assert "근거 검증 통과" in reason


def test_high_when_grounded_and_similar_case(biased_alert):
    """근거 통과 + 유사사례 → 높음 (원인 일관이 없어도 보강 1개면 충분)."""
    confidence, _, capped = score_confidence(
        _proposal(),
        {"similar_case": "지난 4월 재촬영 사례"},
        _no_cause(biased_alert),
        _evaluator(grounding=True),
    )

    assert confidence == RecommendationConfidence.HIGH
    assert capped is False


def test_medium_when_grounded_but_no_reinforcement(biased_alert):
    """근거만 있고 보강 축이 하나도 없으면 중간."""
    confidence, _, capped = score_confidence(
        _proposal(), {"similar_case": None}, _no_cause(biased_alert), _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is False


def test_medium_when_cause_exists_but_not_consistent(biased_alert):
    """원인이 있어도 consistent=False 면 보강으로 안 쳐준다 — 다수가 아닌 원인을 고칠 위험."""
    alert = biased_alert.model_copy(
        update={"root_cause": RootCause(label="사진_색감_오차", count=4, total=20, consistent=False)}
    )

    confidence, reason, _ = score_confidence(
        _proposal(), {"similar_case": None}, alert, _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.MEDIUM
    assert "원인 일관 없음" in reason


def test_low_when_not_grounded_even_if_reinforced(biased_alert):
    """근거는 필수 — 원인 일관·유사사례가 다 있어도 근거 없으면 낮음.

    generate_fallback_proposal() 경로가 정확히 여기로 떨어진다(grounding=False).
    """
    confidence, reason, _ = score_confidence(
        _proposal(),
        {"similar_case": "지난 4월 재촬영 사례"},
        biased_alert,
        _evaluator(grounding=False),
    )

    assert confidence == RecommendationConfidence.LOW
    assert "근거 검증 실패" in reason


def test_image_guide_can_reach_high(biased_alert):
    """[회귀 방지] image_guide 도 '높음'까지 갈 수 있어야 한다.

    개정 전에는 detailpage_grounded 가 copy_draft 전용이라 image_guide 가
    항상 낮음이었다 — 실연동 크로스체크(2026-08-04)에서 발견한 버그.
    """
    confidence, _, _ = score_confidence(
        _proposal(ProposalType.IMAGE_GUIDE),
        {"similar_case": None},
        biased_alert,
        _evaluator(grounding=True),
    )

    assert confidence == RecommendationConfidence.HIGH


# ── 캡핑 (§4-3 · §5-1) ─────────────────────────────────────────────


def test_capped_to_medium_when_detection_confidence_is_medium(biased_alert):
    """§5-1: 탐지 확신도가 중간이면 개선안이 아무리 근거 충분해도 높음 표시 금지."""
    alert = biased_alert.model_copy(update={"detection_confidence": DetectionConfidence.MEDIUM})

    confidence, reason, capped = score_confidence(
        _proposal(), {"similar_case": "지난 4월 재촬영 사례"}, alert, _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is True
    assert "캡핑" in reason


def test_not_capped_when_detection_confidence_already_covers_base(biased_alert):
    """탐지 확신도가 base 라벨보다 넉넉하면 캡핑 안 함(강등 없음)."""
    confidence, _, capped = score_confidence(
        _proposal(), {"similar_case": None}, biased_alert, _evaluator(grounding=False)
    )

    assert confidence == RecommendationConfidence.LOW
    assert capped is False


def test_capped_to_medium_when_root_cause_label_is_etc(biased_alert):
    """§4-3: 원인 라벨이 "기타"면 근거가 충분해도 높음까지는 못 올라간다."""
    alert = biased_alert.model_copy(
        update={"root_cause": RootCause(label="기타", count=5, total=10, consistent=True)}
    )

    confidence, reason, capped = score_confidence(
        _proposal(), {"similar_case": "지난 4월 재촬영 사례"}, alert, _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is True
    assert "기타" in reason


def test_forced_low_for_scope_limit_labels(biased_alert):
    """§4-3 스코프 한계: 실물_염색_편차·실제_원단_문제는 근거 유무와 무관하게 낮음 고정."""
    alert = biased_alert.model_copy(
        update={"root_cause": RootCause(label="실물_염색_편차", count=5, total=10, consistent=True)}
    )

    confidence, reason, capped = score_confidence(
        _proposal(), {"similar_case": "지난 4월 재촬영 사례"}, alert, _evaluator(grounding=True)
    )

    assert confidence == RecommendationConfidence.LOW
    assert capped is True
    assert "실물_염색_편차" in reason
