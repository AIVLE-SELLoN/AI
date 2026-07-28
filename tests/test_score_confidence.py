"""담당: 지인 — pipeline.score_confidence() 테스트 (§4-4 확신도 규칙 + §5-1 탐지 캡핑).

Agent1/2 코드 없이 conftest.py 픽스처(DetectionAlert)만으로 완결되는 로직이라
mock 데이터만으로 전부 검증 가능하다.
"""

from app.core.schemas import DetectionConfidence, ProposalType, RecommendationConfidence
from app.recommendation.pipeline import score_confidence


def _proposal(detailpage_grounded: bool):
    from app.core.schemas import Proposal

    return Proposal(
        type=ProposalType.COPY_DRAFT,
        target_field="색상",
        current_text="아이보리 컬러",
        proposed_text="테스트",
        rationale="테스트",
        detailpage_grounded=detailpage_grounded,
    )


def test_high_when_both_detail_and_similar_case_present(biased_alert):
    assert biased_alert.detection_confidence == DetectionConfidence.HIGH
    proposal = _proposal(detailpage_grounded=True)
    context = {"similar_case": "지난 4월 재촬영 사례"}

    confidence, reason, capped = score_confidence(proposal, context, biased_alert)

    assert confidence == RecommendationConfidence.HIGH
    assert capped is False
    assert "높음" in reason


def test_medium_when_only_detail_grounded(biased_alert):
    proposal = _proposal(detailpage_grounded=True)
    context = {"similar_case": None}

    confidence, _, capped = score_confidence(proposal, context, biased_alert)

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is False


def test_medium_when_only_similar_case_present(biased_alert):
    proposal = _proposal(detailpage_grounded=False)
    context = {"similar_case": "지난 4월 재촬영 사례"}

    confidence, _, capped = score_confidence(proposal, context, biased_alert)

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is False


def test_low_when_neither_present(biased_alert):
    proposal = _proposal(detailpage_grounded=False)
    context = {"similar_case": None}

    confidence, _, capped = score_confidence(proposal, context, biased_alert)

    assert confidence == RecommendationConfidence.LOW
    assert capped is False


def test_capped_to_medium_when_detection_confidence_is_medium(biased_alert):
    """§5-1: 탐지 확신도가 중간이면 개선안이 아무리 근거 충분해도 높음 표시 금지."""
    alert = biased_alert.model_copy(update={"detection_confidence": DetectionConfidence.MEDIUM})
    proposal = _proposal(detailpage_grounded=True)
    context = {"similar_case": "지난 4월 재촬영 사례"}

    confidence, reason, capped = score_confidence(proposal, context, alert)

    assert confidence == RecommendationConfidence.MEDIUM
    assert capped is True
    assert "캡핑" in reason


def test_not_capped_when_detection_confidence_already_covers_base(biased_alert):
    """탐지 확신도가 낮은 base 라벨보다 넉넉하면 캡핑 안 함(강등 없음)."""
    proposal = _proposal(detailpage_grounded=False)
    context = {"similar_case": None}

    confidence, _, capped = score_confidence(proposal, context, biased_alert)

    assert confidence == RecommendationConfidence.LOW
    assert capped is False
