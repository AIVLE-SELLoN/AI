"""app/core/schemas.py 검증 규칙 테스트 (schemas.md §7)."""

import pytest
from pydantic import ValidationError

from app.core.schemas import (
    Citation,
    ClassifiedItem,
    Evaluator,
    EvaluatorChecks,
    Recommendation,
    validate_citations_grounded,
)


# ── DetectionAlert 픽스처 3개가 스키마를 통과하는지 확인 ──────────


def test_biased_alert_is_valid(biased_alert):
    assert biased_alert.verdict == "편중형"
    assert biased_alert.root_cause.consistent is True


def test_global_alert_is_valid(global_alert):
    assert global_alert.channel == "ALL"
    assert global_alert.root_cause is None


def test_indeterminate_alert_is_valid(indeterminate_alert):
    assert indeterminate_alert.verdict == "구분불가"
    assert indeterminate_alert.excluded_channels == ["NAVER", "ZIGZAG"]


# ── source=="review"면 aspect는 색상/사이즈/소재만 ────────────────


def test_review_item_rejects_disallowed_aspect():
    with pytest.raises(ValidationError):
        ClassifiedItem(
            item_id="REV-0001",
            source="review",
            channel="COUPANG",
            product_group_id="P001",
            raw_text="배송이 파손되어 왔어요",
            aspects=[{"aspect": "파손", "sentiment": -1, "mixed_signal": False}],
            created_at="2026-05-28T10:00:00",
        )


def test_review_item_allows_color_size_material():
    item = ClassifiedItem(
        item_id="REV-0002",
        source="review",
        channel="COUPANG",
        product_group_id="P001",
        raw_text="색이 화면과 달라요",
        aspects=[{"aspect": "색상", "sentiment": -1, "mixed_signal": False}],
        created_at="2026-05-28T10:00:00",
    )
    assert item.aspects[0].aspect == "색상"


# ── sentiment 범위 ({-1,0,1} 밖은 에러) ───────────────────────────


def test_sentiment_out_of_range_rejected():
    with pytest.raises(ValidationError):
        ClassifiedItem(
            item_id="CS-0001",
            source="cs",
            channel="COUPANG",
            product_group_id="P001",
            raw_text="문의 내용",
            aspects=[{"aspect": "색상", "sentiment": 2, "mixed_signal": None}],
            created_at="2026-05-28T10:00:00",
        )


# ── citations ⊆ evidence.inquiry_ids (모델 간 교차검증) ───────────


def _recommendation(citation_ids: list[str]) -> Recommendation:
    return Recommendation(
        recommendation_id="REC-20260528-0001",
        alert_id="ALT-20260528-0001",
        created_at="2026-05-28T10:31:40",
        citations=[Citation(inquiry_id=i, quote="발췌") for i in citation_ids],
        evaluator=Evaluator(
            passed=True,
            attempts=1,
            checks=EvaluatorChecks(grounding=True, consistency=True, actionability=True),
        ),
    )


def test_citations_within_evidence_passes(biased_alert):
    rec = _recommendation(["INQ-000412"])
    validate_citations_grounded(rec, biased_alert)


def test_citations_outside_evidence_rejected(biased_alert):
    rec = _recommendation(["INQ-999999"])
    with pytest.raises(ValueError):
        validate_citations_grounded(rec, biased_alert)


# ── recommendation_confidence 범위 (높음/중간/낮음/null 밖은 에러) ──


def test_recommendation_confidence_out_of_range_rejected():
    with pytest.raises(ValidationError):
        Recommendation(
            recommendation_id="REC-20260528-0002",
            alert_id="ALT-20260528-0001",
            created_at="2026-05-28T10:31:40",
            evaluator=Evaluator(
                passed=True,
                attempts=1,
                checks=EvaluatorChecks(grounding=True, consistency=True, actionability=True),
            ),
            recommendation_confidence="매우높음",
        )


def test_recommendation_confidence_null_allowed():
    rec = _recommendation([])
    assert rec.recommendation_confidence is None
