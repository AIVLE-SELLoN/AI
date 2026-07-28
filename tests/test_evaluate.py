"""담당: 지인 — pipeline.evaluate() 테스트.

3기준(grounding/consistency/actionability) 전부 실제 판정으로 구현됐다(2026-07-27).
각 기준을 독립적으로 실패시켜서 다른 기준까지 덩달아 깨지지 않는지 확인한다 —
그래서 기본 `_proposal()` 헬퍼는 나머지 두 기준을 항상 만족하는 값을 깔아두고,
테스트마다 하나씩만 무너뜨린다.
"""

from app.core.schemas import Proposal, ProposalType
from app.recommendation.pipeline import evaluate

_CONSISTENT_RATIONALE = "원인 분류: 사진_색감_오차"  # biased_alert.root_cause.label과 일치
_ACTIONABLE_TEXT = "실물 색감이 잘 드러나도록 확인해보세요."


def _proposal(
    proposal_type: ProposalType,
    current_text: str,
    rationale: str = _CONSISTENT_RATIONALE,
    proposed_text: str = _ACTIONABLE_TEXT,
) -> Proposal:
    return Proposal(
        type=proposal_type,
        target_field="색상",
        current_text=current_text,
        proposed_text=proposed_text,
        rationale=rationale,
        detailpage_grounded=True,
    )


def test_passes_when_all_three_criteria_met(biased_alert):
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러")
    context = {"detail_text": "아이보리 컬러", "cs_summary": "무관", "similar_case": None}

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is True
    assert result.checks.grounding is True
    assert result.checks.consistency is True
    assert result.checks.actionability is True
    assert result.failure_reason is None


def test_copy_draft_fails_when_current_text_is_hallucinated(biased_alert):
    """LLM이 상세페이지에 없는 내용을 인용했다고 우기는 경우 — 진짜 잡아내는지 확인."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "완전히 다른 색상 이야기")
    context = {"detail_text": "아이보리 컬러", "cs_summary": "무관", "similar_case": None}

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is False
    assert result.failure_reason is not None


def test_image_guide_checks_against_cs_summary_not_detail_page(biased_alert):
    """image_guide는 detail_text가 뭐든 상관없이 cs_summary만 본다(§4-3 도구별 분리)."""
    proposal = _proposal(ProposalType.IMAGE_GUIDE, "CS 20건 중 14건 언급")
    context = {
        "detail_text": "이 값과 달라도 상관없음",
        "cs_summary": "CS 20건 중 14건 언급",
        "similar_case": None,
    }

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is True


def test_fails_when_rationale_not_consistent_with_root_cause(biased_alert):
    """grounding은 통과해도 rationale이 엉뚱한 사유를 대면 실패해야 한다."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러", rationale="그냥 한번 해봤습니다")
    context = {"detail_text": "아이보리 컬러", "cs_summary": "무관", "similar_case": None}

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is True
    assert result.checks.consistency is False
    assert "원인 라벨" in result.failure_reason


def test_fails_when_proposed_text_not_actionable(biased_alert):
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러", proposed_text="색상이 좀 이상한 것 같습니다")
    context = {"detail_text": "아이보리 컬러", "cs_summary": "무관", "similar_case": None}

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.actionability is False


def test_attempts_field_reflects_passed_attempt_number(biased_alert):
    """이전엔 attempts가 항상 1로 고정돼 있었다 — run()이 실제 회차를 넘겨주는지 확인."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러")
    context = {"detail_text": "아이보리 컬러", "cs_summary": "무관", "similar_case": None}

    result = evaluate(proposal, biased_alert, context, attempt=2)

    assert result.attempts == 2
