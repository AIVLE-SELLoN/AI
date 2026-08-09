"""담당: 지인 — pipeline.evaluate() 테스트.

3기준(grounding/consistency/actionability) 전부 실제 판정으로 구현됐다(2026-07-27).
각 기준을 독립적으로 실패시켜서 다른 기준까지 덩달아 깨지지 않는지 확인한다 —
그래서 기본 `_proposal()` 헬퍼는 나머지 두 기준을 항상 만족하는 값을 깔아두고,
테스트마다 하나씩만 무너뜨린다.
"""

from app.core.schemas import Proposal, ProposalType
from app.recommendation import pipeline
from app.recommendation.pipeline import evaluate

_CONSISTENT_RATIONALE = "원인 분류: 사진_색감_오차"  # biased_alert.root_cause.label과 일치
_ACTIONABLE_TEXT = "실물 색감이 잘 드러나도록 확인해보세요."


def _context(
    detail_text: str = "아이보리 컬러",
    cs_quotes: str = "무관",
    cs_summary: str = "무관",
) -> dict:
    """evaluate()가 읽는 컨텍스트. **cs_quotes 와 cs_summary 는 별개 슬롯이다** —
    grounding 대조 대상은 cs_quotes 뿐이고, cs_summary 를 대조하면 자기참조가 된다."""
    return {
        "detail_text": detail_text,
        "cs_quotes": cs_quotes,
        "cs_summary": cs_summary,
        "similar_case": None,
    }


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
    context = _context()

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is True
    assert result.checks.grounding is True
    assert result.checks.consistency is True
    assert result.checks.actionability is True
    assert result.failure_reason is None


def test_copy_draft_fails_when_current_text_is_hallucinated(biased_alert):
    """LLM이 상세페이지에 없는 내용을 인용했다고 우기는 경우 — 진짜 잡아내는지 확인."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "완전히 다른 색상 이야기")
    context = _context()

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is False
    assert result.failure_reason is not None


def test_image_guide_checks_against_cs_quotes_not_detail_page(biased_alert):
    """image_guide는 detail_text가 뭐든 상관없이 cs_quotes(고객 원문)만 본다(§4-3 도구별 분리)."""
    proposal = _proposal(ProposalType.IMAGE_GUIDE, "사진이랑 색이 너무 달라요")
    context = _context(
        detail_text="이 값과 달라도 상관없음",
        cs_quotes="- 사진이랑 색이 너무 달라요\n- 화면에서 본 색이랑 다릅니다",
    )

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is True
    assert result.checks.grounding is True


def test_image_guide_rejects_quoting_the_stat_summary(biased_alert):
    """🔴 회귀 테스트 — image_guide grounding 자기참조 버그(2026-08-09 수정).

    cs_summary("CS 20건 중 14건이 …")는 **우리 코드가 만든 문장**이다. 예전엔 그게
    grounding 대조 대상이어서, LLM이 그 문장을 그대로 되풀이하면 무조건 통과했다 —
    "고객 문의에 근거했다"가 아니라 "우리가 쓴 문장을 따라 썼다"였다.
    이제 대조는 cs_quotes 하고만 하므로 통계 문장 인용은 실패해야 한다.
    """
    stat_summary = "CS 20건 중 14건이 '사진_색감_오차' 관련 언급"
    proposal = _proposal(ProposalType.IMAGE_GUIDE, stat_summary)
    context = _context(
        cs_quotes="- 사진이랑 색이 너무 달라요",
        cs_summary=stat_summary,
    )

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is False


def test_image_guide_fails_when_no_cs_quotes_available(biased_alert):
    """원문이 한 건도 없으면(NO_DETAIL_TEXT) 근거가 없는 것이라 통과하면 안 된다."""
    proposal = _proposal(ProposalType.IMAGE_GUIDE, "사진이랑 색이 너무 달라요")
    context = _context(cs_quotes=pipeline.NO_DETAIL_TEXT)

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is False


def test_fails_when_rationale_not_consistent_with_root_cause(biased_alert):
    """grounding은 통과해도 rationale이 엉뚱한 사유를 대면 실패해야 한다."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러", rationale="그냥 한번 해봤습니다")
    context = _context()

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.grounding is True
    assert result.checks.consistency is False
    assert "원인 라벨" in result.failure_reason


def test_passes_with_naturally_paraphrased_rationale(biased_alert):
    """실제 LLM은 라벨을 언더스코어째로 안 베끼고 자연스럽게 풀어쓴다 — 그래도 통과해야 한다.

    2026-07-27 버그: 라벨 전체("사진_색감_오차")를 한 덩어리로 대조해서, 자연스럽게
    풀어쓴 문장은 전부 실패 처리됐었다(API 호출 없이 정적으로 재현·수정 확인).
    """
    proposal = _proposal(
        ProposalType.COPY_DRAFT,
        "아이보리 컬러",
        rationale="사진 색감이 실제 상품과 다르게 촬영되어 발생한 문제로 보입니다",
    )
    context = _context()

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is True
    assert result.checks.consistency is True


def test_fails_when_proposed_text_not_actionable(biased_alert):
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러", proposed_text="색상이 좀 이상한 것 같습니다")
    context = _context()

    result = evaluate(proposal, biased_alert, context)

    assert result.passed is False
    assert result.checks.actionability is False


def test_attempts_field_reflects_passed_attempt_number(biased_alert):
    """이전엔 attempts가 항상 1로 고정돼 있었다 — run()이 실제 회차를 넘겨주는지 확인."""
    proposal = _proposal(ProposalType.COPY_DRAFT, "아이보리 컬러")
    context = _context()

    result = evaluate(proposal, biased_alert, context, attempt=2)

    assert result.attempts == 2
