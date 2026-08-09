"""담당: 지인 — pipeline._build_citations() / _collect_cs_quotes() 테스트.

`citations` 의 정의는 "근거가 된 문의 목록"이 아니라 **"실제로 인용한 문의"** 다
(agent3_logic.md §4-3). 그래서 `evidence.inquiry_ids` 를 통째로 싣는 구현은 틀렸고,
`current_text` 와 실제로 대조해서 맞는 것만 담아야 한다. 예전에 `quote=""` 인
Citation 을 채워두던 것과 같은 종류의 거짓을 다시 만들지 않기 위한 고정이다.

LLM 호출은 없다 — 전부 순수 함수라 비용 0.
"""

from app.core.constants import CS_QUOTE_TOP_N
from app.core.schemas import (
    Citation,
    Evaluator,
    EvaluatorChecks,
    LinkedCSInquiry,
    Proposal,
    ProposalType,
)
from app.recommendation import pipeline

_QUOTE = "사진이랑 색이 너무 달라요"


def _proposal(
    current_text: str = _QUOTE, proposal_type=ProposalType.IMAGE_GUIDE
) -> Proposal:
    return Proposal(
        type=proposal_type,
        target_field="색상",
        current_text=current_text,
        proposed_text="자연광에서 재촬영을 진행하세요.",
        rationale="원인 분류: 사진_색감_오차",
        detailpage_grounded=False,
    )


def _evaluator(grounding: bool = True) -> Evaluator:
    return Evaluator(
        passed=grounding,
        attempts=1,
        checks=EvaluatorChecks(
            grounding=grounding, consistency=True, actionability=True
        ),
    )


def _inquiry(item_id: str, raw_text: str) -> LinkedCSInquiry:
    return LinkedCSInquiry(
        item_id=item_id, raw_text=raw_text, created_at="2026-05-25T09:12:00"
    )


def test_only_the_quoted_inquiry_is_cited(linked_inquiries):
    """인용문이 있는 문의만 담긴다 — 나머지는 근거였을 뿐 인용은 아니다."""
    citations = pipeline._build_citations(_proposal(), _evaluator(), linked_inquiries)

    assert citations == [Citation(inquiry_id="INQ-000412", quote=_QUOTE)]


def test_all_matching_inquiries_are_cited():
    """같은 문구가 여러 문의에 있으면 전부 담는다 — 어느 하나만 고를 근거가 없다."""
    inquiries = [
        _inquiry("INQ-1", "사진이랑 색이 너무 달라요"),
        _inquiry("INQ-2", "사진이랑 색이 너무 달라요 환불해주세요"),
    ]

    citations = pipeline._build_citations(_proposal(), _evaluator(), inquiries)

    assert [c.inquiry_id for c in citations] == ["INQ-1", "INQ-2"]


def test_copy_draft_has_no_cs_citations(linked_inquiries):
    """copy_draft 는 상세페이지를 인용한다 — CS 인용이 없는 게 정상이다."""
    proposal = _proposal(proposal_type=ProposalType.COPY_DRAFT)

    assert pipeline._build_citations(proposal, _evaluator(), linked_inquiries) == []


def test_failed_grounding_produces_no_citations(linked_inquiries):
    """검증에 실패한 인용을 기록하면 안 된다 — fallback·scope_limit 경로가 여기서 걸린다."""
    assert (
        pipeline._build_citations(
            _proposal(), _evaluator(grounding=False), linked_inquiries
        )
        == []
    )


def test_hallucinated_quote_matches_nothing():
    inquiries = [_inquiry("INQ-1", "배송이 너무 늦어요")]

    assert pipeline._build_citations(_proposal(), _evaluator(), inquiries) == []


def test_citations_respect_the_same_cap_as_the_prompt():
    """프롬프트에 안 실린 문의는 인용될 수 없다 — 두 곳이 같은 상한을 써야 한다.

    상한이 어긋나면 "프롬프트엔 5건만 넣었는데 6번째 문의가 인용됐다"가 나온다.
    """
    quoted = [_inquiry(f"INQ-{i}", _QUOTE) for i in range(CS_QUOTE_TOP_N + 3)]

    citations = pipeline._build_citations(_proposal(), _evaluator(), quoted)

    assert len(citations) == CS_QUOTE_TOP_N


def test_collect_cs_quotes_drops_blank_and_caps():
    inquiries = [
        _inquiry("INQ-0", "   "),
        *[_inquiry(f"INQ-{i}", f"문의 {i}") for i in range(1, 9)],
    ]

    quotes = pipeline._collect_cs_quotes(inquiries)

    assert "문의 1" in quotes
    # 빈 원문이 앞자리를 차지한 채 잘리지 않는지 — 잘린 뒤 빈 것만 남으면 근거가 사라진다.
    assert quotes.count("\n") + 1 <= CS_QUOTE_TOP_N


def test_collect_cs_quotes_returns_no_detail_text_when_empty():
    assert pipeline._collect_cs_quotes([]) == pipeline.NO_DETAIL_TEXT
    assert (
        pipeline._collect_cs_quotes([_inquiry("INQ-1", "  ")])
        == pipeline.NO_DETAIL_TEXT
    )
