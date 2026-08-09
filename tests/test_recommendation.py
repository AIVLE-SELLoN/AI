"""담당: 지인 — 개선안 생성 테스트.

TODO(지인): 완료 기준 = 인용검증·재시도 루프·근거없음 경로 작동.
세 경로를 각각 테스트로 고정해두면 프롬프트를 바꿔도 회귀를 잡을 수 있습니다.
현재는 pipeline.py raw 파이프라인(스텁+grounding+retrieve_context+route_proposal_type+
generate_proposal) 기준.

오케스트레이션 테스트는 retrieve_context()(ChromaDB)만 모킹하고, route_proposal_type()·
generate_proposal()은 실제 코드 그대로 돌린다 — get_llm_client()가 반환하는 fake
LLM 클라이언트 하나만 갈아끼워서, "LLM이 실제로 tool을 골라 라우팅하고 그 결과로
생성까지 이어지는지"를 오케스트레이션 레벨에서 검증한다. 실제 OpenAI 호출(과금)은
없다. 각 함수 자체의 로직은 test_route_proposal_type.py / test_retrieve_context.py /
test_generate_proposal.py에서 따로 검증한다.
"""

import pytest

from app.core.schemas import (
    HitlStatus,
    ProposalType,
    Recommendation,
    validate_citations_grounded,
)
from app.recommendation import pipeline


class _FakeAgentLlmClient:
    """route_proposal_type()의 choose_tool()과 generate_proposal()의 complete_json()을
    동시에 흉내낸다 — 두 함수 다 get_llm_client()를 거치기 때문에 이거 하나로 충분하다.
    """

    def __init__(self, tool_name: str, generation_response: dict):
        self._tool_name = tool_name
        self._generation_response = generation_response

    async def choose_tool(self, prompt: str, *, tools, trace_key: str = "-", temperature: float = 0.0):
        return {"name": self._tool_name, "arguments": {"reason": "사진 관련 CS 비중이 높음"}}

    async def complete_json(self, prompt: str, *, trace_key: str = "-", temperature: float = 0.0) -> dict:
        return self._generation_response


def _stub_context(alert, inquiries=()):
    return {
        "detail_text": "아이보리 컬러",
        "cs_quotes": "\n".join(f"- {q.raw_text}" for q in inquiries) or pipeline.NO_DETAIL_TEXT,
        "cs_summary": "CS 20건 중 14건이 '사진_색감_오차' 관련 언급",
        "similar_case": None,
    }


@pytest.mark.asyncio
async def test_run_generates_recommendation_for_biased_alert(
    monkeypatch, biased_alert, linked_inquiries
):
    fake_client = _FakeAgentLlmClient(
        tool_name="use_image_guide",
        generation_response={
            "current_text": "사진이랑 색이 너무 달라요",
            "proposed_text": "자연광 촬영 이미지 추가를 검토하세요.",
            "rationale": "원인 분류: 사진_색감_오차",
        },
    )
    monkeypatch.setattr(pipeline, "retrieve_context", _stub_context)
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    result = await pipeline.run(biased_alert, linked_inquiries)

    assert isinstance(result, Recommendation)
    assert result.alert_id == biased_alert.alert_id
    # route_proposal_type()·generate_proposal() 둘 다 실제 코드로 실행됨 —
    # LLM(fake)이 use_image_guide를 골랐고 그게 그대로 Proposal.type에 반영됐는지 확인.
    assert result.proposal.type == ProposalType.IMAGE_GUIDE
    assert result.evaluator.passed is True
    assert result.evaluator.attempts == 1
    assert result.hitl_status == HitlStatus.PENDING
    assert result.hitl_feedback is None
    assert result.proposal is not None

    # 인용이 실제로 박제된다(2026-08-09). 인용문은 첫 번째 문의에만 있으므로 1건이어야
    # 한다 — inquiries 전체를 그냥 싣는 구현이면 여기서 2건이 되어 걸린다.
    assert [c.inquiry_id for c in result.citations] == ["INQ-000412"]
    assert result.citations[0].quote == "사진이랑 색이 너무 달라요"

    validate_citations_grounded(result, biased_alert)


@pytest.mark.asyncio
async def test_run_returns_none_when_routed_evidence_is_missing(monkeypatch, biased_alert):
    """image_guide 로 갔는데 CS 원문이 없으면 **개선안을 만들지 않는다**(2026-08-09).

    근거 0건은 입력만 보고 아는 사실이라 생성을 태워봐야 일반론밖에 안 나온다.
    예전엔 통계 요약을 근거로 써서 이 경우에도 grounding=True 가 나왔다(자기참조).
    """
    fake_client = _FakeAgentLlmClient(
        tool_name="use_image_guide",
        generation_response={
            "current_text": "사진이랑 색이 너무 달라요",
            "proposed_text": "자연광 촬영 이미지 추가를 검토하세요.",
            "rationale": "원인 분류: 사진_색감_오차",
        },
    )
    monkeypatch.setattr(pipeline, "retrieve_context", _stub_context)
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    # inquiries 없음 → cs_quotes = NO_DETAIL_TEXT
    assert await pipeline.run(biased_alert) is None


@pytest.mark.asyncio
async def test_run_skips_routing_when_no_evidence_at_all(monkeypatch, biased_alert, caplog):
    """근거가 둘 다 없으면 라우팅 LLM 도 안 부른다 — 어느 도구를 골라도 만들 게 없다."""

    class _NeverCalled:
        async def choose_tool(self, *a, **k):
            raise AssertionError("근거가 0건인데 라우팅 LLM 을 불렀다")

        async def complete_json(self, *a, **k):
            raise AssertionError("근거가 0건인데 생성 LLM 을 불렀다")

    monkeypatch.setattr(
        pipeline,
        "retrieve_context",
        lambda alert, inquiries=(): {
            "detail_text": pipeline.NO_DETAIL_TEXT,
            "cs_quotes": pipeline.NO_DETAIL_TEXT,
            "cs_summary": "무관",
            "similar_case": None,
        },
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: _NeverCalled())

    assert await pipeline.run(biased_alert) is None
    assert any("근거가 0건" in r.getMessage() for r in caplog.records), (
        "조용히 넘기면 근거 파이프라인이 깨진 걸 아무도 모른다"
    )


class _FakeRecoveringClient:
    """첫 시도는 근거와 안 맞는 current_text(할루시네이션), 두 번째 시도부터 정상 응답.

    attempts 필드가 항상 1로 고정돼 있던 버그(2026-07-27 이전)를 재현/검증하기 위한
    fake — 재시도가 실제로 일어났고 그 회차가 Evaluator.attempts에 반영되는지 본다.
    prompt/temperature를 회차별로 기록해서, 재시도가 "같은 프롬프트를 온도 0으로
    반복"이 아니라 실패 피드백+온도 상승이 실제로 반영되는지도 같이 확인한다.
    """

    def __init__(self):
        self.complete_json_call_count = 0
        self.prompts: list[str] = []
        self.temperatures: list[float] = []

    async def choose_tool(self, prompt: str, *, tools, trace_key: str = "-", temperature: float = 0.0):
        return {"name": "use_copy_draft", "arguments": {"reason": "테스트"}}

    async def complete_json(self, prompt: str, *, trace_key: str = "-", temperature: float = 0.0) -> dict:
        self.complete_json_call_count += 1
        self.prompts.append(prompt)
        self.temperatures.append(temperature)
        if self.complete_json_call_count == 1:
            return {
                "current_text": "근거와 무관한 할루시네이션 문구",
                "proposed_text": "실물 색감을 확인해보세요.",
                "rationale": "원인 분류: 사진_색감_오차",
            }
        return {
            "current_text": "아이보리 컬러",
            "proposed_text": "실물 색감을 확인해보세요.",
            "rationale": "원인 분류: 사진_색감_오차",
        }


@pytest.mark.asyncio
async def test_run_records_real_attempt_number_after_retry(monkeypatch, biased_alert):
    fake_client = _FakeRecoveringClient()
    monkeypatch.setattr(pipeline, "retrieve_context", _stub_context)
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    result = await pipeline.run(biased_alert)

    assert result.evaluator.passed is True
    assert result.evaluator.attempts == 2
    assert fake_client.complete_json_call_count == 2

    # 2026-07-27 버그 수정 확인: 재시도가 1차와 똑같은 프롬프트·온도로 반복되지 않는다.
    assert fake_client.temperatures[0] != fake_client.temperatures[1]
    assert fake_client.prompts[0] != fake_client.prompts[1]
    assert "이전 시도" in fake_client.prompts[1]
    assert "근거와 무관한 할루시네이션 문구" in fake_client.prompts[1]


@pytest.mark.asyncio
async def test_run_returns_none_when_trigger_not_met(global_alert):
    """트리거 게이트에서 바로 반환 — 근거조회·라우팅까지 도달하지 않는다."""
    assert await pipeline.run(global_alert) is None


@pytest.mark.asyncio
async def test_run_returns_none_for_scope_undetermined_alert(indeterminate_alert):
    assert await pipeline.run(indeterminate_alert) is None
