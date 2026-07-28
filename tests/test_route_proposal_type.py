"""담당: 지인 — pipeline.route_proposal_type() 테스트.

이 라우팅이 Agent3를 "고정 프롬프트 체인"이 아니라 진짜 agent로 만드는 지점이다 —
copy_draft/image_guide 선택을 규칙이 아니라 LLM의 tool 호출로 한다. 실제 OpenAI
호출(과금)은 안 쓴다. get_llm_client()의 choose_tool()만 모킹해서 "모델이 고른
tool 이름 → ProposalType" 매핑과 프롬프트·tools 조립을 검증한다.
"""

import pytest

from app.core.exceptions import LlmParseError
from app.core.schemas import ProposalType
from app.recommendation import pipeline


class _FakeToolChoosingClient:
    def __init__(self, tool_name: str):
        self._tool_name = tool_name
        self.last_prompt: str | None = None
        self.last_tools: list | None = None

    async def choose_tool(self, prompt: str, *, tools, trace_key: str = "-", temperature: float = 0.0):
        self.last_prompt = prompt
        self.last_tools = tools
        return {"name": self._tool_name, "arguments": {"reason": "테스트"}}


def _context():
    return {
        "detail_text": "아이보리 컬러",
        "cs_summary": "CS 20건 중 14건이 '사진_색감_오차' 관련 언급",
        "similar_case": None,
    }


@pytest.mark.asyncio
async def test_maps_use_copy_draft_tool_to_copy_draft_type(monkeypatch, biased_alert):
    fake_client = _FakeToolChoosingClient("use_copy_draft")
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    result = await pipeline.route_proposal_type(biased_alert, _context())

    assert result == ProposalType.COPY_DRAFT


@pytest.mark.asyncio
async def test_maps_use_image_guide_tool_to_image_guide_type(monkeypatch, biased_alert):
    fake_client = _FakeToolChoosingClient("use_image_guide")
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    result = await pipeline.route_proposal_type(biased_alert, _context())

    assert result == ProposalType.IMAGE_GUIDE


@pytest.mark.asyncio
async def test_offers_both_tools_and_includes_both_evidence_in_prompt(monkeypatch, biased_alert):
    """판단이 규칙이 아니라 LLM 몫이라는 증거 — 두 tool·두 근거를 다 보여줘야 한다."""
    fake_client = _FakeToolChoosingClient("use_copy_draft")
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    await pipeline.route_proposal_type(biased_alert, _context())

    tool_names = {tool["function"]["name"] for tool in fake_client.last_tools}
    assert tool_names == {"use_copy_draft", "use_image_guide"}
    assert "아이보리 컬러" in fake_client.last_prompt
    assert "CS 20건 중 14건" in fake_client.last_prompt


@pytest.mark.asyncio
async def test_raises_llm_parse_error_for_unknown_tool_name(monkeypatch, biased_alert):
    """LLM이 우리가 안 준 tool 이름을 반환하면 KeyError로 그냥 죽지 않고 명확한
    예외를 던져야 한다(2026-07-27 발견·수정 — 방어 코드 없던 버그)."""
    fake_client = _FakeToolChoosingClient("use_something_unexpected")
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    with pytest.raises(LlmParseError):
        await pipeline.route_proposal_type(biased_alert, _context())
