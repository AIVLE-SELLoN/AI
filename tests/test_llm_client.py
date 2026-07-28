"""담당: 지인 — core/llm_client.py 테스트.

실제 OpenAI 호출(과금)은 안 쓴다. AsyncOpenAI 클라이언트 자체를 흉내내는 가짜
객체로 choose_tool()의 예외 처리만 검증한다.
"""

from types import SimpleNamespace

import pytest

from app.core.exceptions import LlmParseError
from app.core.llm_client import LlmClient


class _FakeToolCall:
    def __init__(self, name: str, arguments: str):
        self.function = SimpleNamespace(name=name, arguments=arguments)


class _FakeResponse:
    def __init__(self, tool_calls: list):
        message = SimpleNamespace(tool_calls=tool_calls)
        self.choices = [SimpleNamespace(message=message)]


class _FakeAsyncOpenAI:
    """chat.completions.create()만 흉내낸다 — 항상 같은 응답을 반환."""

    def __init__(self, response: _FakeResponse):
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))
        self._response = response

    async def _create(self, **kwargs):
        return self._response


@pytest.mark.asyncio
async def test_choose_tool_raises_llm_parse_error_when_arguments_never_parse():
    """tool 인자 JSON이 계속 깨져 있으면 LlmCallError가 아니라 LlmParseError로
    구분해서 던져야 한다(2026-07-27 발견·수정) — complete_json()과 같은 관례를
    따르지 않고 있던 버그. 재시도를 다 써도 파싱 자체가 안 되는 상황을 재현한다.
    """
    broken_call = _FakeToolCall(name="use_copy_draft", arguments="{이건 유효한 JSON이 아님")
    response = _FakeResponse([broken_call])
    client = LlmClient(_FakeAsyncOpenAI(response), model="gpt-4o-mini")

    with pytest.raises(LlmParseError):
        await client.choose_tool(
            "prompt",
            tools=[{"type": "function", "function": {"name": "use_copy_draft"}}],
        )


@pytest.mark.asyncio
async def test_choose_tool_returns_name_and_arguments_when_valid():
    valid_call = _FakeToolCall(name="use_copy_draft", arguments='{"reason": "테스트"}')
    response = _FakeResponse([valid_call])
    client = LlmClient(_FakeAsyncOpenAI(response), model="gpt-4o-mini")

    result = await client.choose_tool(
        "prompt",
        tools=[{"type": "function", "function": {"name": "use_copy_draft"}}],
    )

    assert result == {"name": "use_copy_draft", "arguments": {"reason": "테스트"}}
