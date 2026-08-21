"""담당: 지인 — core/llm_client.py 테스트.

실제 OpenAI 호출(과금)은 안 쓴다. AsyncOpenAI 클라이언트 자체를 흉내내는 가짜
객체로 choose_tool()의 예외 처리만 검증한다.
"""

from types import SimpleNamespace

import pytest

from app.config import Settings, get_settings
from app.core import llm_client
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


def test_get_llm_client_keeps_default_and_explicit_models_separate(monkeypatch):
    """기본 경로와 원인분류 전용 경로가 서로 다른 모델 클라이언트를 쓴다."""
    settings = get_settings()
    monkeypatch.setattr(settings, "llm_model", "gpt-4o-mini")
    monkeypatch.setattr(llm_client, "AsyncOpenAI", lambda **_kwargs: object())
    monkeypatch.setattr(llm_client, "wrap_openai", lambda client: client)
    llm_client._get_llm_client_for_model.cache_clear()

    try:
        default_client = llm_client.get_llm_client()
        cause_client = llm_client.get_llm_client(model="sentinel-cause-model")

        assert default_client._model == "gpt-4o-mini"
        assert cause_client._model == "sentinel-cause-model"
        assert default_client is llm_client.get_llm_client(model="gpt-4o-mini")
        assert cause_client is llm_client.get_llm_client(model="sentinel-cause-model")
    finally:
        llm_client._get_llm_client_for_model.cache_clear()


def test_cause_llm_model_default_is_gpt_4o():
    """실험⑥으로 확정한 원인분류 기본 모델이 조용히 낮아지는 회귀를 막는다."""
    assert Settings.model_fields["cause_llm_model"].default == "gpt-4o"


@pytest.mark.asyncio
async def test_choose_tool_raises_llm_parse_error_when_arguments_never_parse():
    """tool 인자 JSON이 계속 깨져 있으면 LlmCallError가 아니라 LlmParseError로
    구분해서 던져야 한다 — complete_json()과 같은 관례를
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
