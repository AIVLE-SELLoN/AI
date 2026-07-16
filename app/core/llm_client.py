"""LLM API 호출 래퍼 — 재시도 + 토큰 사용량 로깅.

협업 규칙 1: LLM 호출은 반드시 이 모듈을 경유한다.
각 모듈에서 openai 를 직접 import 하지 말 것. 그래야 비용 집계와 재시도 정책이
한 군데에서 관리된다.

사용 예:
    from app.core.llm_client import get_llm_client

    client = get_llm_client()
    text = await client.complete(prompt, trace_key=f"cs_id={cs_id}")
    data = await client.complete_json(prompt, trace_key=f"sku={sku}")
"""

import asyncio
import json
import logging
from functools import lru_cache
from typing import Any

from openai import APIError, AsyncOpenAI, RateLimitError

from app.config import get_settings
from app.core.constants import MAX_RETRY
from app.core.exceptions import LlmCallError, LlmParseError

logger = logging.getLogger(__name__)


class LlmClient:
    """OpenAI 호출 래퍼.

    - 모든 호출은 async (규칙: LLM 호출 함수는 async def 통일)
    - 일시적 오류는 지수 백오프로 재시도
    - 호출마다 토큰 사용량을 로그로 남김 → 주간 비용 집계의 근거
    """

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self._client = client
        self._model = model

    async def complete(
        self,
        prompt: str,
        *,
        trace_key: str = "-",
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """프롬프트 → 응답 텍스트.

        Args:
            prompt: 완성된 프롬프트 문자열. prompts/ 파일에서 읽어 포맷팅한 결과.
            trace_key: 로그 추적용 키 (`cs_id=123`, `sku=ABC` 등). 규칙상 항상 넣을 것.
            temperature: 분류·추출은 0.0 고정 권장.
            json_mode: True 면 모델에게 JSON 객체만 반환하도록 강제.

        Raises:
            LlmCallError: 재시도를 모두 소진하고도 실패.
        """
        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        last_error: Exception | None = None

        for attempt in range(1 + MAX_RETRY):
            try:
                response = await self._client.chat.completions.create(**kwargs)
            except (RateLimitError, APIError) as exc:
                # 일시적 오류로 보고 재시도. 마지막 시도였으면 아래에서 터뜨린다.
                last_error = exc
                if attempt < MAX_RETRY:
                    backoff = 2**attempt
                    logger.warning(
                        "LLM 호출 실패, %s초 후 재시도 [%s] attempt=%d/%d error=%s",
                        backoff, trace_key, attempt + 1, 1 + MAX_RETRY, exc,
                    )
                    await asyncio.sleep(backoff)
                continue

            self._log_usage(response, trace_key=trace_key)

            content = response.choices[0].message.content
            if content is None:
                raise LlmCallError(f"LLM 응답이 비어있음 [{trace_key}]")
            return content

        raise LlmCallError(
            f"LLM 호출 실패 ({1 + MAX_RETRY}회 시도) [{trace_key}]: {last_error}"
        ) from last_error

    async def complete_json(
        self,
        prompt: str,
        *,
        trace_key: str = "-",
        temperature: float = 0.0,
    ) -> dict[str, Any]:
        """프롬프트 → 파싱된 JSON dict.

        파싱 실패도 재시도 대상. LLM 이 가끔 JSON 앞뒤에 설명을 붙이거나
        따옴표를 깨뜨리는데, 같은 프롬프트로 다시 부르면 대개 성공한다.

        Raises:
            LlmParseError: 재시도를 모두 소진하고도 JSON 파싱 실패.
        """
        last_raw = ""

        for attempt in range(1 + MAX_RETRY):
            raw = await self.complete(
                prompt, trace_key=trace_key, temperature=temperature, json_mode=True
            )
            last_raw = raw
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "LLM 응답 JSON 파싱 실패 [%s] attempt=%d/%d error=%s",
                    trace_key, attempt + 1, 1 + MAX_RETRY, exc,
                )

        raise LlmParseError(
            f"JSON 파싱 실패 ({1 + MAX_RETRY}회 시도) [{trace_key}]: {last_raw[:200]}"
        )

    def _log_usage(self, response: Any, *, trace_key: str) -> None:
        """토큰 사용량 로깅. 주간 비용 집계는 이 로그를 긁어서 낸다.

        비용(원화 환산)까지 여기서 계산하지 않는 이유: 단가가 모델·시점마다 달라서
        코드에 박아두면 금방 틀어진다. 토큰 수만 정확히 남기고 환산은 집계 쪽에서.
        """
        usage = getattr(response, "usage", None)
        if usage is None:
            return

        logger.info(
            "llm_usage [%s] model=%s prompt_tokens=%d completion_tokens=%d total_tokens=%d",
            trace_key,
            self._model,
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens,
        )


@lru_cache
def get_llm_client() -> LlmClient:
    """앱 전역 공용 클라이언트. 커넥션 풀 재사용을 위해 매번 새로 만들지 않는다."""
    settings = get_settings()
    client = AsyncOpenAI(
        api_key=settings.llm_api_key,
        timeout=settings.llm_timeout_seconds,
    )
    return LlmClient(client, settings.llm_model)
