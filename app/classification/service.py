"""담당: 현진 (Agent1) — 분류 로직.

단일 패스 LLM 워크플로우(분기 없음)라 프레임워크 없이 순수 Python 이다.
ClassifiedItem 계약은 docs/schemas.md §4 가 정본.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from string import Template

from pydantic import BaseModel, ValidationError

from app.core.exceptions import LlmParseError
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    Sentiment,
    Source,
)

logger = logging.getLogger(__name__)


def load_llm_prompt(module: str, name: str) -> str:
    """프롬프트 파일에서 "## System Prompt" 이후만 반환한다(헤더는 잘라낸다).

    헤더에 오류분석용으로 평가셋 문항 원문이 인용돼 있을 수 있어, 파일을 통째로 보내면
    토큰이 낭비되고 같은 문항을 재평가할 때 시험지가 유출된다. 구분선이 없는 파일은
    전체를 그대로 반환한다.
    """
    raw = load_prompt(module, name)
    m = re.search(r"## System Prompt\s*\n(.*)", raw, re.DOTALL)
    return m.group(1).strip() if m else raw


# 공개 이름은 load_llm_prompt 다 — eval·테스트가 그쪽을 쓴다. 이 별칭은 하위호환용.
_load_llm_prompt = load_llm_prompt

# ── 입력 타입 ────────────────────────────────────────────────────────────────
#
# REST 전용이 아니다 — 워커도 classify_aspect() 를 직접 부르며 이 타입을 재사용한다.


class ClassifyRequestItem(BaseModel):
    """분류 대상 원문 1건. Kafka 메시지 필드와 1:1 대응."""

    item_id: str
    source: Source
    channel: Channel
    product_group_id: str
    raw_text: str
    created_at: datetime


# ── 프롬프트 버전 (여기서만 관리한다) ────────────────────────────────────────
#
# 둘 다 실데이터 정량 평가를 마쳤다. 갱신하려면 eval/ 을 다시 돌리고 교체할 것.
PROMPT_ASPECT_VERSION = "classify_aspect_v5"        # 프롬프트1(CS) — 실험③ 부정판별 F1 98.3% · FPR 0.64%(2026-08-11 전량 96,524건)
PROMPT_SENTIMENT_VERSION = "classify_sentiment_v4"  # 프롬프트2 — 실험④ F1 87.6%(71630 n=300×3, 정정 골든)


async def classify_aspect(
    items: list[ClassifyRequestItem],
) -> list[ClassifiedItem | Exception]:
    """원문 여러 건 → ClassifiedItem 또는 Exception 리스트(요청과 순서·길이 동일).

    계약:
    1. len(반환) == len(items), 순서 동일 → zip(items, 반환) 이 성립한다.
    2. 성공은 ClassifiedItem, 실패는 예외 객체를 그 자리에 담는다(raise 안 함).
    3. 실패 타입은 LlmCallError·LlmParseError 둘뿐이다 — 그 밖의 예외는
       _classify_one() 이 LlmParseError 로 감싸 재던진다. 단 get_llm_client() 는
       감싸지 않는다: API 키 누락 같은 전역 설정 오류를 item 별 실패로 위장하면
       시스템 장애가 부분 실패로 보인다. 여러 슬롯에 같은 비-LlmParseError 가
       반복되는 것이 호출부에 주는 신호다.
    4. 전부 실패해도 이 함수는 raise 하지 않는다(return_exceptions=True).
    5. items == [] → [] (LLM 호출 0).

    호출부는 zip(items, results) 로 순회하며 isinstance(r, Exception) 으로 개별
    실패를 판별한다.

    아직 진짜 배치(N건을 한 프롬프트에 담아 호출 1회)가 아니다 — asyncio.gather()
    로 1건씩 동시 호출해 배치 효과만 낸다.
    """
    if not items:
        return []
    tasks = [_classify_one(item) for item in items]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def _classify_one(item: ClassifyRequestItem) -> ClassifiedItem:
    """원문 1건 → ClassifiedItem. source 로 프롬프트1(CS)/프롬프트2(리뷰) 를 고른다.

    던지는 예외는 LlmCallError·LlmParseError 로 통일한다(계약 3번). ClassifiedItem
    생성 시 pydantic 이 내는 ValidationError(리뷰에 REVIEW_ALLOWED_ASPECTS 밖 값이
    섞인 경우 등)도 여기서 LlmParseError 로 감싼다.
    """
    item_id = item.item_id
    source: Source = item.source
    trace_key = f"item_id={item_id}"  # 로그에 추적 키를 항상 포함한다

    # try 밖이다 — 전역 설정 오류를 item 별 실패로 위장하지 않기 위해(계약 3번).
    # Template.substitute·load_llm_prompt 만 진짜 item 처리 단계라 그 둘만 감싼다.
    client = get_llm_client()

    try:
        if source == Source.CS:
            template = Template(load_llm_prompt("classification", PROMPT_ASPECT_VERSION))
            # str.format() 이 아니라 Template — JSON 예시의 중괄호와 충돌한다.
            # 원문은 json.dumps 로 감싸 따옴표·줄바꿈이 프롬프트 구조를 안 깨게 한다.
            prompt = template.substitute(cs_text=json.dumps(item.raw_text, ensure_ascii=False))
        else:
            template = Template(load_llm_prompt("classification", PROMPT_SENTIMENT_VERSION))
            prompt = template.substitute(review_text=json.dumps(item.raw_text, ensure_ascii=False))
    except Exception as exc:
        # 프롬프트 파일 누락·플레이스홀더 불일치 등 호출 전 셋업 오류도 계약 3번대로
        # LlmParseError 로 통일한다.
        raise LlmParseError(f"프롬프트 준비 실패 [{trace_key}]: {exc}") from exc

    data = await client.complete_json(prompt, trace_key=trace_key)
    aspects = _parse_llm_response(data, source, trace_key=trace_key)

    try:
        return ClassifiedItem(
            item_id=item_id,
            source=source,
            channel=item.channel,
            product_group_id=item.product_group_id,
            raw_text=item.raw_text,
            aspects=aspects,
            created_at=item.created_at,
        )
    except ValidationError as exc:
        raise LlmParseError(f"ClassifiedItem 검증 실패 [{trace_key}]: {exc}") from exc


def _parse_llm_response(data: dict, source: Source, *, trace_key: str) -> list[AspectSentiment]:
    """LLM JSON 응답({"aspects": [...]}) 을 AspectSentiment 리스트로 변환.

    프롬프트1(CS)은 mixed_signal 을 안 내므로 None 고정, 프롬프트2(리뷰)는 그대로 반영.
    aspect·sentiment 가 enum 범위 밖이면(LLM 환각) LlmParseError 로 통일해서 던진다 —
    호출부가 LlmCallError/LlmParseError 두 가지만 잡으면 되게.

    CS 가 빈 배열이면 raise 하지 않고 _cs_empty_fallback() 으로 채운다. 여기서 던지면
    ClassifiedItem 이 안 만들어져 그 문의가 분모에서 통째로 빠지고 부정률이 실제보다
    높아진다(오탐 방향). 사유는 그 함수 docstring 참고. 리뷰는 빈 배열이 정상 응답이라
    제외한다.
    """
    raw_aspects = data.get("aspects")
    if raw_aspects is None:
        raise LlmParseError(f"LLM 응답에 'aspects' 키 없음 [{trace_key}]: {data}")

    if source == Source.CS and not raw_aspects:
        return _cs_empty_fallback(trace_key)

    try:
        result = []
        for a in raw_aspects:
            result.append(
                AspectSentiment(
                    aspect=Aspect(a["aspect"]),
                    sentiment=Sentiment(a["sentiment"]),
                    mixed_signal=a.get("mixed_signal") if source == Source.REVIEW else None,
                )
            )
        return result
    except (KeyError, ValueError) as exc:
        raise LlmParseError(f"aspects 파싱 실패 [{trace_key}]: {exc} (원본: {raw_aspects})") from exc


def _cs_empty_fallback(trace_key: str) -> list[AspectSentiment]:
    """CS 응답이 빈 배열일 때 '기타/중립' 1건으로 채운다.

    프롬프트1 은 "CS 는 6개 중 하나 이상"을 지시하지만 LLM 이 지키지 않는 경우가 있다
    (실측 284건 중 6건, 2.1%). 리뷰는 빈 배열이 정상 출력이라 CS 전용이다.

    집계 산식만 보면 이 폴백은 no-op 이다 — 분모는 ClassifiedItem 1건당 행 1개를 만든
    뒤 aspect 내용과 무관하게 +1 하므로(aggregate.py:58), 빈 배열이든 기타/중립이든
    분모+1·분자+0 으로 같다. coverage 도 aspect 자식이 아니라 분류 부모 행의 존재로
    판단하므로 빈 배열이 미달을 만들지도 않는다. 이 폴백은 프롬프트1 의 계약과 출력
    형태를 유지하며 빈 응답을 기타/중립으로 정규화하는 것이고, 반대로 LlmParseError
    로 던지면 부모 행이 안 만들어져 진짜 coverage 구멍과 재처리 비용이 생긴다.

    조용히 채우지 않는다 — 빈도가 오르면 프롬프트 회귀 신호다.

    인라인으로 되돌리지 말 것. eval/run_pipeline_eval.py 의 배치 경로가
    _parse_llm_response 를 우회하고 이 함수를 직접 불러 같은 규칙을 적용한다.
    """
    logger.warning(
        "cs_empty_aspects [%s] LLM이 빈 배열 반환 → %s/%d 로 대체",
        trace_key,
        Aspect.ETC.value,
        Sentiment.NEUTRAL.value,
    )
    return [AspectSentiment(aspect=Aspect.ETC, sentiment=Sentiment.NEUTRAL, mixed_signal=None)]


# ── explode 저장 규약 ────────────────────────────────────────────────────────
#
# 문의 1건이 여러 aspect 를 부정 언급하면 aspect 마다 별도 행으로 저장한다.
# 예) "색상도 다르고 사이즈도 안 맞아요" → 색상 부정 1행 + 사이즈 부정 1행.
# 분모(총 문의 수)는 여기가 아니라 집계 쪽에서 item 단위로 1회만 센다 — aspect 별
# 부정률은 독립 지표라 "둘 다 불평한 사람"이 양쪽에 다 들어가야 신호가 안 샌다.


def explode_to_rows(item: ClassifiedItem) -> list[dict]:
    """ClassifiedItem → DB 저장용 행 리스트로 explode.

    반환 dict 는 그대로 INSERT 에 쓸 수 있는 평평한 구조다 —
    scripts/classification_worker.py 가 classified_item_aspect 에 적재한다.
    """
    rows = []
    for a in item.aspects:
        rows.append({
            "item_id": item.item_id,
            "source": item.source.value,
            "channel": item.channel.value,
            "product_group_id": item.product_group_id,
            "aspect": a.aspect.value,
            "sentiment": a.sentiment.value,
            "mixed_signal": a.mixed_signal,
            "created_at": item.created_at,
        })
    return rows
