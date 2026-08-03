"""담당: 현진 (Agent1) — 분류 로직.

성격: LLM 워크플로우(단일 패스, 분기 없음) → 프레임워크 불필요, 순수 Python.
정본: 분류 워커 명세 §2(explode 저장 규약), docs/schemas.md §4(ClassifiedItem)
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from string import Template

from pydantic import BaseModel

from app.core.exceptions import LlmParseError
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import AspectSentiment, Aspect, Channel, ClassifiedItem, Sentiment, Source

def load_llm_prompt(module: str, name: str) -> str:
    """load_prompt()로 파일 전체를 읽되, "## System Prompt" 이전(파일 헤더 — 담당자·변경이력
    등 문서용 메타정보)은 잘라내고 그 이후만 반환한다.

    프롬프트 파일 헤더에는 오류분석 과정에서 평가셋 문항 원문을 인용하는 경우가 있는데
    (예: v4 변경이력에 파일럿 오답 문항 언급), 파일 전체를 그대로 LLM에 보내면 그 인용문까지
    같이 전송돼 ①불필요한 토큰 낭비 ②향후 같은 문항 재평가 시 시험지 유출 위험이 생긴다.
    "## System Prompt" 이후만 잘라 보내면 헤더는 순수 문서로만 남고 이 위험이 원천 차단된다.
    "## System Prompt" 구분선이 없는 파일은(과거 버전 호환) 안전하게 전체를 그대로 반환한다.
    """
    raw = load_prompt(module, name)
    m = re.search(r"## System Prompt\s*\n(.*)", raw, re.S)
    return m.group(1).strip() if m else raw


# 하위호환 별칭 — eval 스크립트 등에서 쓰도록 공개 API로 승격(PR 리뷰 반영).
_load_llm_prompt = load_llm_prompt

# ── 입력 타입 (router.py에서 옮겨옴 — REST뿐 아니라 Kafka 워커도 재사용) ────────
#
# ⚠️ 이 타입은 원래 router.py에 있었으나, 용준님 Kafka 워커도 classify_aspect()를
# 직접 호출하며 이 타입을 재사용하므로 "REST 전용"이 아니라 "분류 로직 자체의
# 입력 타입"으로 보는 게 맞아 여기로 이동(2026-07-28).


class ClassifyRequestItem(BaseModel):
    """분류 대상 원문 1건. Kafka 메시지 필드와 1:1 대응."""

    item_id: str
    source: Source
    channel: Channel
    product_group_id: str
    raw_text: str
    created_at: datetime


# ── 프롬프트 버전 (매직넘버 금지 컨벤션 — 여기서만 관리, 한 줄만 바꾸면 전체 반영) ──
#
# 두 프롬프트 모두 실데이터 정량 평가를 마친 상태(실험③④). 갱신 시 eval/ 재실행 후 교체할 것.
PROMPT_ASPECT_VERSION = "classify_aspect_v5"        # 프롬프트1(CS) — aspect 스코프 확장, 실험③ F1 99.0%(seed=42, n=300)
PROMPT_SENTIMENT_VERSION = "classify_sentiment_v4"  # 프롬프트2(리뷰) — v3 누적 수정 통합, 실험④ F1 84.4%(71603 기준)


async def classify_aspect(items: list[ClassifyRequestItem]) -> list[ClassifiedItem]:
    """원문 여러 건 → ClassifiedItem 리스트.

    items 원소는 item_id/source/channel/product_group_id/raw_text/created_at
    속성을 가진 객체(ClassifyRequestItem 등) 또는 그와 동등한 dict.

    ⚠️ "배치"의 의미: 프롬프트 자체는 1건씩 처리하도록 이미 검증됐음(파일럿 42건
    평가 통과) — 프롬프트를 여러 건 한 번에 받는 구조로 바꾸는 건 검증을 다시
    해야 해서 위험. 대신 asyncio.gather()로 여러 건을 동시에(병렬) 호출해 배치
    처리 효과를 냄. 분류 워커 명세 §1 "LLM 배치 분류 추론" 단계에 대응.
    """
    tasks = [_classify_one(item) for item in items]
    return await asyncio.gather(*tasks)


async def _classify_one(item: ClassifyRequestItem) -> ClassifiedItem:
    """원문 1건 → ClassifiedItem. source에 따라 프롬프트1(CS)/프롬프트2(리뷰) 자동 분기."""
    item_id = item.item_id
    source: Source = item.source
    trace_key = f"item_id={item_id}"  # 컨벤션 4장: 로그에 추적 키 항상 포함
    client = get_llm_client()

    if source == Source.CS:
        template = Template(load_llm_prompt("classification", PROMPT_ASPECT_VERSION))
        # ⚠️ str.format() 대신 Template — JSON 예시의 중괄호와 충돌 방지(core/prompts.py 경고).
        # 원문은 json.dumps로 감싸서 따옴표·줄바꿈이 프롬프트 구조를 깨지 않게 함.
        prompt = template.substitute(cs_text=json.dumps(item.raw_text, ensure_ascii=False))
    else:
        template = Template(load_llm_prompt("classification", PROMPT_SENTIMENT_VERSION))
        prompt = template.substitute(review_text=json.dumps(item.raw_text, ensure_ascii=False))

    data = await client.complete_json(prompt, trace_key=trace_key)
    aspects = _parse_llm_response(data, source, trace_key=trace_key)

    return ClassifiedItem(
        item_id=item_id,
        source=source,
        channel=item.channel,
        product_group_id=item.product_group_id,
        raw_text=item.raw_text,
        aspects=aspects,
        created_at=item.created_at,
    )


def _parse_llm_response(data: dict, source: Source, *, trace_key: str) -> list[AspectSentiment]:
    """LLM JSON 응답({"aspects": [...]})을 AspectSentiment 리스트로 변환.

    프롬프트1(CS)은 mixed_signal을 안 냄 → None 고정(스키마 규칙: CS는 mixed_signal=null).
    프롬프트2(리뷰)는 mixed_signal을 냄 → 그대로 반영.
    aspect·sentiment 값이 enum 범위 밖이면(LLM 환각) LlmParseError로 통일해서 던짐 —
    호출부가 LlmCallError/LlmParseError 두 가지만 잡으면 되게.
    """
    raw_aspects = data.get("aspects")
    if raw_aspects is None:
        raise LlmParseError(f"LLM 응답에 'aspects' 키 없음 [{trace_key}]: {data}")

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


# ── explode 저장 규약 (분류 워커 명세 §2, 2026-07-23 유지인 확정) ────────────────
#
# 문의 1건이 aspect를 여러 개 부정 언급하면, 각 aspect마다 별도 행으로 저장한다.
# 예) "색상도 다르고 사이즈도 안 맞아요" → 색상 부정 행 1개 + 사이즈 부정 행 1개(총 2행)
# 단, 분모(총 문의 수) 집계는 이 함수가 아니라 별도 카운트 로직에서 item 단위로 1회만
# 센다 — 색상·사이즈 각각의 부정률은 독립 지표라 "둘 다 불평한 사람"은 양쪽에 다
# 들어가야 신호 손실이 없다(이상탐지 로직 [0] 집계 방식과 정합).


def explode_to_rows(item: ClassifiedItem) -> list[dict]:
    """ClassifiedItem → DB 저장용 행 리스트로 explode.

    반환된 dict는 그대로 DB INSERT에 쓸 수 있는 평평한(flat) 구조.
    실제 DB 스키마(테이블·컬럼)는 아직 미확정이라 dict로만 반환 — DB 레이어
    확정되면 이 함수 반환 타입을 ORM 모델로 바꾸면 됨(호출부 영향 최소화 목적).
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