"""담당: 현진 (Agent1) — 분류 로직.

성격: LLM 워크플로우(단일 패스, 분기 없음) → 프레임워크 불필요, 순수 Python.
정본: 분류 워커 명세 §2(explode 저장 규약), docs/schemas.md §4(ClassifiedItem)
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
    """load_prompt()로 파일 전체를 읽되, "## System Prompt" 이전(파일 헤더 — 담당자·변경이력
    등 문서용 메타정보)은 잘라내고 그 이후만 반환한다.

    프롬프트 파일 헤더에는 오류분석 과정에서 평가셋 문항 원문을 인용하는 경우가 있는데
    (예: v4 변경이력에 파일럿 오답 문항 언급), 파일 전체를 그대로 LLM에 보내면 그 인용문까지
    같이 전송돼 ①불필요한 토큰 낭비 ②향후 같은 문항 재평가 시 시험지 유출 위험이 생긴다.
    "## System Prompt" 이후만 잘라 보내면 헤더는 순수 문서로만 남고 이 위험이 원천 차단된다.
    "## System Prompt" 구분선이 없는 파일은(과거 버전 호환) 안전하게 전체를 그대로 반환한다.
    """
    raw = load_prompt(module, name)
    m = re.search(r"## System Prompt\s*\n(.*)", raw, re.DOTALL)
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
PROMPT_ASPECT_VERSION = "classify_aspect_v5"        # 프롬프트1(CS) — 실험③ 부정판별 F1 98.3% · FPR 0.64%(2026-08-11 전량 96,524건)
PROMPT_SENTIMENT_VERSION = "classify_sentiment_v4"  # 프롬프트2(리뷰) — 실험④ aspect F1 84.4%(71603 n=300, 1회)


async def classify_aspect(
    items: list[ClassifyRequestItem],
) -> list[ClassifiedItem | Exception]:
    """원문 여러 건 → ClassifiedItem 또는 Exception 리스트(요청과 순서·길이 동일).

    ⚠️ 계약(서영님↔현진 합의, 2026-08-04 — 서영님 안전망 커밋 미push 상태라
       이번 구현에 같이 포함시킴):
    1. len(반환) == len(items), 순서 동일 → zip(items, 반환)이 성립한다.
    2. 성공은 ClassifiedItem, 실패는 예외 객체를 그 자리에 담아 반환한다(raise 안 함).
    3. 실패 종류는 LlmCallError(호출 실패) 또는 LlmParseError(파싱 실패·검증 실패)만
       사용한다 — 그 외 예외(예: ClassifiedItem 생성 시 pydantic ValidationError)는
       _classify_one()이 LlmParseError로 감싸서 재던진다.
       ⚠️ 예외: get_llm_client()는 이 규칙 밖(PR 리뷰 지적, 2026-08-04) — API 키
       누락 같은 프로세스 전역 설정 오류를 item별 LlmParseError로 위장하면 시스템
       장애가 "N건 요청 중 N건 개별 실패"처럼 부분 실패로 보인다. 그래서 이 함수
       호출은 의도적으로 감싸지 않고 원래 예외 타입 그대로 전파한다(같은 배치의
       나머지 item들도 보통 동일한 원인으로 같이 실패하므로, 여러 슬롯에 같은
       비-LlmParseError 타입이 반복되면 호출부가 "아, 이건 개별 실패가 아니라
       시스템 문제구나"라고 알아챌 신호가 된다).
    4. 전부 실패해도 이 함수 자체는 raise하지 않는다(gather의 return_exceptions=True).
    5. items == [] → [] (LLM 호출 0).

    호출부(워커·router.py·eval 스크립트)는 zip(items, results)로 순회하며
    isinstance(r, Exception)으로 개별 실패를 판별할 것.

    ⚠️ "배치"의 의미: 프롬프트 자체는 1건씩 처리하도록 이미 검증됐음(파일럿 42건
    평가 통과, 실험③에서 진짜 배치도 정확도 손실 없이 검증됨) — 다만 이 함수는
    아직 "진짜 배치"(N건을 한 프롬프트에 담아 LLM 호출 1회)는 미구현 상태이고,
    지금은 asyncio.gather()로 여러 건을 동시에(병렬) 호출해 배치 처리 효과만 냄.
    분류 워커 명세 §1 "LLM 배치 분류 추론" 단계에 대응.
    """
    if not items:
        return []
    tasks = [_classify_one(item) for item in items]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def _classify_one(item: ClassifyRequestItem) -> ClassifiedItem:
    """원문 1건 → ClassifiedItem. source에 따라 프롬프트1(CS)/프롬프트2(리뷰) 자동 분기.

    이 함수가 던지는 예외는 LlmCallError 또는 LlmParseError로 통일한다(계약 3번).
    ClassifiedItem 생성 시 pydantic이 던지는 ValidationError(예: source=='review'인데
    aspect에 파손처럼 REVIEW_ALLOWED_ASPECTS 밖 값이 섞인 경우)를 여기서 잡아
    LlmParseError로 감싸지 않으면, classify_aspect()의 계약 3번이 깨지고 호출부의
    "LlmCallError/LlmParseError 두 가지만 잡으면 된다"는 전제도 함께 깨진다.
    """
    item_id = item.item_id
    source: Source = item.source
    trace_key = f"item_id={item_id}"  # 컨벤션 4장: 로그에 추적 키 항상 포함

    # ⚠️ get_llm_client()는 try 밖(PR 리뷰 지적, 2026-08-04) — API 키 누락 같은
    # "프로세스 전역 설정 오류"를 item별 LlmParseError로 감싸면, 시스템 장애가
    # "300건 요청 중 300건 개별 실패"처럼 부분 실패로 위장된다(200 OK + errors
    # 300건). Template.substitute·load_llm_prompt만 진짜 item 처리 단계이므로
    # 그 둘만 감싼다.
    client = get_llm_client()

    try:
        if source == Source.CS:
            template = Template(load_llm_prompt("classification", PROMPT_ASPECT_VERSION))
            # ⚠️ str.format() 대신 Template — JSON 예시의 중괄호와 충돌 방지(core/prompts.py 경고).
            # 원문은 json.dumps로 감싸서 따옴표·줄바꿈이 프롬프트 구조를 깨지 않게 함.
            prompt = template.substitute(cs_text=json.dumps(item.raw_text, ensure_ascii=False))
        else:
            template = Template(load_llm_prompt("classification", PROMPT_SENTIMENT_VERSION))
            prompt = template.substitute(review_text=json.dumps(item.raw_text, ensure_ascii=False))
    except Exception as exc:
        # 프롬프트 파일 누락(FileNotFoundError)·플레이스홀더 불일치(KeyError) 등
        # LLM 호출 전 셋업 단계의 예상 밖 오류도 계약 3번대로 LlmParseError로 통일.
        # 이게 없으면 호출부의 "LlmCallError/LlmParseError만 잡으면 된다"는 전제가 깨진다.
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
    """LLM JSON 응답({"aspects": [...]})을 AspectSentiment 리스트로 변환.

    프롬프트1(CS)은 mixed_signal을 안 냄 → None 고정(스키마 규칙: CS는 mixed_signal=null).
    프롬프트2(리뷰)는 mixed_signal을 냄 → 그대로 반영.
    aspect·sentiment 값이 enum 범위 밖이면(LLM 환각) LlmParseError로 통일해서 던짐 —
    호출부가 LlmCallError/LlmParseError 두 가지만 잡으면 되게.

    ⚠️ CS 안전망(서영님↔현진 합의, 2026-08-04, 이유 서술 재검토 2026-08-04): 프롬프트1은
    "CS는 반드시 6개 중 하나 이상"이라고 명시하는데도 LLM이 가끔 빈 배열을 내는 경우가
    관측됨(예: "궁금한 점 빠르게 답변해주셔서 감사합니다!" 류 — 제품과 무관한 순수 CS
    응대 감사, 300건 중 6건 관측).
    detection/aggregate.py의 분모 집계(§129)는 normalize()가 ClassifiedItem 1건당
    행 1개를 만든 뒤 aspect 내용과 무관하게 무조건 +=1 하는 구조라(aggregate.py:58),
    ClassifiedItem이 일단 만들어지기만 하면 aspects가 비어있든 기타/중립으로
    채워지든 분모·분자 계산은 완전히 동일하다(둘 다 분모+1, 분자+0 — 탐지 산식
    입장에선 no-op). 즉 "빈 배열을 그대로 둔다"는 선택지 자체는 미탐을 유발하지
    않는다.
    진짜 위험은 반대 경우다 — 여기서 LlmParseError로 던지면 ClassifiedItem 자체가
    안 만들어져 normalize()가 그 문의의 행을 아예 못 만든다. 그러면 그 문의가
    분모에서 통째로 빠져(원본 문서 기준 총 문의 수보다 적게 잡힘), 부정률이 실제보다
    높게 계산된다(오탐 방향). dead-letter로 보내도 "커버리지 구멍이 이동"할 뿐 이
    오탐 위험은 그대로 남는다. 그래서 raise 대신 기타/중립으로 채워 ClassifiedItem을
    확실히 만들어내는 쪽을 택한다 — 어느 aspect의 분자도 안 늘리는 가장 보수적인
    값이면서, 동시에 분모에서 빠지는 오탐 경로를 막는다. 얼마나 자주 발생하는지는
    logger.warning으로 남겨 프롬프트 회귀 신호로도 쓴다.
    REVIEW는 원래도 무관 리뷰에 대해 빈 배열이 정상 응답이라 이 로직에서 제외.
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
    """CS 응답이 빈 배열일 때 '기타/중립' 1건으로 채운다. (2026-08-04 현진·서영 합의)

    프롬프트1은 "CS는 반드시 6개 중 하나 이상"을 지시하지만 LLM이 지키지 않는 경우가
    있다(실측 284건 중 6건, 2.1%). 리뷰는 빈 배열이 정상 출력이라 **CS 전용**이다.

    ⚠️ 집계 산식만 보면 이 폴백은 no-op 이다(현진 정정, 2026-08-04). aggregate 의 분모는
    ClassifiedItem 1건당 행 1개를 만든 뒤 aspect 내용과 무관하게 +1 하므로(§129),
    빈 배열이든 기타/중립이든 분모+1·분자+0 으로 완전히 같다. 그러니 "빈 배열을 그냥
    둔다"가 그 자체로 부정률을 왜곡하지는 않는다.

    폴백이 실제로 막는 것은 둘이다:
      ① detection.loader.check_coverage 가 "aspect 1개 이상"을 분류 성공의 기준으로
         삼는다. 빈 배열이 남으면 그 (상품,채널,source) 슬롯이 커버리지 미달로 잡혀
         **검정 자체에서 통째로 빠진다** — 실측으로 P019 의 CS 슬롯 2개가 사라졌고,
         폴백 적용 후 0개가 됐다. 미탐이 아니라 '판정 자체를 안 함'이 된다.
      ② 여기서 LlmParseError 로 던지는 대안은 더 나쁘다. ClassifiedItem 이 아예 안
         만들어져 그 문의가 분모에서 빠지고, 부정률이 실제보다 **높게** 계산된다
         (오탐 방향). 워커가 dead-letter 로 보내도 커버리지 구멍이 이동할 뿐이다.

    '기타/중립'은 분모에 남기되 어느 aspect의 분자도 늘리지 않아 가장 보수적이다.

    ⚠️ 조용히 채우지 않는다. 이게 얼마나 자주 도는지가 프롬프트 개선의 측정 대상이고,
       빈도가 오르면 프롬프트 회귀 신호다.
    ⚠️ 이 함수를 인라인으로 되돌리지 말 것 — eval/run_pipeline_eval.py 의 배치 경로가
       _parse_llm_response 를 우회하므로 이 함수를 직접 불러 같은 규칙을 적용한다.
       인라인으로 두면 규칙이 두 벌이 되어 갈라진다.
    """
    logger.warning(
        "cs_empty_aspects [%s] LLM이 빈 배열 반환 → %s/%d 로 대체",
        trace_key,
        Aspect.ETC.value,
        Sentiment.NEUTRAL.value,
    )
    return [AspectSentiment(aspect=Aspect.ETC, sentiment=Sentiment.NEUTRAL, mixed_signal=None)]


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