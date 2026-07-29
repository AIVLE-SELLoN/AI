"""담당: 서영 (Agent2) — [6] 원인 분류 (이상탐지 로직 V3 §[6]).

편중형 & 스코프 내(색상·사이즈·소재) 일 때만 수행한다. 편중 채널의 해당 aspect 부정
문의 텍스트를 프롬프트3으로 **사전 정의된 원인 후보로 분류**하고, 그 분포로 원인을 특정한다.

- judge_cause   : 라벨 분포 → 주원인·일관 여부 (순수 함수)
- classify_cause: 문의 배치 → LLM(프롬프트3) 분류 결과 (async, llm_client 경유)
- diagnose_cause: 위 둘을 엮은 [6] 진입점 (classify → aspect_match 필터 → judge)

judge_cause 는 순수라 숫자만으로 테스트하고, classify_cause 는 LLM 을 목킹해 테스트한다.
"""

import json
from collections import Counter
from string import Template
from typing import Any

from app.core.constants import CONSISTENT_COUNT, CONSISTENT_RATIO
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt


def judge_cause(classified_causes: list) -> tuple:
    """[6] 분류된 원인 라벨 분포로 주원인을 특정한다. (로직 §[6])

    Args:
        classified_causes: ["사진_색감_오차", "사진_색감_오차", "조명_보정_차이", ...]
            문의 1건당 원인 1개.

    Returns:
        (주원인 또는 None, 일관 여부, 빈도표)
          - 최다 원인이 CONSISTENT_RATIO 이상 AND CONSISTENT_COUNT 이상이면 '일관'
          - 미달이면 원인이 흩어진 것 → 특정하지 않음(None) → [7] 확신도 낮음 경로
    """
    freq = Counter(classified_causes)
    if not freq:
        return None, False, {}

    top_cause, top_count = freq.most_common(1)[0]
    ratio = top_count / len(classified_causes)
    is_consistent = (ratio >= CONSISTENT_RATIO) and (top_count >= CONSISTENT_COUNT)

    if is_consistent:
        return top_cause, True, dict(freq)
    return None, False, dict(freq)


async def classify_cause(
    aspect: str,
    items: list,
    *,
    client: Any = None,
    trace_key: str = "-",
) -> list:
    """[6] 문의 배치를 프롬프트3으로 원인 분류한다 (LLM, 배치 1회 호출).

    Args:
        aspect: 배치 공통 aspect (색상/사이즈/소재).
        items: [{"cs_id": ..., "raw_text": ...}, ...]  — 편중 채널의 해당 aspect 부정 문의.
        client: LlmClient (테스트 목킹용 주입). 없으면 전역 클라이언트.

    Returns:
        프롬프트3 results 배열: [{"cs_id","cause","confidence","evidence","aspect_match"}, ...]
        items 가 비면 LLM 을 호출하지 않고 [] 반환(비용 0).
    """
    if not items:
        return []
    if client is None:
        client = get_llm_client()

    template = Template(load_prompt("detection", "classify_cause_v1"))
    input_json = json.dumps({"aspect": aspect, "items": items}, ensure_ascii=False)
    prompt = template.substitute(input_json=input_json)

    data = await client.complete_json(prompt, trace_key=trace_key)
    results = data.get("results", [])
    return results if isinstance(results, list) else []


async def diagnose_cause(
    aspect: str,
    items: list,
    *,
    client: Any = None,
    trace_key: str = "-",
) -> dict:
    """[6] 진입점 — 배치 분류 후 원인을 특정한다.

    aspect_match=false(다른 aspect 로 오라우팅된 문의)는 이 aspect 원인 집계에서 제외한다.

    Returns:
        {"label", "consistent", "count", "total", "freq", "cs_ids"}
          - label:      주원인(일관 미달 시 None)
          - consistent: 원인 일관 여부
          - count:      주원인 건수 (label 없으면 0)
          - total:      집계에 쓴 문의 수(aspect_match 통과분)
          - freq:       원인별 빈도표 ("20건 중 14건…" 리포트용)
          - cs_ids:     집계에 쓴 문의 ID (total 과 같은 집합)

    cs_ids 를 따로 돌려주는 이유: 이게 그대로 alert.evidence.inquiry_ids 가 되고,
    스키마 §3 이 그 필드를 "원인분류 투입 문의 전체(= root_cause.total 건)"로 정의한다.
    aspect_match=false 로 걷어낸 문의를 인용 경계에 남기면 개수가 total 과 어긋나고,
    **Agent3 가 '다른 aspect 불만'을 근거로 인용할 수 있게 된다.**
    """
    results = await classify_cause(aspect, items, client=client, trace_key=trace_key)
    kept = [r for r in results if r.get("aspect_match", True)]
    causes = [r["cause"] for r in kept]

    label, consistent, freq = judge_cause(causes)
    return {
        "label": label,
        "consistent": consistent,
        "count": freq.get(label, 0),  # label 이 None 이면 자연히 0
        "total": len(causes),
        "freq": freq,
        "cs_ids": [r["cs_id"] for r in kept if r.get("cs_id")],
    }