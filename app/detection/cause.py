"""담당: 서영 (Agent2) — [6] 원인 분류 (이상탐지 로직 V3 §[6]).

편중형 & 스코프 내(색상·사이즈·소재) 일 때만 수행한다. 편중 채널의 해당 aspect 부정
문의 텍스트를 프롬프트3으로 **사전 정의된 원인 후보로 분류**하고, 그 분포로 원인을 특정한다.

- judge_cause   : 라벨 분포 → 주원인·일관 여부 (순수 함수)
- classify_cause: 문의 배치 → LLM(프롬프트3) 분류 결과 (async, llm_client 경유)
- diagnose_cause: 위 둘을 엮은 [6] 진입점 (classify → aspect_match 필터 → judge)

judge_cause 는 순수라 숫자만으로 테스트하고, classify_cause 는 LLM 을 목킹해 테스트한다.
"""

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from string import Template
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.config import get_settings
from app.core.constants import CONSISTENT_COUNT, CONSISTENT_RATIO
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt

logger = logging.getLogger(__name__)

CAUSE_TAXONOMY: dict[str, frozenset[str]] = {
    "색상": frozenset({"사진_색감_오차", "조명_보정_차이", "실물_염색_편차", "기타"}),
    "사이즈": frozenset(
        {"표기_오타", "실측_표기_편차", "채널_사이즈_표준차이", "기타"}
    ),
    "소재": frozenset(
        {"소재_정보_누락", "이미지_질감표현_부족", "실제_원단_문제", "기타"}
    ),
}
"""프롬프트3의 aspect별 허용 원인 라벨. 출력 검증의 단일 허용 목록."""

CAUSE_CHUNK_SIZE = 20
"""한 LLM 호출에 넣는 최대 문의 수. 검증된 평가 배치 크기와 같은 값."""

CAUSE_MAX_PROMPT_CHARS = 24_000
"""렌더링된 요청의 문자 상한. tokenizer 의존성 없이 요청 크기를 보수적으로 제한한다."""


class CauseValidationError(ValueError):
    """원인 분류 입출력이 프롬프트3 계약을 어겼을 때."""


@dataclass
class CauseValidationSummary:
    """스키마는 유효하지만 근거·taxonomy 검증에서 제외된 항목의 집계."""

    invalid_items: int = 0
    invalid_matching_items: int = 0
    invalid_matching_ids: list[str] = field(default_factory=list)
    reasons: Counter[str] = field(default_factory=Counter)

    def reject(self, cs_id: str, reasons: list[str], *, aspect_match: bool) -> None:
        self.invalid_items += 1
        self.invalid_matching_items += int(aspect_match)
        if aspect_match:
            self.invalid_matching_ids.append(cs_id)
        self.reasons.update(reasons)


class CauseResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cs_id: str = Field(min_length=1)
    cause: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: str
    aspect_match: bool


class CauseResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results: list[CauseResult]


def _aspect_value(aspect: Any) -> str:
    return str(getattr(aspect, "value", aspect))


def _render_prompt(template: Template, aspect: str, items: list[dict]) -> str:
    input_json = json.dumps({"aspect": aspect, "items": items}, ensure_ascii=False)
    return template.substitute(input_json=input_json)


def _chunk_cause_items(
    template: Template, aspect: str, items: list[dict]
) -> list[list[dict]]:
    """건수와 렌더링 문자 수를 모두 지키며 입력 순서를 보존해 나눈다."""
    chunks: list[list[dict]] = []
    current: list[dict] = []

    for item in items:
        candidate = [*current, item]
        too_many = len(candidate) > CAUSE_CHUNK_SIZE
        too_large = (
            len(_render_prompt(template, aspect, candidate)) > CAUSE_MAX_PROMPT_CHARS
        )
        if current and (too_many or too_large):
            chunks.append(current)
            current = [item]
        else:
            current = candidate

        if len(_render_prompt(template, aspect, current)) > CAUSE_MAX_PROMPT_CHARS:
            raise CauseValidationError(
                f"원인 분류 문의 1건이 요청 크기 상한을 초과했습니다: "
                f"cs_id={item['cs_id']} limit={CAUSE_MAX_PROMPT_CHARS}chars"
            )

    if current:
        chunks.append(current)
    return chunks


def _validate_items(aspect: str, items: list) -> list[dict]:
    if aspect not in CAUSE_TAXONOMY:
        raise CauseValidationError(f"원인 분류를 지원하지 않는 aspect입니다: {aspect}")

    normalized: list[dict] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise CauseValidationError(f"items[{index}]가 객체가 아닙니다")
        cs_id = item.get("cs_id")
        raw_text = item.get("raw_text")
        if not isinstance(cs_id, str) or not cs_id:
            raise CauseValidationError(f"items[{index}].cs_id가 비어있습니다")
        if not isinstance(raw_text, str):
            raise CauseValidationError(f"items[{index}].raw_text가 문자열이 아닙니다")
        normalized.append({"cs_id": cs_id, "raw_text": raw_text})

    ids = [item["cs_id"] for item in normalized]
    if len(ids) != len(set(ids)):
        raise CauseValidationError("원인 분류 입력 cs_id가 중복됐습니다")
    return normalized


def _validate_response(
    aspect: str,
    items: list[dict],
    data: Any,
    *,
    validation_summary: CauseValidationSummary,
) -> list[dict]:
    try:
        response = CauseResponse.model_validate(data)
    except ValidationError as exc:
        raise CauseValidationError(f"원인 분류 응답 스키마 오류: {exc}") from exc

    expected_ids = [item["cs_id"] for item in items]
    actual_ids = [result.cs_id for result in response.results]
    if actual_ids != expected_ids:
        raise CauseValidationError(
            "원인 분류 응답 ID가 입력과 같은 개수·순서가 아닙니다: "
            f"expected={expected_ids} actual={actual_ids}"
        )

    raw_by_id = {item["cs_id"]: item["raw_text"] for item in items}
    allowed = CAUSE_TAXONOMY[aspect]
    valid: list[dict] = []
    for result in response.results:
        reasons: list[str] = []
        if result.cause not in allowed:
            reasons.append("taxonomy_mismatch")
        if result.aspect_match and not result.evidence:
            reasons.append("empty_evidence")
        if result.evidence and result.evidence not in raw_by_id[result.cs_id]:
            reasons.append("evidence_not_in_source")

        if reasons:
            validation_summary.reject(
                result.cs_id,
                reasons,
                aspect_match=result.aspect_match,
            )
            logger.warning(
                "cause_item_rejected aspect=%s cs_id=%s reasons=%s",
                aspect,
                result.cs_id,
                ",".join(reasons),
            )
            continue
        valid.append(result.model_dump())

    return valid


def judge_cause(classified_causes: list, *, total_count: int | None = None) -> tuple:
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
    denominator = len(classified_causes) if total_count is None else total_count
    if denominator < len(classified_causes):
        raise ValueError("total_count는 분류된 원인 수보다 작을 수 없습니다")
    ratio = top_count / denominator
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
    validation_summary: CauseValidationSummary | None = None,
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
        client = get_llm_client(model=get_settings().cause_llm_model)

    aspect = _aspect_value(aspect)
    items = _validate_items(aspect, items)
    validation_summary = validation_summary or CauseValidationSummary()
    template = Template(load_prompt("detection", "classify_cause_v1"))
    chunks = _chunk_cause_items(template, aspect, items)

    results: list[dict] = []
    for index, chunk in enumerate(chunks, start=1):
        prompt = _render_prompt(template, aspect, chunk)
        chunk_trace = f"{trace_key} chunk={index}/{len(chunks)}"
        data = await client.complete_json(prompt, trace_key=chunk_trace)
        results.extend(
            _validate_response(
                aspect,
                chunk,
                data,
                validation_summary=validation_summary,
            )
        )
    return results


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
    validation = CauseValidationSummary()
    results = await classify_cause(
        aspect,
        items,
        client=client,
        trace_key=trace_key,
        validation_summary=validation,
    )
    kept = [r for r in results if r.get("aspect_match", True)]
    causes = [r["cause"] for r in kept]

    attempted_total = len(causes) + validation.invalid_matching_items
    label, consistent, freq = judge_cause(causes, total_count=attempted_total)
    included_ids = {
        *(r["cs_id"] for r in kept if r.get("cs_id")),
        *validation.invalid_matching_ids,
    }
    return {
        "label": label,
        "consistent": consistent,
        "count": freq.get(label, 0),  # label 이 None 이면 자연히 0
        "total": attempted_total,
        "freq": freq,
        "cs_ids": [item["cs_id"] for item in items if item.get("cs_id") in included_ids],
        "attempted_total": attempted_total,
        "invalid_count": validation.invalid_items,
        "invalid_reasons": dict(validation.reasons),
    }
