"""출력 검증 공통 유틸 — 문서 생성 스키마 §4-4 "그라운딩" 계층.

두 검증기(monthly_report_validator·cs_reply_validator)가 같은 규칙을 쓰므로 여기 모았다.

검사하는 것은 두 가지다.
  1. **수치 팩트체크** — LLM 이 쓴 문장 속 `13%` / `8%p` / `450건` / `0.54` 가
     입력 데이터에 실제로 있는 수치인가. 환각 수치를 잡는 게 목적이다.
  2. **금지 표현** — `p-value` / `FDR` 같은 통계 용어가 셀러용 문서에 새어나갔는가.

⚠️ 검사 대상은 §4-4 가 지정한 필드에만 적용한다. `standard_guideline.*` 처럼
   정책 상수(무상 교환 기간 등)가 들어가는 필드는 제외 대상이라 여기 넘기지 않는다 —
   넘기면 입력에 없는 숫자라고 전부 반려된다.
"""

from __future__ import annotations

import re

from app.core import constants

# `13%`, `8%p`, `450건`, `1,200건`, `0.54` 를 잡는다. 단위 없는 정수(`3개`, `2번`)는
# 오탐이 너무 많아 일부러 제외하고, 단위 없는 **소수**만 점수로 본다.
#
# ⚠️ 천단위 콤마를 반드시 받아야 한다. 없으면 `1,200건` 이 `200건` 으로 잘려
#    맞는 문장을 반려하고(1200 이 허용집합인데 200 을 비교) 틀린 문장을 통과시킨다
#    (허용집합에 200 이 있으면 그대로 승인). 표지 템플릿도 콤마 표기를 쓴다.
_NUMBER_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)\s*(%p|%|건|)")

UNIT_PERCENT = "%"
UNIT_PERCENT_POINT = "%p"
UNIT_COUNT = "건"
UNIT_SCORE = ""  # 단위 없는 소수 = JSD 점수 등


def extract_metric_numbers(text: str) -> list[tuple[float, str]]:
    """문장에서 (수치, 단위) 쌍을 뽑는다. 단위 없는 정수는 버린다."""
    found: list[tuple[float, str]] = []
    for raw_value, unit in _NUMBER_PATTERN.findall(text):
        if unit == UNIT_SCORE and "." not in raw_value:
            continue  # 단위 없는 정수는 팩트체크 대상 아님(순번·개수 표기 오탐 방지)
        found.append((float(raw_value.replace(",", "")), unit))
    return found


def check_numbers_grounded(
    text: str,
    allowed: dict[str, set[float]],
    *,
    field_name: str,
) -> list[str]:
    """text 의 수치가 전부 allowed 안에 있는지 확인하고, 아니면 반려 사유를 돌려준다.

    allowed 는 단위별 허용 수치 집합이다(예: {"%": {13.0, 5.0}, "건": {200.0}}).
    허용 오차는 %/건이 FACTCHECK_NUMBER_TOLERANCE, 점수가 FACTCHECK_SCORE_TOLERANCE.
    """
    errors: list[str] = []

    for value, unit in extract_metric_numbers(text):
        candidates = allowed.get(unit, set())
        tolerance = (
            constants.FACTCHECK_SCORE_TOLERANCE
            if unit == UNIT_SCORE
            else constants.FACTCHECK_NUMBER_TOLERANCE
        )
        if not any(abs(value - c) <= tolerance for c in candidates):
            errors.append(
                f"수치 팩트체크 실패({field_name}): '{value:g}{unit}' 는 입력 데이터에 없는 수치입니다."
            )
    return errors


def find_forbidden_expressions(text: str, *, field_name: str) -> list[str]:
    """금지 표현(p값·FDR 등)이 섞였는지 확인한다(§4-4)."""
    errors: list[str] = []
    lowered = text.lower()

    for expression in constants.FORBIDDEN_METRIC_EXPRESSIONS:
        if expression.lower() in lowered:
            errors.append(f"금지 표현 노출({field_name}): '{expression}'")

    if re.search(constants.FORBIDDEN_P_VALUE_PATTERN, text, flags=re.IGNORECASE):
        errors.append(f"금지 표현 노출({field_name}): p값 수치 표기")

    return errors


def ratio_to_percent(ratio: float) -> float:
    """비율(0.13) → 퍼센트 표기값(13.0). 팩트체크 허용 집합을 만들 때 쓴다."""
    return round(ratio * 100, 4)
