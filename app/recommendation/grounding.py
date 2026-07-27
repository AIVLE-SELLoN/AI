"""담당: 지인 (Agent3) — 인용 검증.

LLM 이 만들어낸 개선안이 "상세페이지(또는 CS)에 실제로 있는 내용"을 근거로
삼았는지 확인한다. 환각을 여기서 거른다.

grounding 소스는 도구별로 분리(agent3_logic.md §4-3):
  - copy_draft   → proposal.current_text 를 상세페이지 detail_text 와 대조
  - image_guide  → proposal.current_text 를 CS citations 원문과 대조
    (image_guide 는 이미지를 참조하지 않고 CS 문의 기반 요약이므로,
     상세페이지로 검증하면 항상 실패한다 — 반드시 CS 원문과 비교할 것)
"""

from __future__ import annotations

import difflib
import re

from app.core.constants import GROUNDING_SIMILARITY_THRESHOLD
from app.core.exceptions import EvidenceNotFoundError


def _normalize(text: str) -> str:
    """공백·구두점 차이로 인한 오탐을 막기 위한 비교 전 정규화."""
    return re.sub(r"[\s\W]+", "", text).lower()


def _longest_match_ratio(quote: str, source_text: str) -> float:
    """quote 문자 중 source_text 안에서 연속으로 이어지는 최장 구간의 비율."""
    matcher = difflib.SequenceMatcher(None, quote, source_text)
    match = matcher.find_longest_match(0, len(quote), 0, len(source_text))
    return match.size / len(quote)


def has_evidence(quote: str, source_text: str) -> bool:
    """quote가 source_text에 실제로 있는지 확인.

    완전일치로만 판정하면 LLM이 조사 하나만 바꿔도 실패하고, 유사도만 느슨하게
    쓰면 환각을 통과시킨다. 그래서 1) 정규화 후 부분일치를 먼저 보고,
    2) 실패하면 최장 연속 일치 구간 비율을 임계값(constants.GROUNDING_SIMILARITY_THRESHOLD)과
    비교해 완화한다.

    모듈 간 재사용 지점 — verify_grounding()의 grounding 판정뿐 아니라 pipeline.py의 consistency
    체크(rationale이 원인 라벨을 실제로 언급하는지)에서도 같은 정규화+부분일치 로직을
    재사용한다.
    """
    if not quote or not source_text:
        return False

    normalized_quote = _normalize(quote)
    normalized_source = _normalize(source_text)

    if not normalized_quote:
        return False

    if normalized_quote in normalized_source:
        return True

    return _longest_match_ratio(normalized_quote, normalized_source) >= GROUNDING_SIMILARITY_THRESHOLD


def verify_grounding(quote: str, source_text: str) -> None:
    """근거 검증. 없으면 EvidenceNotFoundError — 호출부가 근거없음 경로로 분기."""
    if not has_evidence(quote, source_text):
        raise EvidenceNotFoundError(f"근거를 원문에서 찾을 수 없습니다: {quote!r}")
