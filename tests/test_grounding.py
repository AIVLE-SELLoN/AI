"""담당: 지인 — 인용 검증(grounding) 테스트."""

import pytest

from app.core.exceptions import EvidenceNotFoundError
from app.recommendation.grounding import has_evidence, verify_grounding


def test_exact_substring_after_normalization_passes():
    """공백·문장부호만 다르면(정규화 후 부분일치) 근거로 인정."""
    quote = "아이보리 컬러"
    source_text = "상세설명: 아이보리 컬러 원피스입니다."

    assert has_evidence(quote, source_text) is True


def test_near_paraphrase_within_similarity_threshold_passes():
    """어미 등 일부만 달라 완전일치는 실패해도, 유사도 임계값을 넘으면 통과."""
    quote = "색상 표기 오류있음"
    source_text = "고객 문의: 색상 표기 오류있었음"

    assert has_evidence(quote, source_text) is True


def test_paraphrase_below_similarity_threshold_fails():
    """겹치는 부분이 임계값 미만이면 근거 없음으로 판정."""
    quote = "소재 안내가 부족합니다"
    source_text = "상세설명에는 소재 안내가 부족해요"

    assert has_evidence(quote, source_text) is False


def test_unrelated_text_fails():
    quote = "완전히 다른 문장입니다"
    source_text = "상세설명: 아이보리 컬러 원피스입니다."

    assert has_evidence(quote, source_text) is False


@pytest.mark.parametrize("quote,source_text", [("", "아무 내용"), ("아이보리 컬러", "")])
def test_empty_quote_or_source_fails(quote, source_text):
    assert has_evidence(quote, source_text) is False


def test_verify_grounding_raises_when_no_evidence():
    with pytest.raises(EvidenceNotFoundError):
        verify_grounding("완전히 다른 문장입니다", "상세설명: 아이보리 컬러 원피스입니다.")


def test_verify_grounding_passes_silently_when_evidence_found():
    verify_grounding("아이보리 컬러", "상세설명: 아이보리 컬러 원피스입니다.")
