"""담당: 서영 (Agent2) — [7] 확신도·권장조치 · [8] CS·리뷰 종합 테스트.

전부 순수 함수라 숫자·문자열만으로 검증한다. LLM 호출 없음 (비용 0).
근거: 이상탐지 로직 V3 §[7]·§5, 탐지 결과 스키마 §3.1·§3.2.
"""

import pytest

from app.core.schemas import DetectionConfidence, RecommendedAction, Source, Verdict
from app.detection.combine import (
    INTERPRETATION_BOTH,
    INTERPRETATION_CS_ONLY,
    INTERPRETATION_REVIEW_ONLY,
    combine_sources,
    pick_primary_source,
    source_signal,
)
from app.detection.confidence import decide_confidence, decide_recommended_action


# ── [7] 확신도 판정표 (로직 §[7]) ─────────────────────────────────
@pytest.mark.parametrize(
    "verdict,consistent,matched,expected",
    [
        (Verdict.BIASED, True, True, DetectionConfidence.HIGH),      # 편중+일관+시점일치
        (Verdict.BIASED, True, False, DetectionConfidence.MEDIUM),   # 편중+일관, 시점 미확인
        (Verdict.BIASED, False, False, DetectionConfidence.LOW),     # 편중이나 원인 분산
        (Verdict.BIASED, False, True, DetectionConfidence.LOW),      # 시점 일치해도 원인 없으면 낮음
        (Verdict.INDETERMINATE, None, False, DetectionConfidence.MEDIUM),      # 구분불가 고정
        (Verdict.GLOBAL, None, False, DetectionConfidence.NOT_APPLICABLE),
        (Verdict.TENTATIVE_GLOBAL, None, False, DetectionConfidence.NOT_APPLICABLE),
    ],
)
def test_decide_confidence_table(verdict, consistent, matched, expected):
    assert decide_confidence(verdict, consistent, timestamp_matched=matched) == expected


def test_indeterminate_ignores_timestamp():
    """구분불가는 [6]을 생략했으므로 시점이 맞아도 '중간' 고정 — 상향 근거가 없다."""
    assert decide_confidence(Verdict.INDETERMINATE, None, True) == DetectionConfidence.MEDIUM


def test_timestamp_never_rejects():
    """시점 이력은 보강용 — 없다고 기각(해당없음/None)되지 않는다. (로직 §[7])"""
    assert decide_confidence(Verdict.BIASED, True, False) == DetectionConfidence.MEDIUM


# ── [7] 권장조치 7종 (스키마 §3.2) ────────────────────────────────
@pytest.mark.parametrize(
    "verdict,aspect,consistent,expected",
    [
        (Verdict.BIASED, "색상", True, RecommendedAction.GENERATE_RECOMMENDATION),
        (Verdict.BIASED, "사이즈", True, RecommendedAction.GENERATE_RECOMMENDATION),
        (Verdict.BIASED, "소재", True, RecommendedAction.GENERATE_RECOMMENDATION),
        (Verdict.BIASED, "색상", False, RecommendedAction.CHANNEL_OPERATION_CHECK),
        (Verdict.BIASED, "파손", None, RecommendedAction.LOGISTICS_CHECK),
        (Verdict.BIASED, "오배송", None, RecommendedAction.OPERATION_CHECK),
        (Verdict.BIASED, "기타", None, RecommendedAction.OTHER_TYPE_CHECK),
        (Verdict.GLOBAL, "색상", None, RecommendedAction.PRODUCT_CHECK),
        (Verdict.TENTATIVE_GLOBAL, "파손", None, RecommendedAction.PRODUCT_CHECK),
        (Verdict.INDETERMINATE, "사이즈", None, RecommendedAction.SCOPE_UNDETERMINED),
    ],
)
def test_decide_recommended_action_table(verdict, aspect, consistent, expected):
    assert decide_recommended_action(verdict, aspect, consistent) == expected


def test_out_of_scope_aspect_never_generates_recommendation():
    """파손·오배송은 원인 후보 자체가 없어 개선안 경로로 못 간다."""
    for aspect in ("파손", "오배송", "기타"):
        action = decide_recommended_action(Verdict.BIASED, aspect, True)
        assert action != RecommendedAction.GENERATE_RECOMMENDATION


# scope_in 자체는 [5] 소관 — test_detection.py(값) · test_pipeline.py(verdict 무관성).


# ── [8] CS·리뷰 종합 (로직 §5, 스키마 §3.1) ───────────────────────
def _result(fired, verdict=Verdict.BIASED, confidence=DetectionConfidence.MEDIUM):
    return {"fired": fired, "verdict": verdict, "confidence": confidence}


def test_both_fired_upgrades_one_level():
    """양 소스 발화 = 강한 신호 → CS 확신도 1단계 상향. (편중형 한정)"""
    confidence, interpretation = combine_sources(
        _result(True, confidence=DetectionConfidence.MEDIUM), _result(True)
    )
    assert confidence == DetectionConfidence.HIGH
    assert interpretation == INTERPRETATION_BOTH


def test_upgrade_caps_at_high():
    confidence, _ = combine_sources(
        _result(True, confidence=DetectionConfidence.HIGH), _result(True)
    )
    assert confidence == DetectionConfidence.HIGH


def test_upgrade_from_low():
    confidence, _ = combine_sources(
        _result(True, confidence=DetectionConfidence.LOW), _result(True)
    )
    assert confidence == DetectionConfidence.MEDIUM


def test_no_upgrade_for_indeterminate():
    """구분불가도 확신도가 '중간'이라 confidence 만으로는 편중형과 구분 안 된다.

    verdict 를 봐야 상향 제외가 정확히 걸린다 (로직 §5 주의).
    """
    confidence, interpretation = combine_sources(
        _result(True, verdict=Verdict.INDETERMINATE, confidence=DetectionConfidence.MEDIUM),
        _result(True),
    )
    assert confidence == DetectionConfidence.MEDIUM   # 상향되지 않음
    assert interpretation == INTERPRETATION_BOTH


def test_no_upgrade_for_global():
    """전역형은 확신도가 '해당없음'이라 사다리에 없다 — 상향 시도 자체가 없어야 한다."""
    confidence, _ = combine_sources(
        _result(True, verdict=Verdict.GLOBAL, confidence=DetectionConfidence.NOT_APPLICABLE),
        _result(True),
    )
    assert confidence == DetectionConfidence.NOT_APPLICABLE


def test_cs_only():
    confidence, interpretation = combine_sources(
        _result(True, confidence=DetectionConfidence.LOW), _result(False)
    )
    assert confidence == DetectionConfidence.LOW
    assert interpretation == INTERPRETATION_CS_ONLY


def test_review_only():
    confidence, interpretation = combine_sources(
        None, _result(True, confidence=DetectionConfidence.MEDIUM)
    )
    assert confidence == DetectionConfidence.MEDIUM
    assert interpretation == INTERPRETATION_REVIEW_ONLY


def test_neither_fired_is_no_alert():
    assert combine_sources(_result(False), _result(False)) == (None, None)
    assert combine_sources(None, None) == (None, None)


def test_pick_primary_prefers_cs():
    """둘 다 발화하면 종합의 단일 진실은 CS. (스키마 §3.1)"""
    assert pick_primary_source(_result(True), _result(True)) == Source.CS
    assert pick_primary_source(_result(False), _result(True)) == Source.REVIEW
    assert pick_primary_source(None, None) is None


def test_source_signal_distinguishes_held_from_silent():
    """None(보류) 과 False(미발화)는 다른 정보다. (스키마 §3)"""
    assert source_signal(None) is None
    assert source_signal(_result(False)) is False
    assert source_signal(_result(True)) is True
