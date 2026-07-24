"""공용 pytest 픽스처. pytest가 자동 수집하므로 각 tests/test_*.py에서 import 불필요."""

import pytest

from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    RecommendedAction,
    RootCause,
    SourceSignals,
    Verdict,
)


@pytest.fixture
def biased_alert() -> DetectionAlert:
    """편중형 — 색상 + 원인 명확 → 개선안 생성 트리거."""
    return DetectionAlert(
        alert_id="ALT-20260528-0001",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P001",
        channel="COUPANG",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.BIASED,
        significant_channels=["COUPANG"],
        main_aspect="색상",
        sub_aspects=[
            {"aspect": "파손", "delta": 0.07, "recommended_action": "물류 점검 권장"}
        ],
        stats=DetectionStats(
            source="cs",
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=0.00013,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(
            cs=True, review=False, interpretation="CS 선행 신호 — 리뷰는 시차로 미반영 가능"
        ),
        root_cause=RootCause(label="사진_색감_오차", count=14, total=20, consistent=True),
        detection_confidence=DetectionConfidence.HIGH,
        scope_in=True,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        evidence=Evidence(inquiry_ids=["INQ-000412", "INQ-000415"], linked_change_id="CHG-0009"),
    )


@pytest.fixture
def global_alert() -> DetectionAlert:
    """전역형 — 상품 1건, channel=ALL, root_cause 없음."""
    return DetectionAlert(
        alert_id="ALT-20260528-0002",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P002",
        channel="ALL",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.GLOBAL,
        significant_channels=["COUPANG", "NAVER", "ZIGZAG"],
        main_aspect="파손",
        stats=DetectionStats(
            source="cs",
            cur_rate=0.20,
            past_rate=0.06,
            delta=0.14,
            p_value=0.00002,
            bh_significant=True,
            cur_total=180,
        ),
        source_signals=SourceSignals(cs=True, review=True, interpretation="강한 신호(양 소스)"),
        root_cause=None,
        detection_confidence=DetectionConfidence.NOT_APPLICABLE,
        scope_in=False,
        recommended_action=RecommendedAction.PRODUCT_CHECK,
        evidence=Evidence(inquiry_ids=["INQ-000501", "INQ-000502"]),
    )


@pytest.fixture
def indeterminate_alert() -> DetectionAlert:
    """구분불가 — 채널 표본 부족으로 편중/전역 판정 불가."""
    return DetectionAlert(
        alert_id="ALT-20260528-0003",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P003",
        channel="COUPANG",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.INDETERMINATE,
        significant_channels=["COUPANG"],
        excluded_channels=["NAVER", "ZIGZAG"],
        main_aspect="사이즈",
        stats=DetectionStats(
            source="cs",
            cur_rate=0.15,
            past_rate=0.06,
            delta=0.09,
            p_value=0.0009,
            bh_significant=True,
            cur_total=40,
        ),
        source_signals=SourceSignals(
            cs=True, review=None, interpretation="CS 선행 신호 — 리뷰는 시차로 미반영 가능"
        ),
        root_cause=None,
        detection_confidence=DetectionConfidence.MEDIUM,
        scope_in=True,
        recommended_action=RecommendedAction.SCOPE_UNDETERMINED,
        evidence=Evidence(inquiry_ids=["INQ-000601"]),
    )
