"""공용 pytest 픽스처. pytest가 자동 수집하므로 각 tests/test_*.py에서 import 불필요."""

import os

# pipeline.py의 @traceable은 LLM이 mock이어도 트레이스를 실제로 전송한다 — 테스트가
# LangSmith 월 한도를 소진한다(2026-08-04 초과 확인). app import보다 먼저 꺼야 하고,
# app.config의 load_dotenv()는 override=False라 여기서 박아두면 .env가 못 덮는다.
# setdefault라 `LANGSMITH_TRACING=true pytest`로 그 실행만 켤 수 있다.
os.environ.setdefault("LANGSMITH_TRACING", "false")
os.environ.setdefault("LANGCHAIN_TRACING_V2", "false")

import pytest

from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    LinkedCSInquiry,
    RecommendedAction,
    RootCause,
    SourceSignals,
    Verdict,
)


@pytest.fixture
def linked_inquiries() -> list[LinkedCSInquiry]:
    """biased_alert.evidence.inquiry_ids 에 대응하는 CS 원문.

    배치(`app/core/inquiries.py`)가 만들어 Agent3·가이드라인에 같이 넘기는 그 리스트다.
    image_guide 의 근거이자 citations 의 출처라, ID 가 alert 의 것과 어긋나면
    `validate_citations_grounded()` 가 잡는다 — 일부러 같은 ID 를 쓴다.
    """
    return [
        LinkedCSInquiry(
            item_id="INQ-000412",
            raw_text="사진이랑 색이 너무 달라요. 화면에서 본 아이보리가 아니에요.",
            created_at="2026-05-25T09:12:00",
        ),
        LinkedCSInquiry(
            item_id="INQ-000415",
            raw_text="조명 때문인지 실물 색이 훨씬 어둡습니다.",
            created_at="2026-05-26T14:03:00",
        ),
    ]


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
