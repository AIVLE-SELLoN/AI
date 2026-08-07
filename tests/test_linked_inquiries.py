"""담당: 지인 — `evidence.inquiry_ids` → CS 원문 매핑(`app/core/inquiries.py`).

개선안·CS 가이드라인이 같이 쓰는 입력이라, 여기가 조용히 틀리면 양쪽 근거가 같이 틀어진다.
"""

from datetime import date, datetime, timezone

from app.core.inquiries import build_linked_inquiries
from app.core.schemas import (
    Aspect,
    Channel,
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    RecommendedAction,
    Source,
    SourceSignals,
    Verdict,
)


def _alert(inquiry_ids: list[str]) -> DetectionAlert:
    return DetectionAlert(
        alert_id="ALT-20260828-P001-COUPANG",
        detected_at=datetime(2026, 8, 28, 9, 0, tzinfo=timezone.utc),
        product_group_id="P001",
        channel=Channel.COUPANG,
        window_start=date(2026, 8, 22),
        window_end=date(2026, 8, 28),
        verdict=Verdict.BIASED,
        significant_channels=[Channel.COUPANG],
        main_aspect=Aspect.COLOR,
        stats=DetectionStats(
            source=Source.CS,
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=1e-4,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(cs=True, review=None, interpretation="CS 선행"),
        detection_confidence=DetectionConfidence.HIGH,
        scope_in=True,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        evidence=Evidence(inquiry_ids=inquiry_ids),
    )


def _doc(doc_id: str, text: str = "색상이 사진과 다릅니다") -> dict:
    return {
        "id": doc_id,
        "product": "P001",
        "channel": "COUPANG",
        "source": "cs",
        "created_at": datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc),
        "text": text,
    }


def test_maps_in_evidence_order():
    """순서는 evidence.inquiry_ids 를 따른다 — documents 순서가 아니다."""
    alert = _alert(["INQ-2", "INQ-1"])
    inquiries = build_linked_inquiries(alert, [_doc("INQ-1"), _doc("INQ-2")])

    assert [i.item_id for i in inquiries] == ["INQ-2", "INQ-1"]
    assert inquiries[0].raw_text == "색상이 사진과 다릅니다"


def test_deduplicates_repeated_ids():
    """item_id 중복은 CSGuidelineInput 이 거부한다 — 여기서 미리 접는다."""
    alert = _alert(["INQ-1", "INQ-1"])

    assert len(build_linked_inquiries(alert, [_doc("INQ-1")])) == 1


def test_skips_missing_and_blank_without_faking(caplog):
    """원문이 없으면 빈 문자열로 채우지 않고 버린다.

    raw_text="" 항목은 "문의 원문"이라고 주장하는 빈 값이라, 인용·가이드라인이 근거
    없이 만들어진다. 대신 몇 건이 빠졌는지 경고로 남긴다.
    """
    alert = _alert(["INQ-1", "INQ-없음", "INQ-공백"])
    documents = [_doc("INQ-1"), _doc("INQ-공백", text="   ")]

    inquiries = build_linked_inquiries(alert, documents)

    assert [i.item_id for i in inquiries] == ["INQ-1"]
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "못 찾음 1건" in logged
    assert "비어 있음 1건" in logged


def test_no_evidence_returns_empty():
    """inquiry_ids 가 비면 빈 리스트. 던지지 않는다 — 배치 루프 안에서 돈다."""
    assert build_linked_inquiries(_alert([]), [_doc("INQ-1")]) == []
