"""담당: 지인 — `evidence.inquiry_ids` → CS 원문 매핑(`app/core/inquiries.py`).

개선안·CS 가이드라인이 같이 쓰는 입력이라, 여기가 조용히 틀리면 양쪽 근거가 같이 틀어진다.
"""

import sqlite3
from datetime import date, datetime, timezone

import pytest

from app.core import raw_schema
from app.core.inquiries import build_linked_inquiries, fetch_linked_inquiries
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


# ── raw DB 직접 조회 (documents 를 손에 안 든 호출부용) ──────────


def _raw_db(tmp_path, cs_rows=(), review_rows=()):
    """확정 DDL 로 raw DB 를 만든다 — 픽스처에 CREATE TABLE 을 다시 적지 않는다."""
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    conn.executemany(
        "INSERT INTO cs (id, product_group_id, channel_id, content, inquired_at,"
        " created_at) VALUES (?, ?, ?, ?, ?, ?)",
        cs_rows,
    )
    conn.executemany(
        "INSERT INTO reviews (id, product_group_id, channel_id, content, created_at)"
        " VALUES (?, ?, ?, ?, ?)",
        review_rows,
    )
    conn.commit()
    conn.close()
    return str(path)


def test_fetch_reads_inquired_at_not_created_at(tmp_path):
    """🔴 `cs` 는 시각 컬럼이 **둘**이다 — 접수 일시는 `inquired_at` 이다.

    `LinkedCSInquiry.created_at` 의 정의가 "CS 접수 일시" 인데 `cs.created_at`(레코드
    적재 시각)을 이름만 보고 매핑하면 조용히 틀린 값이 들어간다. 두 값을 일부러 다르게
    넣어서 어느 쪽을 읽는지 고정한다.
    """
    db = _raw_db(
        tmp_path,
        cs_rows=[
            (
                "INQ-1",
                "P001",
                "COUPANG",
                "색상이 사진과 다릅니다",
                "2026-08-27T10:00:00+09:00",  # inquired_at — 이게 맞다
                "2026-08-28T03:00:00+09:00",  # created_at — 적재 시각
            )
        ],
    )

    inquiries = fetch_linked_inquiries(_alert(["INQ-1"]), db_path=db)

    assert [i.item_id for i in inquiries] == ["INQ-1"]
    assert inquiries[0].created_at.isoformat() == "2026-08-27T10:00:00+09:00"
    assert inquiries[0].raw_text == "색상이 사진과 다릅니다"


def test_fetch_keeps_evidence_order_and_skips_missing(tmp_path, caplog):
    """정책(순서·누락 처리)이 documents 경로와 같다 — 두 벌이면 REST 와 배치 근거가 갈린다."""
    db = _raw_db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "첫번째", "2026-08-27T10:00:00+09:00", None),
            ("INQ-2", "P001", "COUPANG", "두번째", "2026-08-27T11:00:00+09:00", None),
        ],
    )

    inquiries = fetch_linked_inquiries(_alert(["INQ-2", "INQ-1", "INQ-없음"]), db_path=db)

    assert [i.item_id for i in inquiries] == ["INQ-2", "INQ-1"]
    assert any("못 찾음 1건" in r.getMessage() for r in caplog.records)


def test_fetch_reads_reviews_too(tmp_path):
    """⚠️ 리뷰(`RVW-`)도 딸려 온다 — 여기서 거르지 않는다.

    리뷰 소스 알림이면 `evidence.inquiry_ids` 가 `RVW-*` 라, 그게 그대로 "고객 작성
    문의 원문" 으로 CS 가이드라인에 들어간다. **정책 질문("리뷰를 CS 답변 초안의 근거로
    쓸 것인가", 2026-08-07 미결)이라 코드가 먼저 정하지 않는다.** documents 경로도 같은
    동작이라 경로에 따라 답이 달라지지 않는다는 게 지금 지킬 것이다.
    """
    db = _raw_db(
        tmp_path,
        review_rows=[("RVW-1", "P001", "NAVER", "리뷰 원문", "2026-08-27T10:00:00+09:00")],
    )

    assert [i.item_id for i in fetch_linked_inquiries(_alert(["RVW-1"]), db_path=db)] == [
        "RVW-1"
    ]


def test_fetch_without_evidence_never_opens_the_db(tmp_path):
    """대상이 없으면 연결도 안 연다 — 없는 DB 를 가리켜도 안 던진다."""
    assert fetch_linked_inquiries(_alert([]), db_path=str(tmp_path / "없음.db")) == []


def test_fetch_raises_when_db_is_missing(tmp_path):
    """대상이 있는데 DB 가 없으면 던진다 — 호출부가 판단할 몫이다.

    REST(`service.generate_recommendation`)는 이걸 잡아 원문 없이 진행하고 경고를 남긴다.
    """
    with pytest.raises(FileNotFoundError):
        fetch_linked_inquiries(_alert(["INQ-1"]), db_path=str(tmp_path / "없음.db"))
