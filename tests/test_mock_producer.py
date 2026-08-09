"""Mock Producer 적재 계층 테스트 — 「Raw DB 스키마 확정 (8/7)」 §2-1.

재생 루프(Kafka·배속·sleep)는 다루지 않는다. 여기서 고정하는 것은 **채널 마스터가
오타를 실제로 잡아 주는가** 하나다. 이게 깨지면 오류 없이 결과만 틀어진다:
못 알아본 채널의 부정 의견이 어느 채널쌍에도 안 들어간 채 리포트가 정상처럼 나간다.
"""

from __future__ import annotations

import sqlite3

import pytest

from app.core import raw_schema
from app.core.schemas import Channel
from scripts import mock_producer


def _open(tmp_path) -> sqlite3.Connection:
    conn = mock_producer.open_raw_db(str(tmp_path / "raw.db"))
    mock_producer.seed_channels(conn, {"COUPANG"})
    return conn


def test_channel_master_comes_from_enum_not_from_the_script(tmp_path) -> None:
    """마스터는 대본에서 관측된 값이 아니라 `Channel` enum 으로 채운다(§2-1).

    ⚠️ 관측값으로 채우면 **FK 가 아무것도 못 잡는다** — 대본에 'coupang' 오타가 있으면
       그 오타가 마스터에도 같이 들어가 참조가 항상 성립한다.
    """
    conn = _open(tmp_path)

    seeded = {r[0] for r in conn.execute("SELECT channel_id FROM channel")}

    assert seeded == {c.value for c in Channel if c is not Channel.ALL}
    # 가상 채널은 연동 채널이 아니다
    assert Channel.ALL.value not in seeded
    # 관측된 것이 COUPANG 하나뿐이어도 마스터는 전체를 갖는다
    assert len(seeded) == 3


def test_unknown_channel_row_is_rejected_by_foreign_key(tmp_path) -> None:
    """마스터에 없는 채널의 원문은 적재되지 않는다.

    ⚠️ sqlite 는 FK 가 기본 OFF 라 `open_raw_db()` 가 `PRAGMA foreign_keys=ON` 을 켜야
       실제로 걸린다. 안 켜지면 이 단언이 깨진다 — 그게 리뷰에서 지적된 상태였다.
    """
    conn = _open(tmp_path)
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO cs (id, channel_id, content, inquired_at) VALUES (?,?,?,?)",
            ("INQ-1", "coupang", "색이 달라요", "2026-05-01T10:00:00+09:00"),
        )

    conn.execute(
        "INSERT INTO cs (id, channel_id, content, inquired_at) VALUES (?,?,?,?)",
        ("INQ-2", "COUPANG", "색이 달라요", "2026-05-01T10:00:00+09:00"),
    )
    assert conn.execute("SELECT COUNT(*) FROM cs").fetchone()[0] == 1


def test_unknown_channel_in_script_is_reported_before_loading(tmp_path, caplog) -> None:
    """대본에 마스터 밖 채널이 있으면 적재 전에 알린다.

    FK 위반은 행 단위 오류로만 나와서, 대본 전체가 어긋난 경우 12만 줄짜리 ERROR 로그를
    다 보고서야 원인을 알게 된다.
    """
    conn = mock_producer.open_raw_db(str(tmp_path / "raw.db"))
    with caplog.at_level("ERROR"):
        mock_producer.seed_channels(conn, {"COUPANG", "11ST"})

    assert "11ST" in caplog.text
    assert "FK" in caplog.text


def test_sink_isolates_the_bad_row_and_keeps_the_rest(tmp_path) -> None:
    """FK 로 걸린 행만 떨어져 나가고 나머지 재생은 계속된다.

    한 행이 잘못됐다고 재생 전체(특히 Kafka 발행)가 멈추면 안 된다는 게 sink 의 계약이다.
    """
    conn = _open(tmp_path)
    sink = mock_producer.RawDbSink(conn)

    from datetime import datetime

    def _event(item_id: str, channel: str) -> dict:
        return {
            "table": "cs",
            "event_id": item_id,
            "channel": channel,
            "channel_product_id": "C1",
            "product_group_id": "P001",
            "raw_text": "색이 달라요",
            "time": datetime(2026, 5, 1, 10, 0),
            "payload": {},
        }

    sink.add(_event("INQ-1", "COUPANG"))
    sink.add(_event("INQ-2", "coupang"))  # 마스터에 없다
    sink.add(_event("INQ-3", "NAVER"))
    sink.flush()

    assert sink.written == 2
    assert sink.failed == 1
    ids = {r[0] for r in conn.execute("SELECT id FROM cs")}
    assert ids == {"INQ-1", "INQ-3"}


def test_timestamps_carry_the_kst_offset(tmp_path) -> None:
    """§3 이 날짜 경계를 Asia/Seoul 로 못박아서 오프셋을 붙여 적재한다.

    오프셋 없이 넣으면 TIMESTAMPTZ 로 옮길 때 어느 지역 시각인지 알 수 없어 하루가 밀린다.
    """
    conn = _open(tmp_path)
    sink = mock_producer.RawDbSink(conn)

    from datetime import datetime

    sink.add({
        "table": "cs", "event_id": "INQ-1", "channel": "COUPANG",
        "channel_product_id": "C1", "product_group_id": "P001",
        "raw_text": "색이 달라요", "time": datetime(2026, 5, 1, 10, 0), "payload": {},
    })
    sink.flush()

    occurred = conn.execute(f"SELECT occurred_at FROM {raw_schema.VOC_DOCUMENT}").fetchone()[0]
    assert occurred == "2026-05-01T10:00:00+09:00"
