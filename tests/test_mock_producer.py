"""Mock Producer 적재 계층 테스트 — 「Raw DB 스키마 확정 (8/7)」 §2-1.

재생 루프(Kafka·배속·sleep)는 다루지 않는다. 여기서 고정하는 것은 **채널 마스터가
오타를 실제로 잡아 주는가** 하나다. 이게 깨지면 오류 없이 결과만 틀어진다:
못 알아본 채널의 부정 의견이 어느 채널쌍에도 안 들어간 채 리포트가 정상처럼 나간다.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.batch import daily
from app.core import constants, raw_schema
from app.core.schemas import Channel
from app.detection import service as detection_service
from scripts import mock_producer

ROOT = Path(__file__).resolve().parents[1]


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
            # naive 가 **의도**다 — 대본 CSV 의 시각이 오프셋 없는 한국 벽시계이고,
            # 오프셋을 붙이는 건 sink 의 일이다. tzinfo 를 넣어 "고치면" 그걸 못 본다.
            "time": datetime(2026, 5, 1, 10, 0),  # noqa: DTZ001
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

    # naive 로 넣어야 "sink 가 KST 를 붙인다"를 검증할 수 있다 — 이 테스트의 전부다.
    sink.add({
        "table": "cs", "event_id": "INQ-1", "channel": "COUPANG",
        "channel_product_id": "C1", "product_group_id": "P001",
        "raw_text": "색이 달라요",
        "time": datetime(2026, 5, 1, 10, 0),  # noqa: DTZ001
        "payload": {},
    })
    sink.flush()

    occurred = conn.execute(f"SELECT occurred_at FROM {raw_schema.VOC_DOCUMENT}").fetchone()[0]
    assert occurred == "2026-05-01T10:00:00+09:00"


def test_writer_and_reader_share_one_kst_definition():
    """🔴 오프셋을 **쓰는** 쪽과 날짜를 **자르는** 쪽이 같은 KST 객체를 봐야 한다.

    `mock_producer.to_kst_iso()` 가 원문에 오프셋을 붙여 저장하고,
    `app/batch/daily.py::_to_kst()` 가 그걸 읽어 KST 날짜로 자른다. 두 파일이 각자
    `timezone(timedelta(hours=9))` 를 들고 있으면 **한쪽만 바뀌었을 때 조용히 갈린다** —
    행 수도 `verify_counts` 도 전부 통과하는데 날짜 경계의 문서만 다른 날로 집계된다
    (08-11 밤 생성기 비결정성과 같은 모양: 집계는 같은데 행이 갈림).

    `is` 로 본다. 값 비교(`==`)면 각자 정의해도 통과해서 이 회귀를 못 잡는다 —
    `timezone` 은 UTC(offset 0)만 캐시해서 `timezone(timedelta(hours=9))` 두 개는
    서로 다른 객체인데 `==` 는 True 다. (PR #68 후속)

    🔴 **이 목록은 손으로 등록하는 화이트리스트다.** `constants.KST` 를 import 하는
       모듈이 늘면 여기에 한 줄 추가해야 한다 — 안 그러면 그 모듈이 로컬 재정의로
       빠져나가도 아무것도 안 물린다. (용준님 PR #70 리뷰 ②)
    """
    assert mock_producer.KST is constants.KST, "오프셋을 쓰는 쪽"
    assert daily.KST is constants.KST, "날짜를 자르는 쪽"
    # detected_at → alert_id(`ALT-%Y%m%d`)·가이드라인 기간(`%Y-%m`) 이라 §3 대상이다.
    assert detection_service.KST is constants.KST, "탐지 시각을 찍는 쪽"


def test_aware_input_is_converted_not_relabeled():
    """오프셋이 **있는** 입력은 라벨을 갈아치우지 않고 **변환**한다.

    `to_kst_iso` 는 두 갈래다 — naive 면 KST 로 간주(`replace`), aware 면 KST 로
    변환(`astimezone`). naive 쪽은 위 `test_timestamps_carry_the_kst_offset` 이
    완전일치로 이미 잠갔고, **이쪽 갈래가 비어 있었다**: `astimezone` 분기를 통째로
    지워도 전건 통과했다(용준님 PR #70 리뷰 ①, 재현 확인).

    분기를 지우면 UTC 01:00 이 `01:00+09:00` 으로 **9시간 틀어진 채** 저장된다 —
    같은 순간이 아니게 되므로 날짜 경계에서 하루가 밀린다.
    """
    utc_1am = datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc)  # = KST 10:00

    assert mock_producer.to_kst_iso(utc_1am) == "2026-05-01T10:00:00+09:00"


def test_naive_input_is_kst_even_on_a_utc_host():
    """🔴 naive 를 KST 로 간주하는지 **UTC 호스트에서** 확인한다.

    같은 프로세스에서 재면 개발 머신이 KST 라 `astimezone()` 만 남겨도 통과한다 —
    실제로 naive 분기를 지우고 돌려보면 전건 통과했다(용준님 PR #70 리뷰, 재현 확인).
    그래서 `TZ=UTC` 서브프로세스로 잰다. 배치를 컨테이너로 올렸을 때 도는 조건이다.

    **읽는 쪽은 이미 같은 방식으로 잠겨 있다** —
    `test_load_inputs_from_db.py::test_naive_timestamp_is_kst_even_on_a_utc_host`.
    이 PR 이 "쓰는 쪽·읽는 쪽 한 쌍" 이라고 묶었으니 테스트 조건도 짝이 맞아야 한다.

    ⚠️ 인코딩을 양쪽 다 못박는다 — 실패 시 한글 traceback 이 stderr 에 실리는데 부모가
       로케일(cp949)로 디코드하면 깨지면서 `stderr` 가 통째로 `None` 이 된다(PR #66).
    """
    code = (
        "from datetime import datetime;"
        "from scripts import mock_producer;"
        "print(mock_producer.to_kst_iso(datetime(2026, 8, 28, 20, 0)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env={**os.environ, "TZ": "UTC", "PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=180,
        check=False,  # 종료코드를 직접 본다 — stderr 를 assert 메시지에 실으려고
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "2026-08-28T20:00:00+09:00", (
        "UTC 호스트에서 날짜가 밀렸습니다 — naive 값을 호스트 로컬로 해석하고 있습니다"
    )


def test_only_constants_defines_kst():
    """🔴 `timedelta(hours=9)` 리터럴은 `core/constants.py` 에만 있어야 한다.

    위 `test_writer_and_reader_share_one_kst_definition` 은 **손으로 등록한 소비자**만
    본다. 새 파일이 로컬 정의를 들고 생기면 그 화이트리스트가 못 잡는데, 이 검사가
    그 구멍을 덮는다(용준님 PR #70 리뷰 제안).

    ⚠️ **identity assert 를 대체하지 않는다 — 보완이다.** 텍스트 매칭이라
       `timedelta(minutes=540)` 같은 변종은 못 잡는다. 반대로 이쪽은 import 하지 않는
       파일까지 본다. 두 검사가 서로 다른 구멍을 막으므로 하나를 지우지 말 것.
    """
    hits = sorted(
        path.relative_to(ROOT).as_posix()
        for root in ("app", "scripts", "eval")
        for path in (ROOT / root).rglob("*.py")
        if "timedelta(hours=9)" in path.read_text(encoding="utf-8")
    )

    assert hits == ["app/core/constants.py"], f"KST 를 따로 정의한 파일이 있습니다: {hits}"
