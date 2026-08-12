"""담당: 지인 — `daily.load_inputs_from_db()` (raw DB → 탐지 입력).

**스키마는 `app/core/raw_schema.py` 의 DDL 을 그대로 써서 만든다.** 픽스처에 CREATE
TABLE 을 다시 적으면 확정 문서가 바뀔 때 테스트만 옛 스키마로 남아, 실제로는 깨지는
쿼리가 여기서는 통과한다.

이 모듈이 지키는 계약 3개:
  1. **분모는 원문(`voc_document`)에서 센다** — 분류 결과가 없는 문서도 documents 에 남는다.
  2. **날짜 절단은 KST** (확정 문서 §3). UTC 로 자르면 오전 9시 이전 문의가 전날로 밀린다.
  3. **35일만 읽는다** — 그 밖은 탐지가 어차피 안 보고, 매 배치 풀스캔이 된다.

LLM·네트워크 없음. sqlite 파일만 만든다.
"""

import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.batch import daily
from app.core import raw_schema

KST = timezone(timedelta(hours=9))

WINDOW_END = date(2026, 8, 28)

ROOT = Path(__file__).resolve().parents[1]


def _db(tmp_path, cs_rows=(), review_rows=(), classified=()):
    """확정 DDL 로 raw DB 를 만든다. 반환값은 경로 문자열.

    Args:
        cs_rows: (id, product_group_id, channel_id, content, inquired_at)
        review_rows: (id, product_group_id, channel_id, content, created_at)
        classified: (item_id, source, [(aspect, sentiment), ...])
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    raw_schema.create_classified_tables(conn)
    conn.executemany(
        "INSERT INTO channel (channel_id) VALUES (?)",
        [("COUPANG",), ("NAVER",), ("ZIGZAG",)],
    )
    conn.executemany(
        "INSERT INTO cs (id, product_group_id, channel_id, content, inquired_at)"
        " VALUES (?, ?, ?, ?, ?)",
        cs_rows,
    )
    conn.executemany(
        "INSERT INTO reviews (id, product_group_id, channel_id, content, created_at)"
        " VALUES (?, ?, ?, ?, ?)",
        review_rows,
    )
    for item_id, source, aspects in classified:
        conn.execute(
            "INSERT INTO classified_item (item_id, source) VALUES (?, ?)",
            (item_id, source),
        )
        conn.executemany(
            "INSERT INTO classified_item_aspect (item_id, aspect, sentiment)"
            " VALUES (?, ?, ?)",
            [(item_id, aspect, sentiment) for aspect, sentiment in aspects],
        )
    conn.commit()
    conn.close()
    return str(path)


def _at(day: date, hour: int = 12) -> str:
    """저장 형식 그대로 — 오프셋이 붙은 KST ISO 문자열(`mock_producer._iso`)."""
    return datetime(day.year, day.month, day.day, hour, tzinfo=KST).isoformat()


def test_unclassified_document_stays_in_the_denominator(tmp_path):
    """🔴 분류 결과가 없는 문서도 documents 에 남는다 — **분모의 정본은 원문이다.**

    여기서 빠지면 부정률의 분모가 깎여 p값이 부풀려진다(오탐 방향). aspect 0개가
    정상 출력인 리뷰에서 특히 크다.
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END)),
            ("INQ-2", "P001", "COUPANG", "잘 받았어요", _at(WINDOW_END)),
        ],
        review_rows=[("RVW-1", "P001", "NAVER", "포장 깔끔", _at(WINDOW_END))],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )

    items, documents = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-1", "INQ-2", "RVW-1"}
    assert [i.item_id for i in items] == ["INQ-1"]


def test_review_aspects_survive_the_join(tmp_path):
    """리뷰도 같은 뷰로 읽힌다 — 시각 컬럼명이 달라도(`created_at`) 하나로 맞춰진다."""
    db = _db(
        tmp_path,
        review_rows=[("RVW-1", "P002", "ZIGZAG", "소재가 얇아요", _at(WINDOW_END))],
        classified=[("RVW-1", "review", [("소재", -1)])],
    )

    items, documents = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert documents[0]["source"] == "review"
    assert documents[0]["channel"] == "ZIGZAG"
    assert items[0].aspects[0].aspect.value == "소재"
    assert items[0].aspects[0].sentiment == -1
    assert items[0].raw_text == "소재가 얇아요"


def test_classified_item_without_aspect_is_still_an_item(tmp_path):
    """aspect 0개인 분류 결과도 item 으로 만든다.

    리뷰는 빈 배열이 정상 출력이라(허용 aspect 3개뿐) 여기서 버리면 "분류됐다"는
    사실 자체가 사라져 분류 커버리지 집계가 어긋난다.
    """
    db = _db(
        tmp_path,
        review_rows=[("RVW-1", "P001", "NAVER", "배송 빨라요", _at(WINDOW_END))],
        classified=[("RVW-1", "review", [])],
    )

    items, _ = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert [i.item_id for i in items] == ["RVW-1"]
    assert items[0].aspects == []


def test_window_reads_exactly_35_days(tmp_path):
    """35일 = 현재 7 + 과거 28. 하루라도 더 읽으면 매 배치 풀스캔에 가까워진다."""
    inside = date.fromordinal(WINDOW_END.toordinal() - daily.INPUT_WINDOW_DAYS + 1)
    outside = date.fromordinal(inside.toordinal() - 1)
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-IN", "P001", "COUPANG", "안", _at(inside)),
            ("INQ-OUT", "P001", "COUPANG", "밖", _at(outside)),
            ("INQ-FUTURE", "P001", "COUPANG", "미래", _at(WINDOW_END + timedelta(1))),
        ],
    )

    _, documents = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-IN"}
    assert daily.INPUT_WINDOW_DAYS == 35


def test_window_none_reads_everything(tmp_path):
    """백필·첫 실행 경로 — 범위를 안 주면 전량을 읽고 호출부가 최신 날짜를 정한다."""
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-OLD", "P001", "COUPANG", "옛날", _at(date(2020, 1, 1))),
            ("INQ-NEW", "P001", "COUPANG", "최근", _at(WINDOW_END)),
        ],
    )

    _, documents = daily.load_inputs_from_db(None, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-OLD", "INQ-NEW"}


def test_day_boundary_uses_kst_not_utc(tmp_path):
    """🔴 날짜 절단은 KST 다(확정 문서 §3). **오프셋이 다른 행으로 재야 진짜 검사다.**

    목 프로듀서는 전부 `+09:00` 으로 저장하므로, KST 문자열로만 재면 UTC 로 잘라도
    똑같이 통과한다(문자열의 날짜 부분이 이미 KST 라서). 그래서 **같은 순간을 UTC
    표기로** 넣는다 — `2026-07-24T23:00+00:00` 은 KST 로 07-25 08:00 이고, 07-25 가
    윈도우 첫날이다. UTC 로 자르면 07-24 로 밀려 윈도우 밖이 된다.
    """
    first_day = date.fromordinal(WINDOW_END.toordinal() - daily.INPUT_WINDOW_DAYS + 1)
    same_instant_in_utc = (
        datetime(first_day.year, first_day.month, first_day.day, 8, tzinfo=KST)
        .astimezone(timezone.utc)
        .isoformat()
    )
    assert same_instant_in_utc.startswith(str(first_day - timedelta(1))), (
        "UTC 표기로는 전날이어야 이 테스트가 의미가 있다(테스트 전제)"
    )

    db = _db(
        tmp_path,
        cs_rows=[("INQ-EARLY", "P001", "COUPANG", "이른 아침", same_instant_in_utc)],
    )

    _, documents = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert [d["id"] for d in documents] == ["INQ-EARLY"]
    # build_rows 가 `.date()` 로 날짜를 다시 뽑으므로 넘겨주는 값도 KST 여야 한다 —
    # 여기서 갈리면 "읽히긴 했는데 집계에선 다른 날"이 된다.
    assert documents[0]["created_at"].date() == first_day


def test_naive_timestamp_is_read_as_kst_not_host_local():
    """🔴 오프셋 없는 값은 **KST 로 못박는다** — 실행 호스트 시간대를 보면 안 된다.

    `.astimezone()` 만 쓰면 naive 값을 호스트 로컬로 해석해서, 같은 행이 KST
    노트북에선 08-28 · UTC 컨테이너에선 08-29 가 된다. §3(KST 경계)을 지키려고 만든
    함수가 배포 환경에 따라 §3 을 어기는 셈이다.

    ⚠️ **이 테스트는 KST 머신에서는 옛 코드로도 통과한다** — 호스트가 마침 KST 라서다.
       무는 건 UTC 컨테이너다. 그래도 계약을 글로만 두지 않으려고 박아둔다.
       (2026-08-11 리뷰 ⑥)
    """
    got = daily._to_kst("2026-08-28T20:00:00")

    assert got.utcoffset() == timedelta(hours=9)
    # 벽시계가 그대로여야 한다. 호스트를 봤다면 UTC 컨테이너에서 08-29 05:00 이 된다.
    assert (got.year, got.month, got.day, got.hour) == (2026, 8, 28, 20)


def test_offset_aware_timestamp_is_converted_not_relabeled():
    """오프셋이 있으면 **변환**한다 — 라벨만 갈아치우면 같은 순간이 아니게 된다."""
    got = daily._to_kst("2026-07-24T23:00:00+00:00")

    assert (got.year, got.month, got.day, got.hour) == (2026, 7, 25, 8)


def test_naive_timestamp_is_kst_even_on_a_utc_host():
    """🔴 위 계약을 **UTC 호스트에서** 확인한다 — 개발 머신이 KST 라 여기서만 잡힌다.

    같은 프로세스에서 재면 호스트가 마침 KST 라 옛 코드(`.astimezone()` 만)도 통과한다.
    그래서 `TZ=UTC` 로 서브프로세스를 띄워서 잰다 — 배치를 컨테이너(UTC)로 올렸을 때
    실제로 도는 조건이다. 인코딩 배선을 서브프로세스로 검증한 PR #46 과 같은 방식이다.
    """
    code = (
        "from app.batch import daily;"
        "print(daily._to_kst('2026-08-28T20:00:00').isoformat())"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        # 인코딩을 양쪽 다 못박는다. 한글 traceback 이 stderr 에 실렸을 때 부모가
        # 로케일(cp949)로 디코드하면 깨지면서 `stderr` 가 통째로 `None` 이 된다 —
        # 아래 assert 가 실패 원인을 보여주려고 그 값을 쓰는데 그게 사라진다.
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


def test_unmapped_product_is_dropped_with_a_warning(tmp_path, caplog):
    """상품매핑이 안 붙은 원문은 어느 상품의 분모인지 모른다 — 세지 않고 건수만 남긴다."""
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", None, "COUPANG", "매핑 없음", _at(WINDOW_END)),
            ("INQ-2", "P001", "COUPANG", "정상", _at(WINDOW_END)),
        ],
    )

    _, documents = daily.load_inputs_from_db(WINDOW_END, db_path=db)

    assert [d["id"] for d in documents] == ["INQ-2"]
    assert any("상품매핑 없음" in r.getMessage() for r in caplog.records)


def test_source_only_db_fails_loudly(tmp_path):
    """🔴 원문만 적재되고 워커를 안 돌린 DB — **조용히 0건으로 넘어가면 안 된다.**

    실제로 나는 상태다(`mock_producer` 만 돌린 직후). 분자가 통째로 비면 알림이 한
    건도 안 나오는데 배치는 정상 종료해서, 무동작이 성공으로 보고된다.
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)  # 워커 소유 테이블은 안 만든다
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="분류 결과 테이블"):
        daily.load_inputs_from_db(WINDOW_END, db_path=str(path))


def test_child_table_alone_missing_fails_loudly(tmp_path):
    """🔴 `classified_item` 은 있고 **`classified_item_aspect` 만 없는** DB 도 막는다.

    위 `test_source_only_db_fails_loudly` 는 두 테이블이 **다 없는** 경우라, 가드를
    `classified_item` 하나만 보게 좁혀도 그대로 통과한다 — 이 경우가 그 구멍이다.

    안 막으면 조회 단계에서 `no such table: main.classified_item_aspect` 가 그대로
    올라온다. 가드가 있는 이유가 정확히 그 원문을 안 보여주려는 것이라(어느 테이블을
    누가 만들어야 하는지가 메시지에 안 드러난다) **막는 지점이 여기여야 한다.**

    부모만 만들어지는 건 지어낸 상태가 아니다 — `create_classified_tables` 는
    `IF NOT EXISTS` 라 중간에 끊긴 실행·부분 덤프가 이 모양으로 남는다.
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    # 확정 DDL 을 그대로 쓴다 — 여기에 CREATE TABLE 을 다시 적으면 `find_legacy_tables`
    # 가 옛 스키마로 오인해서, 이 테스트가 **다른 가드**를 재게 된다.
    conn.execute(raw_schema.CLASSIFIED_ITEM_DDL)
    # 뷰까지 만들어 둔다. 없으면 가드를 지웠을 때 `voc_document` 쪽에서 먼저 터져
    # "aspect 테이블이 없어서 터졌다" 를 확인하지 못한다.
    conn.execute(raw_schema.VOC_DOCUMENT_VIEW)
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="classified_item_aspect"):
        daily.load_inputs_from_db(WINDOW_END, db_path=str(path))


def test_legacy_schema_fails_before_the_query(tmp_path):
    """8/7 확정 이전 구조로 남은 DB 는 조회가 아니라 여기서 막는다.

    `CREATE TABLE IF NOT EXISTS` 가 옛 테이블을 그대로 두기 때문에, 안 막으면 한참
    뒤 `no such column` 으로 터져 원인이 메시지에 안 드러난다(PR #37 워커와 같은 함정).
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    conn.execute("CREATE TABLE classified_item (item_id TEXT PRIMARY KEY, raw_text TEXT)")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="확정 이전 스키마"):
        daily.load_inputs_from_db(WINDOW_END, db_path=str(path))


def test_missing_db_fails_loudly(tmp_path):
    """경로가 틀렸을 때 빈 파일을 새로 만들어 '문서 0건' 으로 통과하면 안 된다.

    그러면 배치가 아무 알림도 안 내고 **정상 종료**한다 — 조용한 무동작이 제일 나쁘다.
    """
    with pytest.raises(FileNotFoundError):
        daily.load_inputs_from_db(WINDOW_END, db_path=str(tmp_path / "없음.db"))


@pytest.mark.asyncio
async def test_batch_runs_end_to_end_on_the_db_loader(tmp_path, monkeypatch):
    """로더가 배치에 실제로 물린다 — 반환 형태가 `detect_anomaly` 입력과 맞는지 본다.

    알림이 뜨는지는 여기서 볼 게 아니다(표본이 작다). 배선만 고정한다.
    """
    db = _db(
        tmp_path,
        cs_rows=[("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END))],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    monkeypatch.setattr(
        daily.get_settings(), "raw_db_path", db, raising=False
    )

    summary = await daily.run_batch(
        window_end=WINDOW_END, dry_run=True, state_path=tmp_path / "state.json"
    )

    assert summary["documents"] == 1
    assert summary["items"] == 1
    assert summary["input_source"] == "load_inputs_from_db"
