"""담당: 지인 — `inputs.load_inputs_from_db()` (raw DB → 탐지 입력).

**스키마는 `app/core/raw_schema.py` 의 DDL 을 그대로 써서 만든다.** 픽스처에 CREATE
TABLE 을 다시 적으면 확정 문서가 바뀔 때 테스트만 옛 스키마로 남아, 실제로는 깨지는
쿼리가 여기서는 통과한다.

이 모듈이 지키는 계약 3개:
  1. **분모는 원문(`voc_document`)에서 센다** — 분류 결과가 없는 문서도 documents 에 남는다.
  2. **날짜 절단은 KST** (확정 문서 §3). UTC 로 자르면 오전 9시 이전 문의가 전날로 밀린다.
  3. **35일만 읽는다** — 그 밖은 탐지가 어차피 안 보고, 매 배치 풀스캔이 된다.

LLM·네트워크 없음. sqlite 파일만 만든다.
"""

import asyncio
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.batch import daily, inputs
from app.config import get_settings
from app.core import raw_schema
from app.detection.service import detect_anomaly

KST = timezone(timedelta(hours=9))

WINDOW_END = date(2026, 8, 28)

ROOT = Path(__file__).resolve().parents[1]


def _db(tmp_path, cs_rows=(), review_rows=(), classified=(), prompt_version=None):
    """확정 DDL 로 raw DB 를 만든다. 반환값은 경로 문자열.

    Args:
        cs_rows: (id, product_group_id, channel_id, content, inquired_at)
        review_rows: (id, product_group_id, channel_id, content, created_at)
        classified: (item_id, source, [(aspect, sentiment), ...])
        prompt_version: 분류 결과에 남길 프롬프트 버전. 기본은 **활성 버전** — 워커가
            실제로 적재하는 값이다. 탐지가 활성 버전 행만 읽으므로(2026-08-12), 이걸
            안 채우면 픽스처가 "옛 분류기로 분류된 DB" 가 되어 전 테스트가 cutover
            에러로 죽는다. 그 상태 자체를 검증하는 테스트만 다른 값을 넘긴다.
            모델·파이프라인 축은 항상 활성 값으로 채운다(프롬프트 축만 흔들어 본다).
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
    # 활성 3축은 탐지가 실제로 거르는 값에서 그대로 가져온다 — 여기서 따로 적으면
    # 필터를 고쳤을 때 픽스처만 옛 값으로 남아 전 테스트가 cutover 에러로 죽는다.
    active_cs, active_review, active_model, active_pipeline = inputs._active_version_params()
    for item_id, source, aspects in classified:
        version = prompt_version or (active_cs if source == "cs" else active_review)
        conn.execute(
            "INSERT INTO classified_item"
            " (item_id, source, prompt_version, model_version, pipeline_version)"
            " VALUES (?, ?, ?, ?, ?)",
            (item_id, source, version, active_model, active_pipeline),
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

    items, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-1", "INQ-2", "RVW-1"}
    assert [i.item_id for i in items] == ["INQ-1"]


def test_review_aspects_survive_the_join(tmp_path):
    """리뷰도 같은 뷰로 읽힌다 — 시각 컬럼명이 달라도(`created_at`) 하나로 맞춰진다."""
    db = _db(
        tmp_path,
        review_rows=[("RVW-1", "P002", "ZIGZAG", "소재가 얇아요", _at(WINDOW_END))],
        classified=[("RVW-1", "review", [("소재", -1)])],
    )

    items, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

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

    items, _ = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert [i.item_id for i in items] == ["RVW-1"]
    assert items[0].aspects == []


def test_window_reads_exactly_35_days(tmp_path):
    """35일 = 현재 7 + 과거 28. 하루라도 더 읽으면 매 배치 풀스캔에 가까워진다."""
    inside = date.fromordinal(WINDOW_END.toordinal() - inputs.INPUT_WINDOW_DAYS + 1)
    outside = date.fromordinal(inside.toordinal() - 1)
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-IN", "P001", "COUPANG", "안", _at(inside)),
            ("INQ-OUT", "P001", "COUPANG", "밖", _at(outside)),
            ("INQ-FUTURE", "P001", "COUPANG", "미래", _at(WINDOW_END + timedelta(1))),
        ],
    )

    _, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-IN"}
    assert inputs.INPUT_WINDOW_DAYS == 35


def test_window_none_reads_everything(tmp_path):
    """백필·첫 실행 경로 — 범위를 안 주면 전량을 읽고 호출부가 최신 날짜를 정한다."""
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-OLD", "P001", "COUPANG", "옛날", _at(date(2020, 1, 1))),
            ("INQ-NEW", "P001", "COUPANG", "최근", _at(WINDOW_END)),
        ],
    )

    _, documents = inputs.load_inputs_from_db(None, db_path=db)

    assert {d["id"] for d in documents} == {"INQ-OLD", "INQ-NEW"}


def test_day_boundary_uses_kst_not_utc(tmp_path):
    """🔴 날짜 절단은 KST 다(확정 문서 §3). **오프셋이 다른 행으로 재야 진짜 검사다.**

    목 프로듀서는 전부 `+09:00` 으로 저장하므로, KST 문자열로만 재면 UTC 로 잘라도
    똑같이 통과한다(문자열의 날짜 부분이 이미 KST 라서). 그래서 **같은 순간을 UTC
    표기로** 넣는다 — `2026-07-24T23:00+00:00` 은 KST 로 07-25 08:00 이고, 07-25 가
    윈도우 첫날이다. UTC 로 자르면 07-24 로 밀려 윈도우 밖이 된다.
    """
    first_day = date.fromordinal(WINDOW_END.toordinal() - inputs.INPUT_WINDOW_DAYS + 1)
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

    _, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

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
    got = inputs._to_kst("2026-08-28T20:00:00")

    assert got.utcoffset() == timedelta(hours=9)
    # 벽시계가 그대로여야 한다. 호스트를 봤다면 UTC 컨테이너에서 08-29 05:00 이 된다.
    assert (got.year, got.month, got.day, got.hour) == (2026, 8, 28, 20)


def test_offset_aware_timestamp_is_converted_not_relabeled():
    """오프셋이 있으면 **변환**한다 — 라벨만 갈아치우면 같은 순간이 아니게 된다."""
    got = inputs._to_kst("2026-07-24T23:00:00+00:00")

    assert (got.year, got.month, got.day, got.hour) == (2026, 7, 25, 8)


def test_naive_timestamp_is_kst_even_on_a_utc_host():
    """🔴 위 계약을 **UTC 호스트에서** 확인한다 — 개발 머신이 KST 라 여기서만 잡힌다.

    같은 프로세스에서 재면 호스트가 마침 KST 라 옛 코드(`.astimezone()` 만)도 통과한다.
    그래서 `TZ=UTC` 로 서브프로세스를 띄워서 잰다 — 배치를 컨테이너(UTC)로 올렸을 때
    실제로 도는 조건이다. 인코딩 배선을 서브프로세스로 검증한 PR #46 과 같은 방식이다.
    """
    code = (
        "from app.batch import inputs;"
        "print(inputs._to_kst('2026-08-28T20:00:00').isoformat())"
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
    """상품매핑이 안 붙은 원문은 어느 상품의 분모인지 모른다 — 세지 않고 건수만 남긴다.

    ⚠️ 수집기(`dropped=`)를 같이 본다. **경고와 수집기는 같은 사실의 두 출구**라
       한쪽만 잠그면 나머지가 조용히 빠진다 — 실제로 이 카운터는 오랫동안 경고 로그로만
       나갔고, 아무도 CronJob 로그를 안 봐서 미매핑이 늘어도 몰랐다.
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", None, "COUPANG", "매핑 없음", _at(WINDOW_END)),
            ("INQ-2", "P001", "COUPANG", "정상", _at(WINDOW_END)),
        ],
    )

    dropped: Counter[str] = Counter()
    _, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db, dropped=dropped)

    assert [d["id"] for d in documents] == ["INQ-2"]
    assert any("상품매핑 없음" in r.getMessage() for r in caplog.records)
    assert dropped == Counter({"상품매핑 없음": 1})


def test_public_loader_contract_stays_a_pair(tmp_path):
    """🔴 공개 로더는 **2-tuple 을 유지한다** — 수집기를 안 줘도 형태가 안 바뀐다.

    같은 seam 의 반대쪽(`scripts/golden_inputs.load_golden_inputs`)과 시그니처가 맞아야
    하고, `(items, documents)` 로 언패킹하는 호출부가 저장소에 9곳 있다
    (`scripts/detection_experiments/` 7 · `eval/` 1 · Postgres 게이트 1). 그중 실험
    스크립트에는 **테스트가 없어서**, 여기서 3-tuple 로 늘리면 아무 데서도 안 걸리고
    실행 시점에야 `too many values to unpack` 으로 터진다.
    """
    db = _db(tmp_path, cs_rows=[("INQ-1", "P001", "COUPANG", "정상", _at(WINDOW_END))])

    result = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert len(result) == 2
    items, documents = result
    assert [d["id"] for d in documents] == ["INQ-1"]
    assert items == []


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
        inputs.load_inputs_from_db(WINDOW_END, db_path=str(path))


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
        inputs.load_inputs_from_db(WINDOW_END, db_path=str(path))


def test_legacy_schema_fails_before_the_query(tmp_path):
    """확정본과 다른 구조로 남은 DB 는 조회가 아니라 여기서 막는다.

    `CREATE TABLE IF NOT EXISTS` 가 이미 있는 테이블을 그대로 두기 때문에, 안 막으면 한참
    뒤 `no such column` 으로 터져 원인이 메시지에 안 드러난다(PR #37 워커와 같은 함정).
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    conn.execute("CREATE TABLE classified_item (item_id TEXT PRIMARY KEY, raw_text TEXT)")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="확정 스키마와 다릅니다"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=str(path))


def test_missing_unique_constraint_also_stops_the_batch(tmp_path):
    """🔴 컬럼은 확정본과 같고 **UNIQUE 제약만 빠진** DB 도 여기서 막는다.

    이 사유는 위와 **증상이 다르다.** 컬럼이 옛것이면 조회가 `no such column` 으로 시끄럽게
    죽지만, 제약이 빠진 경우는 **아무것도 안 죽고** 재분류가 같은 `(item_id, aspect)` 를
    중복 적재해 탐지 분자가 부푼다 — 오탐 방향이라 조용하다. 인프라가 낡은 문서로 테이블을
    먼저 세워 뒀을 때 나오는 모양이다(2026-08-18).

    ⚠️ **메시지가 사유를 단정하면 안 된다.** 예전 문구("8/7 확정 이전 스키마")를 그대로 두면
       제약이 빠진 사람이 스키마 버전을 뒤지게 된다 — 그래서 문구도 같이 고정한다.
    """
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(path))
    raw_schema.create_source_tables(conn)
    conn.execute(raw_schema.CLASSIFIED_ITEM_DDL)
    # 확정 DDL 과 컬럼은 같고 UNIQUE (item_id, aspect) 만 없다.
    conn.execute(
        "CREATE TABLE classified_item_aspect ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " item_id TEXT NOT NULL REFERENCES classified_item(item_id),"
        " aspect TEXT NOT NULL, sentiment INTEGER NOT NULL, mixed_signal INTEGER)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="UNIQUE 제약") as exc:
        inputs.load_inputs_from_db(WINDOW_END, db_path=str(path))
    # 어느 테이블인지 알려 준다 — 사유만 알고 대상을 모르면 조치를 못 한다.
    assert "classified_item_aspect" in str(exc.value)


# ── 분류기 버전 (2026-08-12) ────────────────────────────────────────────────
#
# 탐지는 35일(현재 7 + 과거 28)을 한 번에 읽는다. 그 사이 분류기가 바뀌면 한 검정 안에
# 두 라벨러의 결과가 섞인다. **혼재는 표본이 준 것이 아니라 검정 전제가 깨진 것**이라
# 경고가 아니라 중단으로 처리한다(fail-closed, 2026-08-12 결정).


def test_partially_stale_window_stops_the_batch(tmp_path):
    """🔴 일부만 옛 버전이어도 **세운다** — 경고로 넘기면 오탐이 난다.

    필터가 분자에만 걸리기 때문이다. 분모(documents)는 원문이라 필터를 안 타므로, 과거
    구간이 옛 버전이면 기준선 부정률이 작아지는 게 아니라 **0 이 되고 그대로 오탐**이 된다
    (아래 통합 테스트에서 실제 발화까지 재현한다).
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END)),
            ("INQ-2", "P001", "COUPANG", "사이즈가 작아요", _at(WINDOW_END)),
        ],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    # INQ-2 만 옛 프롬프트로 분류된 상태를 만든다(프롬프트 교체 직후의 실제 모양).
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO classified_item (item_id, source, prompt_version) VALUES (?, ?, ?)",
        ("INQ-2", "cs", "classify_aspect_v4"),
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="옛 분류기") as exc:
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    # 메시지가 활성 3축과 건수를 다 담아야 "설정 오타"와 "backfill 필요"를 가를 수 있다.
    message = str(exc.value)
    assert "활성 1건" in message and "옛 버전 1건" in message
    for axis in ("cs=", "review=", "model=", "pipeline="):
        assert axis in message
    assert "--reclassify-stale" in message


def test_null_prompt_version_counts_as_old(tmp_path):
    """버전을 안 남기던 시절의 행(`NULL`)도 옛것으로 본다.

    `=` 비교로 세면 NULL 이 **어느 쪽으로도 안 걸려** stale 집계에서 조용히 빠진다 —
    가장 오래된, 그래서 가장 확실히 옛것인 행이 하필 안 잡힌다. 두 곳 다 null-safe
    비교(`IS`)를 쓴다.
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END)),
            ("INQ-2", "P001", "COUPANG", "사이즈가 작아요", _at(WINDOW_END)),
        ],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO classified_item (item_id, source) VALUES (?, ?)", ("INQ-2", "cs"))
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="옛 분류기"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)


def test_full_stale_window_fails_loudly(tmp_path):
    """🔴 윈도우가 통째로 옛 프롬프트면 **세운다** — 조용한 무동작이 제일 나쁘다.

    그냥 두면 분자가 통째로 비어 배치가 "이상 없음"으로 정상 종료한다. 그건 관측 결과가
    아니라 우리가 라벨을 못 읽은 것인데, 로그만 보면 구분이 안 된다. 미탐 방향이라
    아무도 눈치채지 못한 채 며칠이 지나간다.
    """
    db = _db(
        tmp_path,
        cs_rows=[("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END))],
        classified=[("INQ-1", "cs", [("색상", -1)])],
        prompt_version="classify_aspect_v4",
    )

    with pytest.raises(RuntimeError, match="옛 분류기"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)


def test_model_change_alone_makes_rows_unreadable(tmp_path, monkeypatch):
    """🔴 프롬프트가 그대로여도 **모델이 바뀌면** 그 행은 안 읽는다.

    프롬프트 축만 거르면 "같은 프롬프트, 다른 라벨러" 가 통째로 새어 나간다 — `LLM_MODEL`
    을 갈아끼우면 프롬프트 파일은 한 글자도 안 바뀌었는데 라벨이 달라진다. 그 행들이 35일
    창에 섞이면 모델 교체가 고객 이상 알림으로 발화한다.

    세우는 쪽으로 도는 것이 맞다 — `LLM_MODEL` 오타면 설정을 고치면 되고, 의도한 교체면
    backfill 하면 된다. 둘 다 사람이 보고 정할 일이지 조용히 섞을 일이 아니다.
    """
    db = _db(
        tmp_path,
        cs_rows=[("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END))],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    # 픽스처는 적재 시점 모델로 채워졌다. 탐지 시점 설정만 바꾼다.
    settings = get_settings()
    monkeypatch.setattr(settings, "llm_model", "other-model", raising=False)

    with pytest.raises(RuntimeError, match="옛 분류기"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)


def test_pipeline_version_change_alone_makes_rows_unreadable(tmp_path, monkeypatch):
    """파이프라인 버전만 올려도 같다 — 후처리·폴백 변경이 분포를 움직이기 때문이다.

    `_cs_empty_fallback` 이 실측 2.1%(284건 중 6건)를 채운다. 프롬프트도 모델도 그대로인데
    그 폴백을 끄면 CS 의 aspect 분포가 달라진다.
    """
    db = _db(
        tmp_path,
        cs_rows=[("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END))],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    monkeypatch.setattr(inputs, "CLASSIFIER_PIPELINE_VERSION", "classify_pipeline_v2")

    with pytest.raises(RuntimeError, match="옛 분류기"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)


def test_unclassified_window_does_not_trip_the_version_guard(tmp_path):
    """아직 분류를 안 돌린 DB 는 버전 문제가 아니다 — 커버리지 쪽이 잡을 일이다.

    여기서까지 세우면 "워커를 안 돌렸다"가 "프롬프트를 안 맞췄다"로 잘못 안내된다.
    """
    db = _db(
        tmp_path,
        cs_rows=[("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END))],
    )

    items, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert items == []
    assert len(documents) == 1


# ── 혼재 윈도우의 다운스트림 결과 (통합) ────────────────────────────────────
#
# 🔴 **위 단위 테스트들은 "필터가 거르는가"까지만 본다.** 걸러진 결과로 실제 검정을 돌리면
#    무슨 일이 나는지는 안 본다 — 그 자리가 비어서 "경고만 하고 통과" 설계의 오탐을
#    놓쳤다(2026-08-12 리뷰 §1). 여기서 다운스트림까지 본다.


def _mixed_window_db(tmp_path, *, past_stale: bool):
    """현재·과거 부정률이 **똑같은**(변화 없음) 리뷰 데이터. 알림이 나오면 안 되는 상태다.

    리뷰 소스인 이유: CS 는 과거 구간 aspect 가 0 이 되면 `check_coverage` 가 갭으로 잡아
    `unreliable_slots` 로 빠지지만, 리뷰는 `COVERAGE_CHECKED_SOURCES` 가 CS 전용이라 안
    잡힌다. **방어선이 없는 쪽**이라 여기서 재야 한다.

    Args:
        past_stale: True 면 과거 28일 구간만 옛 프롬프트로 분류된 상태로 만든다.
    """
    days = [date.fromordinal(WINDOW_END.toordinal() - i) for i in range(35)]
    current_start = date.fromordinal(WINDOW_END.toordinal() - 6)  # 현재 7일

    review_rows, classified = [], []
    stale_ids = set()
    for day_no, day in enumerate(days):
        for i in range(20):  # 하루 20건 × 35일 = 700건
            rid = f"RVW-{day_no:02d}-{i:02d}"
            review_rows.append((rid, "P001", "NAVER", "리뷰 원문", _at(day)))
            # 부정률을 현재·과거 똑같이 5% 로 고정한다 — 진짜 변화가 없는 데이터다.
            sentiment = -1 if i == 0 else 1
            classified.append((rid, "review", [("색상", sentiment)]))
            if day < current_start:
                stale_ids.add(rid)

    db = _db(tmp_path, review_rows=review_rows, classified=classified)
    if past_stale:
        conn = sqlite3.connect(db)
        conn.executemany(
            "UPDATE classified_item SET prompt_version = 'classify_sentiment_v3'"
            " WHERE item_id = ?",
            [(rid,) for rid in sorted(stale_ids)],
        )
        conn.commit()
        conn.close()
    return db


def test_control_window_without_stale_raises_no_alert(tmp_path):
    """대조군 — 전부 활성 버전이면 부정률 변화가 없으니 **알림 0건**이다.

    아래 혼재군과 짝이다. 이게 0건이어야 혼재군의 발화가 "데이터 탓"이 아니라
    "버전 혼재 탓"임이 성립한다.
    """
    db = _mixed_window_db(tmp_path, past_stale=False)

    items, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)
    alerts, _ = asyncio.run(
        detect_anomaly(items, documents=documents, window_end=WINDOW_END)
    )

    assert alerts == [], f"변화 없는 데이터에서 알림이 나왔다: {alerts}"


def test_mixed_window_stops_before_detection(tmp_path):
    """🔴 과거 구간만 옛 버전이면 **탐지 전에 세운다.**

    경고만 하고 통과시키면 여기서 최대 강도 오탐이 난다. 필터가 분자에만 걸려서
    `past_neg` 만 0 이 되고 `past_total` 은 그대로 남기 때문이다 — 기준선이 작아지는 게
    아니라 0 이 된다:

        대조군(전부 활성)   documents 700 / items 700  →  알림 0건
        섞임(과거=옛 버전)  documents 700 / items 140  →  past_rate 0.0000 vs
                                                          cur_rate 0.0500 로 발화 🚨

    데이터는 대조군과 **한 글자도 다르지 않고** 과거 구간의 `prompt_version` 만 다르다.
    """
    db = _mixed_window_db(tmp_path, past_stale=True)

    with pytest.raises(RuntimeError, match="옛 분류기"):
        inputs.load_inputs_from_db(WINDOW_END, db_path=db)


def test_blank_content_stale_row_does_not_deadlock_the_batch(tmp_path):
    """🔴 **불변식: 배치를 세우는 집합 ⊆ `--reclassify-stale` 이 고칠 수 있는 집합.**

    워커의 stale 조회는 `TRIM(content) <> ''` 를 요구한다. 탐지의 cutover 가드가 같은
    조건을 안 걸면, 본문이 빈 원문의 stale 분류행 하나로 **빠져나갈 길이 없는 교착**이 난다:

        워커 count_stale()       = 0   ← 재분류 대상 없음
        워커 fetch_stale_batch() = 0 rows
        배치                     = RuntimeError 로 매일 중단

    에러가 시키는 `--reclassify-stale` 은 "재분류할 문서가 없습니다"로 끝나고, 손으로
    SQL 을 치는 것 말고는 방법이 없다. 경고만 하던 때는 무해했고 fail-closed 로 바뀌면서
    교착이 됐다. (2026-08-12 리뷰 §1 후속)
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END)),
            # 본문이 공백만 남은 원문 — 워커는 이 문서를 재분류 대상으로 안 잡는다.
            ("INQ-BLANK", "P001", "COUPANG", "   ", _at(WINDOW_END)),
        ],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO classified_item (item_id, source, prompt_version) VALUES (?, ?, ?)",
        ("INQ-BLANK", "cs", "classify_aspect_v4"),
    )
    conn.commit()
    conn.close()

    # 고칠 수 없는 행이므로 세우지 않는다. 나머지는 정상적으로 읽힌다.
    items, documents = inputs.load_inputs_from_db(WINDOW_END, db_path=db)

    assert [i.item_id for i in items] == ["INQ-1"]
    assert {d["id"] for d in documents} == {"INQ-1", "INQ-BLANK"}


def test_missing_db_fails_loudly(tmp_path):
    """경로가 틀렸을 때 빈 파일을 새로 만들어 '문서 0건' 으로 통과하면 안 된다.

    그러면 배치가 아무 알림도 안 내고 **정상 종료**한다 — 조용한 무동작이 제일 나쁘다.
    """
    with pytest.raises(FileNotFoundError):
        inputs.load_inputs_from_db(WINDOW_END, db_path=str(tmp_path / "없음.db"))


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
        inputs.get_settings(), "raw_db_path", db, raising=False
    )

    summary = await daily.run_batch(
        window_end=WINDOW_END, dry_run=True, state_path=tmp_path / "state.json"
    )

    assert summary["documents"] == 1
    assert summary["items"] == 1
    assert summary["input_source"] == "load_inputs_from_db"
    # 버린 게 없으면 **빈 dict** 다 — `None`("보고 안 함")과 다른 값이다.
    assert summary["input_dropped"] == {}


@pytest.mark.asyncio
async def test_batch_summary_reports_dropped_inputs(tmp_path, monkeypatch):
    """🔴 미매핑 건수가 **배치 요약과 화면까지** 간다.

    로더가 세는 것만으로는 절반이다 — 지금까지도 세고는 있었고 `logger.warning` 으로만
    나갔다. **CronJob 로그는 아무도 안 본다**(이 저장소가 반복해서 전제해 온 사실)라,
    운영에서 상류 매핑이 밀려도 조용했다. 배선이 빠지면 그 상태로 돌아간다.

    ⚠️ 종료코드는 **안 건드린다**. 미매핑은 상류의 데이터 갭이지 우리 고장이 아니고,
       재매핑은 사람이 손으로 하는 흐름이라 배치를 세워도 할 수 있는 게 없다.
    """
    db = _db(
        tmp_path,
        cs_rows=[
            ("INQ-1", "P001", "COUPANG", "색이 달라요", _at(WINDOW_END)),
            ("INQ-2", None, "COUPANG", "매핑 없음", _at(WINDOW_END)),
            ("INQ-3", "", "NAVER", "매핑 없음", _at(WINDOW_END)),
        ],
        classified=[("INQ-1", "cs", [("색상", -1)])],
    )
    monkeypatch.setattr(inputs.get_settings(), "raw_db_path", db, raising=False)

    summary = await daily.run_batch(
        window_end=WINDOW_END, dry_run=True, state_path=tmp_path / "state.json"
    )

    assert summary["input_dropped"] == {"상품매핑 없음": 2}
    assert summary["documents"] == 1, "버린 원문이 분모에 남아 있습니다"
