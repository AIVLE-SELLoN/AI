"""담당: 지인 — sqlite ↔ Postgres 문법이 갈리는 자리의 계약(Postgres 이식 ⓑ).

여기서 잠그는 것은 **DB 없이도 확인되는 것들**이다. 실연결이 있어야만 갈리는 것
(`?`→`%s`, 널 안전 비교, TIMESTAMPTZ)은 `tests/test_raw_db_postgres*.py` 가 게이트 뒤에서 본다.

  1. `RawRow` 가 `sqlite3.Row` 와 같은 계약이다 — 위치·이름 양쪽으로 읽힌다.
  2. `upsert_sql()` 이 두 백엔드에서 같은 뜻인 표준 `ON CONFLICT` 를 낸다.
  3. `execute_ddl()` 이 "이미 있음" 만 삼키고 나머지는 올린다.
  4. `unique_column_sets()` 가 유니크 제약을 보고, `find_legacy_tables()` 가 그걸 쓴다.
  5. 두 백엔드 DDL 이 **같은 테이블·같은 컬럼**을 낸다.

⚠️ LLM·네트워크 없음. sqlite in-memory 와 가짜 연결만 쓴다.
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from app.core import raw_db, raw_schema

# ── ① RawRow 계약 ───────────────────────────────────────────────────────────


def test_raw_row_reads_like_sqlite_row():
    """🔴 위치·이름 **양쪽**이어야 한다 — 저장소가 둘 다 쓴다.

    탐지·CS 원문 조회는 `row["item_id"]`, 월간 집계는 `.fetchone()[0]` 과 언패킹이다.
    psycopg 기본값으로는 한쪽만 산다. 특히 `dict_row` 는 **순회가 값이 아니라 키를 주므로**
    언패킹 쪽이 에러 없이 컬럼 이름을 값으로 받는다 — 조용히 틀리는 쪽이라 더 나쁘다.
    """
    reference = sqlite3.connect(":memory:")
    reference.row_factory = sqlite3.Row
    expected = reference.execute("SELECT 1 AS a, 'x' AS b").fetchone()

    row = raw_db.RawRow(("a", "b"), (1, "x"))

    for probe in (expected, row):
        assert probe[0] == 1
        assert probe["b"] == "x"
        assert list(probe) == [1, "x"]  # 순회는 **값**이다
        assert len(probe) == 2
        assert list(probe.keys()) == ["a", "b"]

    with pytest.raises(IndexError):
        row["없는컬럼"]


# ── ② upsert_sql ────────────────────────────────────────────────────────────


def test_upsert_sql_uses_standard_on_conflict():
    """`INSERT OR IGNORE` · `INSERT OR REPLACE` 는 sqlite 전용이라 안 쓴다.

    Postgres 에는 그 문법이 없어 **구문 오류**다. 표준 `ON CONFLICT` 는 sqlite 3.24+ 가
    같은 뜻으로 받으므로 한 벌로 통일한다.
    """
    do_nothing = raw_db.upsert_sql("t", ("a", "b"), conflict=("a",), update=())
    do_update = raw_db.upsert_sql("t", ("a", "b"), conflict=("a",))

    for sql in (do_nothing, do_update):
        assert "INSERT OR" not in sql.upper()

    assert do_nothing.endswith("ON CONFLICT DO NOTHING")
    assert do_update.endswith("ON CONFLICT (a) DO UPDATE SET b = excluded.b")


def test_upsert_sql_actually_runs_on_sqlite():
    """조립한 문장이 sqlite 에서 **실제로 도는지**까지 본다.

    ⚠️ 문자열만 단언하면 문법이 틀려도 통과한다 — 그건 운영에서만 드러난다.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (a TEXT, b TEXT, v TEXT, UNIQUE (a, b))")

    keep = raw_db.upsert_sql("t", ("a", "b", "v"), conflict=("a", "b"), update=())
    conn.execute(keep, ("1", "2", "처음"))
    conn.execute(keep, ("1", "2", "나중"))
    assert conn.execute("SELECT v FROM t").fetchone()[0] == "처음"

    overwrite = raw_db.upsert_sql("t", ("a", "b", "v"), conflict=("a", "b"))
    conn.execute(overwrite, ("1", "2", "덮어씀"))
    assert conn.execute("SELECT v FROM t").fetchone()[0] == "덮어씀"
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1


# ── ③ execute_ddl ───────────────────────────────────────────────────────────


class _FakeConn:
    """DDL 실행만 흉내 내는 가짜 연결. 지정한 순번에서 지정한 예외를 던진다."""

    dialect = raw_db.POSTGRES

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.executed: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql: str, params=()):
        # `params` 는 안 쓴다 — DDL 에는 바인딩이 없다. 시그니처만 실제 연결과 맞춘다.
        self.executed.append(sql)
        if self._error is not None:
            raise self._error

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _pg_error(sqlstate: str) -> Exception:
    error = RuntimeError("boom")
    error.sqlstate = sqlstate  # psycopg 예외가 갖는 속성
    return error


def test_execute_ddl_swallows_concurrent_create_collision():
    """🔴 Postgres 의 `CREATE TABLE IF NOT EXISTS` 는 동시 실행에 안전하지 않다.

    검사와 생성 사이에 창이 있어서, 두 프로세스가 같은 순간에 만들면 진 쪽이
    `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`(23505)
    로 **죽는다** — "이미 있으니 넘어간다" 가 아니다. 워커가 CronJob 이라 겹쳐 뜰 수 있고,
    진 쪽이 죽으면 스케줄러가 무한 재시도한다.
    """
    conn = _FakeConn(_pg_error("23505"))

    assert raw_db.execute_ddl(conn, "CREATE TABLE IF NOT EXISTS t (a TEXT)") is False
    assert conn.rollbacks == 1
    assert conn.commits == 0


def test_execute_ddl_reraises_real_failures():
    """"이미 있음" 이 아닌 것은 **삼키지 않는다.**

    ⚠️ 여기가 뒤집히면 가드가 아니라 은폐 장치가 된다 — 권한 부족(42501)·문법 오류로
       테이블이 안 만들어졌는데 워커가 그대로 진행해 조회 단계에서 엉뚱한 곳에서 죽는다.
    """
    conn = _FakeConn(_pg_error("42501"))
    with pytest.raises(RuntimeError):
        raw_db.execute_ddl(conn, "CREATE TABLE IF NOT EXISTS t (a TEXT)")


def test_execute_ddl_commits_each_statement():
    """문장마다 커밋한다 — Postgres 는 한 문장이 실패하면 트랜잭션 전체가 abort 라,
    한 트랜잭션에 몰아 넣으면 뒤 문장이 전부 `InFailedSqlTransaction` 으로 죽는다."""
    conn = _FakeConn()
    assert raw_db.execute_ddl(conn, "CREATE TABLE IF NOT EXISTS t (a TEXT)") is True
    assert conn.commits == 1


# ── ④ 유니크 제약 가드 ──────────────────────────────────────────────────────


def _sqlite_with_classified(unique: bool) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    raw_schema.create_source_tables(conn)
    if unique:
        raw_schema.create_classified_tables(conn)
        return conn
    # 확정 DDL 과 컬럼은 같고 **UNIQUE 만 없는** 테이블 — 컬럼만 보는 가드는 통과한다.
    conn.execute(raw_schema.CLASSIFIED_ITEM_DDL)
    conn.execute(
        "CREATE TABLE classified_item_aspect ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " item_id TEXT NOT NULL REFERENCES classified_item(item_id),"
        " aspect TEXT NOT NULL, sentiment INTEGER NOT NULL, mixed_signal INTEGER)"
    )
    conn.execute(raw_schema.FAILURE_DDL)
    conn.execute(raw_schema.CURSOR_DDL)
    return conn


def test_unique_column_sets_sees_the_aspect_constraint():
    conn = _sqlite_with_classified(unique=True)
    assert frozenset({"item_id", "aspect"}) in raw_db.unique_column_sets(
        conn, "classified_item_aspect"
    )
    assert raw_db.unique_column_sets(conn, "없는테이블") == set()


def test_legacy_scan_catches_a_table_that_lost_only_its_unique_constraint():
    """🔴 컬럼만 보는 가드는 이 상태를 **통과시킨다** — 그래서 제약까지 본다.

    이 모양이 실제로 나올 수 있는 이유: 인프라가 낡은 문서로 `classified_item_aspect` 를
    먼저 세워 두면 우리 `CREATE TABLE IF NOT EXISTS` 가 조용히 건너뛴다. 그러면 워커의
    `ON CONFLICT DO NOTHING` 이 아무것도 못 막아 재분류가 같은 `(item_id, aspect)` 를
    중복 적재하고 **탐지 분자가 부푼다** — 오탐 방향이라 시끄럽지도 않다.
    """
    healthy = _sqlite_with_classified(unique=True)
    assert raw_schema.find_legacy_tables(healthy) == []

    broken = _sqlite_with_classified(unique=False)
    # 컬럼은 확정본과 같다 — 옛 가드(마커 컬럼)는 여기서 아무것도 못 잡는다.
    assert "pipeline_version" in raw_db.table_columns(broken, "classified_item")
    assert raw_schema.find_legacy_tables(broken) == ["classified_item_aspect"]


def test_missing_tables_are_not_legacy():
    """없는 테이블은 대상이 아니다 — 새로 만들면 되는 정상 상태다."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    assert raw_schema.find_legacy_tables(conn) == []


# ── ⑤ 두 백엔드 DDL 대조 ────────────────────────────────────────────────────

_CREATE_TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\((.*)\)\s*;", re.DOTALL)


def _declared_columns(ddl: str) -> tuple[str, set[str]]:
    """`CREATE TABLE` 문 → (테이블명, 컬럼명 집합). 표 수준 제약 줄은 뺀다."""
    match = _CREATE_TABLE.search(ddl)
    assert match, f"CREATE TABLE 을 못 읽었습니다: {ddl[:60]}"
    table, body = match.group(1), match.group(2)

    columns: set[str] = set()
    depth = 0
    current = ""
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            columns.add(current.strip().split()[0])
            current = ""
        else:
            current += char
    if current.strip():
        columns.add(current.strip().split()[0])
    # UNIQUE (...) · PRIMARY KEY (...) 같은 표 수준 제약은 컬럼이 아니다.
    return table, {c for c in columns if c.upper() not in {"UNIQUE", "PRIMARY", "FOREIGN", "CHECK"}}


@pytest.mark.parametrize("group", ["source", "classified"])
def test_both_dialects_declare_the_same_tables_and_columns(group: str):
    """🔴 두 판이 갈리면 **조회 단계에 가서야** 드러난다.

    `CREATE TABLE IF NOT EXISTS` 라 이미 있는 쪽은 조용히 넘어가므로, 목에만 있는 컬럼이
    생겨도 목 테스트는 전부 초록이고 운영에서 `column ... does not exist` 로 터진다.
    타입은 일부러 다르다(TEXT ↔ VARCHAR(n) 등) — 여기서 보는 것은 **구성**이다.
    """
    table = raw_schema._SOURCE_DDL if group == "source" else raw_schema._CLASSIFIED_DDL
    sqlite_side = dict(_declared_columns(d) for d in table[raw_db.SQLITE])
    postgres_side = dict(_declared_columns(d) for d in table[raw_db.POSTGRES])

    assert sqlite_side.keys() == postgres_side.keys()
    for name, columns in sqlite_side.items():
        assert columns == postgres_side[name], f"{name} 의 컬럼 구성이 갈렸습니다"


def test_postgres_view_uses_or_replace():
    """Postgres 에는 `CREATE VIEW IF NOT EXISTS` 가 없다 — 그대로 두면 구문 오류다."""
    postgres_view = raw_schema._VOC_DOCUMENT_DDL[raw_db.POSTGRES]
    assert "CREATE OR REPLACE VIEW" in postgres_view
    assert "IF NOT EXISTS" not in postgres_view
    # 두 판의 SELECT 본문은 같아야 한다 — 뷰가 갈리면 분모의 정본이 둘이 된다.
    sqlite_view = raw_schema._VOC_DOCUMENT_DDL[raw_db.SQLITE]
    assert postgres_view.split("AS", 1)[1] == sqlite_view.split("AS", 1)[1]


def test_postgres_ddl_has_no_sqlite_only_syntax():
    """`AUTOINCREMENT` 는 Postgres 에 없다(구문 오류)."""
    for statements in (
        raw_schema._SOURCE_DDL[raw_db.POSTGRES],
        raw_schema._CLASSIFIED_DDL[raw_db.POSTGRES],
    ):
        for ddl in statements:
            assert "AUTOINCREMENT" not in ddl.upper()
