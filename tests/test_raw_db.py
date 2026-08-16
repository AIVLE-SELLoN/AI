"""담당: 지인 — `app/core/raw_db.connect_readonly()` (raw DB 읽기 연결).

이 모듈이 지키는 계약 2개. **둘 다 조용히 깨지는 종류라 테스트가 유일한 방어선이다:**
  1. **읽기 전용이다** — 읽는 쪽이 원문을 고칠 수 없다(§5-2 권한 그대로).
  2. **없는 경로를 조용히 넘기지 않는다** — 기본 연결은 빈 DB 를 새로 만들어서, 조회가
     0건으로 성공하고 배치가 알림 없이 **정상 종료**한다.

Postgres 이식 1단계(2026-08-16)로 계약이 하나 늘었다:
  3. **백엔드를 가르는 것은 `RAW_DB_DSN` 하나다** — 비어 있으면 sqlite 그대로다.
     데모가 이 기본값 위에서 돌기 때문에, 이 갈림이 뒤집히면 데모가 빈 Postgres 를 본다.

⚠️ Postgres **실연결** 검증은 `tests/test_raw_db_postgres.py` 에 있고 DSN 이 있을 때만
   돈다. 여기는 DB 없이 확인할 수 있는 것만 둔다 — LLM·네트워크·DB 없음.
"""

import sqlite3

import pytest

from app.core import raw_db
from app.core.raw_db import connect_readonly, describe_target, translate_placeholders


def _seed(path) -> None:
    """읽어서 구분할 수 있는 표식 1행. 엉뚱한 파일을 열면 이 행이 없다."""
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE marker (x INTEGER)")
    conn.execute("INSERT INTO marker VALUES (1)")
    conn.commit()
    conn.close()


def test_path_with_hash_opens_the_real_file(tmp_path):
    """🔴 경로에 `#` 이 있어도 **그 DB 를 읽기 전용으로 연다.**

    `f"file:{path}?mode=ro"` 로 만들면 `#` 뒤가 URI fragment 로 잘려 **`mode=ro` 가
    통째로 날아가고**, 남은 앞부분(`.../we`)을 경로로 잡아 **빈 DB 를 새로 만든다.**
    그러면 위 계약 두 개가 **동시에** 깨진다 — 쓰기가 열리고, 경로 오타가 "문서 0건"
    으로 조용히 통과한다. `as_uri()` 가 `#` 을 `%23` 으로 인코딩해서 막는다.

    ⚠️ `#` 은 지어낸 입력이 아니다. Windows 사용자 폴더·브랜치명이 섞인 워크트리 경로에
       실제로 들어간다(`RAW_DB_PATH` 는 `.env` 로 각자 지정한다).

    세 가지를 따로 본다. 하나만 보면 옛 형태로 되돌려도 통과하는 조합이 생긴다:
      - 표식 행이 읽힌다 → **그 파일**을 열었다
      - 쓰기가 거부된다 → `mode=ro` 가 살아 있다
      - 옆에 파일이 안 생긴다 → 빈 DB 를 새로 만들지 않았다
    """
    weird_dir = tmp_path / "we#ird"
    weird_dir.mkdir()
    db = weird_dir / "raw.db"
    _seed(db)

    conn = connect_readonly(str(db))
    try:
        assert conn.execute("SELECT x FROM marker").fetchone()["x"] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO marker VALUES (2)")
    finally:
        conn.close()

    # 옛 형태는 여기에 `we` 라는 0바이트 DB 를 만든다(2026-08-11 리뷰 ③ 실측).
    assert [p.name for p in tmp_path.iterdir()] == ["we#ird"]


def test_missing_path_raises_instead_of_creating_an_empty_db(tmp_path):
    """경로가 틀리면 던진다 — 빈 DB 를 만들어 '문서 0건' 으로 통과하면 안 된다."""
    missing = tmp_path / "없음.db"

    with pytest.raises(FileNotFoundError):
        connect_readonly(str(missing))

    assert not missing.exists()


# ── 백엔드 갈림 (Postgres 이식 1단계) ────────────────────────────────────────


def test_empty_dsn_keeps_the_sqlite_path(tmp_path):
    """🔴 **기본값이 계약이다** — `RAW_DB_DSN` 이 비면 sqlite 로 간다.

    데모가 이 경로 위에서 돈다. 갈림이 뒤집히면 배치가 빈 Postgres 를 읽고 **문서 0건
    으로 정상 종료**하는데, 그건 이 파일 계약②가 막으려던 바로 그 모양이다.
    """
    db = tmp_path / "raw.db"
    _seed(db)

    conn = connect_readonly(str(db), dsn="")
    try:
        assert conn.execute("SELECT x FROM marker").fetchone()["x"] == 1
    finally:
        conn.close()


def test_dsn_switches_to_postgres_without_touching_the_file(tmp_path, monkeypatch):
    """DSN 이 있으면 sqlite 경로를 **아예 안 본다.**

    파일 존재 검사가 먼저 돌면 DSN 을 넣은 환경이 `FileNotFoundError` 로 죽는다 —
    `RAW_DB_PATH` 에 DSN 을 넣지 못하는 이유가 그 검사였다.
    """
    opened = {}

    class _Stub:
        dialect = raw_db.POSTGRES

        def __init__(self, dsn):
            opened["dsn"] = dsn

    monkeypatch.setattr(raw_db, "PostgresConnection", _Stub)

    conn = connect_readonly(str(tmp_path / "없음.db"), dsn="postgresql://x@h/rawdb")

    assert isinstance(conn, _Stub)
    assert opened["dsn"] == "postgresql://x@h/rawdb"


def test_describe_target_never_leaks_the_password():
    """🔴 오류 메시지·로그에 raw DB 비밀번호를 싣지 않는다.

    `_require_classified_tables` 가 "어느 DB 를 봤는지" 를 메시지에 넣는데, DSN 을
    그대로 찍으면 그 문자열이 배치 요약·로그·PR 본문으로 그대로 나간다.
    """
    described = describe_target(dsn="postgresql://sellon_ai:S3cr3t@db.internal:5432/rawdb")

    assert "S3cr3t" not in described
    assert "sellon_ai" not in described
    assert "db.internal:5432/rawdb" in described


def test_describe_target_drops_query_credentials():
    """자격증명이 `@` 가 아니라 쿼리 인자로 오는 형태도 막는다."""
    described = describe_target(dsn="postgresql://db.internal/rawdb?password=S3cr3t")

    assert "S3cr3t" not in described


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT ? , ?", "SELECT %s , %s"),
        # 리터럴 안의 `?` 는 바인딩이 아니다 — 바꾸면 개수가 어긋난다.
        ("SELECT '? 물음표', ?", "SELECT '? 물음표', %s"),
        # `''` 이스케이프를 지나도 리터럴 판정이 유지돼야 한다.
        ("SELECT 'it''s ?', ?", "SELECT 'it''s ?', %s"),
        # psycopg 는 인자를 넘길 때 `%` 를 서식 문자로 읽는다.
        ("SELECT x WHERE y LIKE '%키워드%'", "SELECT x WHERE y LIKE '%%키워드%%'"),
    ],
)
def test_translate_placeholders(sql, expected):
    """`?` 바인딩 SQL 을 psycopg 의 `%s` 로 옮긴다. **한 벌로 쓰는 근거다.**"""
    assert translate_placeholders(sql) == expected


def test_null_safe_comparison_spelling_works_on_sqlite():
    """🔴 `IS NOT DISTINCT FROM` 이 sqlite 에서도 `IS` 와 같은 뜻이어야 한다.

    두 방언의 교집합이라 이 철자 하나로 쓰는데, **sqlite 3.39 미만이면 구문 오류**다.
    그 환경에서는 탐지 배치와 분류 워커가 통째로 못 돈다 — 팀원 로컬마다 파이썬이
    다르므로 "내 PC 에서 되니까" 로 넘길 수 없어 테스트로 잠근다.
    (호스트 3.49 · 런타임 이미지 python:3.12-slim 3.46 실측, 2026-08-16)
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t (a TEXT)")
        conn.execute("INSERT INTO t VALUES (NULL)")
        # NULL 끼리도 참이어야 한다 — `=` 는 여기서 NULL 을 돌려준다.
        assert conn.execute(
            "SELECT COUNT(*) FROM t WHERE a IS NOT DISTINCT FROM ?", (None,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM t WHERE NOT (a IS NOT DISTINCT FROM ?)", ("v5",)
        ).fetchone()[0] == 1
    finally:
        conn.close()
