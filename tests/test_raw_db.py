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


@pytest.mark.parametrize(
    "dsn",
    [
        # URI — 자격증명이 `@` 앞에
        "postgresql://sellon_ai:S3cr3t@db.internal:5432/rawdb",
        # URI — 자격증명이 쿼리 인자로
        "postgresql://db.internal/rawdb?password=S3cr3t",
        # 🔴 키워드 형식. psycopg 가 이것도 받는데 `@`·`?` 가 없어서, 문자열을 잘라내는
        #    방식으로는 **통째로 샌다**(2026-08-16 용준님 리뷰 §2, 실측).
        "host=db.internal dbname=rawdb user=sellon_ai password=S3cr3t",
        "dbname=rawdb password=S3cr3t",
    ],
)
def test_describe_target_never_leaks_the_password(dsn):
    """🔴 오류 메시지·로그에 raw DB 비밀번호를 싣지 않는다 — **DSN 형식 불문.**

    `_require_classified_tables` 가 "어느 DB 를 봤는지" 를 메시지에 넣는데, DSN 이 새면
    그 문자열이 배치 요약·로그로 그대로 나가 회수가 안 된다.

    ⚠️ **URI 만 재면 안 된다.** 세 번째 케이스가 옛 구현에서 실제로 통째로 샜다 —
       한 형식만 잠그면 나머지 형식으로 같은 사고가 그대로 재발한다.
    """
    described = describe_target(dsn=dsn)

    assert "S3cr3t" not in described
    assert "sellon_ai" not in described


def test_describe_target_reports_where_it_looked():
    """가리되 **쓸모는 남긴다** — 어느 호스트·DB 를 봤는지는 나와야 진단이 된다."""
    assert describe_target(
        dsn="postgresql://u:pw@db.internal:5432/rawdb"
    ) == "Postgres db.internal:5432/rawdb"
    assert describe_target(
        dsn="host=db.internal dbname=rawdb user=u password=pw"
    ) == "Postgres db.internal/rawdb"


def test_describe_target_survives_an_unreadable_dsn():
    """🔴 읽기 실패해도 **던지지 않고, 원문도 안 싣는다.**

    이 함수는 인자 자리에서 항상 평가되므로(`_require_classified_tables(conn,
    describe_target(...))`) 여기서 던지면 **진단이 진짜 원인을 가린다.** 그렇다고 폴백에
    DSN 을 넣으면 막으려던 것이 폴백으로 새어나간다 — 둘 다 안 되게 잠근다.
    """
    described = describe_target(dsn="=== password=S3cr3t ===")

    assert "S3cr3t" not in described
    assert "Postgres" in described


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

    sqlite·Postgres 양쪽에서 같은 뜻이라 이 철자 하나로 쓰는데, **sqlite 3.39 미만이면 구문 오류**다.
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


def test_connection_error_types_covers_both_psycopg_bases():
    """🔴 Postgres 실패가 두 베이스로 갈린다 — 한쪽만 잡으면 절반이 샌다.

    호출부(`daily.main()` 의 exit 2 분류, `service.generate_recommendation` 의 degrade)가
    이 목록으로 "환경 탓" 을 가른다. `psycopg.OperationalError` 로 좁히면 **DSN 형식 오타 ·
    DB 이름 틀림 · 뷰 없음 · GRANT 누락**이 전부 빠져나간다(전부 `ProgrammingError` 계열).
    하필 그 넷이 첫 연동에서 제일 잦다.

    ⚠️ **`FileNotFoundError`·`RuntimeError` 가 아닌 것까지 같이 본다.** 그게 이 함수가
       존재하는 이유이므로(둘 중 하나였다면 호출부가 이미 잡고 있었다), 그 전제가
       psycopg 버전이 올라가며 바뀌면 여기서 먼저 알려준다.
    """
    import psycopg

    covered = raw_db.connection_error_types()
    assert covered, "드라이버가 있는데 빈 튜플이면 호출부가 아무것도 새로 못 잡는다"

    for code, kind in [
        ("28P01", "비밀번호 틀림"),
        ("3D000", "DB 이름 틀림"),
        ("42P01", "뷰·테이블 없음"),
        ("42501", "GRANT 누락"),
    ]:
        exc_type = psycopg.errors.lookup(code)
        assert issubclass(exc_type, covered), f"{kind}({code}) 이 안 잡힌다: {exc_type}"
        assert not issubclass(exc_type, FileNotFoundError | RuntimeError), (
            f"{kind}({code}) 이 이미 호출부 분기에 걸린다면 이 함수가 필요 없다"
        )
