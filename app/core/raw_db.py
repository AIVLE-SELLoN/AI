"""raw DB(`cs`·`reviews`·`classified_item`) 읽기 연결. **읽는 쪽 공용.**

AI 노드는 원본 DB 에 **읽기 권한만** 있다(「Raw DB 스키마 확정 (8/7)」 §5-2 — 쓰기는
분류 워커의 `classified_item` 계열뿐이고, 서비스 DB 는 main server 단독 소유다).

⚠️ 이게 `app/core/` 에 있는 이유: **읽는 쪽이 둘로 갈려 있다.** 탐지 배치
(`app/batch/daily.py`)와 CS 원문 조회(`app/core/inquiries.py`)가 같은 파일을 여는데,
한쪽에만 두면 나머지가 연결 문자열을 다시 적게 되고 `mode=ro` 같은 조건이 조용히
갈린다 — `raw_schema.py` 가 core 에 있는 것과 같은 사유다.

── 백엔드 2종 (Postgres 이식 1단계, 2026-08-16) ─────────────────────────────────
운영 raw DB 는 **Postgres**(`rawdb`)이고 로컬·목 파이프라인은 **sqlite 파일**이다.
둘을 가르는 것은 `RAW_DB_DSN` 하나다 — 값이 있으면 Postgres, 없으면 sqlite.

    RAW_DB_DSN 있음  →  Postgres            (운영 · 로컬 compose 검증)
    RAW_DB_DSN 없음  →  sqlite raw_db_path  (기본값. 데모가 도는 경로)

⚠️ **`RAW_DB_PATH` 에 DSN 을 넣는 방식은 안 된다.** 아래 sqlite 경로가 `Path.exists()`
   를 먼저 보기 때문에 DSN 은 `FileNotFoundError` 로 떨어진다. 키를 나눈 이유이고,
   기본값이 비어 있으므로 **아무것도 설정하지 않으면 동작이 이전과 완전히 같다.**

🔴 **sqlite 는 `sqlite3.Connection` 을 그대로 돌려주고 Postgres 만 감싼다 — 의도적
   비대칭이다.** 양쪽을 다 감싸면 코드는 예뻐지지만 데모가 도는 경로가 바뀐다:
   `run_monthly_oracle_eval` 이 쓰는 `Connection.backup()`, 읽기 전용 위반이
   `sqlite3.OperationalError` 로 오는 것(`tests/test_raw_db.py` 가 계약으로 고정),
   `sqlite3.Row` 의 위치 접근이 전부 래퍼를 통과해야 한다. **1단계의 조건이 "데모를
   안 건드린다" 이므로** 이식은 새 경로에만 코드를 얹는다.

두 백엔드에 걸친 SQL 은 아래 규칙으로 **한 벌만** 쓴다. sqlite 와 Postgres 는
문법이 다른 곳이 여기 셋뿐이라, 나머지 코드는 어느 DB 인지 몰라도 된다:
  1. **바인딩은 `?`** — Postgres 래퍼가 `%s` 로 옮긴다(`translate_placeholders`).
  2. **널 안전 비교는 `IS NOT DISTINCT FROM`** — sqlite 의 `IS` 와 같은 뜻이고 3.39+
     에서 이 철자를 그대로 받는다(호스트 3.49 · 이미지 3.46 실측). Postgres 는 `IS` 를
     널 안전 비교로 안 쓰므로 이쪽 철자가 유일한 교집합이다.
  3. **스키마 조회(`PRAGMA`·`sqlite_master` ↔ `information_schema`)는 이 모듈의
     `table_columns()`·`existing_tables()` 를 쓴다** — 여기서 갈라 준다.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from app.config import get_settings

SQLITE = "sqlite"
POSTGRES = "postgres"


def translate_placeholders(sql: str) -> str:
    """`?` 바인딩 SQL → psycopg 의 `%s` 바인딩 SQL.

    두 가지를 한 번에 한다:
      - 문자열 리터럴 **밖**의 `?` 만 `%s` 로 바꾼다.
      - 모든 `%` 를 `%%` 로 escape 한다 — psycopg 는 인자를 넘길 때 `%` 를 서식
        문자로 읽으므로, `LIKE '%키워드%'` 같은 리터럴이 들어오면 그대로는 터진다.

    리터럴 판정은 작은따옴표 토글이다. SQL 이 리터럴 안의 `'` 를 `''` 로 이스케이프하므로
    (토글 off → on) 이 방식으로 정확히 맞는다.

    ⚠️ 리터럴 안의 `?` 는 **바꾸면 안 된다.** 지금 우리 SQL 에는 없지만, 있는데 바뀌면
       바인딩 개수가 어긋나 `ProgrammingError` 가 나거나 — 더 나쁘게 — 엉뚱한 값이
       비교된다.
    """
    out: list[str] = []
    in_literal = False
    for ch in sql:
        if ch == "'":
            in_literal = not in_literal
            out.append(ch)
        elif ch == "%":
            out.append("%%")
        elif ch == "?" and not in_literal:
            out.append("%s")
        else:
            out.append(ch)
    return "".join(out)


class PostgresConnection:
    """`sqlite3.Connection` 처럼 쓰는 Postgres 읽기 연결.

    호출부가 아는 것은 `execute(sql, params) -> 커서` 와 `close()` 둘뿐이다 — sqlite
    쪽과 같은 모양이라 조회 코드가 백엔드를 몰라도 된다. 행은 컬럼명으로 읽는다
    (`row["item_id"]`, psycopg 의 `dict_row`).

    ⚠️ **execute 마다 새 커서를 만든다.** sqlite 의 `Connection.execute()` 가 그렇고,
       커서를 재사용하면 앞 결과를 다 읽기 전에 다음 질의를 돌렸을 때 조용히 결과가
       사라진다(`fetch_linked_inquiries` 가 청크마다 execute 한다).
    """

    dialect = POSTGRES

    def __init__(self, dsn: str) -> None:
        import psycopg
        from psycopg.rows import dict_row

        self._conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        # 🔴 **읽기 전용을 세션에도 건다.** 운영에서는 GRANT 가 막지만(§5-2), 로컬
        #    compose 는 우리가 superuser 라 아무것도 안 막는다 — sqlite 쪽 `mode=ro`
        #    가 지키던 계약①(읽는 쪽이 원문을 못 고친다)이 백엔드를 바꾸는 순간
        #    조용히 사라지는 자리다. 위반은 `psycopg.errors.ReadOnlySqlTransaction`.
        self._conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")

    def execute(self, sql: str, params: Iterable[Any] = ()) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(translate_placeholders(sql), tuple(params))
        return cursor

    def close(self) -> None:
        self._conn.close()


RawDbConnection = sqlite3.Connection | PostgresConnection
"""조회 코드가 받는 연결. 백엔드별 타입을 호출부 시그니처에 흘리지 않으려고 둔다."""


def dialect_of(conn: RawDbConnection) -> str:
    """이 연결이 어느 DB 인지. `SQLITE` / `POSTGRES`."""
    return getattr(conn, "dialect", SQLITE)


def connect_readonly(
    db_path: str | None = None, *, dsn: str | None = None
) -> RawDbConnection:
    """raw DB 를 **읽기 전용**으로 연다. sqlite 파일이면 없을 때 만들지 않고 던진다.

    `dsn`(또는 `settings.raw_db_dsn`)이 있으면 Postgres 로 붙고, 없으면 sqlite 파일을
    연다. **기본값은 sqlite** 라 설정을 안 건드리면 동작이 이전과 같다.

    sqlite 를 `mode=ro` 로 여는 이유가 둘이다:
      1. 권한 그대로다 — 읽는 쪽이 원문을 고칠 일이 없다.
      2. **경로 오타를 조용히 넘기지 않는다.** 기본 연결은 없는 경로에 빈 DB 를 새로
         만들어서, 조회가 0건으로 성공하고 배치는 알림 없이 정상 종료한다.
    Postgres 에서는 1 을 GRANT(+세션 read-only)가, 2 를 접속 실패가 대신한다 —
    없는 DB 에 붙으면 psycopg 가 `OperationalError` 로 던지지 빈 DB 를 만들지 않는다.

    Args:
        db_path: sqlite DB 경로. 기본은 `settings.raw_db_path`.
        dsn: Postgres 접속 문자열. 기본은 `settings.raw_db_dsn`.

    Raises:
        FileNotFoundError: sqlite 파일이 없을 때. 목 파이프라인은 `scripts/mock_producer.py`
            가 원문을, `scripts/classification_worker.py` 가 분류 결과를 채운다.
    """
    dsn = dsn if dsn is not None else get_settings().raw_db_dsn
    if dsn:
        return PostgresConnection(dsn)

    path = Path(db_path or get_settings().raw_db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(
            f"raw DB 가 없습니다: {path} — 목 파이프라인은 scripts/mock_producer.py 로"
            " 원문을 적재한 뒤 scripts/classification_worker.py 로 분류해야 합니다."
        )

    # ⚠️ **`as_uri()` 로 만든다.** `f"file:{path}?mode=ro"` 는 경로에 `#` 이 있으면
    #    그 뒤가 URI fragment 로 잘려 **`mode=ro` 가 통째로 날아간다.** 그러면 남은
    #    앞부분을 경로로 잡고 **빈 DB 를 새로 만들어** 위 두 이유가 동시에 깨진다
    #    (실측: `.../we#ird/raw.db` → `.../we` 라는 0바이트 파일 생성).
    #    `as_uri()` 가 `#` 을 `%23` 으로 인코딩한다. (2026-08-11 리뷰 ③)
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def describe_target(db_path: str | None = None, *, dsn: str | None = None) -> str:
    """지금 연결이 가리키는 곳을 **사람이 읽을 수 있게**. 오류 메시지·로그용.

    🔴 **DSN 의 비밀번호를 절대 싣지 않는다.** 이 문자열은
       `daily._require_classified_tables()` 의 `RuntimeError` 로 나가 배치 로그·요약에
       박히므로, 한 번 새면 회수가 안 된다. 필요한 정보는 "어느 DB 를 봤나" 뿐이라
       host·port·dbname 만 남기고 나머지는 버린다.
       (2026-08-16 `Settings` 진단에서 `input` 을 뺀 것과 같은 사유)

    🔴 **`@`·`?` 로 잘라내는 방식은 쓰지 않는다 — 절반만 막힌다.** psycopg 는 URI 와
       키워드, **두 형식을 다 받는다.** 문자열을 직접 자르면 키워드 형식이 통째로 샌다:

           postgresql://u:pw@host:5432/rawdb              →  가려짐
           host=10.0.0.5 dbname=rawdb user=u password=pw  →  **전체 노출** 🔴

       그래서 파싱을 psycopg 에 맡긴다(`conninfo_to_dict` 가 두 형식을 다 읽는다).
       인프라가 어느 형식으로 줄지 모르므로 이건 가정이 아니라 대비다.
       (2026-08-16 용준님 리뷰 §2, 실측)

    ⚠️ **읽기에 실패해도 절대 원문을 되돌려주지 않는다.** 폴백에 DSN 을 넣으면 막으려던
       것이 폴백으로 새어나간다. 그리고 이 함수는 인자 자리에서 **항상 평가**되므로
       (`_require_classified_tables(conn, describe_target(...))`) 여기서 던지면 **진단이
       원인을 가린다** — 실제로는 `connect_readonly()` 가 먼저 붙어 잘못된 DSN 은 그쪽에서
       걸리지만, 진단 함수가 스스로 터지는 경로는 남겨두지 않는다.
    """
    dsn = dsn if dsn is not None else get_settings().raw_db_dsn
    if not dsn:
        return str(db_path or get_settings().raw_db_path)

    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    try:
        info = conninfo_to_dict(dsn)
    except psycopg.Error:
        return "Postgres (DSN 형식을 읽지 못했습니다)"

    host = info.get("host") or "?"
    port = info.get("port")
    dbname = info.get("dbname") or "?"
    return f"Postgres {f'{host}:{port}' if port else host}/{dbname}"


# ── 스키마 조회 ──────────────────────────────────────────────────────────────
#
# 두 DB 의 문법이 완전히 다른 자리다(`PRAGMA`·`sqlite_master` ↔ `information_schema`).
# 가드(`daily._require_classified_tables` · `raw_schema.find_legacy_tables`)가 이걸
# 직접 쓰면 Postgres 에서 **가드 자신이 구문 오류로 죽는다** — 스키마가 멀쩡한지
# 확인하려던 코드가 먼저 터지는 모양이라, 원인이 메시지에 안 드러난다.


def table_columns(conn: RawDbConnection, table: str) -> set[str]:
    """그 테이블의 컬럼 이름. **테이블이 없으면 빈 집합**이다(예외 아님).

    "없음" 과 "있는데 컬럼이 옛것" 을 호출부가 갈라야 하기 때문이다
    (`raw_schema.find_legacy_tables`).
    """
    if dialect_of(conn) == POSTGRES:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
        return {row["column_name"] for row in rows}

    # PRAGMA 는 바인딩을 못 받는다. `table` 은 우리 코드의 리터럴(LEGACY_MARKERS 키)
    # 이라 외부 입력이 아니다.
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def existing_tables(conn: RawDbConnection, names: Iterable[str]) -> set[str]:
    """`names` 중 **실제로 있는 테이블** 이름. 뷰는 세지 않는다.

    뷰를 빼는 이유: 호출부가 이걸로 "워커가 돌았는가" 를 판정하는데, 뷰는 원문만
    있어도 만들어져 있어서 같이 세면 분류 결과가 없는 DB 가 통과한다.
    """
    names = tuple(names)
    if not names:
        return set()
    placeholders = ",".join("?" * len(names))

    if dialect_of(conn) == POSTGRES:
        rows = conn.execute(
            "SELECT table_name AS name FROM information_schema.tables"
            " WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
            f" AND table_name IN ({placeholders})",
            names,
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
            names,
        ).fetchall()
    return {row["name"] for row in rows}
