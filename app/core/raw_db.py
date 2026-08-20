"""raw DB(`cs`·`reviews`·`classified_item`) 연결. 읽는 쪽·쓰는 쪽 공용.

AI 노드가 쓰는 것은 자기 소유 4개뿐이고 원문(`cs`·`reviews`)과 카탈로그는 main server
소유라 읽기만 한다. **그 경계를 DB 는 안 지켜 준다** — 인프라가 raw DB RW 를 전면 부여해서,
남는 방어선은 읽는 쪽의 세션 read-only(`connect_readonly`)와 쓰기 SQL 의 대상 테이블을
고정하는 테스트(`tests/test_raw_db_write_scope.py`) 둘뿐이다.

core 에 있는 이유는 읽는 쪽이 탐지 배치와 CS 원문 조회로 갈려 있어서다 — 한쪽에만 두면
나머지가 연결 문자열을 다시 적게 되고 `mode=ro` 같은 조건이 조용히 갈린다.

백엔드 2종
----------
운영은 Postgres(`rawdb`), 로컬·목은 sqlite 파일이고 가르는 것은 `RAW_DB_HOST` 하나다.
기본값이 비어 있어 아무것도 설정하지 않으면 동작이 이전과 같다. `RAW_DB_PATH` 에 접속
정보를 넣는 방식은 안 된다 — sqlite 경로가 `Path.exists()` 를 먼저 봐서
`FileNotFoundError` 로 떨어진다.

접속 문자열은 원자값에서 우리가 조립한다 — 남이 준 것을 파싱해 쓰지 않는다. 공유하는 것은
host·port·dbname·user·password 라는 **사실**이고, 그것을 어떤 문자열로 만드는지는 각자의
몫이다.

sqlite 는 `sqlite3.Connection` 을 그대로 돌려주고 Postgres 만 감싼다 — **의도적 비대칭이다.**
양쪽을 다 감싸면 데모가 도는 경로가 바뀐다(`Connection.backup()`, 읽기 전용 위반이
`sqlite3.OperationalError` 로 오는 계약, `sqlite3.Row` 의 위치 접근이 전부 래퍼를 통과한다).

두 백엔드에 걸친 SQL 은 아래 규칙으로 한 벌만 쓴다. 문법이 다른 곳이 셋뿐이라 나머지 코드는
어느 DB 인지 몰라도 된다:
  1. 바인딩은 `?` — Postgres 래퍼가 `%s` 로 옮긴다(`translate_placeholders`).
  2. 널 안전 비교는 `IS NOT DISTINCT FROM` — 양쪽에서 같은 뜻인 유일한 철자다.
  3. 스키마 조회는 `table_columns()`·`existing_tables()` 경유 — 여기서 갈라 준다.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from app.config import Settings, get_settings

SQLITE = "sqlite"
POSTGRES = "postgres"


def conninfo_from_settings(settings: Settings | None = None) -> str:
    """raw DB 접속 원자값 → psycopg 접속 문자열. 호스트가 비면 빈 문자열(= sqlite).

    f-string 으로 조립하지 말 것 — `make_conninfo` 가 이스케이프를 한다. 값에 공백·
    작은따옴표·역슬래시가 들어가면 키워드 형식이 통째로 어긋나는데(비밀번호가 그런 자리다)
    손으로 붙이면 인증 실패로만 보이고 원인이 안 드러난다.

    빈 값은 넘기지 않고 뺀다 — libpq 는 빈 문자열을 "빈 비밀번호를 쓰겠다" 로 읽어
    `.pgpass` 조회를 막는다. `sslmode` 와 `connect_timeout` 은 항상 싣는다: 전자를 빼면
    기본값 `prefer` 로 떨어져 **서버가 거부할 때 조용히 평문으로 붙고**, 후자를 빼면 무한
    대기가 되어 접속 못 하는 배치가 CronJob 자리를 130초씩 잡는다.
    """
    settings = settings if settings is not None else get_settings()
    if not settings.raw_db_host:
        return ""

    from psycopg.conninfo import make_conninfo

    return make_conninfo(
        host=settings.raw_db_host,
        port=settings.raw_db_port,
        dbname=settings.raw_db_name,
        user=settings.raw_db_username,
        password=settings.raw_db_password or None,
        sslmode=settings.raw_db_sslmode,
        sslrootcert=settings.raw_db_sslrootcert or None,
        connect_timeout=settings.raw_db_connect_timeout,
    )


def translate_placeholders(sql: str) -> str:
    """`?` 바인딩 SQL → psycopg 의 `%s` 바인딩 SQL.

    문자열 리터럴 밖의 `?` 만 바꾸고, 모든 `%` 를 `%%` 로 escape 한다(psycopg 가 인자를
    넘길 때 `%` 를 서식 문자로 읽어서 `LIKE` 패턴이 그대로는 터진다).

    리터럴 판정은 작은따옴표 토글이다 — SQL 이 리터럴 안의 따옴표를 두 번 겹쳐 이스케이프
    하므로 이 방식으로 정확히 맞는다. 리터럴 안의 `?` 를 바꾸면 바인딩 개수가 어긋나
    `ProgrammingError` 가 나거나, 더 나쁘게는 엉뚱한 값이 비교된다.
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


def upsert_sql(
    table: str,
    columns: Sequence[str],
    *,
    conflict: Sequence[str],
    update: Sequence[str] | None = None,
) -> str:
    """멱등 INSERT 문. 두 백엔드에서 글자 그대로 같다.

    `INSERT OR IGNORE`·`INSERT OR REPLACE` 는 sqlite 전용이라 못 쓴다 — 표준 `ON CONFLICT`
    는 sqlite 3.24+ 가 같은 뜻으로 받는다. **두 형태의 치환은 기계적이지 않다**: 전자는
    지우고 다시 넣는 것이고 후자는 제자리 갱신이라, 자식이 `ON DELETE CASCADE` 로 붙어
    있으면 앞에서만 자식이 사라진다. 우리 스키마엔 CASCADE 가 없어 지금은 동작이 같지만,
    새로 거는 순간 이 문장이 거짓이 된다.

    Args:
        conflict: 충돌 판정 키(=PK 또는 유니크 조합).
        update: 충돌 시 덮어쓸 컬럼. `None` 이면 `conflict` 를 뺀 나머지 전부,
            빈 시퀀스면 `DO NOTHING`.
    """
    placeholders = ", ".join("?" * len(columns))
    head = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    targets = list(update) if update is not None else [c for c in columns if c not in conflict]
    if not targets:
        return f"{head} ON CONFLICT DO NOTHING"
    assignments = ", ".join(f"{c} = excluded.{c}" for c in targets)
    return f"{head} ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {assignments}"


class RawRow:
    """`sqlite3.Row` 와 같은 모양의 행. 위치·이름 양쪽으로 읽힌다.

    psycopg 기본 행 타입으로는 코드가 한 벌로 안 선다 — 저장소의 조회 코드가 이름·튜플
    언패킹·위치를 섞어 쓰는데 `dict_row` 는 앞쪽만, `tuple_row` 는 뒤쪽만 산다. 게다가
    `dict` 를 순회하면 값이 아니라 키가 나오므로 언패킹 쪽은 **에러 없이 컬럼 이름을 값으로
    받아 조용히 틀린다.** 같은 계약을 흉내 내면 호출부를 한 글자도 안 바꾸고 두 백엔드가
    같은 코드를 탄다.

    순회는 값을 준다. dict 가 필요하면 `dict(zip(row.keys(), row))` 를 쓸 것.
    """

    __slots__ = ("_names", "_values")

    def __init__(self, names: tuple[str, ...], values: Sequence[Any]) -> None:
        self._names = names
        self._values = tuple(values)

    def __getitem__(self, key: int | str | slice) -> Any:
        if isinstance(key, str):
            try:
                return self._values[self._names.index(key)]
            except ValueError:
                raise IndexError(key) from None  # sqlite3.Row 와 같은 예외 타입
        return self._values[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._names)

    def __repr__(self) -> str:
        return f"RawRow({dict(zip(self._names, self._values))!r})"


def _raw_row_factory(cursor: Any) -> Any:
    """psycopg 행 팩토리 — 결과 행을 `RawRow` 로 만든다."""
    description = cursor.description
    if description is None:  # INSERT/DDL 처럼 결과 집합이 없는 문장
        return lambda values: values
    names = tuple(column.name for column in description)
    return lambda values: RawRow(names, values)


class PostgresConnection:
    """`sqlite3.Connection` 처럼 쓰는 Postgres 연결.

    호출부가 아는 것은 `execute` · `executemany` · `commit` · `rollback` · `close` 로
    sqlite 쪽과 같은 모양이라, 조회·적재 코드가 백엔드를 몰라도 된다.

    execute 마다 새 커서를 만든다 — 재사용하면 앞 결과를 다 읽기 전에 다음 질의를 돌렸을 때
    조용히 결과가 사라진다(`fetch_linked_inquiries` 가 청크마다 execute 한다).

    **읽기 연결만 autocommit 이다.** 쓰기 연결에서 켜면 워커 `persist_batch()` 의 "적재 +
    실패 기록 + 커서 전진을 한 트랜잭션으로" 계약이 조용히 깨져, 중간에 죽으면 커서만
    전진하고 dead-letter 가 빠져 그 건이 영구 유실된다.
    """

    dialect = POSTGRES

    def __init__(self, dsn: str, *, readonly: bool = True) -> None:
        import psycopg

        self._conn = psycopg.connect(
            dsn, autocommit=readonly, row_factory=_raw_row_factory
        )
        self.readonly = readonly
        if readonly:
            # 인프라가 RW 를 전면 부여해 운영에서도 GRANT 가 아무것도 안 막으므로,
            # sqlite 쪽 `mode=ro` 가 지키던 계약이 이 한 줄로만 남아 있다.
            # 위반은 `psycopg.errors.ReadOnlySqlTransaction`.
            self._conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")

    def execute(self, sql: str, params: Iterable[Any] = ()) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(translate_placeholders(sql), tuple(params))
        return cursor

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> Any:
        cursor = self._conn.cursor()
        cursor.executemany(translate_placeholders(sql), [tuple(p) for p in seq_of_params])
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


RawDbConnection = sqlite3.Connection | PostgresConnection
"""조회 코드가 받는 연결. 백엔드별 타입을 호출부 시그니처에 흘리지 않으려고 둔다."""


def dialect_of(conn: RawDbConnection) -> str:
    """이 연결이 어느 DB 인지. `SQLITE` / `POSTGRES`."""
    return getattr(conn, "dialect", SQLITE)


def connection_error_types() -> tuple[type[BaseException], ...]:
    """raw DB 를 환경 탓에 못 열거나 못 읽는 것을 뜻하는 예외 타입.

    sqlite 는 `connect_readonly()` 가 `FileNotFoundError` 하나로 모아 주지만 Postgres 는
    드라이버 계층이 그대로 올라온다. `psycopg.Error` 는 `FileNotFoundError` 도
    `RuntimeError` 도 `OSError` 도 아니라서 두 호출부의 기존 분기를 그냥 통과한다 — 배치는
    exit 1 + raw traceback, REST 는 500 이 된다. 호출부가 각자 적지 않고 여기서 주는 이유는
    `app/` 이 psycopg 를 직접 import 하면 드라이버 없는 환경의 import 가 깨지기 때문이다.

    **`psycopg.Error` 전부다** — `OperationalError` 만으로는 절반이 샌다:

        OperationalError   DB 미기동 · 호스트 오타 · 비밀번호 틀림
        ProgrammingError   DSN 형식 오타 · DB 이름 틀림 · 뷰/테이블 없음 · GRANT 누락

    뒷줄이 하필 이식에서 제일 잦을 것들이다. 대가로 우리가 쓴 SQL 의 버그도
    `ProgrammingError` 라 환경 문제로 오분류되는데, 우리 SQL 은 테스트가 먼저 보고 뷰·GRANT
    는 테스트가 볼 수 없다는 판단이다(두 호출부 모두 사유를 전문으로 남긴다).

    드라이버가 없으면 빈 튜플이라 아무것도 새로 잡지 않는다.
    """
    try:
        import psycopg
    except ModuleNotFoundError:
        return ()
    return (psycopg.Error,)


def connect_readonly(
    db_path: str | None = None, *, dsn: str | None = None
) -> RawDbConnection:
    """raw DB 를 읽기 전용으로 연다. sqlite 파일이면 없을 때 만들지 않고 던진다.

    sqlite 를 `mode=ro` 로 여는 이유가 둘이다 — 읽는 쪽이 원문을 고칠 일이 없고, **경로
    오타를 조용히 안 넘긴다**(기본 연결은 없는 경로에 빈 DB 를 만들어서 조회가 0건으로
    성공하고 배치가 알림 없이 정상 종료한다). Postgres 에서는 앞을 세션 read-only 가, 뒤를
    접속 실패가 대신한다.

    Args:
        db_path: sqlite DB 경로. 기본은 `settings.raw_db_path`.
        dsn: Postgres 접속 문자열. 기본은 `conninfo_from_settings()` 가 조립한 값. 빈
            문자열을 명시하면 원자값이 설정돼 있어도 sqlite 로 간다. 문자열을 직접 넘기면
            `connect_timeout` 도 직접 넣어야 한다 — 기본값 주입은
            `conninfo_from_settings()` 한 곳에서만 한다.

    Raises:
        FileNotFoundError: sqlite 파일이 없을 때.
    """
    dsn = dsn if dsn is not None else conninfo_from_settings()
    if dsn:
        return PostgresConnection(dsn)

    path = Path(db_path or get_settings().raw_db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(
            f"raw DB 가 없습니다: {path} — 목 파이프라인은 scripts/mock_producer.py 로"
            " 원문을 적재한 뒤 scripts/classification_worker.py 로 분류해야 합니다."
        )

    # `as_uri()` 로 만든다. 직접 조립하면 경로에 `#` 이 있을 때 그 뒤가 URI fragment 로
    # 잘려 `mode=ro` 가 통째로 날아가고, 남은 앞부분을 경로로 잡아 빈 DB 를 새로 만들어
    # 위 두 이유가 동시에 깨진다.
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_readwrite(db_path: str | None = None, *, dsn: str | None = None) -> RawDbConnection:
    """raw DB 를 쓰기 가능하게 연다. sqlite 파일이면 없을 때 새로 만든다.

    읽기와 갈라 둔 이유는 `connect_readonly()` 의 `mode=ro`·세션 read-only 로는 적재가 아예
    안 되기 때문이다. 쓰는 쪽은 목 프로듀서와 분류 워커 둘이다.

    **이 연결에는 "AI 소유 테이블만" 을 강제하는 장치가 없다** — GRANT 도 안 막으므로 오타
    하나로 main server 소유 `cs`·`reviews` 에 쓸 수 있다. 대신 쓰기 SQL 의 대상 테이블을
    테스트가 고정한다(`tests/test_raw_db_write_scope.py`). 계정 분리는 인프라가 이미 거절한
    요청이라 다시 올리지 않는다.

    sqlite 는 없는 파일을 만든다 — 읽기와 반대다. 프로듀서가 첫 실행에서 DB 를 만드는 것이
    정상 절차라 경로 오타를 여기서 못 가르고, 그 위험은 읽는 쪽이 대신 잡는다.
    """
    dsn = dsn if dsn is not None else conninfo_from_settings()
    if dsn:
        return PostgresConnection(dsn, readonly=False)

    path = Path(db_path or get_settings().raw_db_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    # WAL: 프로듀서가 쓰는 동안 워커가 같은 파일을 읽어도 서로 막히지 않게 한다.
    # Postgres 에는 대응물이 없어(MVCC 가 기본) 이 세 줄이 sqlite 분기 안에 있다.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    # sqlite 는 FK 가 기본 OFF 라 연결마다 켜야 한다. 안 켜면 DDL 의 REFERENCES 가 장식으로
    # 남아 채널 오타가 조용히 통과하고, 운영 Postgres 에 올라가서야 처음 터진다.
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def retryable_error_types(conn: RawDbConnection) -> tuple[type[BaseException], ...]:
    """다시 해 볼 가치가 있는 DB 오류 타입. 잠금 경합·직렬화 실패 계열이다.

    워커가 이걸로 "잠깐 기다렸다 재시도" 와 "사람이 봐야 하는 고장" 을 가른다
    (`persist_batch`). 둘을 안 가르면 스키마 오류에도 지수 백오프로 매달리거나, 반대로 잠금
    경합 한 번에 워커가 서서 다음 실행이 같은 배치를 LLM 에 다시 태운다.

    Postgres 쪽 세 이름은 **지금 핀(psycopg 3.3.4)에서는 `OperationalError` 하위라 사실상
    중복이다**(실측: 셋 다 `OperationalError -> DatabaseError -> Error`). 그래도 나열하는
    것은 어느 실패를 재시도 대상으로 보는지 이름으로 남기고, 드라이버가 계층을 바꾸더라도
    이 집합이 안 흔들리게 하려는 것이다 — 지우면 동작은 같지만 그 의도가 사라진다.
    """
    if dialect_of(conn) != POSTGRES:
        return (sqlite3.OperationalError,)
    import psycopg

    return (
        psycopg.OperationalError,
        psycopg.errors.DeadlockDetected,
        psycopg.errors.SerializationFailure,
        psycopg.errors.LockNotAvailable,
    )


def db_error_types(conn: RawDbConnection) -> tuple[type[BaseException], ...]:
    """그 백엔드의 DB 오류 최상위 타입. `retryable_error_types()` 를 포함한다.

    `connection_error_types()` 와 뜻이 다르다 — 저쪽은 접속·환경을 못 여는 것이라 호출부가
    종료 코드를 가르는 데 쓰고, 이쪽은 이미 연 연결에서 문장이 실패한 것이라 적재 루프가
    롤백/중단을 정하는 데 쓴다.
    """
    if dialect_of(conn) != POSTGRES:
        return (sqlite3.Error,)
    import psycopg

    return (psycopg.Error,)


# 이미 있는 객체를 또 만들려다 나는 오류의 SQLSTATE.
#   23505 unique_violation      — 동시 CREATE 가 pg_type/pg_class 유니크 인덱스에서 부딪힘
#   42P07 duplicate_table       42710 duplicate_object       42P16 invalid_table_definition
DUPLICATE_OBJECT_SQLSTATES = frozenset({"23505", "42P07", "42710", "42P16"})


def execute_ddl(conn: RawDbConnection, statement: str) -> bool:
    """DDL 한 문장을 동시 실행에 견디게 돌린다. 이미 있어서 건너뛰었으면 False.

    `CREATE TABLE IF NOT EXISTS` 는 Postgres 에서 동시 실행에 안전하지 않다 — 검사와 생성
    사이 창에서 진 쪽이 유니크 제약 위반(23505)으로 **죽는다**(넘어가는 게 아니다). sqlite
    는 파일 락이 직렬화해 줘서 이 창이 없다.

    분류 워커가 CronJob 이라 겹쳐 뜰 수 있고, 그러면 진 쪽이 exit 1 로 죽어 무한 재시도가
    된다. 창은 최초 배포 때뿐이지만 하필 그때 사람이 보고 있다. 인프라의
    `concurrencyPolicy` 에는 기대지 않는다 — 남의 매니페스트는 우리 모르게 바뀌고, 바뀌어도
    우리 테스트는 초록이다.

    문장마다 commit 한다. Postgres 는 한 문장이 실패하면 트랜잭션 전체가 abort 라 몰아 넣으면
    뒤가 전부 `InFailedSqlTransaction` 으로 죽는다.
    """
    try:
        conn.execute(statement)
    except Exception as exc:
        # 타입으로 못 가른다 — 백엔드마다 다르고, psycopg 쪽은 같은 예외 클래스가 다른
        # 사유로도 온다. SQLSTATE 만 보고 아니면 그대로 올린다(=삼키지 않는다).
        if getattr(exc, "sqlstate", None) not in DUPLICATE_OBJECT_SQLSTATES:
            raise
        conn.rollback()
        return False
    conn.commit()
    return True


def describe_target(db_path: str | None = None, *, dsn: str | None = None) -> str:
    """지금 연결이 가리키는 곳을 사람이 읽을 수 있게. 오류 메시지·로그용.

    **DSN 의 비밀번호를 절대 싣지 않는다.** 이 문자열은 `RuntimeError` 로 나가 배치 로그·
    요약에 박히므로 한 번 새면 회수가 안 된다. 필요한 정보는 "어느 DB 를 봤나" 뿐이다.

    구분자로 잘라내는 방식은 절반만 막는다 — psycopg 는 URI 와 키워드 두 형식을 다 받는데
    문자열을 직접 자르면 키워드 형식이 통째로 샌다. 그래서 파싱을 psycopg 에 맡긴다.

    읽기에 실패해도 원문을 되돌려주지 않는다(폴백으로 새면 막으려던 게 무의미하다). 이 함수는
    인자 자리에서 항상 평가되므로 여기서 던지면 진단이 원인을 가린다.
    """
    dsn = dsn if dsn is not None else conninfo_from_settings()
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
# 가드가 이걸 직접 쓰면 Postgres 에서 **가드 자신이 구문 오류로 죽어** 스키마가 멀쩡한지
# 확인하려던 코드가 먼저 터진다.


def table_columns(conn: RawDbConnection, table: str) -> set[str]:
    """그 테이블의 컬럼 이름. 테이블이 없으면 빈 집합이다(예외 아님).

    "없음" 과 "있는데 컬럼이 옛것" 을 호출부가 갈라야 하기 때문이다.
    """
    if dialect_of(conn) == POSTGRES:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
        return {row["column_name"] for row in rows}

    # PRAGMA 는 바인딩을 못 받는다. `table` 은 우리 코드의 리터럴이라 외부 입력이 아니다.
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def existing_tables(conn: RawDbConnection, names: Iterable[str]) -> set[str]:
    """`names` 중 실제로 있는 테이블 이름. **뷰는 세지 않는다.**

    호출부가 이걸로 "워커가 돌았는가" 를 판정하는데, 뷰는 원문만 있어도 만들어져 있어서 같이
    세면 분류 결과가 없는 DB 가 통과한다.
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


def unique_column_sets(conn: RawDbConnection, table: str) -> set[frozenset[str]]:
    """그 테이블에 걸린 유니크 제약이 덮는 컬럼 조합. 없으면 빈 집합.

    컬럼만 보는 가드는 "컬럼은 다 있는데 제약이 빠진" 테이블을 통과시키는데, 하필 우리가 제일
    의존하는 `UNIQUE (item_id, aspect)` 가 없어도 적재는 성공하고 재분류가 같은 쌍을 중복
    적재해 **탐지 분자가 부푼다**(오탐 방향이라 시끄럽지도 않다).

    Postgres 는 `information_schema.table_constraints` 로 보면 안 된다 — 거기에는
    `CREATE UNIQUE INDEX` 로 만든 맨 유니크 인덱스가 안 잡혀서(제약이 아니라 인덱스라)
    멀쩡한 DB 를 "구버전" 이라고 세운다. `pg_index` 는 둘 다 본다.

    **이름은 안 본다** — 인라인 `UNIQUE (a, b)` 와 명시 인덱스는 이름이 다른데 뜻은 같다.
    컬럼 조합이 계약이고 이름은 아니다.
    """
    if dialect_of(conn) == POSTGRES:
        rows = conn.execute(
            "SELECT i.indexrelid AS idx, a.attname AS col"
            " FROM pg_index i"
            " JOIN pg_class c ON c.oid = i.indrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)"
            " WHERE c.relname = ? AND n.nspname = current_schema() AND i.indisunique",
            (table,),
        ).fetchall()
        grouped: dict[Any, set[str]] = {}
        for row in rows:
            grouped.setdefault(row["idx"], set()).add(row["col"])
        return {frozenset(cols) for cols in grouped.values()}

    # sqlite: index_list 가 유니크 인덱스를, index_info 가 그 컬럼을 준다. 인라인
    # `UNIQUE (a, b)` 는 origin='u' 인 자동 인덱스로 잡힌다.
    # `INTEGER PRIMARY KEY`(rowid 별칭)는 여기 안 나온다 — 우리가 계약으로 삼는 것이 복합
    # 유니크라 문제되지 않지만, "PK 도 세겠지" 하고 쓰면 안 된다.
    indexes = [row for row in conn.execute(f"PRAGMA index_list({table})") if row[2]]
    result: set[frozenset[str]] = set()
    for index in indexes:
        columns = {row[2] for row in conn.execute(f"PRAGMA index_info({index[1]})")}
        if columns:
            result.add(frozenset(columns))
    return result
