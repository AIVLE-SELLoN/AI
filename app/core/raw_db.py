"""raw DB(`cs`·`reviews`·`classified_item`) 연결. **읽는 쪽·쓰는 쪽 공용.**

AI 노드가 **쓰는** 것은 자기 소유 4개(`classified_item`·`classified_item_aspect`·
`classification_failure`·`classification_cursor`)뿐이고, 원문(`cs`·`reviews`)과 카탈로그는
main server 소유라 읽기만 한다(「Raw DB 스키마 확정 (8/7)」 §1·§5-2).

🔴 **그 경계를 이제 DB 가 안 지켜 준다.** 인프라가 *"모든 테이블에 대한 RW 권한을 기능마다
   따로 부여할 순 없다"* 며 **AI 노드에 raw DB RW 전면 부여**로 회신했다(2026-08-18).
   §5-2 의 "읽기 전용" 전제가 인프라 차원에서 사라졌으므로 남는 방어선은 코드 쪽 둘뿐이다:
     - 읽는 쪽: `connect_readonly()` 의 세션 read-only (아래)
     - 쓰는 쪽: **없다.** 대신 워커가 실제로 어느 테이블에 쓰는지를 테스트가 고정한다
       (`tests/test_raw_db_write_scope.py`) — 오타 하나로 `cs`·`reviews` 에 쓰는 것을
       DB 가 더는 안 막기 때문이다.

⚠️ 이게 `app/core/` 에 있는 이유: **읽는 쪽이 둘로 갈려 있다.** 탐지 배치
(`app/batch/daily.py`)와 CS 원문 조회(`app/core/inquiries.py`)가 같은 파일을 여는데,
한쪽에만 두면 나머지가 연결 문자열을 다시 적게 되고 `mode=ro` 같은 조건이 조용히
갈린다 — `raw_schema.py` 가 core 에 있는 것과 같은 사유다. 쓰는 쪽(`scripts/`)도 같은
이유로 여기를 경유한다.

── 백엔드 2종 (Postgres 이식 1단계, 2026-08-16) ─────────────────────────────────
운영 raw DB 는 **Postgres**(`rawdb`)이고 로컬·목 파이프라인은 **sqlite 파일**이다.
둘을 가르는 것은 `RAW_DB_HOST` 하나다 — 값이 있으면 Postgres, 없으면 sqlite.

    RAW_DB_HOST 있음  →  Postgres            (운영 · 로컬 compose 검증)
    RAW_DB_HOST 없음  →  sqlite raw_db_path  (기본값. 데모가 도는 경로)

⚠️ **`RAW_DB_PATH` 에 접속 정보를 넣는 방식은 안 된다.** 아래 sqlite 경로가
   `Path.exists()` 를 먼저 보기 때문에 `FileNotFoundError` 로 떨어진다. 키를 나눈
   이유이고, 기본값이 비어 있으므로 **아무것도 설정하지 않으면 동작이 이전과 같다.**

🔴 **접속 문자열은 원자값에서 우리가 조립한다 — 남이 준 것을 파싱해 쓰지 않는다**
   (2026-08-18 확정, `conninfo_from_settings` 참고). 백엔드와 공유하는 것은 host·port·
   dbname·user·password 라는 **사실**이고, 그것을 어떤 문자열로 만드는지는 각자의 몫이다.

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
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from app.config import Settings, get_settings

SQLITE = "sqlite"
POSTGRES = "postgres"


def conninfo_from_settings(settings: Settings | None = None) -> str:
    """raw DB 접속 원자값 → psycopg 접속 문자열. **호스트가 비면 빈 문자열**(= sqlite).

    🔴 **f-string 으로 조립하지 말 것 — `make_conninfo` 가 이스케이프를 한다.** 값에
       공백·작은따옴표·역슬래시가 들어가면 키워드 형식이 통째로 어긋나는데, 비밀번호는
       그런 문자가 흔한 자리다(실측: `p w` → `password='p w'`, `p'w` → `password=p\\'w`,
       역슬래시까지 round-trip 확인). 손으로 붙이면 **인증 실패로만 보이고 원인이
       안 드러난다.**

    🔴 **빈 값은 넘기지 않고 뺀다 — `password=''` 는 "생략" 과 다른 뜻이다.** libpq 에
       빈 문자열을 명시하면 *빈 비밀번호를 쓰겠다*는 뜻이라 `.pgpass` 조회가 막힌다
       (실측: `make_conninfo(password='')` → `password=''` 가 실제로 실린다). 그래서
       채워진 값만 싣는다 — 정확히 그래서 `RAW_DB_SSLROOTCERT` 를 **키만 두고 비워도**
       아무 일이 없다.

    ⚠️ `sslmode` 는 항상 싣는다. 이 값을 빼면 libpq 기본값 `prefer` 로 떨어져 **서버가
       거부하면 조용히 평문으로** 붙는다(`config.raw_db_sslmode` 주석 참고).

    ⚠️ `connect_timeout` 도 항상 싣는다. libpq 기본값이 미지정이라 빼면 **무한 대기**가
       되고, 접속 못 하는 배치가 CronJob 자리를 130초씩 잡는다(실측). 값 검증은
       `Settings._check_raw_db` 가 한다 — 0·음수가 다시 무한을 뜻하기 때문이다.
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


def upsert_sql(
    table: str,
    columns: Sequence[str],
    *,
    conflict: Sequence[str],
    update: Sequence[str] | None = None,
) -> str:
    """멱등 INSERT 문. **두 백엔드에서 글자 그대로 같다.**

    🔴 **`INSERT OR IGNORE` · `INSERT OR REPLACE` 는 sqlite 전용이라 못 쓴다.** Postgres 에는
       그 문법이 없고(구문 오류), 표준 `ON CONFLICT` 는 sqlite 3.24+ 가 같은 뜻으로 받는다 —
       그래서 갈라 쓰지 않고 이쪽 한 벌로 통일한다.

    ⚠️ **`INSERT OR REPLACE` → `ON CONFLICT DO UPDATE` 는 기계적 치환이 아니다.** 앞은
       *지우고 다시 넣는* 것이고 뒤는 *제자리 갱신*이라, 그 행을 참조하는 자식이
       `ON DELETE CASCADE` 로 붙어 있으면 앞에서만 자식이 사라진다.
       **우리 스키마에는 CASCADE 가 한 곳도 없어서 이 치환이 동작을 안 바꾼다** — 실측으로도
       `PRAGMA foreign_keys=ON` 인 sqlite 에서 `mapped_data` 가 참조 중인 `products` 행을
       두 형태로 각각 다시 넣어 보면 둘 다 성공하고 자식 행 수도 같다.
       🔴 **CASCADE 를 새로 거는 순간 이 문장이 거짓이 된다** — 그때 다시 볼 것.

    Args:
        conflict: 충돌 판정 키(=PK 또는 유니크 조합).
        update: 충돌 시 덮어쓸 컬럼. `None` 이면 `conflict` 를 뺀 나머지 전부,
            **빈 시퀀스면 `DO NOTHING`**(이미 있으면 그대로 둔다).
    """
    placeholders = ", ".join("?" * len(columns))
    head = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    targets = list(update) if update is not None else [c for c in columns if c not in conflict]
    if not targets:
        return f"{head} ON CONFLICT DO NOTHING"
    assignments = ", ".join(f"{c} = excluded.{c}" for c in targets)
    return f"{head} ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {assignments}"


class RawRow:
    """`sqlite3.Row` 와 같은 모양의 행. 위치·이름 **양쪽**으로 읽힌다.

    🔴 **psycopg 기본 행 타입(`tuple`·`dict_row`)으로는 코드가 한 벌로 안 선다.**
       저장소의 조회 코드가 두 방식을 섞어 쓴다 — 탐지·CS 원문 조회는 `row["item_id"]`
       인데, 월간 집계는 `for aspect, sentiment, count in rows` 로 풀고 커버리지 집계는
       `.fetchone()[0]` 을 쓴다. `dict_row` 면 앞쪽만, `tuple_row` 면 뒤쪽만 산다.
       그런데 **`dict` 를 순회하면 값이 아니라 키가 나오므로**, 언패킹 쪽은 에러 없이
       컬럼 **이름**을 값으로 받아 조용히 틀린다 — 제일 나쁜 실패 모양이다.

       sqlite 는 `sqlite3.Row` 로 둘 다 되므로, 여기서 같은 계약을 흉내 내면 호출부를
       한 글자도 안 바꾸고 두 백엔드가 같은 코드를 탄다.

    ⚠️ 순회는 **값**을 준다(`sqlite3.Row` 와 같다). `dict(row)` 가 필요하면
       `dict(zip(row.keys(), row))` 를 쓸 것 — `dict(row)` 는 sqlite 에서도 안 된다.
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

    호출부가 아는 것은 `execute(sql, params) -> 커서` · `executemany` · `commit` ·
    `rollback` · `close` 다 — sqlite 쪽과 같은 모양이라 조회·적재 코드가 백엔드를 몰라도
    된다. 행은 위치·이름 양쪽으로 읽힌다(`RawRow`).

    ⚠️ **execute 마다 새 커서를 만든다.** sqlite 의 `Connection.execute()` 가 그렇고,
       커서를 재사용하면 앞 결과를 다 읽기 전에 다음 질의를 돌렸을 때 조용히 결과가
       사라진다(`fetch_linked_inquiries` 가 청크마다 execute 한다).

    ⚠️ **읽기 연결만 autocommit 이다.** 쓰기 연결에서 autocommit 을 켜면 워커의
       `persist_batch()` 가 "적재 + 실패 기록 + 커서 전진을 한 트랜잭션으로" 라고 적어 둔
       계약이 조용히 깨진다 — 중간에 죽으면 커서만 전진하고 dead-letter 가 빠져 그 건이
       영구 유실된다.
    """

    dialect = POSTGRES

    def __init__(self, dsn: str, *, readonly: bool = True) -> None:
        import psycopg

        self._conn = psycopg.connect(
            dsn, autocommit=readonly, row_factory=_raw_row_factory
        )
        self.readonly = readonly
        if readonly:
            # 🔴 **읽기 전용을 세션에도 건다.** 인프라가 RW 전면 부여로 회신해(2026-08-18)
            #    운영에서도 GRANT 가 아무것도 안 막는다 — sqlite 쪽 `mode=ro` 가 지키던
            #    계약①(읽는 쪽이 원문을 못 고친다)이 **이 한 줄로만** 남아 있다.
            #    위반은 `psycopg.errors.ReadOnlySqlTransaction`.
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
    """raw DB 를 **환경 탓에** 못 열거나 못 읽는 것을 뜻하는 예외 타입.

    sqlite 는 `connect_readonly()` 가 `FileNotFoundError` 하나로 모아 주지만, Postgres 는
    드라이버 계층이 그대로 올라온다. `psycopg.Error` 는 `FileNotFoundError` 도
    `RuntimeError` 도 `OSError` 도 **아니라서**(실측, psycopg 3.3.4) 두 호출부의 기존
    분기를 그냥 통과한다 — 배치는 exit 1 + raw traceback, REST 는 500 이 된다.

    호출부가 각자 적지 않고 여기서 주는 이유: `app/` 이 psycopg 를 직접 import 하면
    드라이버가 없는 sqlite 전용 환경의 import 가 깨진다. 여기서 늦게 import 한다.

    ⚠️ **`psycopg.Error` 전부다 — `OperationalError` 만으로는 절반이 샌다.** 실패가 두
       베이스로 갈리기 때문이다(실측):

           OperationalError   DB 미기동 · 호스트 오타 · 비밀번호 틀림
           ProgrammingError   DSN 형식 오타 · DB 이름 틀림 · 뷰/테이블 없음 · GRANT 누락

       뒷줄이 하필 이식에서 제일 잦을 것들이다 — `voc_document` 뷰와 읽기 GRANT 는
       지금 인프라에 요청해 둔 상태라, 첫 연동에서 정확히 이 모양으로 실패한다.

    ⚠️ **우리가 쓴 SQL 의 버그도 `ProgrammingError` 라 여기 걸린다.** 그때는 환경 문제로
       오분류된다. 우리 SQL 은 테스트가 먼저 보고 뷰·GRANT 는 테스트가 볼 수 없다는
       판단이고, 두 호출부 모두 사유를 전문으로 남기므로 메시지에는 진짜 원인이 있다.

    ⚠️ 드라이버가 없으면 **빈 튜플**이라 아무것도 새로 잡지 않는다. 그 환경에서
       `RAW_DB_HOST` 를 켜면 `PostgresConnection` 의 `ModuleNotFoundError` 가 그대로
       올라간다 — psycopg 는 `requirements.txt` 가 고정하는 의존이라 남는 것은 재설치를
       건너뛴 개발 머신뿐이고, 거기서는 그 traceback 자체가 이미 정확한 안내다.
    """
    try:
        import psycopg
    except ModuleNotFoundError:
        return ()
    return (psycopg.Error,)


def connect_readonly(
    db_path: str | None = None, *, dsn: str | None = None
) -> RawDbConnection:
    """raw DB 를 **읽기 전용**으로 연다. sqlite 파일이면 없을 때 만들지 않고 던진다.

    `dsn`(또는 원자값으로 조립한 `conninfo_from_settings()`)이 있으면 Postgres 로 붙고,
    없으면 sqlite 파일을 연다. **기본값은 sqlite** 라 설정을 안 건드리면 동작이 이전과 같다.

    sqlite 를 `mode=ro` 로 여는 이유가 둘이다:
      1. 권한 그대로다 — 읽는 쪽이 원문을 고칠 일이 없다.
      2. **경로 오타를 조용히 넘기지 않는다.** 기본 연결은 없는 경로에 빈 DB 를 새로
         만들어서, 조회가 0건으로 성공하고 배치는 알림 없이 정상 종료한다.
    Postgres 에서는 1 을 GRANT(+세션 read-only)가, 2 를 접속 실패가 대신한다 —
    없는 DB 에 붙으면 psycopg 가 `OperationalError` 로 던지지 빈 DB 를 만들지 않는다.

    Args:
        db_path: sqlite DB 경로. 기본은 `settings.raw_db_path`.
        dsn: Postgres 접속 문자열. 기본은 `conninfo_from_settings()` 가 조립한 값.
            `""` 를 명시하면 **원자값이 설정돼 있어도** sqlite 로 간다
            (`eval/run_monthly_oracle_eval.py` 가 그렇게 쓴다).
            ⚠️ 여기에 **문자열을 직접 넘기면 `connect_timeout` 도 직접 넣어야 한다** —
            기본값 주입은 `conninfo_from_settings()` 한 곳에서만 한다. 두 곳에서 같은
            기본값을 넣으면 한쪽만 바뀌었을 때 조용히 갈리기 때문이고, 그래서 이 인자는
            "전부 네가 정한다" 는 뜻이다. 지금 이 경로를 쓰는 것은 sqlite 로 되돌리는
            `dsn=""` 뿐이다.

    Raises:
        FileNotFoundError: sqlite 파일이 없을 때. 목 파이프라인은 `scripts/mock_producer.py`
            가 원문을, `scripts/classification_worker.py` 가 분류 결과를 채운다.
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

    # ⚠️ **`as_uri()` 로 만든다.** `f"file:{path}?mode=ro"` 는 경로에 `#` 이 있으면
    #    그 뒤가 URI fragment 로 잘려 **`mode=ro` 가 통째로 날아간다.** 그러면 남은
    #    앞부분을 경로로 잡고 **빈 DB 를 새로 만들어** 위 두 이유가 동시에 깨진다
    #    (실측: `.../we#ird/raw.db` → `.../we` 라는 0바이트 파일 생성).
    #    `as_uri()` 가 `#` 을 `%23` 으로 인코딩한다. (2026-08-11 리뷰 ③)
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_readwrite(db_path: str | None = None, *, dsn: str | None = None) -> RawDbConnection:
    """raw DB 를 **쓰기 가능**하게 연다. sqlite 파일이면 없을 때 새로 만든다.

    쓰는 쪽은 목 프로듀서(main server 대역)와 분류 워커 둘이다. 읽기와 갈라 둔 이유는
    `connect_readonly()` 가 sqlite 를 `mode=ro` 로 열고 Postgres 세션에 read-only 를
    걸어서, 그 연결로는 적재가 아예 안 되기 때문이다.

    🔴 **이 연결에는 "AI 소유 테이블만" 을 강제하는 장치가 없다.** 인프라가 RW 전면
       부여로 회신해(2026-08-18) GRANT 도 안 막으므로, 오타 하나로 main server 소유
       `cs`·`reviews` 에 쓸 수 있다. 대신 **쓰기 SQL 의 대상 테이블을 테스트가 고정한다**
       (`tests/test_raw_db_write_scope.py`) — 새 쓰기 문장이 그 밖으로 나가면 거기서 걸린다.
       계정 분리는 인프라가 이미 거절한 요청이라 다시 올리지 않는다.

    ⚠️ sqlite 는 없는 파일을 **만든다** — 읽기와 반대다. 프로듀서가 첫 실행에서 DB 를
       만드는 것이 정상 절차라 경로 오타를 여기서 못 가른다. 그 위험은 읽는 쪽
       (`connect_readonly` 의 `FileNotFoundError`)이 대신 잡는다.

    Args:
        db_path: sqlite DB 경로. 기본은 `settings.raw_db_path`.
        dsn: Postgres 접속 문자열. 기본은 `conninfo_from_settings()` 가 조립한 값.
            `""` 를 명시하면 원자값이 설정돼 있어도 sqlite 로 간다.
            ⚠️ `connect_readonly` 와 같다 — **문자열을 직접 넘기면 `connect_timeout` 도
            직접 넣어야 한다.** 기본값 주입은 `conninfo_from_settings()` 한 곳에서만 하고,
            그래서 이 인자는 "전부 네가 정한다" 는 뜻이다.
    """
    dsn = dsn if dsn is not None else conninfo_from_settings()
    if dsn:
        return PostgresConnection(dsn, readonly=False)

    path = Path(db_path or get_settings().raw_db_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    # WAL: 프로듀서가 쓰는 동안 워커가 같은 파일을 읽어도 서로 막히지 않게 한다.
    # ⚠️ Postgres 에는 대응물이 없다(MVCC 가 기본) — 그래서 이 세 줄이 sqlite 분기 안에 있다.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    # ⚠️ sqlite 는 FK 가 **기본 OFF** 라 연결마다 켜야 한다. 안 켜면 DDL 의 REFERENCES 가
    #    장식으로만 남아 채널 오타가 조용히 통과한다 — Postgres 는 항상 켜져 있으므로,
    #    목에서만 못 잡으면 운영에 올라가서야 처음 터진다.
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def retryable_error_types(conn: RawDbConnection) -> tuple[type[BaseException], ...]:
    """**다시 해 볼 가치가 있는** DB 오류 타입. 잠금 경합·직렬화 실패 계열이다.

    워커가 이걸로 "잠깐 기다렸다 재시도" 와 "사람이 봐야 하는 고장" 을 가른다
    (`persist_batch`). 둘을 안 가르면 스키마 오류에도 지수 백오프로 매달리거나, 반대로
    잠금 경합 한 번에 워커가 서서 **다음 실행이 같은 배치를 LLM 에 다시 태운다.**

    ⚠️ **Postgres 는 `OperationalError` 만으로 부족하다.** 거기서 잠금·직렬화 실패는
       `DeadlockDetected`·`SerializationFailure`·`LockNotAvailable` 이고 이들은
       `OperationalError` 가 아니라 **`DatabaseError` 계열**이라, 그것만 잡으면 잠깐 기다리면
       될 것이 "치명적 오류" 로 분류돼 워커가 선다.
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

    `connection_error_types()` 와 뜻이 다르다 — 저쪽은 **접속·환경**을 못 여는 것이라
    호출부가 종료 코드를 가르는 데 쓰고, 이쪽은 **이미 연 연결에서 문장이 실패한 것**이라
    적재 루프가 롤백/중단을 정하는 데 쓴다.
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
    """DDL 한 문장을 **동시 실행에 견디게** 돌린다. 이미 있어서 건너뛰었으면 False.

    🔴 **`CREATE TABLE IF NOT EXISTS` 는 Postgres 에서 동시 실행에 안전하지 않다.**
       `IF NOT EXISTS` 검사와 실제 생성 사이에 창이 있어서, 두 프로세스가 같은 순간에
       만들면 진 쪽이 `duplicate key value violates unique constraint "pg_type_typname_
       nsp_index"`(23505) 로 죽는다 — "이미 있으니 넘어간다" 가 아니라 **에러**다.
       sqlite 는 파일 락이 직렬화해 줘서 이 창이 없다(그래서 지금까지 안 드러났다).

       분류 워커가 k8s CronJob 이라 겹쳐 뜰 수 있고, 그러면 진 쪽이 exit 1 로 죽어
       **CronJob 이 무한 재시도**한다. 창은 최초 배포 때뿐이지만 하필 그때 사람이 보고 있다.

    ⚠️ **인프라의 `concurrencyPolicy` 에 기대지 않는다.** 남의 매니페스트 설정은 우리
       모르게 바뀌고, 바뀌어도 우리 테스트는 초록이다. 여기서 삼키는 쪽이 싸다.

    ⚠️ **문장마다 commit 한다.** Postgres 는 문장 하나가 실패하면 트랜잭션 전체가 abort 라,
       한 트랜잭션에 몰아 넣으면 뒤 문장이 전부 `InFailedSqlTransaction` 으로 죽는다.
       DDL 은 각각 멱등이므로 쪼개도 잃는 것이 없다.
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
    """지금 연결이 가리키는 곳을 **사람이 읽을 수 있게**. 오류 메시지·로그용.

    🔴 **DSN 의 비밀번호를 절대 싣지 않는다.** 이 문자열은
       `daily._require_classified_tables()` 의 `RuntimeError` 로 나가 배치 로그·요약에
       박히므로, 한 번 새면 회수가 안 된다. 필요한 정보는 "어느 DB 를 봤나" 뿐이라
       host·port·dbname 만 남기고 나머지는 버린다.
       (2026-08-16 `Settings` 진단에서 `input` 을 뺀 것과 같은 사유)

    🔴 **`@`·`?` 로 잘라내는 방식은 쓰지 않는다 — 절반만 막힌다.** psycopg 는 URI 와
       키워드, **두 형식을 다 받는다.** 우리가 조립하는 값은 이제 키워드 형식이지만
       `dsn=` 로 URI 를 직접 넘기는 경로(`RAW_DB_TEST_DSN` 게이트)가 남아 있어 둘 다 온다.
       문자열을 직접 자르면 키워드 형식이 통째로 샌다:

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


def unique_column_sets(conn: RawDbConnection, table: str) -> set[frozenset[str]]:
    """그 테이블에 걸린 **유니크 제약이 덮는 컬럼 조합**. 없으면 빈 집합.

    컬럼만 보는 가드는 "컬럼은 다 있는데 제약이 빠진" 테이블을 통과시킨다. 하필 우리가
    제일 의존하는 제약(`classified_item_aspect` 의 `UNIQUE (item_id, aspect)`)이 없어도
    적재는 성공하고 **재분류가 같은 쌍을 중복 적재해 탐지 분자가 부푼다** — 오탐 방향이라
    시끄럽지도 않다. 그래서 `raw_schema.find_legacy_tables()` 가 이걸 같이 본다.

    🔴 **Postgres 는 `information_schema.table_constraints` 로 보면 안 된다.** 거기에는
       `CREATE UNIQUE INDEX` 로 만든 **맨 유니크 인덱스가 안 잡힌다**(제약이 아니라 인덱스라).
       우리 로컬 init SQL(`docker/postgres/init/02_ai_read_model.sql`)이 정확히 그 형태로
       만들고, 인프라가 먼저 세운 스키마도 어느 쪽일지 모른다 — information_schema 로 보면
       멀쩡한 DB 를 "구버전" 이라고 잘못 세운다. `pg_index` 는 둘 다 본다.

    ⚠️ 이름은 안 본다. 인라인 `UNIQUE (a, b)` 와 명시 인덱스는 이름이 다른데 뜻은 같다 —
       **컬럼 조합이 계약이고 이름은 아니다.**
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

    # sqlite: PRAGMA index_list 가 유니크 인덱스를, index_info 가 그 컬럼을 준다.
    # 인라인 `UNIQUE (a, b)` 는 origin='u' 인 자동 인덱스로 잡힌다.
    # ⚠️ `INTEGER PRIMARY KEY`(rowid 별칭)는 여기 안 나온다 — 우리가 계약으로 삼는 것이
    #    복합 유니크라 문제되지 않지만, "PK 도 세겠지" 하고 쓰면 안 된다.
    indexes = [row for row in conn.execute(f"PRAGMA index_list({table})") if row[2]]
    result: set[frozenset[str]] = set()
    for index in indexes:
        columns = {row[2] for row in conn.execute(f"PRAGMA index_info({index[1]})")}
        if columns:
            result.add(frozenset(columns))
    return result
