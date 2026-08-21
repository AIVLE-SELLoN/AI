"""담당: 지인 — raw DB **Postgres 적재 경로**(이식 ⓑ). `RAW_DB_TEST_DSN` 게이트.

`tests/test_raw_db_postgres.py` 가 **조회**를 보는 자리라면 여기는 **쓰기**다. 파일을 나눈
이유는 성격이 달라서다 — 저쪽은 남이 심어 둔 행을 읽고, 여기는 스키마를 직접 만들고
적재한다(그래서 뒷정리 방식도 다르다).

    docker compose up -d rawdb
    RAW_DB_TEST_DSN="postgresql://sellon:sellon@localhost:5433/rawdb?sslmode=disable" \
        pytest tests/test_raw_db_postgres_write.py

**`sslmode=disable` 을 붙인다** — compose 는 SSL 을 안 켜는데 우리 기본값이 `require` 다.

여기서 잠그는 것은 **sqlite 에서는 원리적으로 못 잡는 것들**이다:
  1. 우리 Postgres DDL 이 **실제로 돈다** — sqlite 판이 통과해도 이쪽은 별개다
     (`AUTOINCREMENT`·`CREATE VIEW IF NOT EXISTS` 는 여기서만 죽는다)
  2. `UNIQUE (item_id, aspect)` 가 실제로 걸려 재분류 중복 적재를 막는다
  3. `find_legacy_tables()` 가 `pg_index` 로 돌아 **맨 유니크 인덱스**도 알아본다
  4. 워커 적재 SQL(`ON CONFLICT`·커서·dead-letter)이 Postgres 에서 돈다
  5. 커서 시작값이 TIMESTAMPTZ 와 비교된다 — 빈 문자열이면 조회 자체가 죽는다
  6. 월간 집계가 이름 기반 행 접근으로 Postgres 결과를 읽는다

LLM·네트워크 없음. 분류기는 스텁이고, 표식 접두사가 붙은 행만 넣고 지운다.
"""

from __future__ import annotations

import os
import threading
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from app.classification.service import ClassifyRequestItem
from app.config import get_settings
from app.core import raw_db, raw_schema
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    Sentiment,
    Source,
)
from app.reporting.monthly_aggregator import aggregate_monthly_inputs
from scripts import classification_worker as worker

DSN = os.getenv("RAW_DB_TEST_DSN", "")

pytestmark = pytest.mark.skipif(
    not DSN, reason="RAW_DB_TEST_DSN 없음 — 로컬 Postgres 검증 전용"
)

PREFIX = "PGW"
AI_TABLES = (
    "classified_item_aspect",  # 자식 먼저 — FK 가 부모를 가리킨다
    "classified_item",
    "classification_failure",
    "classification_cursor",
)


@pytest.fixture
def pg(monkeypatch):
    """접속 원자값을 게이트 DSN 으로 돌리고, **AI 소유 스키마를 우리 DDL 로 다시 세운다.**

    **일부러 지우고 다시 만든다.** 로컬 compose 의 init SQL 이 같은 테이블을 이미
       만들어 두는데, `CREATE TABLE IF NOT EXISTS` 는 그러면 조용히 건너뛴다 — 즉 그대로
       두면 **우리 Postgres DDL 이 한 번도 안 돈다.** 검증하려는 대상이 실행되지 않는
       것이라 통과해도 아무 뜻이 없다.

    지운 뒤 우리 DDL 로 되세우므로 다른 게이트 테스트에는 같은 모양이 남는다(뷰·제약
       포함). 08-18 확정대로 **AI 소유 4개 + 뷰는 우리가 만드는 것**이라 이게 정본이다.

    접속 원자값 세팅이 `tests/test_raw_db_postgres.py` 와 겹친다 — 일부러 복제했다.
       그쪽은 조회 전용 픽스처라 스키마를 손대지 않고, 공용으로 묶으면 이 파일이 하는
       "지우고 다시 만들기" 가 그쪽 테스트에도 딸려 간다.
    """
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    settings = get_settings()
    atoms = conninfo_to_dict(DSN)
    atoms["port"] = int(atoms["port"]) if atoms.get("port") else None
    for field, key, default in (
        ("raw_db_host", "host", "localhost"),
        ("raw_db_port", "port", 5432),
        ("raw_db_name", "dbname", "rawdb"),
        ("raw_db_username", "user", ""),
        ("raw_db_password", "password", ""),
        ("raw_db_sslmode", "sslmode", "disable"),
    ):
        monkeypatch.setattr(settings, field, atoms.get(key) or default)

    with psycopg.connect(DSN, autocommit=True) as admin:
        for table in AI_TABLES:
            admin.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        admin.execute(f"DROP VIEW IF EXISTS {raw_schema.VOC_DOCUMENT}")
        admin.execute(
            "INSERT INTO channel (channel_id, display_name) VALUES (%s, %s)"
            " ON CONFLICT DO NOTHING",
            ("COUPANG", "쿠팡"),
        )

    conn = raw_db.connect_readwrite()
    raw_schema.create_classified_tables(conn)  # ← 검증 대상: 우리 Postgres DDL
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute("DELETE FROM cs WHERE id LIKE %s", (f"{PREFIX}-%",))
            admin.execute("DELETE FROM reviews WHERE id LIKE %s", (f"{PREFIX}-%",))


def _seed_cs(conn, item_id: str, occurred: str, content: str = "사진이랑 색이 달라요") -> None:
    conn.execute(
        "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id,"
        " content, inquired_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
        " ON CONFLICT (id) DO NOTHING",
        (item_id, "CP-1", f"{PREFIX}-P1", "COUPANG", content, occurred, occurred),
    )
    conn.commit()


def _classified(item_id: str, aspect: Aspect) -> ClassifiedItem:
    return ClassifiedItem(
        item_id=item_id,
        source=Source.CS,
        channel=Channel.COUPANG,
        product_group_id=f"{PREFIX}-P1",
        raw_text="사진이랑 색이 달라요",
        created_at=datetime(2026, 7, 5, 10, tzinfo=timezone.utc),
        aspects=[AspectSentiment(aspect=aspect, sentiment=Sentiment.NEGATIVE)],
    )


# ── ① 우리 DDL 이 Postgres 에서 실제로 돈다 ─────────────────────────────────


def test_our_postgres_ddl_creates_the_ai_owned_schema(pg):
    """08-18 확정 — AI 소유 4개 + 뷰는 **우리가 만든다.** 그 DDL 이 여기서 처음 돈다.

    sqlite 판이 초록이어도 이쪽은 별개다: `AUTOINCREMENT` 와
    `CREATE VIEW IF NOT EXISTS` 는 Postgres 에서 구문 오류라, 갈라 두지 않으면 워커가
    운영 첫 실행에서 스키마를 못 세우고 CronJob 이 무한 재시도한다.
    """
    tables = raw_db.existing_tables(pg, AI_TABLES)
    assert tables == set(AI_TABLES)
    # 뷰는 `existing_tables` 가 안 센다(원문만 있는 DB 가 "워커 돌았음" 으로 통과하면 안 된다)
    assert pg.execute(f"SELECT COUNT(*) AS n FROM {raw_schema.VOC_DOCUMENT}").fetchone()["n"] >= 0
    assert "pipeline_version" in raw_db.table_columns(pg, "classified_item")


def test_source_ddl_is_valid_postgres(pg):
    """main server 소유 6개 DDL 도 **문법이 서는지** 확인한다.

    운영에서는 이 DDL 이 안 돈다(그 6개는 인프라·BE 소유다) — 그래도 재는 이유는
       목 프로듀서를 로컬 Postgres 로 돌려 원문을 넣어 봐야 워커·집계를 실연결로 검증할
       수 있기 때문이다. 게다가 여기서 안 재면 이 6개 DDL 은 **아무 데서도 실행되지 않는
       문자열**이 되어, 오타가 있어도 영원히 안 드러난다.

    별도 스키마에 만들고 지운다 — `public` 에 대고 하면 `IF NOT EXISTS` 가 조용히
       건너뛰어 (이미 init SQL 이 만들어 뒀다) 아무것도 검증하지 못한다.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute("DROP SCHEMA IF EXISTS ddl_probe CASCADE")
        admin.execute("CREATE SCHEMA ddl_probe")
        try:
            admin.execute("SET search_path TO ddl_probe")
            for ddl in raw_schema._SOURCE_DDL[raw_db.POSTGRES]:
                admin.execute(ddl)
            admin.execute(raw_schema._VOC_DOCUMENT_DDL[raw_db.POSTGRES])
            created = admin.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'ddl_probe' AND table_type = 'BASE TABLE'"
            ).fetchall()
            assert {row[0] for row in created} == {
                "channel",
                "products",
                "mapped_data",
                "cs",
                "reviews",
                "orders",
            }
        finally:
            admin.execute("RESET search_path")
            admin.execute("DROP SCHEMA IF EXISTS ddl_probe CASCADE")


def test_unique_constraint_is_real_and_the_guard_sees_it(pg):
    """제약이 **실제로 걸려 있고**, 가드가 `pg_index` 로 그걸 본다.

    `information_schema.table_constraints` 로 보면 안 되는 이유가 여기 있다 —
       거기에는 `CREATE UNIQUE INDEX` 로 만든 맨 유니크 인덱스가 **안 잡힌다**(실측:
       로컬 init SQL 이 그렇게 만든 `ux_classified_item_aspect` 가 그 목록에 없다).
       그러면 멀쩡한 DB 를 "구버전" 으로 잘못 세운다.
    """
    assert frozenset({"item_id", "aspect"}) in raw_db.unique_column_sets(
        pg, "classified_item_aspect"
    )
    assert raw_schema.find_legacy_tables(pg) == []


def test_legacy_scan_catches_a_missing_unique_constraint_on_postgres(pg):
    """제약만 뺀 테이블을 만들어 가드가 무는 것을 확인한다.

    이 상태는 지어낸 것이 아니다 — 인프라가 낡은 문서로 먼저 세워 두면
       `CREATE TABLE IF NOT EXISTS` 가 조용히 건너뛰어 정확히 이 모양이 남는다.
    """
    pg.execute("DROP TABLE classified_item_aspect")
    pg.execute(
        "CREATE TABLE classified_item_aspect ("
        " id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,"
        " item_id VARCHAR(20) NOT NULL REFERENCES classified_item(item_id),"
        " aspect VARCHAR(10) NOT NULL, sentiment SMALLINT NOT NULL, mixed_signal BOOLEAN)"
    )
    pg.commit()

    assert raw_schema.find_legacy_tables(pg) == ["classified_item_aspect"]


def test_create_classified_tables_survives_concurrent_runs(pg):
    """`CREATE TABLE IF NOT EXISTS` 는 Postgres 에서 동시 실행에 안전하지 않다.

    검사와 생성 사이의 창에서 진 쪽이 SQLSTATE 23505 로 **죽는다.** 워커가 CronJob 이라
    겹쳐 뜰 수 있고, 죽으면 스케줄러가 무한 재시도한다. 인프라의 `concurrencyPolicy` 에
    기대지 않는다 — 남의 설정은 우리 모르게 바뀌고, 바뀌어도 우리 테스트는 초록이다.

    창이 좁아서 이 테스트가 **매번 충돌을 재현하지는 않는다.** 여기서 보는 것은
       "동시에 돌려도 아무도 예외로 죽지 않는다" 이고, 충돌 자체의 처리는
       `tests/test_raw_db_dialects.py` 가 SQLSTATE 로 결정론적으로 잠근다.
    """
    with __import__("psycopg").connect(DSN, autocommit=True) as admin:
        for table in AI_TABLES:
            admin.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def build() -> None:
        conn = raw_db.connect_readwrite()
        try:
            barrier.wait(timeout=10)
            raw_schema.create_classified_tables(conn)
            conn.commit()
        except BaseException as exc:  # noqa: BLE001 — 스레드 밖으로 안 나가므로 모아서 본다
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=build) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, f"동시 실행이 죽었습니다: {errors!r}"
    assert raw_db.existing_tables(pg, AI_TABLES) == set(AI_TABLES)


# ── ② 워커 적재 경로 ────────────────────────────────────────────────────────


def test_worker_persists_a_batch_on_postgres(pg):
    """적재 + dead-letter + 커서 전진이 Postgres 에서 돈다.

    한 번에 여러 개가 걸린다: `?`→`%s` 바인딩, `ON CONFLICT` 문법, `mixed_signal` 이
    BOOLEAN 이라 `int()` 캐스팅이면 타입 오류, 커서 upsert.
    """
    instance = worker.ClassificationWorker()
    instance.conn = pg
    occurred = "2026-07-05T10:00:00+09:00"
    for index in range(2):
        _seed_cs(pg, f"{PREFIX}-INQ-{index}", occurred)
    rows = pg.execute(
        f"SELECT * FROM {worker.SOURCE_VIEW} WHERE item_id LIKE ? ORDER BY item_id",
        (f"{PREFIX}-%",),
    ).fetchall()
    assert len(rows) == 2

    async def _one_fails(items: list[ClassifyRequestItem]):
        from app.core.exceptions import LlmParseError

        return [
            LlmParseError("검증 실패")
            if item.item_id.endswith("-1")
            else _classified(item.item_id, Aspect.COLOR)
            for item in items
        ]

    with patch.object(worker, "classify_aspect", _one_fails):
        instance.process_batch(rows)

    assert pg.execute("SELECT COUNT(*) AS n FROM classified_item").fetchone()["n"] == 1
    aspects = pg.execute("SELECT aspect, mixed_signal FROM classified_item_aspect").fetchall()
    assert [row["aspect"] for row in aspects] == ["색상"]
    assert aspects[0]["mixed_signal"] is None

    dead = pg.execute("SELECT * FROM classification_failure").fetchall()
    assert [row["item_id"] for row in dead] == [f"{PREFIX}-INQ-1"]
    # 이 값이 비면 `--retry-failed` 의 페이지 커서 정렬이 깨진다
    assert dead[0]["occurred_at"] is not None

    cursor_row = pg.execute("SELECT * FROM classification_cursor").fetchone()
    assert cursor_row["last_item_id"] == f"{PREFIX}-INQ-1"


def test_review_mixed_signal_is_stored_as_boolean(pg):
    """`mixed_signal` 을 `int()` 로 캐스팅하면 Postgres 에서 적재가 죽는다.

    그 컬럼은 BOOLEAN 이라 정수를 안 받는다(실측:
    `column "mixed_signal" is of type boolean but expression is of type smallint`).
    sqlite 는 INTEGER 라 `int()` 든 `bool()` 이든 통과한다 — **sqlite 로는 원리적으로 못
    잡는 회귀다.**

    **CS 로는 이 경로가 안 걸린다.** CS 는 `mixed_signal` 이 항상 `None` 이라
       `None if ... is None else ...` 의 앞가지로 빠진다. 뮤테이션(`bool()` → `int()`)이
       처음에 안 물렸던 이유가 정확히 이것이고, 그래서 **리뷰** 항목을 따로 태운다.
    """
    instance = worker.ClassificationWorker()
    instance.conn = pg
    item_id = f"{PREFIX}-RVW-MIX"
    item = ClassifiedItem(
        item_id=item_id,
        source=Source.REVIEW,
        channel=Channel.COUPANG,
        product_group_id=f"{PREFIX}-P1",
        raw_text="색은 예쁜데 사진보다 어둡네요",
        created_at=datetime(2026, 7, 9, 10, tzinfo=timezone.utc),
        aspects=[
            AspectSentiment(
                aspect=Aspect.COLOR, sentiment=Sentiment.POSITIVE, mixed_signal=True
            )
        ],
    )

    instance.save_classified_items([item])
    pg.commit()

    stored = pg.execute(
        "SELECT mixed_signal FROM classified_item_aspect WHERE item_id = ?", (item_id,)
    ).fetchone()
    assert stored["mixed_signal"] is True


def test_reclassifying_does_not_duplicate_aspect_rows(pg):
    """같은 문서를 다시 적재해도 `(item_id, aspect)` 가 한 행이다.

    이게 `UNIQUE (item_id, aspect)` 를 요구하는 이유다. 제약이 없으면 재분류가 돌 때마다
       같은 쌍이 쌓여 **탐지 분자가 부풀고**, 그건 오탐 방향이라 시끄럽지도 않다.
    """
    instance = worker.ClassificationWorker()
    instance.conn = pg
    item_id = f"{PREFIX}-INQ-DUP"
    _seed_cs(pg, item_id, "2026-07-06T10:00:00+09:00")

    for _ in range(2):
        instance.save_classified_items([_classified(item_id, Aspect.COLOR)])
        pg.commit()

    assert (
        pg.execute(
            "SELECT COUNT(*) AS n FROM classified_item_aspect WHERE item_id = ?", (item_id,)
        ).fetchone()["n"]
        == 1
    )


def test_cursor_origin_is_comparable_to_a_timestamptz_column(pg):
    """커서 시작값이 빈 문자열이면 **조회 자체가 죽는다.**

    조건절이 `occurred_at > ?` 인데 그 컬럼이 TIMESTAMPTZ 라, `''` 를 넘기면 비교가 실패하는
    게 아니라 `invalid input syntax for type timestamp with time zone` 이 난다. sqlite 는
    컬럼이 TEXT 라 지금까지 이 값이 통했다 — **sqlite 에서는 원리적으로 못 잡는 회귀다.**
    """
    instance = worker.ClassificationWorker()
    instance.conn = pg
    _seed_cs(pg, f"{PREFIX}-INQ-CUR", "2026-07-07T10:00:00+09:00")

    assert instance.load_cursor()[0] == worker.cursor_origin(pg)
    fetched = instance.fetch_next_batch()
    assert f"{PREFIX}-INQ-CUR" in {row["item_id"] for row in fetched}

    # 재처리·재분류 페이지 커서도 같은 시작값을 쓴다(빈 문자열이면 여기서 죽는다)
    assert instance.fetch_failed_batch() == []
    assert instance.fetch_stale_batch() == []
    assert instance.count_stale() == 0


# ── ③ 월간 집계 ────────────────────────────────────────────────────────────


def test_monthly_aggregate_reads_postgres(pg):
    """월간 집계가 Postgres 결과를 읽는다.

    여기서 갈리는 것은 **행 접근 방식**이다. 예전에는 `for aspect, sentiment, count in
       rows` 처럼 위치로 풀었는데, psycopg 기본 행 타입에 따라 그게 통째로 조용히 틀린다
       (`dict` 를 순회하면 값이 아니라 키가 나온다).
    """
    instance = worker.ClassificationWorker()
    instance.conn = pg
    item_id = f"{PREFIX}-INQ-AGG"
    _seed_cs(pg, item_id, "2026-07-08T10:00:00+09:00")
    instance.save_classified_items([_classified(item_id, Aspect.COLOR)])
    pg.commit()

    reader = raw_db.connect_readonly()
    try:
        inputs = aggregate_monthly_inputs(
            reader, "2026-07", product_group_ids=[f"{PREFIX}-P1"], n_permutations=50
        )
    finally:
        reader.close()

    assert len(inputs) == 1
    result = inputs[0]
    assert result.total_voc_count == 1
    assert result.start_date == date(2026, 7, 1)
    color = next(d for d in result.aspect_distributions if d.aspect == Aspect.COLOR.value)
    assert color.total_count == 1
    assert color.negative_ratio == 1.0
