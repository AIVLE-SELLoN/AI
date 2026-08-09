"""분류 워커(`scripts/classification_worker.py`)의 실패 격리 회귀 테스트.

여기서 재는 것 하나: **분류 실패 1건이 그 건에만 국한되는가.**

이게 깨지면 조용히 깨진다 — 예외 객체가 성공 목록에 섞여 들어가고, 실패는
dead-letter 에 안 남고, persist 단계에서 배치가 통째로 터진다. 그 건은 커서만
전진한 채 어디에도 남지 않아 **영구 유실**된다(분모 합의의 전제인 커버리지가 깨진다).

`classify_aspect()` 는 계약상 **raise 하지 않고** 실패를 결과 자리에 담아 돌려준다
(길이·순서가 요청과 동일). 워커가 이걸 gather 로 한 번 더 감싸면 바깥 gather 는
예외를 볼 일이 없어 실패 판정이 영원히 False 가 된다. 아래 테스트가 그 형태를 막는다.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from app.classification.service import ClassifyRequestItem
from app.core import raw_schema
from app.core.exceptions import LlmCallError, LlmParseError
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    Sentiment,
    Source,
)
from scripts import classification_worker as worker

FAILING_ID = "I-2"


def _request(item_id: str) -> ClassifyRequestItem:
    return ClassifyRequestItem(
        item_id=item_id,
        source=Source.REVIEW,
        channel=Channel.NAVER,
        product_group_id="P001",
        raw_text="색상이 사진과 달라요",
        created_at=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
    )


async def _fake_classify_aspect(
    items: list[ClassifyRequestItem],
) -> list[ClassifiedItem | Exception]:
    """계약대로 동작하는 대역 — raise 하지 않고 실패를 그 자리에 담아 돌려준다."""
    outcomes: list[ClassifiedItem | Exception] = []
    for item in items:
        if item.item_id == FAILING_ID:
            outcomes.append(LlmParseError(f"ClassifiedItem 검증 실패 [item_id={item.item_id}]"))
            continue
        outcomes.append(
            ClassifiedItem(
                item_id=item.item_id,
                source=item.source,
                channel=item.channel,
                product_group_id=item.product_group_id,
                raw_text=item.raw_text,
                aspects=[AspectSentiment(aspect=Aspect.COLOR, sentiment=Sentiment.NEGATIVE)],
                created_at=item.created_at,
            )
        )
    return outcomes


@pytest.fixture
def worker_instance():
    """워커 1개. `__init__` 이 DB 를 열지 않으므로 그대로 만들어 쓴다.

    이벤트 루프도 `__init__` 이 만들고 `asyncio.set_event_loop` 까지 부른다. 여기서 새로
    만들면 그 루프가 닫히지 않은 채 스레드 기본 루프로 남는다.
    """
    instance = worker.ClassificationWorker()
    try:
        yield instance
    finally:
        instance.loop.close()


@pytest.fixture
def classify_items(worker_instance):
    return worker_instance.classify_items


def test_failed_item_is_reported_as_failure(classify_items) -> None:
    """실패 1건은 실패 목록으로 빠지고 나머지는 성공으로 남는다.

    실제 dead-letter 기록까지는 `test_dead_letter_records_occurred_at` 이 본다.
    """
    items = [_request("I-1"), _request(FAILING_ID), _request("I-3")]

    with patch.object(worker, "classify_aspect", _fake_classify_aspect):
        results, failures = classify_items(items)

    assert [r.item_id for r in results] == ["I-1", "I-3"]
    assert [(f[0], f[1]) for f in failures] == [(FAILING_ID, "classify")]
    assert "검증 실패" in failures[0][2]


def test_success_list_never_contains_exceptions(classify_items) -> None:
    """성공 목록에는 ClassifiedItem 만 들어간다.

    ⚠️ 이 테스트가 막는 것: 워커가 `classify_aspect([item])` 를 건별로 부르고 그 바깥을
       다시 `gather(return_exceptions=True)` 로 감싸는 형태. 그러면 outcome 이
       `[LlmParseError(...)]` 라는 **리스트**로 와서 `isinstance(outcome, BaseException)`
       이 False 가 되고, 예외 객체가 그대로 results 에 섞인다. 아래 `.aspects` 접근이
       그 시점에 AttributeError 로 터진다 — persist 단계에서 나던 바로 그 오류다.
    """
    items = [_request("I-1"), _request(FAILING_ID)]

    with patch.object(worker, "classify_aspect", _fake_classify_aspect):
        results, _ = classify_items(items)

    assert all(isinstance(r, ClassifiedItem) for r in results)
    # persist 가 실제로 읽는 속성 — 예외 객체가 섞였다면 여기서 AttributeError 다
    assert results[0].aspects[0].aspect is Aspect.COLOR


def test_classify_aspect_called_once_for_whole_batch(classify_items) -> None:
    """배치 1건당 classify_aspect 호출도 1번이다 — 건별로 쪼개 부르지 않는다.

    쪼개 부르면 위 실패 판정이 무력화되고, 프롬프트가 1건씩 처리하므로 LLM 호출 수도
    줄지 않는다(격리는 classify_aspect 가 이미 해준다).
    """
    items = [_request("I-1"), _request(FAILING_ID), _request("I-3")]
    calls: list[int] = []

    async def _counting(reqs: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
        calls.append(len(reqs))
        return await _fake_classify_aspect(reqs)

    with patch.object(worker, "classify_aspect", _counting):
        classify_items(items)

    assert calls == [3]


def test_empty_batch_skips_llm(classify_items) -> None:
    """빈 배치는 LLM 을 아예 부르지 않는다."""
    with patch.object(worker, "classify_aspect") as mock_classify:
        results, failures = classify_items([])

    mock_classify.assert_not_called()
    assert (results, failures) == ([], [])


# ── 적재 구조 (Raw DB 스키마 제안 §2-6) ──────────────────────────────────────


def _open_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(raw_schema.CLASSIFIED_ITEM_DDL)
    conn.execute(raw_schema.CLASSIFIED_ITEM_ASPECT_DDL)
    for stmt in raw_schema.CLASSIFIED_INDEXES:
        conn.execute(stmt)
    return conn


def _classified(item_id: str, *aspects: Aspect) -> ClassifiedItem:
    return ClassifiedItem(
        item_id=item_id,
        source=Source.REVIEW,
        channel=Channel.NAVER,
        product_group_id="P001",
        raw_text="색상이 사진과 달라요",
        aspects=[AspectSentiment(aspect=a, sentiment=Sentiment.NEGATIVE) for a in aspects],
        created_at=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
    )


def _save(conn: sqlite3.Connection, items: list[ClassifiedItem]) -> int:
    holder = type("H", (), {"conn": conn})()
    return worker.ClassificationWorker.save_classified_items(holder, items)


def test_no_raw_text_copy_is_persisted() -> None:
    """원문 사본을 만들지 않는다 — 아키텍처 확정 §6.

    "CS·리뷰 원문은 AI가 사본을 안 만들고 원본 DB에서 바로 읽음"이 확정인데, 예전 스키마는
    classified_item.raw_text 에 원문을 복사하고 있었다. 채널·상품그룹·발생 시각도 같은
    이유로 두지 않는다(원문 테이블에 있다).
    """
    conn = _open_memory_db()
    columns = {row[1] for row in conn.execute("PRAGMA table_info(classified_item)")}

    assert columns == {"item_id", "source", "classified_at", "prompt_version"}
    for leaked in ("raw_text", "channel", "product_group_id", "created_at"):
        assert leaked not in columns


def test_persists_parent_and_aspect_rows() -> None:
    """문의 1건 = 부모 1행, aspect N개 = 자식 N행."""
    conn = _open_memory_db()
    inserted = _save(conn, [_classified("I-1", Aspect.COLOR, Aspect.SIZE), _classified("I-2", Aspect.COLOR)])

    assert conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 3
    assert inserted == 3  # 반환값은 aspect 행 수


def test_item_with_no_aspect_still_has_parent_row() -> None:
    """언급된 속성이 없어도 부모 행은 남는다.

    안 남기면 "분류했는데 속성이 없었다"와 "아직 분류 안 했다"가 구분되지 않아
    커버리지 확인이 깨진다.
    """
    conn = _open_memory_db()
    _save(conn, [_classified("I-1")])

    assert conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 0


def test_reinsert_is_idempotent() -> None:
    """같은 배치를 다시 적재해도 행이 늘지 않는다(재시도·재실행 안전)."""
    conn = _open_memory_db()
    items = [_classified("I-1", Aspect.COLOR, Aspect.SIZE)]
    _save(conn, items)
    second = _save(conn, items)

    assert second == 0
    assert conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 2


# ── 전량 실패 감지 ───────────────────────────────────────────────────────


async def _all_call_errors(items: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
    """401·레이트리밋 소진 — 배치 전체가 **같은 이유로** 죽는 상황."""
    return [LlmCallError("401 Unauthorized") for _ in items]


async def _all_parse_errors(items: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
    """원문마다 결정적으로 실패 — 재처리 모드에서는 정상적인 모습이다."""
    return [LlmParseError("ClassifiedItem 검증 실패") for _ in items]


def test_batch_wide_call_failure_halts_worker(worker_instance) -> None:
    """배치가 통째로 호출 단계에서 죽으면 워커를 세운다.

    ⚠️ 안 세우면 시스템 장애가 "N건 개별 실패"로 위장된다. 커서는 계속 전진하므로
       96,531건이면 배치 9,654개가 전부 dead-letter 로 넘어간 채 정상 종료한다.
       장애가 길어져 재처리가 DEAD_LETTER_MAX_ATTEMPTS 를 넘기면 회수 대상에서도 빠진다.
    """
    items = [_request(f"I-{i}") for i in range(10)]

    with patch.object(worker, "classify_aspect", _all_call_errors):
        results, failures = worker_instance.classify_items(items)

    assert results == []
    assert len(failures) == 10
    assert worker_instance.is_running is False


def test_batch_wide_parse_failure_keeps_running(worker_instance) -> None:
    """전량 실패라도 **파싱·검증 실패**뿐이면 세우지 않는다.

    `--retry-failed` 는 이미 실패한 건만 모아 돌리는 모드라 전량 실패가 정상이다.
    건수만 보고 세우면 회수 작업이 첫 배치에서 멈춘다.
    """
    items = [_request(f"I-{i}") for i in range(10)]

    with patch.object(worker, "classify_aspect", _all_parse_errors):
        _, failures = worker_instance.classify_items(items)

    assert len(failures) == 10
    assert worker_instance.is_running is True


def test_partial_failure_keeps_running(worker_instance) -> None:
    """일부만 실패하면 계속 돈다 — 격리가 이 PR 의 원래 목적이다."""
    items = [_request("I-1"), _request(FAILING_ID), _request("I-3")]

    with patch.object(worker, "classify_aspect", _fake_classify_aspect):
        results, failures = worker_instance.classify_items(items)

    assert len(results) == 2
    assert len(failures) == 1
    assert worker_instance.is_running is True


# ── dead-letter 기록 (process_batch 통합) ────────────────────────────────


def _open_pipeline_db() -> sqlite3.Connection:
    """확정 스키마 원문 테이블 + 통합 뷰 + 분류 결과 + dead-letter 까지 갖춘 최소 DB."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    raw_schema.create_source_tables(conn)
    raw_schema.create_classified_tables(conn)
    return conn


def _insert_cs(conn: sqlite3.Connection, item_id: str, occurred: str) -> sqlite3.Row:
    """§2-4 cs 에 원문 1건을 넣고 워커가 읽는 뷰의 행을 돌려준다."""
    conn.execute(
        "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id, "
        "content, inquired_at, created_at) VALUES (?,?,?,?,?,?,?)",
        (item_id, "cp", "P001", "COUPANG", "색이 달라요", occurred, occurred),
    )
    return conn.execute(
        f"SELECT * FROM {worker.SOURCE_VIEW} WHERE item_id = ?", (item_id,)
    ).fetchone()


def test_dead_letter_records_occurred_at(worker_instance) -> None:
    """실패 건이 dead-letter 에 **발생 시각과 함께** 기록된다.

    ⚠️ 여기에 숨은 결합이 있다. `record_failures` 는 `occurred_at_by_id[item_id]` 로
       조회하는데, 그 키를 채우는 것은 `classify_items` 가 돌려준 `item.item_id` 다.
       둘이 같은 값이라는 보장은 `_to_request_item` 의 `"item_id": row["item_id"]`
       한 줄뿐이다(§5-1 A안이 그 근거다 — item_id 는 원문 PK 재사용). 어긋나면
       occurred_at 이 "" 로 들어가고 `FETCH_FAILED_SQL` 의 페이지 커서 정렬
       (`f.occurred_at, f.item_id`)이 깨져 `--retry-failed` 회수가 조용히 망가진다.
       이 테스트가 그 등식을 고정한다.
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    occurred = "2026-05-01T10:00:00+09:00"
    rows = [_insert_cs(conn, f"INQ-{i}", occurred) for i in range(2)]
    conn.commit()

    # 1건 성공 / 1건 실패
    async def _one_fails(items: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
        return [
            LlmParseError("검증 실패") if item.item_id == "INQ-1" else _classified(item.item_id, Aspect.COLOR)
            for item in items
        ]

    with patch.object(worker, "classify_aspect", _one_fails):
        worker_instance.process_batch(rows)

    dead = conn.execute("SELECT * FROM classification_failure").fetchall()
    assert [r["item_id"] for r in dead] == ["INQ-1"]
    # 이 값이 "" 면 재처리 페이지 커서가 깨진다
    assert dead[0]["occurred_at"] == occurred
    assert dead[0]["stage"] == "classify"
    assert dead[0]["attempts"] == 1

    # 성공한 건은 정상 적재되고 커서도 전진한다
    assert conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0] == 1
    cursor_row = conn.execute("SELECT * FROM classification_cursor").fetchone()
    # §2-8 컬럼명 — 뷰의 occurred_at 이 여기로 들어간다
    assert cursor_row["last_inquired_at"] == occurred
    assert cursor_row["last_item_id"] == "INQ-1"


def test_dead_letter_is_cleared_when_item_succeeds(worker_instance) -> None:
    """재처리에서 성공하면 dead-letter 에서 지워진다(회수 경로)."""
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    occurred = "2026-05-01T10:00:00+09:00"
    rows = [_insert_cs(conn, "INQ-0", occurred)]
    conn.execute(
        worker.FAILURE_UPSERT, ("INQ-0", occurred, "classify", "이전 실패", occurred, occurred)
    )
    conn.commit()

    async def _succeeds(items: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
        return [_classified(item.item_id, Aspect.COLOR) for item in items]

    with patch.object(worker, "classify_aspect", _succeeds):
        worker_instance.process_batch(rows, advance_cursor=False)

    assert conn.execute("SELECT COUNT(*) FROM classification_failure").fetchone()[0] == 0
    # advance_cursor=False 라 커서는 그대로다(재처리는 지나간 구간을 훑는다)
    assert conn.execute("SELECT COUNT(*) FROM classification_cursor").fetchone()[0] == 0


def test_view_merges_cs_and_reviews_on_one_time_axis() -> None:
    """`voc_document` 가 cs.inquired_at 과 reviews.created_at 을 같은 축으로 맞춘다.

    두 테이블의 시각 컬럼명이 갈려 있어(§2-4 / §2-5), 뷰가 엉뚱한 컬럼을 고르면 워커의
    타임라인 커서가 리뷰만 건너뛰거나 같은 건을 반복해서 잡는다.
    """
    conn = _open_pipeline_db()
    conn.execute(
        "INSERT INTO cs (id, channel_id, content, inquired_at, created_at) VALUES (?,?,?,?,?)",
        # 적재 시각(created_at)은 일부러 훨씬 나중으로 둔다 — 이 값이 잡히면 순서가 뒤집힌다
        ("INQ-1", "COUPANG", "문의", "2026-05-01T10:00:00+09:00", "2026-09-09T00:00:00+09:00"),
    )
    conn.execute(
        "INSERT INTO reviews (id, channel_id, content, rating, created_at) VALUES (?,?,?,?,?)",
        ("RVW-1", "NAVER", "리뷰", 2, "2026-05-02T10:00:00+09:00"),
    )
    conn.commit()

    rows = conn.execute(
        f"SELECT item_id, source, occurred_at FROM {worker.SOURCE_VIEW} ORDER BY occurred_at, item_id"
    ).fetchall()

    assert [(r["item_id"], r["source"]) for r in rows] == [("INQ-1", "cs"), ("RVW-1", "review")]
    assert rows[0]["occurred_at"] == "2026-05-01T10:00:00+09:00"  # inquired_at, created_at 아님


# ── 구버전 raw DB 감지 ───────────────────────────────────────────────────

# 8/7 확정 이전 커서 테이블. 컬럼명이 last_occurred_at / last_event_id 였다(§2-8 이전).
LEGACY_CURSOR_DDL = """
CREATE TABLE classification_cursor (
    worker_id        TEXT PRIMARY KEY,
    last_occurred_at TEXT,
    last_event_id    TEXT,
    updated_at       TEXT NOT NULL
);
"""


def test_fresh_db_is_not_flagged_as_legacy() -> None:
    """확정 스키마로 만든 DB·AI 테이블이 아직 없는 DB 는 구버전이 아니다.

    여기서 오탐이 나면 정상적인 팀원 전원이 워커를 못 돌린다 — 감지 로직 자체보다
    이쪽이 더 위험해서 같이 고정한다.
    """
    empty = sqlite3.connect(":memory:")
    raw_schema.create_source_tables(empty)
    assert raw_schema.find_legacy_tables(empty) == []

    raw_schema.create_classified_tables(empty)
    assert raw_schema.find_legacy_tables(empty) == []


def test_legacy_raw_db_is_rejected_with_guidance(tmp_path, caplog) -> None:
    """확정 이전 구조가 남아 있으면 안내하고 멈춘다.

    ⚠️ `CREATE TABLE IF NOT EXISTS` 는 옛 테이블을 그대로 둔다. 이걸 안 잡으면 스키마
       생성은 조용히 통과하고 `load_cursor()` 가 `no such column: last_inquired_at` 로
       터진다 — 스택트레이스만 보고는 "DB 를 다시 만들어야 한다"를 알 수 없다.
    """
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    raw_schema.create_source_tables(conn)  # 원문 테이블은 정상이라 기존 가드는 통과한다
    conn.execute(LEGACY_CURSOR_DDL)
    conn.commit()
    conn.close()

    with caplog.at_level("ERROR"), pytest.raises(SystemExit):
        worker.open_db(str(db_path))

    assert "classification_cursor" in caplog.text
    # 원문까지 지우라고 하면 12.8만 행을 다시 재생해야 한다 — AI 소유 테이블만 지운다
    assert "DROP TABLE classification_cursor;" in caplog.text
    assert "DROP TABLE cs" not in caplog.text


# ── FK 실효성 (리뷰 3번) ─────────────────────────────────────────────────


def test_foreign_keys_are_actually_enforced(tmp_path) -> None:
    """`PRAGMA foreign_keys=ON` 이 켜져 DDL 의 REFERENCES 가 실제로 걸린다.

    ⚠️ sqlite 는 FK 가 **기본 OFF** 라, 안 켜면 REFERENCES 가 장식으로만 남는다. 그러면
       부모 없는 aspect 행이 조용히 생기고 — "분류 결과는 있는데 문서가 없는" 상태 —
       원문에서 분모를 세는 합의 아래에서 커버리지 집계가 어긋난다.

    ⚠️ 평문 INSERT 가 아니라 **프로덕션과 같은 `INSERT OR IGNORE`** 로 찌른다. 워커의
       적재 구문이 `OR IGNORE` 라, 평문으로 검증하면 "OR IGNORE 니까 실제로는 조용히
       넘어가는 것 아닌가" 라는 의심이 그대로 남는다. sqlite 에서 `OR IGNORE` 는 제약
       위반 중 **FK 만은 삼키지 않는다** — 그 사실까지 여기서 같이 고정한다.
    """
    db_path = tmp_path / "fk.db"
    seed = sqlite3.connect(db_path)
    raw_schema.create_source_tables(seed)
    seed.commit()
    seed.close()

    conn = worker.open_db(str(db_path))
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(worker.CLASSIFIED_ITEM_ASPECT_INSERT, ("없는-부모", "색상", -1, None))

    # 대조군: OR IGNORE 가 실제로 삼키는 것(UNIQUE 중복)은 그대로 통과해야 한다.
    # 이게 없으면 위 단언이 "OR IGNORE 가 아무것도 안 삼킨다"로 오해될 수 있다.
    conn.execute(worker.CLASSIFIED_ITEM_INSERT, ("INQ-1", "cs", None, "v1"))
    conn.execute(worker.CLASSIFIED_ITEM_ASPECT_INSERT, ("INQ-1", "색상", -1, None))
    conn.execute(worker.CLASSIFIED_ITEM_ASPECT_INSERT, ("INQ-1", "색상", -1, None))  # 중복
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 1

    conn.close()
