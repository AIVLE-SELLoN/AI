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

    # ⚠️ **리터럴로 적는다 — `raw_schema.VERSION_COLUMNS` 를 펼치지 말 것.** 검사 대상
    #    코드를 그대로 참조하면 그 상수에 오타가 나도, 확정 문서와 갈려도 통과한다.
    #    "테스트가 정답을 잘못 베꼈으면 테스트도 같이 통과한다"를 막는 게 이 단언의
    #    존재 이유다. (2026-08-12 리뷰 §3)
    assert columns == {
        "item_id",
        "source",
        "classified_at",
        # 버전 3종은 감사용 메타가 아니라 **탐지의 조회 조건**이다 — 35일 창에 서로
        # 다른 분류기의 결과가 섞이면 분류기 개선이 고객 이상 알림으로 발화한다.
        "prompt_version",
        "model_version",
        "pipeline_version",
    }
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
    """같은 배치를 다시 적재해도 행이 늘지 않는다(재시도·재실행 안전).

    ⚠️ 반환값은 이제 **다시 쓴 aspect 행 수**다(예전엔 0 이었다). 적재가 upsert +
       aspect 교체로 바뀌어서, 재적재는 "무시"가 아니라 "같은 값으로 덮어쓰기"다.
       DB 상태가 안 변한다는 계약은 그대로이므로 아래 두 COUNT 로 확인한다.
    """
    conn = _open_memory_db()
    items = [_classified("I-1", Aspect.COLOR, Aspect.SIZE)]
    _save(conn, items)
    second = _save(conn, items)

    assert second == 2
    assert conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 2


def test_reclassification_replaces_the_previous_result() -> None:
    """프롬프트를 바꿔 재분류하면 **옛 결과가 덮인다.**

    🔴 이게 예전 `INSERT OR IGNORE` 의 실제 버그다. 이미 있는 item_id 를 통째로 무시해서,
       프롬프트를 갈아끼우고 재분류를 돌려도 `prompt_version` 도 aspect 도 옛 값 그대로
       남았다. 그러면 탐지가 읽는 35일 창에 두 프롬프트 결과가 섞이고, 라벨러 교체가
       고객 이상처럼 발화한다.

    두 축을 다 본다:
      - 부모의 `prompt_version` 이 새 값으로 갱신되는가
      - **새 프롬프트가 더 이상 안 내는 aspect 가 사라지는가** (upsert 만으로는 안 되고
        옛 aspect 를 지워야 한다 — 안 지우면 부모는 새 버전인데 자식은 섞인 상태가 된다)
    """
    conn = _open_memory_db()
    _save(conn, [_classified("I-1", Aspect.COLOR, Aspect.SIZE)])

    with patch.object(worker.service_module, "PROMPT_SENTIMENT_VERSION", "classify_x_v99"):
        _save(conn, [_classified("I-1", Aspect.COLOR)])

    assert conn.execute("SELECT prompt_version FROM classified_item").fetchone()[0] == "classify_x_v99"
    aspects = {row[0] for row in conn.execute("SELECT aspect FROM classified_item_aspect")}
    assert aspects == {Aspect.COLOR.value}, "새 프롬프트가 안 낸 사이즈 행이 남았다"


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


def test_denominator_counts_source_not_classified_rows() -> None:
    """커버리지 분모는 **원문**에서 센다 — 분류 안 된 문의도 남는다(§2-4).

    ⚠️ **탐지 분모가 아니다.** `COUNT_SOURCE_SQL` 은 `log_coverage()` 한 곳에서만 쓰이는
       커버리지 로그용 카운터다. 이상탐지가 쓰는 분모는 `daily.py::load_inputs_from_db`
       쪽이라 여기를 뒤집어도 부정률·발화 기준은 안 움직인다.
       (2026-08-11 리뷰 정정 — 예전 문장은 "그 값이 그대로 이상탐지 발화 기준이 된다"
        였는데 틀렸다. 그대로 두면 다음 사람이 "탐지 분모가 여기 있다" 고 믿는다.)

    그래도 고정할 값어치가 있다: 이 카운터가 `classified_item` 을 세도록 바뀌면 커버리지
    로그의 total 이 **분류 성공분만** 세게 되어, "원문 대비 얼마나 분류됐나" 라는 이 로그의
    존재 이유가 사라진다. 미달을 못 보게 되는 것이라 조용히 무의미해진다.

    §2-4 가 "분류 안 된 문의도 반드시 남는다"고 못박은 것이 그 비교의 전제다. 그래서
    3건 중 1건만 분류된 상태를 만들어, 분모가 **3** 인지(원문 기준) **1** 인지(분류 기준)
    가른다.
    """
    conn = _open_pipeline_db()
    for i in range(1, 4):
        conn.execute(
            "INSERT INTO cs (id, channel_id, content, inquired_at, created_at) VALUES (?,?,?,?,?)",
            (f"INQ-{i}", "COUPANG", "문의", f"2026-05-0{i}T10:00:00+09:00", None),
        )
    # 3건 중 1건만 분류됐다 — 나머지 2건은 실패했거나 아직 안 돌았다.
    conn.execute(
        worker.CLASSIFIED_ITEM_UPSERT,
        ("INQ-1", "cs", "2026-05-01T11:00:00+09:00", "v1", "m1", "p1"),
    )
    conn.commit()

    denominator = conn.execute(worker.COUNT_SOURCE_SQL, worker.CLASSIFY_SOURCES).fetchone()[0]
    classified = conn.execute("SELECT COUNT(*) FROM classified_item").fetchone()[0]

    assert classified == 1, "픽스처 전제가 깨졌다 — 1건만 분류돼 있어야 한다"
    assert denominator == 3, (
        f"분모가 {denominator} 다 — 분류 결과({classified}건)를 세고 있다. "
        "원문(voc_document)에서 세야 분류 실패분이 분모에 남는다"
    )


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


def test_schema_matches_the_confirmed_ddl() -> None:
    """🔴 `raw_schema` 가 확정 DDL 전문과 컬럼이 일치한다 — 부분집합이면 안 된다.

    ⚠️ 이 파일은 우리가 정한 규칙이 아니라 **확정 문서를 옮겨 적은 것**이다(모듈 docstring).
       옮기다 빠뜨려도 당장은 안 깨진다 — 빠진 컬럼을 아무도 안 읽으면 그만이다. 실제로
       `products.fetched_at/updated_at` 과 `mapped_data` 의 매핑 메타 3종이 그렇게 빠져
       있었다(2026-08-11 리뷰에서 발견).

    조용히 아픈 이유가 둘이다:
      · `mapped_at` 은 §5-3 이 확정한 스냅샷 동기화의 근거 컬럼이다. "최신 매핑을 고른다"
        는 쿼리가 생기면 운영에선 돌고 **목에서만 no such column 으로 죽는다.**
      · DDL 이 `CREATE TABLE IF NOT EXISTS` 라, 좁은 정의로 만들어진 raw.db 는 나중에
        컬럼을 채워도 **영원히 옛 모양으로 남는다.** `find_legacy_tables()` 는
        `LEGACY_MARKERS` 에 등록된 테이블만 보므로 이 형태는 못 잡는다.
    """
    expected = {
        "channel": {"channel_id", "display_name", "connected_at", "status"},
        "products": {
            "variant_row_id", "channel_product_id", "channel_id", "channel_product_name",
            "option_group_names", "channel_option_name", "sale_price", "original_price",
            "fetched_at", "updated_at",
        },
        "mapped_data": {
            "variant_row_id", "product_group_id", "mapping_method",
            "mapping_confidence", "mapped_at",
        },
        "orders": {
            "channel_id", "channel_product_id", "order_date",
            "quantity", "order_amount", "created_at",
        },
        "cs": {
            "id", "channel_product_id", "product_group_id", "channel_id",
            "content", "inquired_at", "created_at",
        },
        "reviews": {
            "id", "channel_product_id", "product_group_id", "channel_id",
            "content", "rating", "created_at",
        },
        # ⚠️ 다른 테이블과 마찬가지로 **리터럴이다.** `raw_schema.VERSION_COLUMNS` 를
        #    펼치면 검사 대상 코드를 정답지로 쓰는 셈이라 이 테스트가 눈을 감는다.
        "classified_item": {
            "item_id", "source", "classified_at",
            "prompt_version", "model_version", "pipeline_version",
        },
        "classified_item_aspect": {"id", "item_id", "aspect", "sentiment", "mixed_signal"},
        "classification_failure": {
            "item_id", "occurred_at", "stage", "error",
            "attempts", "first_failed_at", "last_failed_at",
        },
        "classification_cursor": {
            "worker_id", "last_inquired_at", "last_item_id", "updated_at",
        },
    }
    conn = _open_pipeline_db()

    for table, columns in expected.items():
        actual = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        assert actual == columns, (
            f"{table} 이 확정 DDL 과 다르다 — 누락 {sorted(columns - actual)} / "
            f"초과 {sorted(actual - columns)}"
        )


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


def test_version_columns_missing_is_flagged_as_legacy() -> None:
    """🔴 버전 컬럼이 없는 옛 `classified_item` 도 구버전으로 잡힌다.

    `LEGACY_MARKERS` 의 판정이 "마커 컬럼이 없으면 옛것"이라 **마커는 그 테이블에 가장
    나중에 들어온 컬럼이어야 한다.** 컬럼을 추가하고 마커를 안 옮기면 그 사이 버전의
    테이블이 전부 최신으로 통과한다.

    실제로 버전 컬럼 2개를 넣으면서(2026-08-12) 마커가 `prompt_version` 에 남아 있었고,
    4컬럼 시절 테이블이 이 함수를 통과했다. `IF NOT EXISTS` 가 옛 테이블을 그대로 두는데
    인덱스는 새 컬럼을 참조해서, 가드가 막으려던 자리(`no such column`)로 되돌아갔다:

        find_legacy_tables()       = []
        create_classified_tables() → OperationalError: no such column: model_version

    ⚠️ **컬럼명을 리터럴로 적는다** — `raw_schema.VERSION_COLUMNS[-1]` 로 쓰면 마커를
       안 옮겼을 때 이 테스트도 같이 통과해서 아무것도 못 잡는다.
    """
    old = sqlite3.connect(":memory:")
    old.execute(
        "CREATE TABLE classified_item ("
        " item_id TEXT PRIMARY KEY, source TEXT NOT NULL,"
        " classified_at TEXT, prompt_version TEXT)"
    )

    assert raw_schema.find_legacy_tables(old) == ["classified_item"]


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
    assert "DROP TABLE IF EXISTS classification_cursor;" in caplog.text

    # 🔴 **원문 보호 가드 — 문구를 좁히지 말 것.** 원문까지 지우라고 하면 12.8만 행을
    #    다시 재생해야 한다. `"DROP TABLE IF EXISTS cs;"` 처럼 정확한 문장으로 단언하면
    #    안내가 `DROP TABLE cs;`(IF EXISTS 없이)로 바뀌었을 때 **그냥 통과한다.**
    #    실제로 IF EXISTS 를 붙이면서 한 번 좁혔다(2026-08-13 리뷰 §2). 테이블 이름이
    #    DROP 문에 등장하는지로 넓게 본다.
    for protected in ("cs", "reviews", "channel", "products", "mapped_data", "orders"):
        assert f"DROP TABLE IF EXISTS {protected};" not in caplog.text
        assert f"DROP TABLE {protected};" not in caplog.text


def test_legacy_guidance_always_drops_the_child_table(tmp_path, caplog) -> None:
    """🔴 안내가 `classified_item_aspect` 도 함께 지우게 한다.

    그 테이블에는 `LEGACY_MARKERS` 마커가 없어서(8/7 이전엔 아예 없던 테이블) `legacy`
    목록에 **절대 안 들어온다.** 안내를 목록으로만 만들면 `DROP TABLE classified_item;`
    하나만 나오고, 그대로 따르면 **부모 없는 aspect 행이 남는다.**

    탐지는 `FROM classified_item ci LEFT JOIN classified_item_aspect` 라 부모를 거쳐 읽어
    무해하지만, **월간 집계는 `FROM voc_document r JOIN classified_item_aspect a` 로 부모를
    안 거친다** — 원문이 그대로 있으니 그 행들이 계속 집계에 잡히고 옛 분류기 라벨이
    리포트에 섞인다.

    문서 §4-3 이 두 테이블을 지우라고 하므로, 안내와 절차가 같은 끝 상태를 만들어야 한다.
    (2026-08-13 리뷰 §3)
    """
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    raw_schema.create_source_tables(conn)
    # 버전 컬럼이 없는 옛 부모 + 정상 자식 = "부분적으로 지워진 DB" 의 실제 모양
    conn.execute(
        "CREATE TABLE classified_item ("
        " item_id TEXT PRIMARY KEY, source TEXT NOT NULL,"
        " classified_at TEXT, prompt_version TEXT)"
    )
    conn.execute(raw_schema.CLASSIFIED_ITEM_ASPECT_DDL)
    conn.commit()
    conn.close()

    with caplog.at_level("ERROR"), pytest.raises(SystemExit):
        worker.open_db(str(db_path))

    assert "DROP TABLE IF EXISTS classified_item;" in caplog.text
    assert "DROP TABLE IF EXISTS classified_item_aspect;" in caplog.text

    # 🔴 **자식이 부모보다 앞에 와야 한다.** `PRAGMA foreign_keys=ON` 세션에서 부모부터
    #    지우면 `FOREIGN KEY constraint failed` 로 안내가 통째로 실패한다. sqlite3 CLI 는
    #    기본이 OFF 라 보통은 돌지만, 켜 둔 셸에서는 안 돈다. (2026-08-13 리뷰 §4)
    assert caplog.text.index("DROP TABLE IF EXISTS classified_item_aspect;") < caplog.text.index(
        "DROP TABLE IF EXISTS classified_item;"
    )


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
    conn.execute(worker.CLASSIFIED_ITEM_UPSERT, ("INQ-1", "cs", None, "v1", "m1", "p1"))
    conn.execute(worker.CLASSIFIED_ITEM_ASPECT_INSERT, ("INQ-1", "색상", -1, None))
    conn.execute(worker.CLASSIFIED_ITEM_ASPECT_INSERT, ("INQ-1", "색상", -1, None))  # 중복
    assert conn.execute("SELECT COUNT(*) FROM classified_item_aspect").fetchone()[0] == 1

    conn.close()


# ── 분류기 버전 backfill (--reclassify-stale, 2026-08-12) ────────────────────


def _active_of(source: str) -> tuple[str, str, str]:
    """그 source 의 활성 3축 — `(프롬프트, 모델, 파이프라인)`."""
    prompt_cs, prompt_review, model, pipeline = worker.active_version_params()
    return (prompt_cs if source == "cs" else prompt_review), model, pipeline


def _mark_classified(
    conn: sqlite3.Connection,
    item_id: str,
    source: str,
    prompt=None,
    model=None,
    pipeline=None,
    *,
    active: bool = False,
) -> None:
    """분류 결과 1행. `active=True` 면 3축을 전부 활성 값으로 채운다."""
    if active:
        prompt, model, pipeline = _active_of(source)
    conn.execute(
        "INSERT INTO classified_item"
        " (item_id, source, prompt_version, model_version, pipeline_version)"
        " VALUES (?,?,?,?,?)",
        (item_id, source, prompt, model, pipeline),
    )


def test_stale_scan_finds_only_old_version_rows(worker_instance) -> None:
    """활성 버전이 아닌 행만 재분류 대상이다.

    ⚠️ **신규 조회(FETCH_BATCH_SQL)로는 절대 안 잡히는 행들이다.** 그쪽은 커서보다 뒤에
       있는 원문만 보는데, 이 행들은 커서가 이미 지나간 자리에 있다 — 분류기를 바꿔도
       지난 문서가 영원히 옛 라벨로 남던 이유가 이것이다.
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    for i in (1, 2, 3):
        _insert_cs(conn, f"INQ-{i}", f"2026-05-0{i}T10:00:00+09:00")
    _mark_classified(conn, "INQ-1", "cs", active=True)
    _mark_classified(conn, "INQ-2", "cs", "classify_aspect_v4", *_active_of("cs")[1:])
    _mark_classified(conn, "INQ-3", "cs")  # 버전을 안 남기던 시절 (전부 NULL)
    conn.commit()

    stale = worker_instance.fetch_stale_batch()

    assert [row["item_id"] for row in stale] == ["INQ-2", "INQ-3"]
    assert worker_instance.count_stale() == 2


def test_stale_scan_covers_model_and_pipeline_axes(worker_instance) -> None:
    """🔴 프롬프트가 같아도 **모델·파이프라인이 다르면 stale 이다.**

    프롬프트 축만 보면 "같은 프롬프트, 다른 라벨러" 가 통째로 새어 나간다 — 모델을
    갈아끼우거나 후처리·폴백을 손보면 프롬프트 파일은 한 글자도 안 바뀌었는데 분포가
    달라진다(`CLASSIFIER_PIPELINE_VERSION` 실측 2.1%). 그 행들이 35일 창에 섞이면
    분류기 변경이 고객 이상 알림으로 발화한다.
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    for i in (1, 2, 3):
        _insert_cs(conn, f"INQ-{i}", f"2026-05-0{i}T10:00:00+09:00")
    prompt, model, pipeline = _active_of("cs")
    _mark_classified(conn, "INQ-1", "cs", prompt, model, pipeline)  # 전부 활성
    _mark_classified(conn, "INQ-2", "cs", prompt, "other-model", pipeline)  # 모델만 다름
    _mark_classified(conn, "INQ-3", "cs", prompt, model, "classify_pipeline_v0")  # 파이프라인만
    conn.commit()

    assert [row["item_id"] for row in worker_instance.fetch_stale_batch()] == [
        "INQ-2",
        "INQ-3",
    ]


def test_stale_scan_catches_null_model_and_pipeline_axes(worker_instance) -> None:
    """🔴 프롬프트는 맞고 **모델·파이프라인만 NULL** 인 행도 재분류 대상이다.

    위 테스트와 행 하나 차이인데 잡는 것이 다르다. 저쪽은 축 값이 *다른* 경우라
    `=` 로 비교해도 FALSE 가 나와 `NOT (...)` 이 참이 된다 — 즉 **널 안전 비교가
    아니어도 통과한다.** 갈리는 것은 널일 때뿐이다:

        prompt 일치 + model NULL 로 두고 stale 조회
            IS NOT DISTINCT FROM  →  FALSE  →  NOT FALSE = 참  →  재분류 대상 ✅
            =                     →  NULL   →  NOT NULL  = 널  →  **영원히 안 잡힌다**

    그리고 이 행은 지어낸 것이 아니다 — 버전 컬럼이 `prompt_version` 하나뿐이던 시절
    (4컬럼)에 적재된 뒤 2026-08-12 에 컬럼 2개가 늘면서 정확히 이 모양이 된다.

    🔴 **놓치면 교착이다.** 배치 쪽 `_VERSION_COUNT_SQL` 은 `SUM(CASE WHEN ... ELSE 0)`
       이라 널을 FALSE 와 똑같이 "옛 버전" 으로 세서 **배치는 선다.** 그런데 여기 조회가
       그 행을 못 집으면 `--reclassify-stale` 이 "재분류할 문서가 없습니다" 로 끝나,
       에러가 시키는 조치로는 빠져나갈 수 없다("배치를 세우는 집합 ⊆ 재분류가 고칠 수
       있는 집합", 2026-08-12).
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    for i in (1, 2, 3):
        _insert_cs(conn, f"INQ-{i}", f"2026-05-0{i}T10:00:00+09:00")
    prompt, model, pipeline = _active_of("cs")
    _mark_classified(conn, "INQ-1", "cs", prompt, model, pipeline)  # 전부 활성
    _mark_classified(conn, "INQ-2", "cs", prompt, None, pipeline)  # 모델만 NULL
    _mark_classified(conn, "INQ-3", "cs", prompt, model, None)  # 파이프라인만 NULL
    conn.commit()

    assert [row["item_id"] for row in worker_instance.fetch_stale_batch()] == [
        "INQ-2",
        "INQ-3",
    ]
    assert worker_instance.count_stale() == 2


def test_stale_scan_is_per_source(worker_instance) -> None:
    """🔴 활성 버전은 source 마다 다르다 — CS 는 프롬프트1, 리뷰는 프롬프트2.

    값 하나로 거르면 한쪽 source 전체가 stale 로 잡혀서, 멀쩡한 96,524건을 통째로 다시
    LLM 에 태우게 된다(= 비용 전액 재지불).
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    _insert_cs(conn, "INQ-1", "2026-05-01T10:00:00+09:00")
    conn.execute(
        "INSERT INTO reviews (id, channel_product_id, product_group_id, channel_id,"
        " content, rating, created_at) VALUES (?,?,?,?,?,?,?)",
        ("RVW-1", "cp", "P001", "NAVER", "소재가 얇아요", 2, "2026-05-02T10:00:00+09:00"),
    )
    _mark_classified(conn, "INQ-1", "cs", active=True)
    _mark_classified(conn, "RVW-1", "review", active=True)
    conn.commit()

    assert worker_instance.count_stale() == 0, "각자 자기 프롬프트를 쓰고 있는데 stale 로 잡혔다"

    # CS 쪽 프롬프트만 올린다 → CS 행만 대상이 되어야 한다.
    with patch.object(worker.service_module, "PROMPT_ASPECT_VERSION", "classify_aspect_v6"):
        assert [row["item_id"] for row in worker_instance.fetch_stale_batch()] == ["INQ-1"]


def test_orphan_stale_rows_are_counted_separately(worker_instance) -> None:
    """🔴 원문이 사라진 stale 행은 **재분류로 없앨 수 없다** — 따로 센다.

    예전에는 `count_stale()` 이 `classified_item` 만 세고 `fetch_stale_batch()` 는 원문 뷰와
    INNER JOIN 을 타서 범위가 갈렸다. 그러면 `--reclassify-stale` 이 "1건 남았다"고 알리고
    곧바로 "대상을 모두 처리했습니다"로 끝난 뒤 종료 경고가 **영원히 남는다** — 고치라는데
    고칠 수단이 없는 경고라 다음 사람이 시간을 쓴다. (2026-08-12 리뷰 §5)

    목 데이터를 다시 만들면(원문만 갈아끼우면) 실제로 나는 상태다.
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    _insert_cs(conn, "INQ-1", "2026-05-01T10:00:00+09:00")
    _mark_classified(conn, "INQ-1", "cs", "classify_aspect_v4", *_active_of("cs")[1:])
    # 원문만 사라진 분류 결과 — 재분류하려 해도 태울 본문이 없다.
    _mark_classified(conn, "INQ-GONE", "cs", "classify_aspect_v4", *_active_of("cs")[1:])
    conn.commit()

    # 두 값이 갈리지 않는다: 셀 수 있는 것 = 고칠 수 있는 것
    assert worker_instance.count_stale() == 1
    assert [row["item_id"] for row in worker_instance.fetch_stale_batch()] == ["INQ-1"]
    assert worker_instance.count_orphan_stale() == 1


def test_reclassified_rows_leave_the_stale_scan(worker_instance) -> None:
    """재분류된 행은 다음 조회에서 빠진다 — 나눠 돌린 backfill 이 이어진다는 계약.

    이게 깨지면 `--limit` 로 나눠 돌릴 때 매번 같은 앞부분만 다시 태운다.
    """
    conn = _open_pipeline_db()
    worker_instance.conn = conn
    _insert_cs(conn, "INQ-1", "2026-05-01T10:00:00+09:00")
    _mark_classified(conn, "INQ-1", "cs", "classify_aspect_v4", *_active_of("cs")[1:])
    conn.commit()

    assert worker_instance.count_stale() == 1

    worker_instance.save_classified_items(
        [
            ClassifiedItem(
                item_id="INQ-1",
                source=Source.CS,
                channel=Channel.COUPANG,
                product_group_id="P001",
                raw_text="색이 달라요",
                aspects=[AspectSentiment(aspect=Aspect.COLOR, sentiment=Sentiment.NEGATIVE)],
                created_at=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
            )
        ]
    )

    assert worker_instance.count_stale() == 0
    assert worker_instance.fetch_stale_batch() == []
