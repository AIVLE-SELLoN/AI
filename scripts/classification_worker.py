"""분류 워커 — raw DB 의 원본 텍스트를 읽어 분류하고 classified_item 에 적재한다.

워크플로우 회의(2026-08-02) 반영:
  기존   : Kafka consumer 로 raw.* 토픽 구독 → 분류 → 로그 출력 (docker 컨테이너로 상주)
  변경후 : **raw DB(raw_event) 조회** → 분류 → **classified_item 테이블 적재(타임라인 순)**

  Kafka 구독을 걷어냈으므로 이 워커는 더 이상 docker compose 에 올라가지 않는다.
  브로커/컨슈머그룹/오프셋 커밋 대신, raw_event 를 (occurred_at, event_id) 순으로 훑는
  커서(classification_cursor)로 진행 상황을 관리한다.

분류 실패 건은 버리지 않고 dead-letter(classification_failure)에 남긴다.
탐지의 분모를 원본 테이블에서 세고 classified_item 을 LEFT JOIN 하기로 한 합의
(탐지 분모 산출 방식, 2026-08-03) 아래에서는 **분류 커버리지가 곧 분자의 정확도**다.
실패 건이 조용히 사라지면 분모는 그대로인데 분자만 비어 부정률이 과소추정된다.

실행:
  python scripts/classification_worker.py                 # 밀린 원본 전부 처리하고 종료
  python scripts/classification_worker.py --limit 50      # 시험 실행(과금 상한)
  python scripts/classification_worker.py --follow        # 프로듀서 재생을 준실시간 추종
  python scripts/classification_worker.py --retry-failed  # dead-letter 재처리(회수)
  python scripts/classification_worker.py --dry-run       # DB 없이 샘플 2건으로 추론만 확인

DB 는 mock_producer 와 같은 sqlite 파일을 기본으로 본다(추가 의존성 없음).
운영 DB 로 옮길 때는 open_db() 와 DDL 상수만 교체하면 되도록 표준 SQL 범위로 유지했다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app.classification.service as service_module
from app.classification.service import (
    ClassifyRequestItem,
    classify_aspect,
    explode_to_rows,
)
from app.core import constants
from app.core.exceptions import LlmParseError
from app.core.schemas import Aspect, AspectSentiment, ClassifiedItem, Sentiment, Source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ClassificationWorker")


# ── LLM 출력값 동적 정규화 몽키패치 ──────────────────────────────────────────────

def _normalize_sentiment(val: Any) -> Sentiment:
    if isinstance(val, Sentiment):
        return val
    try:
        return Sentiment(val)
    except (ValueError, TypeError):
        pass

    s_str = str(val).strip()
    mapping = {
        "1": "POSITIVE", "+1": "POSITIVE", "긍정": "POSITIVE", "positive": "POSITIVE",
        "-1": "NEGATIVE", "부정": "NEGATIVE", "negative": "NEGATIVE",
        "0": "NEUTRAL", "중립": "NEUTRAL", "neutral": "NEUTRAL",
    }
    target = mapping.get(s_str, mapping.get(s_str.lower(), s_str))

    for candidate in (target, target.upper(), target.lower()):
        if hasattr(Sentiment, candidate):
            return getattr(Sentiment, candidate)
        try:
            return Sentiment[candidate]
        except KeyError:
            pass
        try:
            return Sentiment(candidate)
        except ValueError:
            pass

    raise ValueError(f"'{val}'은(는) 유효한 Sentiment 값이 아닙니다.")


def _normalize_aspect(val: Any) -> Aspect:
    if isinstance(val, Aspect):
        return val
    try:
        return Aspect(val)
    except (ValueError, TypeError):
        pass

    a_str = str(val).strip()
    mapping = {
        "색상": "COLOR", "사이즈": "SIZE", "소재": "MATERIAL",
        "파손": "DAMAGE", "오배송": "MISDELIVERY", "기타": "ETC",  # Aspect.ETC (OTHERS 아님)
    }
    target = mapping.get(a_str, mapping.get(a_str.lower(), a_str))

    for candidate in (target, target.upper(), target.lower()):
        if hasattr(Aspect, candidate):
            return getattr(Aspect, candidate)
        try:
            return Aspect[candidate]
        except KeyError:
            pass
        try:
            return Aspect(candidate)
        except ValueError:
            pass

    raise ValueError(f"'{val}'은(는) 유효한 Aspect 값이 아닙니다.")


def _patched_parse_llm_response(data: dict, source: Source, *, trace_key: str) -> list[AspectSentiment]:
    raw_aspects = data.get("aspects")
    if raw_aspects is None:
        raise LlmParseError(f"LLM 응답에 'aspects' 키 없음 [{trace_key}]: {data}")

    try:
        result = []
        for a in raw_aspects:
            result.append(
                AspectSentiment(
                    aspect=_normalize_aspect(a["aspect"]),
                    sentiment=_normalize_sentiment(a["sentiment"]),
                    mixed_signal=a.get("mixed_signal") if source == Source.REVIEW else None,
                )
            )
        return result
    except (KeyError, ValueError) as exc:
        raise LlmParseError(f"aspects 파싱 실패 [{trace_key}]: {exc} (원본: {raw_aspects})") from exc


service_module._parse_llm_response = _patched_parse_llm_response


# ── DB 설정 ─────────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = os.getenv("RAW_DB_PATH", "./data/raw.db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", getattr(constants, "BATCH_SIZE", 10)))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))
WORKER_ID = os.getenv("CLASSIFICATION_WORKER_ID", "classification-worker-1")

# DB 적재 재시도 횟수. 잠금 경합(다른 프로세스가 쓰는 중)만 노린 값이라 짧게 잡는다 —
# LLM 재시도와 달리 비용이 들지 않지만, 무한정 붙들고 있어도 상황이 나아지지 않는다.
DB_MAX_RETRY = 3

# dead-letter 재처리 상한. 이 횟수만큼 실패한 건은 --retry-failed 대상에서 빠진다
# (결정적 실패를 계속 다시 부르면 과금만 늘고 결과는 같다). 기록은 남으므로 커버리지
# 집계에서는 계속 보인다.
DEAD_LETTER_MAX_ATTEMPTS = int(os.getenv("DEAD_LETTER_MAX_ATTEMPTS", "3"))

# 분류 대상 source. 주문(ORDER)·상세변경(DETAIL_CHANGE)은 원문 텍스트 분류 대상이 아니라
# raw_event 에 source=NULL 로 들어오므로 자연히 제외된다.
CLASSIFY_SOURCES = (Source.CS.value, Source.REVIEW.value)

# classified_item: explode 규약(분류 워커 명세 §2)대로 aspect 1개당 1행.
#   created_at             원문 발생 시각 = raw_event.occurred_at (타임라인 정렬 키)
#   classified_item_id     적재 순번. 타임라인 순으로 INSERT 하므로 이 값의 순서 = 타임라인 순서
#   UNIQUE(item_id, aspect) 재실행/재시도 시 같은 행이 중복 적재되지 않게 하는 멱등 키
#
# ⚠️ 이 테이블로 **분모를 세면 안 된다** (탐지 분모 산출 방식 논의, 2026-08-03 확정).
#    자세한 내용은 아래 DDL 주석 참고.
CLASSIFIED_ITEM_DDL = """
-- ⚠️ 이 테이블은 "aspect 언급 목록"이지 "문서 목록"이 아니다.
--    언급된 aspect 가 하나도 없는 리뷰는 explode 결과가 0행이라 여기에 아예 안 남는다
--    (리뷰 프롬프트가 대상 속성이 없으면 빈 배열을 내도록 지시하고 있다).
--    따라서 COUNT(DISTINCT item_id) 로 총 리뷰 수(= 탐지의 분모)를 셀 수 없다 — 과대추정된다.
--    분모는 원본 테이블(raw_event → 실서비스 cs/reviews)에서 세고, 이 테이블을 LEFT JOIN 해서
--    분자(부정 언급)만 가져올 것.
CREATE TABLE IF NOT EXISTS classified_item (
    classified_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id            TEXT NOT NULL,
    source             TEXT NOT NULL,
    channel            TEXT NOT NULL,
    product_group_id   TEXT NOT NULL,
    aspect             TEXT NOT NULL,
    sentiment          INTEGER NOT NULL,
    mixed_signal       INTEGER,
    raw_text           TEXT,
    created_at         TEXT NOT NULL,
    classified_at      TEXT NOT NULL,
    UNIQUE (item_id, aspect)
);
"""

# classification_failure: 분류에 실패한 원문의 dead-letter 기록.
#
# 이게 없으면 실패 건이 로그로만 남고 커서가 지나가 버려 **영구 유실**된다. 그러면
# "원본 테이블에서 분모를 세고 classified_item 을 LEFT JOIN 한다"는 합의의 전제인
# 분류 커버리지 100% 가 조용히 깨진다(분자만 빠져 부정률이 과소추정된다).
# 실패를 여기 남겨야 ①얼마나 빠졌는지 셀 수 있고 ②나중에 재처리할 수 있다.
#   stage    parse(스키마 변환 실패) / classify(LLM 호출·파싱 실패)
#   attempts 재처리 시도 횟수. 결정적 실패를 무한 재과금하지 않도록 상한을 건다.
FAILURE_DDL = """
CREATE TABLE IF NOT EXISTS classification_failure (
    event_id        TEXT PRIMARY KEY,
    occurred_at     TEXT NOT NULL,
    stage           TEXT NOT NULL,
    error           TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 1,
    first_failed_at TEXT NOT NULL,
    last_failed_at  TEXT NOT NULL
);
"""

FAILURE_UPSERT = """
INSERT INTO classification_failure
    (event_id, occurred_at, stage, error, attempts, first_failed_at, last_failed_at)
VALUES (?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET
    stage          = excluded.stage,
    error          = excluded.error,
    attempts       = classification_failure.attempts + 1,
    last_failed_at = excluded.last_failed_at
"""

FAILURE_DELETE = "DELETE FROM classification_failure WHERE event_id = ?"

# 재처리 대상 조회. 시도 횟수가 상한 미만인 것만 — 결정적 실패(예: 리뷰에 허용되지 않는
# aspect)를 매번 다시 LLM 에 태우면 돈만 쓰고 결과는 같다.
#
# ⚠️ (occurred_at, event_id) 페이지 커서가 꼭 필요하다. 이게 없으면 재처리에 또 실패한
#    건이 다음 조회에 **다시 잡혀서**, 한 번 실행하는 동안 같은 건을 상한까지 반복
#    호출한다(1회 실행 = 건당 max_attempts 회 과금). 한 실행에서는 건당 1회만 시도한다.
FETCH_FAILED_SQL = """
SELECT r.event_id, r.source, r.channel, r.channel_product_id, r.product_group_id,
       r.raw_text, r.occurred_at
FROM classification_failure f
JOIN raw_event r ON r.event_id = f.event_id
WHERE f.attempts < ?
  AND (f.occurred_at > ? OR (f.occurred_at = ? AND f.event_id > ?))
ORDER BY f.occurred_at, f.event_id
LIMIT ?
"""

# 커서: Kafka 컨슈머 오프셋을 대체한다. 어디까지 읽었는지만 기록하고,
# classified_item INSERT 와 같은 트랜잭션에서 갱신해 원자성을 보장한다.
CURSOR_DDL = """
CREATE TABLE IF NOT EXISTS classification_cursor (
    worker_id        TEXT PRIMARY KEY,
    last_occurred_at TEXT,
    last_event_id    TEXT,
    updated_at       TEXT NOT NULL
);
"""

CLASSIFIED_ITEM_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_classified_item_timeline ON classified_item (created_at, classified_item_id);",
    "CREATE INDEX IF NOT EXISTS idx_classified_item_group ON classified_item (product_group_id, aspect, created_at);",
]

CLASSIFIED_ITEM_INSERT = """
INSERT OR IGNORE INTO classified_item
    (item_id, source, channel, product_group_id, aspect, sentiment, mixed_signal,
     raw_text, created_at, classified_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# (occurred_at, event_id) 복합 커서보다 큰 행만 타임라인 순으로 가져온다.
# 튜플 비교 대신 풀어 쓴 이유는 구버전 sqlite 호환(row value 는 3.15+).
FETCH_BATCH_SQL = f"""
SELECT event_id, source, channel, channel_product_id, product_group_id, raw_text, occurred_at
FROM raw_event
WHERE source IN ({', '.join(['?'] * len(CLASSIFY_SOURCES))})
  AND raw_text IS NOT NULL AND TRIM(raw_text) <> ''
  AND (occurred_at > ? OR (occurred_at = ? AND event_id > ?))
ORDER BY occurred_at, event_id
LIMIT ?
"""


def open_db(db_path_str: str) -> sqlite3.Connection:
    """raw DB 연결 + 분류 결과 테이블 보장.

    raw_event 는 mock_producer 가 만든다. 없으면 아직 원본이 한 건도 적재되지 않은
    상태이므로, 잘못된 경로를 조용히 새 빈 파일로 만들어 버리지 않도록 여기서 멈춘다.
    """
    db_path = Path(db_path_str).resolve()
    if not db_path.exists():
        logger.error(f"[DB ERROR] raw DB 가 없습니다: {db_path} (mock_producer 를 먼저 실행하세요)")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='raw_event'"
    ).fetchone()
    if not exists:
        logger.error(f"[DB ERROR] raw_event 테이블이 없습니다: {db_path} (mock_producer 를 먼저 실행하세요)")
        sys.exit(1)

    conn.execute(CLASSIFIED_ITEM_DDL)
    conn.execute(CURSOR_DDL)
    conn.execute(FAILURE_DDL)
    for stmt in CLASSIFIED_ITEM_INDEXES:
        conn.execute(stmt)
    conn.commit()

    logger.info(f"[DB] 연결 완료: {db_path}")
    return conn


def _to_request_item(row: sqlite3.Row) -> ClassifyRequestItem:
    """raw_event 1행 → 분류 입력 1건.

    raw_event 는 CSV 원본을 거의 그대로 담고 있어서 스키마 enum 과 표기가 어긋날 수 있다
    (채널 대소문자 등). 여기서만 맞춰 주고 분류 로직 자체는 건드리지 않는다.
    """
    return ClassifyRequestItem.model_validate({
        "item_id": row["event_id"],
        "source": str(row["source"]).lower(),
        "channel": str(row["channel"] or "").upper(),
        "product_group_id": str(
            row["product_group_id"] or row["channel_product_id"] or "PG-UNKNOWN"
        ),
        "raw_text": row["raw_text"],
        "created_at": row["occurred_at"],
    })


class ClassificationWorker:
    """raw DB → 분류 → classified_item 적재."""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        batch_size: int = BATCH_SIZE,
        follow: bool = False,
        poll_interval: float = POLL_INTERVAL_SECONDS,
        dry_run: bool = False,
        limit: int | None = None,
        retry_failed: bool = False,
        max_attempts: int = DEAD_LETTER_MAX_ATTEMPTS,
    ) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.follow = follow
        self.poll_interval = poll_interval
        self.dry_run = dry_run
        # 처리 상한(원문 건수). data/ 에 12.8만 건이 있어 상한 없이 --follow 로 돌리면
        # 전량이 LLM 으로 간다. 시험 실행은 --limit 를 걸고 하라는 뜻의 안전장치.
        self.limit = limit
        # dead-letter 재처리 모드 — 신규 원본 대신 classification_failure 를 훑는다.
        self.retry_failed = retry_failed
        self.max_attempts = max_attempts
        # 재처리 전용 페이지 커서(메모리에만 있음). 한 실행에서 같은 건을 두 번 부르지
        # 않으려고 둔다 — DB 의 classification_cursor 와는 무관하다.
        self.retry_page_cursor: tuple[str, str] = ("", "")
        self.processed = 0
        self.conn: sqlite3.Connection | None = None
        self.is_running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.total_items = 0
        self.total_rows = 0
        self.total_failed = 0
        self.total_recovered = 0

    # ── 실행 진입점 ──────────────────────────────────────────────────────────

    def start(self) -> None:
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

        if self.dry_run:
            logger.info("[DRY-RUN MODE] DB 없이 로컬 모의 데이터로 추론만 실행합니다.")
            self.run_dry_run()
            return

        self.conn = open_db(self.db_path)
        last_occurred_at, last_event_id = self.load_cursor()
        logger.info(
            f"[WORKER STARTED] db={self.db_path}, batch={self.batch_size}, follow={self.follow}, "
            f"retry_failed={self.retry_failed}, cursor=({last_occurred_at}, {last_event_id})"
        )

        try:
            self.run_retry_loop() if self.retry_failed else self.run_loop()
        finally:
            self.log_coverage()
            if self.conn:
                self.conn.close()
            self.loop.close()
            logger.info(
                f"[WORKER STOPPED] 원문 {self.total_items}건 → classified_item {self.total_rows}행 적재"
                f"{f', 실패 {self.total_failed}건' if self.total_failed else ''}"
                f"{f', 재처리 성공 {self.total_recovered}건' if self.total_recovered else ''}"
            )

    def run_loop(self) -> None:
        while self.is_running:
            if self.limit is not None and self.processed >= self.limit:
                logger.info(f"[LIMIT REACHED] 상한 {self.limit}건 처리 완료 — 종료합니다.")
                return

            rows = self.fetch_next_batch()

            if not rows:
                if not self.follow:
                    logger.info("[DONE] 처리할 신규 원본이 없습니다.")
                    return
                # 프로듀서가 배속 재생 중이면 잠시 뒤 새 행이 들어온다
                time.sleep(self.poll_interval)
                continue

            self.processed += len(rows)
            self.process_batch(rows)

    def run_retry_loop(self) -> None:
        """dead-letter(classification_failure) 재처리 전용 루프.

        커서는 건드리지 않는다 — 신규 원본 진행과 독립이다. 성공하면 dead-letter 에서
        지우고, 또 실패하면 attempts 만 올라가 상한(max_attempts)에서 자동으로 멈춘다.
        """
        while self.is_running:
            if self.limit is not None and self.processed >= self.limit:
                logger.info(f"[LIMIT REACHED] 상한 {self.limit}건 재처리 완료 — 종료합니다.")
                return

            rows = self.fetch_failed_batch()
            if not rows:
                logger.info(
                    f"[DONE] 재처리할 실패 건이 없습니다 (시도 {self.max_attempts}회 미만 기준)."
                )
                return

            self.processed += len(rows)
            # 이번 실행에서 이미 시도한 구간은 다시 잡지 않는다(건당 1회 시도 보장)
            self.retry_page_cursor = (rows[-1]["occurred_at"], rows[-1]["event_id"])
            self.process_batch(rows, advance_cursor=False)

    # ── 커서 ────────────────────────────────────────────────────────────────

    def load_cursor(self) -> tuple[str, str]:
        row = self.conn.execute(
            "SELECT last_occurred_at, last_event_id FROM classification_cursor WHERE worker_id = ?",
            (WORKER_ID,),
        ).fetchone()
        if row and row["last_occurred_at"] is not None:
            return row["last_occurred_at"], row["last_event_id"] or ""
        return "", ""  # 빈 문자열은 모든 ISO 시각 문자열보다 작다 → 처음부터

    def save_cursor(self, occurred_at: str, event_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO classification_cursor (worker_id, last_occurred_at, last_event_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                last_occurred_at = excluded.last_occurred_at,
                last_event_id    = excluded.last_event_id,
                updated_at       = excluded.updated_at
            """,
            (WORKER_ID, occurred_at, event_id, datetime.now(timezone.utc).astimezone().isoformat()),
        )

    # ── 조회 ────────────────────────────────────────────────────────────────

    def fetch_next_batch(self) -> list[sqlite3.Row]:
        last_occurred_at, last_event_id = self.load_cursor()
        batch_size = self.batch_size
        if self.limit is not None:
            # 상한을 넘겨서 가져오면 그만큼 LLM 을 더 부른다 — 남은 만큼만 조회한다
            batch_size = min(batch_size, self.limit - self.processed)
        params = (
            *CLASSIFY_SOURCES,
            last_occurred_at, last_occurred_at, last_event_id,
            batch_size,
        )
        return self.conn.execute(FETCH_BATCH_SQL, params).fetchall()

    def fetch_failed_batch(self) -> list[sqlite3.Row]:
        batch_size = self.batch_size
        if self.limit is not None:
            batch_size = min(batch_size, self.limit - self.processed)
        occurred_at, event_id = self.retry_page_cursor
        return self.conn.execute(
            FETCH_FAILED_SQL, (self.max_attempts, occurred_at, occurred_at, event_id, batch_size)
        ).fetchall()

    # ── 처리 ────────────────────────────────────────────────────────────────

    def process_batch(self, rows: list[sqlite3.Row], *, advance_cursor: bool = True) -> None:
        """배치 1개(원문 N건)를 분류해 적재하고 커서를 전진시킨다.

        rows 는 이미 (occurred_at, event_id) 오름차순이라, 이 순서대로 INSERT 하면
        classified_item 이 타임라인 순으로 쌓인다.

        실패한 건은 **버리지 않고** dead-letter(classification_failure)에 남긴다 —
        분모를 원본 테이블에서 세는 합의(2026-08-03) 아래에서 분류 커버리지가 곧
        분자의 정확도라, 무엇이 빠졌는지 셀 수 없으면 부정률이 조용히 과소추정된다.

        advance_cursor=False 는 재처리 모드용 — 이미 지나간 구간을 다시 훑는 것이라
        커서를 건드리면 안 된다.
        """
        logger.info(f"[BATCH] {len(rows)}건 처리 시작 (~{rows[-1]['occurred_at']})")

        occurred_at_by_id = {row["event_id"]: row["occurred_at"] for row in rows}
        failures: list[tuple[str, str, str]] = []  # (event_id, stage, error)

        request_items: list[ClassifyRequestItem] = []
        raw_text_by_id: dict[str, str] = {}
        for row in rows:
            try:
                item = _to_request_item(row)
                request_items.append(item)
                raw_text_by_id[item.item_id] = item.raw_text
            except Exception as exc:
                failures.append((row["event_id"], "parse", str(exc)))
                logger.error(f"[PARSE ERROR] event_id={row['event_id']}: {exc}")

        classified_items, classify_failures = self.classify_items(request_items)
        failures.extend(classify_failures)
        self.total_failed += len(failures)

        # 분류 결과가 원문 순서를 잃지 않도록 타임라인 기준으로 다시 정렬한다.
        classified_items.sort(key=lambda i: (i.created_at, i.item_id))

        # 성공한 건은 dead-letter 에서 지운다(재처리 모드에서 회수되는 경로).
        resolved_ids = [item.item_id for item in classified_items]

        # 적재 + 실패 기록 + 커서 전진은 한 트랜잭션이다. 실패하면 워커를 세운다(아래 참고).
        inserted = self.persist_batch(
            classified_items,
            raw_text_by_id,
            rows[-1],
            failures=failures,
            occurred_at_by_id=occurred_at_by_id,
            resolved_ids=resolved_ids,
            advance_cursor=advance_cursor,
        )
        if inserted is None:
            return

        if self.retry_failed:
            self.total_recovered += len(classified_items)

        self.total_items += len(classified_items)
        self.total_rows += inserted
        logger.info(
            f"[BATCH COMPLETE] 원문 {len(classified_items)}건 → classified_item {inserted}행 적재"
            f"{f', 실패 {len(failures)}건 dead-letter 기록' if failures else ''}"
            f"{f' (커서: {rows[-1]["occurred_at"]} / {rows[-1]["event_id"]})' if advance_cursor else ''}"
        )

    def classify_items(
        self, items: list[ClassifyRequestItem]
    ) -> tuple[list[ClassifiedItem], list[tuple[str, str, str]]]:
        """원문 N건을 **건별로 격리해서** 동시에 분류한다. 실패는 그 건에만 국한된다.

        ⚠️ 배치 재호출을 하지 않는 이유(비용):
        classify_aspect() 안의 asyncio.gather 는 return_exceptions 를 쓰지 않아 1건만
        실패해도 배치 전체가 예외로 떨어진다. 그런데 이 실패는 대부분 **결정적**이다 —
        예를 들어 리뷰에 대해 LLM 이 '파손'을 뱉으면 schemas 의 리뷰 aspect 제약
        (색상/사이즈/소재)에 매번 걸린다. 배치를 통째로 다시 부르면 성공했을 나머지
        건까지 매 재시도마다 다시 과금된다(batch 10 기준 9건 × 재시도 횟수).

        그래서 처음부터 건별 호출을 gather(return_exceptions=True) 로 묶는다:
          - LLM 호출 수는 그대로다(원래도 프롬프트는 1건씩 처리한다 — 1건 = 1콜).
          - 동시 실행이라 처리 속도도 그대로다.
          - 일시적 오류(네트워크·레이트리밋) 재시도는 llm_client 가 MAX_RETRY 로 이미 한다.
            여기서 또 재시도하면 이중 과금이다.

        Returns:
            (분류 성공 목록, [(event_id, "classify", 오류메시지)]) — 실패는 호출부가
            dead-letter 에 기록한다.
        """
        if not items:
            return [], []

        outcomes = self.loop.run_until_complete(self._classify_all(items))

        results: list[ClassifiedItem] = []
        failures: list[tuple[str, str, str]] = []
        for item, outcome in zip(items, outcomes):
            if isinstance(outcome, BaseException):
                failures.append((item.item_id, "classify", f"{type(outcome).__name__}: {outcome}"))
                logger.error(f"[ITEM FAILED] item_id={item.item_id} 분류 실패: {outcome!s}")
                continue
            results.extend(outcome)
        return results, failures

    async def _classify_all(self, items: list[ClassifyRequestItem]) -> list[Any]:
        """건별 classify_aspect 를 동시에 돌리고 예외를 결과 자리에 그대로 담아 온다."""
        return await asyncio.gather(
            *(classify_aspect([item]) for item in items), return_exceptions=True
        )

    def persist_batch(
        self,
        classified_items: list[ClassifiedItem],
        raw_text_by_id: dict[str, str],
        last_row: sqlite3.Row,
        *,
        failures: list[tuple[str, str, str]] | None = None,
        occurred_at_by_id: dict[str, str] | None = None,
        resolved_ids: list[str] | None = None,
        advance_cursor: bool = True,
    ) -> int | None:
        """적재 + 실패 기록 + 커서 전진을 한 트랜잭션으로 커밋한다. 실패하면 None.

        셋을 한 트랜잭션에 묶는 이유: 커서만 전진하고 실패 기록이 빠지면 그 건은
        어디에도 남지 않고 영구 유실된다(분모 합의의 전제인 커버리지가 깨진다).

        커서는 "이 배치를 어디까지 읽었는지" 기준으로 항상 끝까지 전진시킨다. 분류에
        실패한 건이 있어도 배치가 통째로 다시 걸려 무한 재시도되는 걸 막기 위함이고,
        빠진 건은 dead-letter 에 남아 `--retry-failed` 로 따로 회수한다.

        ⚠️ DB 오류를 그냥 위로 던지면 커서가 안 움직인 채 워커가 죽고, 재시작하면 같은
           배치를 **다시 LLM 에 태워서** 같은 자리에서 또 죽는다(무한 재과금). 그래서
           잠금 경합만 잠깐 재시도하고, 그래도 안 되면 롤백 후 워커를 정지시킨다 —
           사람이 보게 만드는 쪽이 조용히 돈을 태우는 것보다 낫다.
        """
        for attempt in range(1, DB_MAX_RETRY + 1):
            try:
                inserted = self.save_classified_items(classified_items, raw_text_by_id)
                self.record_failures(failures or [], occurred_at_by_id or {})
                self.clear_failures(resolved_ids or [])
                if advance_cursor:
                    self.save_cursor(last_row["occurred_at"], last_row["event_id"])
                self.conn.commit()
                return inserted
            except sqlite3.OperationalError as exc:
                # 대개 잠금 경합(다른 프로세스가 쓰는 중) — 일시적이라 재시도 가치가 있다
                self.conn.rollback()
                logger.warning(f"[DB RETRY] 적재 실패 ({attempt}/{DB_MAX_RETRY}): {exc}")
                if attempt < DB_MAX_RETRY:
                    time.sleep(2 ** attempt)
            except sqlite3.Error as exc:
                self.conn.rollback()
                logger.error(f"[DB ERROR] 적재 실패 — 재시도하지 않습니다: {exc}")
                break

        logger.error(
            "[WORKER HALT] classified_item 적재에 실패해 워커를 정지합니다. "
            f"커서는 전진하지 않았으므로 재시작하면 이 배치(~{last_row['event_id']})부터 "
            "다시 처리합니다 — LLM 이 다시 호출되니 DB 문제를 먼저 해결하세요."
        )
        self.is_running = False
        return None

    def record_failures(
        self, failures: list[tuple[str, str, str]], occurred_at_by_id: dict[str, str]
    ) -> None:
        """실패 건을 dead-letter 에 기록(upsert)한다. 같은 건이 또 실패하면 attempts 만 오른다."""
        if not failures:
            return
        now = datetime.now(timezone.utc).astimezone().isoformat()
        for event_id, stage, error in failures:
            self.conn.execute(
                FAILURE_UPSERT,
                (event_id, occurred_at_by_id.get(event_id, ""), stage, error[:500], now, now),
            )

    def clear_failures(self, event_ids: list[str]) -> None:
        """분류에 성공한 건을 dead-letter 에서 제거한다(재처리 회수 경로)."""
        for event_id in event_ids:
            self.conn.execute(FAILURE_DELETE, (event_id,))

    def log_coverage(self) -> None:
        """분류 커버리지를 남긴다 — 분모를 원본에서 세는 합의의 전제 확인용.

        원본(raw_event 의 분류 대상) 대비 classified_item 에 실제로 남은 문서 수를 비교한다.
        차이는 곧 "분자에서 빠진 문서"이고, aspect 0개 정상 분류와 구분하려고 dead-letter
        대기 건수를 함께 찍는다. 미달이면 경고로 올려 로더가 집계하기 전에 눈에 띄게 한다.
        """
        if self.conn is None:
            return
        try:
            placeholders = ", ".join(["?"] * len(CLASSIFY_SOURCES))
            total = self.conn.execute(
                f"SELECT COUNT(*) FROM raw_event WHERE source IN ({placeholders}) "
                "AND raw_text IS NOT NULL AND TRIM(raw_text) <> ''",
                CLASSIFY_SOURCES,
            ).fetchone()[0]
            pending = self.conn.execute("SELECT COUNT(*) FROM classification_failure").fetchone()[0]
            exhausted = self.conn.execute(
                "SELECT COUNT(*) FROM classification_failure WHERE attempts >= ?",
                (self.max_attempts,),
            ).fetchone()[0]
        except sqlite3.Error as exc:
            logger.warning(f"[COVERAGE] 집계 실패: {exc}")
            return

        message = (
            f"[COVERAGE] 분류 대상 원본 {total}건 | dead-letter 대기 {pending}건"
            f"(재시도 상한 소진 {exhausted}건)"
        )
        if pending:
            # 분모는 원본에서 세므로, 여기 남은 건수만큼 분자가 비어 부정률이 과소추정된다
            logger.warning(
                f"{message} — 미분류分 만큼 부정률이 과소추정됩니다. "
                "`--retry-failed` 로 회수하거나 원인을 확인하세요."
            )
        else:
            logger.info(message)

    def save_classified_items(
        self, items: list[ClassifiedItem], raw_text_by_id: dict[str, str]
    ) -> int:
        """ClassifiedItem 을 explode 해서 classified_item 에 적재. 반환값은 실제 INSERT 행 수."""
        classified_at = datetime.now(timezone.utc).astimezone().isoformat()
        inserted = 0

        for item in items:
            for row in explode_to_rows(item):
                created_at = row["created_at"]
                cur = self.conn.execute(
                    CLASSIFIED_ITEM_INSERT,
                    (
                        row["item_id"],
                        row["source"],
                        row["channel"],
                        row["product_group_id"],
                        row["aspect"],
                        row["sentiment"],
                        None if row["mixed_signal"] is None else int(row["mixed_signal"]),
                        raw_text_by_id.get(row["item_id"]),
                        created_at.isoformat() if isinstance(created_at, datetime) else str(created_at),
                        classified_at,
                    ),
                )
                inserted += cur.rowcount  # INSERT OR IGNORE 로 중복 무시된 행은 0

        return inserted

    # ── 부가 기능 ────────────────────────────────────────────────────────────

    def run_dry_run(self) -> None:
        sample_items = [
            ClassifyRequestItem.model_validate(payload)
            for payload in [
                {
                    "item_id": "INQ-DRYRUN-001",
                    "source": "cs",
                    "channel": "COUPANG",
                    "product_group_id": "C1101",
                    "raw_text": "배송은 빨랐는데 옷 사이즈가 너무 작게 나왔네요. L 사이즈인데 M 느낌입니다.",
                    "created_at": "2026-07-28T10:00:00",
                },
                {
                    "item_id": "RVW-DRYRUN-002",
                    "source": "review",
                    "channel": "NAVER",
                    "product_group_id": "N1102",
                    "raw_text": "색감이 사진이랑 똑같고 배송도 빠릅니다.",
                    "created_at": "2026-07-28T10:05:00",
                },
            ]
        ]

        classified_items, _ = self.classify_items(sample_items)
        for item in classified_items:
            for row in explode_to_rows(item):
                logger.info(f"[DRY-RUN ROW] {json.dumps(row, ensure_ascii=False, default=str)}")

    def request_shutdown(self, signum: int, frame: Any) -> None:
        logger.info(f"[SHUTDOWN SIGNAL] 시그널({signum}) 수신 - 워커 종료 절차 개시")
        self.is_running = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classification Worker (raw DB → classified_item)")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="raw DB 경로(sqlite)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="한 번에 분류할 원문 건수")
    parser.add_argument(
        "--follow", action="store_true",
        help="처리할 원본이 없어도 종료하지 않고 폴링(프로듀서 재생을 준실시간 추종)",
    )
    parser.add_argument(
        "--poll-interval", type=float, default=POLL_INTERVAL_SECONDS,
        help="--follow 시 폴링 간격(초)",
    )
    parser.add_argument(
        "--limit", "-n", type=int, default=None,
        help="분류할 원문 상한(건). 지정하면 그만큼만 처리하고 종료 — 시험 실행 시 과금 통제용",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="신규 원본 대신 dead-letter(classification_failure)를 재처리한다",
    )
    parser.add_argument(
        "--max-attempts", type=int, default=DEAD_LETTER_MAX_ATTEMPTS,
        help=f"재처리 시도 상한(기본 {DEAD_LETTER_MAX_ATTEMPTS}). 넘으면 재처리 대상에서 제외",
    )
    parser.add_argument("--dry-run", action="store_true", help="DB 없이 모의 데이터로 추론만 확인")
    args = parser.parse_args()

    worker = ClassificationWorker(
        db_path=args.db,
        batch_size=args.batch_size,
        follow=args.follow,
        poll_interval=args.poll_interval,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
        max_attempts=args.max_attempts,
    )
    worker.start()
