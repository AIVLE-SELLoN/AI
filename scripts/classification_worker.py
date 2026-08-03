"""분류 워커 — raw DB 의 원본 텍스트를 읽어 분류하고 classified_item 에 적재한다.

워크플로우 회의(2026-08-02) 반영:
  기존   : Kafka consumer 로 raw.* 토픽 구독 → 분류 → 로그 출력 (docker 컨테이너로 상주)
  변경후 : **raw DB(raw_event) 조회** → 분류 → **classified_item 테이블 적재(타임라인 순)**

  Kafka 구독을 걷어냈으므로 이 워커는 더 이상 docker compose 에 올라가지 않는다.
  브로커/컨슈머그룹/오프셋 커밋 대신, raw_event 를 (occurred_at, event_id) 순으로 훑는
  커서(classification_cursor)로 진행 상황을 관리한다.

실행:
  python scripts/classification_worker.py                 # 밀린 원본 전부 처리하고 종료
  python scripts/classification_worker.py --follow        # 프로듀서 재생을 준실시간 추종
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
        "파손": "DAMAGE", "오배송": "MISDELIVERY", "기타": "OTHERS",
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

# 분류 대상 source. 주문(ORDER)·상세변경(DETAIL_CHANGE)은 원문 텍스트 분류 대상이 아니라
# raw_event 에 source=NULL 로 들어오므로 자연히 제외된다.
CLASSIFY_SOURCES = (Source.CS.value, Source.REVIEW.value)

# classified_item: explode 규약(분류 워커 명세 §2)대로 aspect 1개당 1행.
#   created_at             원문 발생 시각 = raw_event.occurred_at (타임라인 정렬 키)
#   classified_item_id     적재 순번. 타임라인 순으로 INSERT 하므로 이 값의 순서 = 타임라인 순서
#   UNIQUE(item_id, aspect) 재실행/재시도 시 같은 행이 중복 적재되지 않게 하는 멱등 키
CLASSIFIED_ITEM_DDL = """
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
    ) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.follow = follow
        self.poll_interval = poll_interval
        self.dry_run = dry_run
        self.conn: sqlite3.Connection | None = None
        self.is_running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.total_items = 0
        self.total_rows = 0
        self.total_failed = 0

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
            f"cursor=({last_occurred_at}, {last_event_id})"
        )

        try:
            self.run_loop()
        finally:
            if self.conn:
                self.conn.close()
            logger.info(
                f"[WORKER STOPPED] 원문 {self.total_items}건 → classified_item {self.total_rows}행 적재"
                f"{f', 실패 {self.total_failed}건' if self.total_failed else ''}"
            )

    def run_loop(self) -> None:
        while self.is_running:
            rows = self.fetch_next_batch()

            if not rows:
                if not self.follow:
                    logger.info("[DONE] 처리할 신규 원본이 없습니다.")
                    return
                # 프로듀서가 배속 재생 중이면 잠시 뒤 새 행이 들어온다
                time.sleep(self.poll_interval)
                continue

            self.process_batch(rows)

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
        params = (
            *CLASSIFY_SOURCES,
            last_occurred_at, last_occurred_at, last_event_id,
            self.batch_size,
        )
        return self.conn.execute(FETCH_BATCH_SQL, params).fetchall()

    # ── 처리 ────────────────────────────────────────────────────────────────

    def process_batch(self, rows: list[sqlite3.Row]) -> None:
        """배치 1개(원문 N건)를 분류해 적재하고 커서를 전진시킨다.

        rows 는 이미 (occurred_at, event_id) 오름차순이라, 이 순서대로 INSERT 하면
        classified_item 이 타임라인 순으로 쌓인다.
        """
        logger.info(f"[BATCH] {len(rows)}건 처리 시작 (~{rows[-1]['occurred_at']})")

        request_items: list[ClassifyRequestItem] = []
        raw_text_by_id: dict[str, str] = {}
        for row in rows:
            try:
                item = _to_request_item(row)
                request_items.append(item)
                raw_text_by_id[item.item_id] = item.raw_text
            except Exception as exc:
                self.total_failed += 1
                logger.error(f"[PARSE ERROR] event_id={row['event_id']} 스킵: {exc}")

        classified_items = self.classify_with_retry(request_items)

        # 분류 결과가 원문 순서를 잃지 않도록 타임라인 기준으로 다시 정렬한다
        # (asyncio.gather 는 순서를 보존하지만, 개별 재시도 경로를 타면 섞일 수 있다).
        classified_items.sort(key=lambda i: (i.created_at, i.item_id))

        inserted = self.save_classified_items(classified_items, raw_text_by_id)

        # 커서는 "이 배치를 어디까지 읽었는지" 기준으로 항상 끝까지 전진시킨다.
        # 분류에 실패한 건이 있어도 배치가 통째로 다시 걸려 무한 재시도되는 걸 막기 위함
        # (실패 건은 로그에 event_id 로 남으므로 나중에 개별 재처리 가능).
        last = rows[-1]
        self.save_cursor(last["occurred_at"], last["event_id"])
        self.conn.commit()

        self.total_items += len(classified_items)
        self.total_rows += inserted
        logger.info(
            f"[BATCH COMPLETE] 원문 {len(classified_items)}건 → classified_item {inserted}행 적재 "
            f"(커서: {last['occurred_at']} / {last['event_id']})"
        )

    def classify_with_retry(
        self, items: list[ClassifyRequestItem], max_retries: int = 3
    ) -> list[ClassifiedItem]:
        """배치 단위로 분류하고, 실패하면 지수 백오프 재시도 → 최종적으로 건별 분리 처리.

        classify_aspect()는 asyncio.gather 로 묶여 있어 1건만 실패해도 배치 전체가
        예외로 떨어진다. 재시도까지 실패하면 건별로 쪼개 성공분이라도 살린다.
        """
        if not items:
            return []

        for attempt in range(1, max_retries + 1):
            try:
                return self.loop.run_until_complete(classify_aspect(items))
            except Exception as exc:
                logger.warning(f"[BATCH RETRY] 분류 실패 ({attempt}/{max_retries}): {exc}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)

        logger.error(f"[FALLBACK] 배치({len(items)}건) 재시도 소진 — 건별 처리로 전환합니다.")
        results: list[ClassifiedItem] = []
        for item in items:
            try:
                results.extend(self.loop.run_until_complete(classify_aspect([item])))
            except Exception as exc:
                self.total_failed += 1
                logger.error(f"[ITEM DROPPED] item_id={item.item_id} 분류 실패: {exc}")
        return results

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

        classified_items = self.classify_with_retry(sample_items)
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
    parser.add_argument("--dry-run", action="store_true", help="DB 없이 모의 데이터로 추론만 확인")
    args = parser.parse_args()

    worker = ClassificationWorker(
        db_path=args.db,
        batch_size=args.batch_size,
        follow=args.follow,
        poll_interval=args.poll_interval,
        dry_run=args.dry_run,
    )
    worker.start()
