"""Mock Producer — CSV 대본 데이터를 시각순으로 재생하는 스크립트.

워크플로우 회의(2026-08-02) 반영:
  기존   : CSV → Kafka → classification_worker(Kafka consumer)
  변경후 : CSV → Kafka(발행 그대로 유지)
              └→ **raw DB(raw_event 테이블)** ← classification_worker 가 여기를 참조

  즉 이 스크립트는 "발행"과 "원본 적재"를 동시에 하는 이중 기록(dual write) 구조가 된다.
  분류 워커는 더 이상 Kafka 를 구독하지 않고 raw DB 만 읽으므로, Kafka 브로커(EC2)가
  없어도 `--dry-run` 으로 raw DB 만 채워서 분류 파이프라인을 돌릴 수 있다.

raw DB 는 일단 stdlib sqlite3 로 러프하게 구성한다(추가 의존성 없음).
운영 DB(Postgres/MySQL)로 옮길 때는 `open_raw_db()` / `RAW_EVENT_DDL` 두 곳만 갈면 되도록
DDL·INSERT 를 표준 SQL 범위로 유지했다.
공통 메타 필드(수집 서버, 파티션 키, 스키마 버전 등)는 인프라 팀 요청이 오면 추가한다 —
지금은 원본 CSV 행 전체를 `payload` JSON 으로 통째로 남겨 두어 나중에 컬럼만 뽑아 쓸 수 있게 했다.
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Kafka 내부 재시도 로그 차단
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logging.getLogger("kafka").setLevel(logging.ERROR)
logger = logging.getLogger("MockProducer")

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


DEFAULT_DATA_DIR = "./data/input"
DEFAULT_RAW_DB = "./data/raw.db"

# DB flush 정책: 아래 건수마다, 또는 마지막 flush 후 아래 초가 지나면 commit.
# 배속 재생 중에도 워커가 준실시간으로 따라올 수 있을 만큼 자주 내보내되,
# 초기 대량 적재(약 13만 행) 때 행마다 commit 해서 느려지는 것도 피한다.
DB_FLUSH_ROWS = 200
DB_FLUSH_SECONDS = 1.0

STREAMING_FILE_CONFIGS: dict[str, dict[str, Any]] = {
    "orders": {
        "file_candidates": ["input_orders.csv"],
        "time_column": "order_date",
        "topic": "raw.orders",
        "event_type": "ORDER",
        "id_column": None,          # 주문 CSV 에는 고유 ID 컬럼이 없어 행 순번으로 생성
        "id_prefix": "ORD",
        "source": None,             # 분류 대상 아님(이상탐지 분모용 데이터)
        "text_column": None,
    },
    "inquiries": {
        "file_candidates": ["input_cs_inquiries.csv"],
        "time_column": "inquired_at",
        "topic": "raw.inquiries",
        "event_type": "INQUIRY",
        "id_column": "inquiry_id",
        "id_prefix": "INQ",
        "source": "cs",             # schemas.Source.CS
        "text_column": "content",
    },
    "reviews": {
        "file_candidates": ["input_reviews.csv"],
        "time_column": "created_at",
        "topic": "raw.reviews",
        "event_type": "REVIEW",
        "id_column": "review_id",
        "id_prefix": "RVW",
        "source": "review",         # schemas.Source.REVIEW
        "text_column": "content",
    },
    "detail_changes": {
        # ⚠️ 현재 data/input 에는 `input_detail_fields.csv`(상세페이지 스냅샷, 505행)만 있고
        #    시각 컬럼이 없다 — 시계열 이벤트가 아니라 정적 참조 테이블이라 재생 대상이 아니다.
        #    변경 이벤트 대본(input_detail_changes.csv)이 생성되면 그때부터 자동으로 잡힌다.
        "file_candidates": ["input_detail_changes.csv"],
        "time_column": "changed_at",
        "topic": "raw.detail_changes",
        "event_type": "DETAIL_CHANGE",
        "id_column": "change_id",
        "id_prefix": "CHG",
        "source": None,             # 상세페이지 변경은 분류가 아니라 탐지 근거(linked_change_id)로 쓰임
        "text_column": "new_value",
    },
}


# ── raw DB (원본 적재) ────────────────────────────────────────────────────────
#
# raw_event: mock producer 가 발행한 원본 이벤트 1건 = 1행.
#   occurred_at  대본상 이벤트 발생 시각. **타임라인 정렬 키** — 워커가 이 순서로 읽어간다.
#   published_at 실제 발행된 벽시계 시각(재생 시점). 정렬에는 쓰지 않는다.
#   payload      원본 CSV 행 전체(JSON). 컬럼 추가 요청이 오면 여기서 뽑아 승격시키면 된다.

RAW_EVENT_DDL = """
CREATE TABLE IF NOT EXISTS raw_event (
    event_id           TEXT PRIMARY KEY,
    event_type         TEXT NOT NULL,
    topic              TEXT NOT NULL,
    source             TEXT,
    channel            TEXT,
    channel_product_id TEXT,
    product_group_id   TEXT,
    raw_text           TEXT,
    occurred_at        TEXT NOT NULL,
    published_at       TEXT,
    payload            TEXT NOT NULL
);
"""

RAW_EVENT_INDEXES = [
    # 워커의 타임라인 순차 조회용 (occurred_at, event_id) 복합 커서
    "CREATE INDEX IF NOT EXISTS idx_raw_event_timeline ON raw_event (occurred_at, event_id);",
    "CREATE INDEX IF NOT EXISTS idx_raw_event_source ON raw_event (source, occurred_at);",
]

RAW_EVENT_COLUMNS = [
    "event_id", "event_type", "topic", "source", "channel", "channel_product_id",
    "product_group_id", "raw_text", "occurred_at", "published_at", "payload",
]

RAW_EVENT_INSERT = (
    f"INSERT OR REPLACE INTO raw_event ({', '.join(RAW_EVENT_COLUMNS)}) "
    f"VALUES ({', '.join(['?'] * len(RAW_EVENT_COLUMNS))})"
)


def open_raw_db(db_path_str: str) -> sqlite3.Connection:
    """raw DB 연결 + 스키마 보장. 없으면 파일째로 새로 만든다."""
    db_path = Path(db_path_str).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    # WAL: 프로듀서가 쓰는 동안 워커가 같은 파일을 읽어도 서로 막히지 않게 한다.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute(RAW_EVENT_DDL)
    for stmt in RAW_EVENT_INDEXES:
        conn.execute(stmt)
    conn.commit()

    logger.info(f"[RAW DB] 연결 완료: {db_path}")
    return conn


class RawEventSink:
    """raw_event 적재 버퍼. 일정 건수/시간마다 모아서 commit 한다."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.buffer: list[tuple] = []
        self.last_flush = time.monotonic()
        self.written = 0

    def add(self, event: dict[str, Any]) -> None:
        payload = event["payload"]
        self.buffer.append((
            event["event_id"],
            payload.get("event_type"),
            event["topic"],
            event["source"],
            event["channel"],
            event["channel_product_id"],
            event["product_group_id"],
            event["raw_text"],
            event["time"].isoformat(),
            payload.get("published_at"),
            json.dumps(payload, ensure_ascii=False),
        ))

        if len(self.buffer) >= DB_FLUSH_ROWS or (time.monotonic() - self.last_flush) >= DB_FLUSH_SECONDS:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            self.last_flush = time.monotonic()
            return
        self.conn.executemany(RAW_EVENT_INSERT, self.buffer)
        self.conn.commit()
        self.written += len(self.buffer)
        self.buffer.clear()
        self.last_flush = time.monotonic()

    def close(self) -> None:
        self.flush()
        self.conn.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mock Producer: CSV 대본 데이터를 시각순으로 Kafka/raw DB에 배속 재생하는 스크립트"
    )
    parser.add_argument("--data-dir", "-m", default=DEFAULT_DATA_DIR, help="재생 대상 csv 파일 디렉토리 경로")
    parser.add_argument("--from", "-f", dest="start", default=None, help="재생 시작 시각 필터")
    parser.add_argument("--to", "-t", dest="end", default=None, help="재생 종료 시각 필터")
    parser.add_argument("--speed", "-s", type=float, default=1.0, help="배속 상수")
    parser.add_argument("--topics", "-p", default=None, help="쉼표 분리 화이트리스트 필터")
    parser.add_argument(
        "--dry-run", "-d", action="store_true",
        help="Kafka 전송 없이 재생 검증(raw DB 적재는 그대로 수행 — 브로커 없이 워커 테스트 가능)",
    )
    parser.add_argument("--bootstrap-servers", "-b", default="localhost:9092", help="Kafka 브로커 접속 주소")
    parser.add_argument("--raw-db", default=DEFAULT_RAW_DB, help="원본 데이터를 적재할 raw DB 경로(sqlite)")
    parser.add_argument("--no-db", action="store_true", help="raw DB 적재 생략(Kafka 발행만)")
    return parser.parse_args()


def validate_data_directory(data_dir_str: str) -> Path:
    resolved_path = Path(data_dir_str or DEFAULT_DATA_DIR).resolve()
    if "golden" in str(resolved_path).lower():
        sys.stderr.write("[SECURITY_VIOLATION] Mock Producer는 golden 데이터에 접근할 수 없습니다.\n")
        sys.exit(1)
    return resolved_path


def resolve_source_file(data_dir: Path, candidates: list[str]) -> Path | None:
    for name in candidates:
        path = data_dir / name
        if path.exists():
            return path
    return None


def _to_jsonable(value: Any) -> Any:
    """CSV 셀 값을 JSON 직렬화 가능한 형태로 정리."""
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):  # numpy 스칼라 → 파이썬 기본형
        return value.item()
    return value


def load_and_merge_csvs(data_dir: Path, topics_filter_str: str | None) -> list[dict[str, Any]]:
    merged_events: list[dict[str, Any]] = []
    topics_filter = [t.strip() for t in topics_filter_str.split(",")] if topics_filter_str else []

    for key, config in STREAMING_FILE_CONFIGS.items():
        if topics_filter and "all" not in topics_filter and key not in topics_filter:
            continue

        file_path = resolve_source_file(data_dir, config["file_candidates"])
        if file_path is None:
            logger.warning(f"대본 파일 미존재 스킵: {data_dir / config['file_candidates'][0]}")
            continue

        # utf-8-sig: 대본 CSV 선두에 BOM 이 붙어 있어 첫 컬럼명이 '﻿inquiry_id' 로
        # 읽히는 문제가 있다. BOM 을 떼지 않으면 id 컬럼 조회가 전부 빗나간다.
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        time_col = config["time_column"]

        if time_col not in df.columns:
            logger.warning(
                f"시각 컬럼('{time_col}') 없음 → 시계열 재생 대상이 아니므로 스킵: {file_path.name}"
            )
            continue

        df[time_col] = pd.to_datetime(df[time_col])

        for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
            event_time: datetime = row[time_col].to_pydatetime()
            raw_dict = row.to_dict()

            sanitized_payload: dict[str, Any] = {k: _to_jsonable(v) for k, v in raw_dict.items()}
            sanitized_payload[time_col] = event_time.isoformat()
            sanitized_payload["event_type"] = config["event_type"]

            channel = str(sanitized_payload.get("channel") or "")
            channel_product_id = str(sanitized_payload.get("channel_product_id") or "")
            message_key = f"{channel}:{channel_product_id}" if channel and channel_product_id else None

            # 이벤트 고유 ID: CSV 의 자연키(inquiry_id/review_id/...)를 그대로 쓰고,
            # 없는 파일(주문)은 파일 내 행 순번으로 만든다. raw_event 의 PK 이므로
            # 같은 대본을 다시 재생해도 중복 행이 쌓이지 않고 덮어써진다.
            id_column = config["id_column"]
            natural_id = sanitized_payload.get(id_column) if id_column else None
            event_id = str(natural_id) if natural_id else f"{config['id_prefix']}-{row_idx:06d}"

            text_column = config["text_column"]
            raw_text = sanitized_payload.get(text_column) if text_column else None

            merged_events.append({
                "time": event_time,
                "topic": config["topic"],
                "event_id": event_id,
                "source": config["source"],
                "channel": channel or None,
                "channel_product_id": channel_product_id or None,
                # ⚠️ 마스터 상품 그룹(P001…)은 답 노출 방지 설계상 input CSV 에 없다
                #    (generate_mock_data.py 참고). 매핑 테이블이 붙기 전까지는 채널 상품 ID 를
                #    그대로 그룹 키로 쓴다 — 실제 연동 시 이 한 줄만 조인으로 바꾸면 된다.
                "product_group_id": (
                    sanitized_payload.get("product_group_id") or channel_product_id or None
                ),
                "raw_text": str(raw_text) if raw_text else None,
                "message_key": message_key,
                "payload": sanitized_payload,
            })

        logger.info(f"[LOAD] {file_path.name}: {len(df)}행 적재 대상 로드")

    return merged_events


def sort_by_timestamp(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # event_id 를 2차 키로 둬서 같은 시각 이벤트의 순서도 재생마다 동일하게 고정한다
    # (워커의 (occurred_at, event_id) 커서와 같은 정렬 기준).
    events.sort(key=lambda x: (x["time"], x["event_id"]))
    return events


def filter_by_time_range(
    events: list[dict[str, Any]], start_str: str | None, end_str: str | None
) -> list[dict[str, Any]]:
    start_dt = datetime.fromisoformat(start_str) if start_str else None
    end_dt = datetime.fromisoformat(end_str) if end_str else None

    filtered = []
    for e in events:
        if start_dt and e["time"] < start_dt:
            continue
        if end_dt and e["time"] > end_dt:
            continue
        filtered.append(e)
    return filtered


def publish(event: dict[str, Any], producer: Any | None, sink: RawEventSink | None, dry_run: bool) -> bool:
    payload = event["payload"]
    payload["published_at"] = datetime.now(timezone.utc).astimezone().isoformat()

    # 원본 적재를 먼저 한다 — 워커가 참조하는 정본은 이제 raw DB 쪽이다.
    if sink is not None:
        try:
            sink.add(event)
        except Exception as err:
            logger.error(f"[RAW DB] 적재 실패 (event_id: {event['event_id']}): {err}")
            return False

    if dry_run:
        logger.debug(
            f"[DRY-RUN] Virtual Time: {event['time'].isoformat()} | Topic: {event['topic']} | "
            f"EventType: {payload['event_type']} | PublishedAt: {payload['published_at']}"
        )
        return True

    try:
        future = producer.send(
            topic=event["topic"],
            key=event["message_key"],
            value=payload,
        )
        future.get(timeout=10.0)
        return True
    except Exception as err:
        logger.error(f"Kafka 전송 실패 (Topic: {event['topic']}): {err}")
        return False


def print_summary(summary_counts: dict[str, int], db_written: int | None) -> None:
    print("\n========== [발행 결과 요약 SUMMARY] ==========")
    total = 0
    for topic, count in summary_counts.items():
        print(f" - {topic}: {count} 건")
        total += count
    print(f" 총 발행 이벤트 수: {total} 건")
    if db_written is not None:
        print(f" raw DB 적재 행 수: {db_written} 행")
    print("==============================================")


def main() -> None:
    args = parse_arguments()
    data_dir = validate_data_directory(args.data_dir)

    events = load_and_merge_csvs(data_dir, args.topics)
    events = sort_by_timestamp(events)
    events = filter_by_time_range(events, args.start, args.end)
    logger.info(f"[PLAN] 재생 대상 {len(events)}건 (speed={args.speed})")

    producer = None
    if not args.dry_run:
        if not KAFKA_AVAILABLE:
            logger.error("kafka-python 라이브러리가 설치되어 있지 않습니다.")
            sys.exit(1)
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    sink = None if args.no_db else RawEventSink(open_raw_db(args.raw_db))

    summary_counts: dict[str, int] = {config["topic"]: 0 for config in STREAMING_FILE_CONFIGS.values()}
    prev_time: datetime | None = None

    try:
        for e in events:
            if prev_time and not args.dry_run:
                delta_seconds = (e["time"] - prev_time).total_seconds()
                if delta_seconds > 0 and args.speed > 0:
                    # 대기 중에도 워커가 최신 행을 볼 수 있게, 잠들기 전에 버퍼를 비운다
                    if sink is not None:
                        sink.flush()
                    time.sleep(delta_seconds / args.speed)

            if publish(e, producer, sink, args.dry_run):
                summary_counts[e["topic"]] += 1
            prev_time = e["time"]
    finally:
        if producer:
            producer.flush()
            producer.close()
        db_written = None
        if sink is not None:
            sink.close()
            db_written = sink.written

    print_summary(summary_counts, db_written)


if __name__ == "__main__":
    main()
