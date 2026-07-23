import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


# ===================================================================
# 전역 상수 및 4종 raw.* 스트리밍 화이트리스트 매핑
# ===================================================================
DEFAULT_DATA_DIR = "./data/input"

STREAMING_FILE_CONFIGS: Dict[str, Dict[str, str]] = {
    "orders": {
        "file_name": "input_orders.csv",
        "time_column": "order_date",
        "topic": "raw.orders",
        "event_type": "ORDER",
    },
    "inquiries": {
        "file_name": "input_cs_inquiries.csv",
        "time_column": "inquired_at",
        "topic": "raw.inquiries",
        "event_type": "INQUIRY",
    },
    "reviews": {
        "file_name": "input_reviews.csv",
        "time_column": "created_at",
        "topic": "raw.reviews",
        "event_type": "REVIEW",
    },
    "detail_changes": {
        "file_name": "input_detail_changes.csv",
        "time_column": "changed_at",
        "topic": "raw.detail_changes",
        "event_type": "DETAIL_CHANGE",
    },
}


# ===================================================================
# CLI 매개변수 파싱 (축약형 및 기본값 정의 준수)
# ===================================================================
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mock Producer: CSV 대본 데이터를 시각순으로 Kafka에 배속 재생하는 파이썬 스크립트"
    )
    parser.add_argument(
        "--data-dir",
        "-m",
        default=DEFAULT_DATA_DIR,
        help="재생 대상 csv 파일이 위치한 디렉토리 경로(golden 지정 불가)",
    )
    parser.add_argument(
        "--from",
        "-f",
        dest="start",
        default=None,
        help="재생 시작 시각 필터 (ex: 2026-05-28T00:00:00)",
    )
    parser.add_argument(
        "--to",
        "-t",
        dest="end",
        default=None,
        help="재생 종료 시각 필터 (ex: 2026-05-28T00:00:00)",
    )
    parser.add_argument(
        "--speed",
        "-s",
        type=float,
        default=1.0,
        help="배속 상수(ex: 8640 입력 시 1일 데이터 10초에 압축 재생)",
    )
    parser.add_argument(
        "--topics",
        "-p",
        default=None,
        help="쉼표 분리 화이트리스트 필터 (orders,inquiries,reviews,detail_changes), 누락 시 전역 토픽 재생",
    )
    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="action='store_true'. Kafka 전송 없이 콘솔에 재생 타임라인만 검증",
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        default="localhost:9092",
        help="Kafka 브로커 접속 주소",
    )
    return parser.parse_args()


# ===================================================================
# 보안 가드레일 (Golden 접근 차단)
# ===================================================================
def validate_data_directory(data_dir_str: str) -> Path:
    resolved_path = Path(data_dir_str or DEFAULT_DATA_DIR).resolve()
    if "golden" in str(resolved_path).lower():
        sys.stderr.write("[SECURITY_VIOLATION] Mock Producer는 golden 데이터에 접근할 수 없습니다.\n")
        sys.exit(1)
    return resolved_path


# ===================================================================
# 메인 루프 모듈 연산 함수
# ===================================================================
def load_and_merge_csvs(data_dir: Path, topics_filter_str: Optional[str]) -> List[Dict[str, Any]]:
    """4개 CSV -> 병합 (detail_changes 포함)"""
    merged_events: List[Dict[str, Any]] = []
    topics_filter = [t.strip() for t in topics_filter_str.split(",")] if topics_filter_str else []

    for key, config in STREAMING_FILE_CONFIGS.items():
        if topics_filter and "all" not in topics_filter and key not in topics_filter:
            continue

        file_path = data_dir / config["file_name"]
        if not file_path.exists():
            print(f"[WARN] 대본 파일이 존재하지 않아 스킵합니다: {file_path}")
            continue

        df = pd.read_csv(file_path)
        time_col = config["time_column"]

        if time_col not in df.columns:
            continue

        df[time_col] = pd.to_datetime(df[time_col])

        for _, row in df.iterrows():
            event_time: datetime = row[time_col].to_pydatetime()
            raw_dict = row.to_dict()

            sanitized_payload: Dict[str, Any] = {}
            for k, v in raw_dict.items():
                if pd.isna(v):
                    sanitized_payload[k] = None
                elif isinstance(v, (pd.Timestamp, datetime)):
                    sanitized_payload[k] = v.isoformat()
                else:
                    sanitized_payload[k] = v

            # 이벤트 내 시각 필드(inquired_at, changed_at 등)는 원본 값 유지
            sanitized_payload[time_col] = event_time.isoformat()
            sanitized_payload["event_type"] = config["event_type"]

            channel = str(sanitized_payload.get("channel", ""))
            product_id = str(sanitized_payload.get("channel_product_id", ""))
            message_key = f"{channel}:{product_id}" if channel and product_id else None

            merged_events.append({
                "time": event_time,
                "topic": config["topic"],
                "message_key": message_key,
                "payload": sanitized_payload,
            })

    return merged_events


def sort_by_timestamp(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """시각순 정렬"""
    events.sort(key=lambda x: x["time"])
    return events


def filter_by_time_range(
    events: List[Dict[str, Any]], 
    start_str: Optional[str], 
    end_str: Optional[str]
) -> List[Dict[str, Any]]:
    """--from/--to 구간만 필터링"""
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


def publish(
    event: Dict[str, Any], 
    producer: Optional[Any], 
    dry_run: bool
) -> None:
    """dry-run이면 print, 아니면 Kafka 발행 (published_at에만 실제 발행 시각 기록)"""
    payload = event["payload"]
    payload["published_at"] = datetime.now(timezone.utc).astimezone().isoformat()

    if dry_run:
        print(
            f"[DRY-RUN] Virtual Time: {event['time'].isoformat()} | "
            f"Topic: {event['topic']} | EventType: {payload['event_type']} | "
            f"PublishedAt: {payload['published_at']}"
        )
    else:
        try:
            producer.send(
                topic=event["topic"],
                key=event["message_key"],
                value=payload,
            )
        except Exception as err:
            sys.stderr.write(f"[ERROR] Kafka 전송 실패 (Topic: {event['topic']}): {err}\n")


def print_summary(summary_counts: Dict[str, int]) -> None:
    """토픽별 발행 건수 요약 출력"""
    print("\n========== [발행 결과 요약 SUMMARY] ==========")
    total = 0
    for topic, count in summary_counts.items():
        print(f" - {topic}: {count} 건")
        total += count
    print(f" 총 발행 이벤트 수: {total} 건")
    print("==============================================")


# ===================================================================
# 메인 실행부
# ===================================================================
def main() -> None:
    # 1. 인자 파싱
    args = parse_arguments()

    # 2. 데이터 디렉토리 검증
    data_dir = validate_data_directory(args.data_dir)

    # 3. 파이프라인 가동
    events = load_and_merge_csvs(data_dir, args.topics)
    events = sort_by_timestamp(events)
    events = filter_by_time_range(events, args.start, args.end)

    producer = None
    if not args.dry_run:
        if not KAFKA_AVAILABLE:
            sys.stderr.write("[ERROR] kafka-python 라이브러리가 설치되어 있지 않습니다.\n")
            sys.exit(1)
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    summary_counts: Dict[str, int] = {config["topic"]: 0 for config in STREAMING_FILE_CONFIGS.values()}
    prev_time: Optional[datetime] = None

    for e in events:
        if prev_time and not args.dry_run:
            delta_seconds = (e["time"] - prev_time).total_seconds()
            if delta_seconds > 0 and args.speed > 0:
                sleep_duration = delta_seconds / args.speed
                time.sleep(sleep_duration)

        publish(e, producer, args.dry_run)
        summary_counts[e["topic"]] += 1
        prev_time = e["time"]

    if producer:
        producer.flush()
        producer.close()

    print_summary(summary_counts)


if __name__ == "__main__":
    main()