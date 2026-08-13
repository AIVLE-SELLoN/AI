"""Mock Producer — CSV 대본 데이터를 시각순으로 재생하는 스크립트.

워크플로우 회의(2026-08-02) 반영:
  기존   : CSV → Kafka → classification_worker(Kafka consumer)
  변경후 : CSV → Kafka(발행 그대로 유지)
              └→ **raw DB** ← classification_worker 가 여기를 참조

  즉 이 스크립트는 "발행"과 "원본 적재"를 동시에 하는 이중 기록(dual write) 구조가 된다.
  분류 워커는 더 이상 Kafka 를 구독하지 않고 raw DB 만 읽으므로, Kafka 브로커(EC2)가
  없어도 `--dry-run` 으로 raw DB 만 채워서 분류 파이프라인을 돌릴 수 있다.

「Raw DB 스키마 확정 (8/7)」 반영:
  기존   : 이벤트 종류를 컬럼으로 구분하는 단일 `raw_event` 테이블
  변경후 : 확정 문서 §2 의 실테이블 — `cs` · `reviews` · `orders` (+ `channel` 마스터)

  이 스크립트는 목 파이프라인에서 **main server 자리를 대신한다**(§1 소유권). 그래서
  main server 소유 테이블만 쓰고, AI 소유 테이블(classified_*)은 건드리지 않는다.
  DDL 은 `app/core/raw_schema.py` 한 곳에 있다 — 워커와 같은 정의를 봐야 한다.

⚠️ 원본 CSV 행 전체를 담던 `payload` 컬럼은 없앴다. 스키마가 확정되기 전 "나중에 컬럼을
   뽑아 쓰려고" 남겨 둔 보험이었는데, 이제 컬럼이 정해져서 목적이 사라졌다. Kafka 메시지는
   그대로 전체 행을 싣는다.

raw DB 는 stdlib sqlite3 로 구성한다(추가 의존성 없음). 운영 DB(Postgres)로 옮길 때는
`open_raw_db()` 와 `raw_schema` 의 DDL 만 갈면 되도록 표준 SQL 범위를 유지했다.
"""

import argparse
import csv
import json
import logging
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core import raw_schema
from app.core.constants import KST
from app.core.schemas import Channel

# §2-1 channel 마스터에 넣을 채널. `Channel` enum 이 정본이다.
# ALL 은 전역형 알림을 가리키는 가상 채널이라 연동 채널이 아니다.
MASTER_CHANNELS: tuple[str, ...] = tuple(c.value for c in Channel if c is not Channel.ALL)

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

# 이 시간 이상 자야 할 때만 잠들기 전에 강제 flush 한다.
# 이벤트마다 무조건 flush 하면 위의 배칭이 사실상 무력화된다 — 촘촘한 구간(sleep 수 ms)에서는
# 배칭에 맡기고, 오래 쉬는 구간에서만 워커가 최신 행을 보도록 내보낸다.
FLUSH_BEFORE_SLEEP_SECONDS = 0.5

STREAMING_FILE_CONFIGS: dict[str, dict[str, Any]] = {
    "orders": {
        "file_name": "input_orders.csv",
        "time_column": "order_date",
        "topic": "raw.orders",
        "event_type": "ORDER",
        "id_column": None,          # 주문 CSV 에는 고유 ID 컬럼이 없어 행 순번으로 생성
        "id_prefix": "ORD",
        "source": None,             # 분류 대상 아님(이상탐지 분모용 데이터)
        "text_column": None,
        "table": "orders",          # §2-9
    },
    "inquiries": {
        "file_name": "input_cs_inquiries.csv",
        "time_column": "inquired_at",
        "topic": "raw.inquiries",
        "event_type": "INQUIRY",
        "id_column": "inquiry_id",
        "id_prefix": "INQ",
        "source": "cs",             # schemas.Source.CS
        "text_column": "content",
        "table": "cs",              # §2-4
    },
    "reviews": {
        "file_name": "input_reviews.csv",
        "time_column": "created_at",
        "topic": "raw.reviews",
        "event_type": "REVIEW",
        "id_column": "review_id",
        "id_prefix": "RVW",
        "source": "review",         # schemas.Source.REVIEW
        "text_column": "content",
        "table": "reviews",         # §2-5
    },
    "detail_changes": {
        # ⚠️ 현재 data/input 에는 `input_detail_fields.csv`(상세페이지 스냅샷, 505행)만 있고
        #    시각 컬럼이 없다 — 시계열 이벤트가 아니라 정적 참조 테이블이라 재생 대상이 아니다.
        #    변경 이벤트 대본(input_detail_changes.csv)이 생성되면 그때부터 자동으로 잡힌다.
        "file_name": "input_detail_changes.csv",
        "time_column": "changed_at",
        "topic": "raw.detail_changes",
        "event_type": "DETAIL_CHANGE",
        "id_column": "change_id",
        "id_prefix": "CHG",
        "source": None,             # 상세페이지 변경은 분류가 아니라 탐지 근거(linked_change_id)로 쓰임
        "text_column": "new_value",
        # ⚠️ 확정 스키마 §1 테이블 목록에 상세페이지 변경 테이블이 없다. 적재할 자리가
        #    없으므로 Kafka 발행만 하고 raw DB 는 건너뛴다 — 아무 테이블에나 밀어 넣어
        #    스키마를 임의로 늘리지 않는다. 테이블이 정해지면 여기에 이름만 넣으면 된다.
        "table": None,
    },
}


# ── raw DB (원본 적재) ────────────────────────────────────────────────────────
#
# 확정 스키마 §2 의 실테이블에 그대로 넣는다. 테이블별 컬럼은 `app/core/raw_schema.py` 참고.
#
# ⚠️ INSERT OR REPLACE 를 쓰는 이유: 같은 대본을 다시 재생해도 중복 행이 쌓이지 않게
#    한다(PK 로 덮어쓴다). cs/reviews 는 원문 PK, orders 는 복합 PK 가 그 역할을 한다.

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "cs": (
        "id", "channel_product_id", "product_group_id", "channel_id",
        "content", "inquired_at", "created_at",
    ),
    "reviews": (
        "id", "channel_product_id", "product_group_id", "channel_id",
        "content", "rating", "created_at",
    ),
    "orders": (
        "channel_id", "channel_product_id", "order_date",
        "quantity", "order_amount", "created_at",
    ),
}

TABLE_INSERTS: dict[str, str] = {
    table: (
        f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['?'] * len(columns))})"
    )
    for table, columns in TABLE_COLUMNS.items()
}

# §3 날짜 경계는 Asia/Seoul 로 통일한다. 대본 CSV 의 시각은 오프셋 없는 한국 벽시계라
# 그대로 넣으면 TIMESTAMPTZ 로 옮길 때 어느 지역 시각인지 알 수 없어 하루가 밀린다.
#
# 🔴 **`KST` 를 여기서 다시 정의하지 말 것 — `app.core.constants` 것을 쓴다.**
#    이 파일은 오프셋을 붙여 **쓰는** 쪽이고, `app/batch/daily.py::_to_kst` 가 그걸 읽어
#    날짜를 **자르는** 쪽이다. 두 벌이 되면 한쪽만 바뀌었을 때 행 수도 `verify_counts` 도
#    전부 통과하는데 **날짜 경계의 문서만 다른 날로 집계된다** — 08-11 밤 생성기
#    비결정성과 같은 모양(집계는 같은데 행이 갈림)이라 집계 검산으로는 안 잡힌다.
#    (PR #68 후속)


def to_kst_iso(value: datetime) -> str:
    """이벤트 시각 → 오프셋이 붙은 ISO 문자열. naive 면 KST 로 간주한다."""
    aware = value.replace(tzinfo=KST) if value.tzinfo is None else value.astimezone(KST)
    return aware.isoformat()


def open_raw_db(db_path_str: str) -> sqlite3.Connection:
    """raw DB 연결 + 스키마 보장. 없으면 파일째로 새로 만든다."""
    db_path = Path(db_path_str).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    # WAL: 프로듀서가 쓰는 동안 워커가 같은 파일을 읽어도 서로 막히지 않게 한다.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    # ⚠️ sqlite 는 FK 가 **기본 OFF** 라 연결마다 켜야 한다. 안 켜면 DDL 의
    #    REFERENCES 가 장식으로만 남아 채널 오타가 조용히 통과한다 — 운영 Postgres 는
    #    기본으로 잡아 주므로, 목에서만 못 잡히면 거기서 처음 터진다.
    conn.execute("PRAGMA foreign_keys=ON;")
    raw_schema.create_source_tables(conn)
    conn.commit()

    logger.info(f"[RAW DB] 연결 완료: {db_path}")
    return conn


def seed_channels(conn: sqlite3.Connection, observed: set[str]) -> None:
    """§2-1 channel 마스터를 채운다. 마스터의 정본은 **`Channel` enum** 이다.

    확정 문서 §2-1 이 "channel_id 는 문자열 자체가 PK — 우리 `Channel` enum 과 그대로
    일치" 로 못박았다. 그래서 대본에서 관측된 값이 아니라 enum 을 넣는다.

    ⚠️ 예전에는 관측된 채널을 그대로 넣었는데, 그러면 **FK 가 아무것도 못 잡는다** —
       대본에 'coupang' 오타가 있으면 그 오타가 마스터에도 같이 들어가 버려서 참조가
       항상 성립한다. 마스터를 enum 으로 고정해야 오타가 FK 위반으로 걸린다.
       (걸린 행은 `RawDbSink` 가 행 단위로 격리해 실패 건수로 집계한다.)

    `Channel.ALL` 은 전역형 알림을 가리키는 가상 채널이라 연동 채널 마스터에 넣지 않는다.
    """
    now = datetime.now(KST).isoformat()
    for channel_id in MASTER_CHANNELS:
        conn.execute(
            "INSERT OR IGNORE INTO channel (channel_id, display_name, connected_at, status) "
            "VALUES (?, ?, ?, 'active')",
            (channel_id, channel_id, now),
        )
    conn.commit()
    logger.info(f"[RAW DB] channel 마스터 {len(MASTER_CHANNELS)}건 보장: {list(MASTER_CHANNELS)}")

    # 적재가 시작되기 전에 미리 알려 준다 — FK 위반은 행 단위 오류로만 나와서, 대본
    # 전체가 어긋난 경우 12만 줄짜리 ERROR 로그를 보고서야 원인을 알게 된다.
    unknown = sorted(observed - set(MASTER_CHANNELS))
    if unknown:
        logger.error(
            f"[RAW DB] 대본에 마스터에 없는 채널이 있습니다: {unknown} — "
            f"해당 행은 FK 위반으로 적재되지 않습니다(마스터: {list(MASTER_CHANNELS)}). "
            "CSV 의 channel 표기를 확인하세요."
        )


def seed_product_catalog(
    conn: sqlite3.Connection,
    products: list[dict[str, str]],
    group_of_variant: dict[str, str],
) -> None:
    """§2-2 products · §2-3 mapped_data 를 채운다.

    운영에서는 **백엔드(Spring Boot)가 상품 매핑을 수행해 이 두 테이블에 적재한다**
    (2026-08-11 확정). producer 는 목 파이프라인에서 main server 자리를 대신할 뿐이라,
    여기서 하는 일은 그 적재를 흉내 내는 것이지 매핑 규칙을 정하는 것이 아니다.

    ⚠️ `channel` 다음에 불러야 한다 — products.channel_id 가 채널 마스터를 참조한다.

    ⚠️ mapped_data 는 products 에 있는 variant 만 넣는다. 없는 variant 를 넣으면 FK 위반으로
       행이 통째로 빠지는데, 그러면 "매핑은 했는데 조회는 안 되는" 상태가 되어 원인을
       찾기 어렵다. 빠진 건수를 세어 알린다.
    """
    if not products:
        logger.warning(
            f"[RAW DB] {CHANNEL_PRODUCTS_FILE} 없음 — products·mapped_data 를 비워 둡니다. "
            "월간 리포트의 상품 표기명 조회가 product_group_id 로 대체됩니다."
        )
        return

    # 수집·매핑 시각. 대본 CSV 에는 없어서 **적재 시각**으로 둔다 — 목에서는 producer 가
    # main server 자리를 대신하므로 그게 실제로 이 행이 생긴 시각이다.
    #
    # ⚠️ `mapping_method`·`mapping_confidence` 는 채우지 않는다(§2-3). "무엇으로 묶었는지"
    #    (sim_embedding/rule_naming/manual)와 그 확신도는 **백엔드 매핑의 산물**이라 우리가
    #    아는 값이 아니다. 그럴듯한 값을 넣으면 나중에 실제 매핑 품질을 볼 때 지어낸 수치가
    #    섞인다. 모르는 것은 NULL 로 둔다.
    now = datetime.now(KST).isoformat()

    # ⚠️ `known` 과 INSERT 가 **같은 값**을 봐야 한다. 예전에는 여기서만 strip 하고 INSERT 는
    #    원본을 넣어서, CSV 에 공백이 섞이면 products 엔 공백 포함으로 들어가고 mapped_data
    #    는 orphan 으로 빠졌다 — 매핑은 했는데 조회가 안 되는, 제일 찾기 어려운 형태다.
    known: set[str] = set()
    for row in products:
        variant_row_id = str(row.get("variant_row_id") or "").strip()
        known.add(variant_row_id)
        conn.execute(
            "INSERT OR REPLACE INTO products (variant_row_id, channel_id, channel_product_id, "
            "channel_product_name, option_group_names, channel_option_name, sale_price, "
            "original_price, fetched_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                variant_row_id,
                row.get("channel"),
                row.get("channel_product_id"),
                row.get("channel_product_name"),
                row.get("option_group_names"),
                row.get("channel_option_name"),
                row.get("sale_price") or None,
                row.get("original_price") or None,
                now,
                now,
            ),
        )

    orphan = 0
    for vrid, group in group_of_variant.items():
        if vrid not in known:
            orphan += 1
            continue
        conn.execute(
            "INSERT OR REPLACE INTO mapped_data (variant_row_id, product_group_id, mapped_at) "
            "VALUES (?, ?, ?)",
            (vrid, group, now),
        )
    conn.commit()

    mapped = len(group_of_variant) - orphan
    logger.info(
        f"[RAW DB] products {len(products)}행 · mapped_data {mapped}행 적재 "
        f"(상품그룹 {len(set(group_of_variant.values()))}종)"
    )
    if orphan:
        logger.error(
            f"[RAW DB] mapped_data 에 products 에 없는 variant {orphan}건 — 적재하지 않았습니다. "
            f"{CHANNEL_PRODUCTS_FILE} 와 {MAPPED_DATA_FILE} 이 같은 생성분인지 확인하세요."
        )


def build_db_row(event: dict[str, Any]) -> tuple | None:
    """이벤트 1건 → 대상 테이블의 INSERT 파라미터. 적재 대상이 아니면 None.

    `created_at`(레코드 적재 시각)은 재생 시점의 벽시계다 — 대본상 발생 시각
    (`inquired_at` / `reviews.created_at`)과 다르며, §2-4 가 둘을 구분해 두었다.
    """
    table = event["table"]
    if table is None:
        return None

    payload = event["payload"]
    occurred_at = to_kst_iso(event["time"])
    loaded_at = datetime.now(KST).isoformat()

    if table == "cs":
        return (
            event["event_id"], event["channel_product_id"], event["product_group_id"],
            event["channel"], event["raw_text"], occurred_at, loaded_at,
        )
    if table == "reviews":
        rating = payload.get("rating")
        return (
            event["event_id"], event["channel_product_id"], event["product_group_id"],
            event["channel"], event["raw_text"], None if rating is None else int(rating),
            occurred_at,
        )
    if table == "orders":
        # §2-9 order_date 는 DATE 다 — 하루 합산 행이라 시각·오프셋이 없다.
        order_date = event["time"].date() if isinstance(event["time"], datetime) else event["time"]
        return (
            event["channel"], event["channel_product_id"],
            order_date.isoformat() if isinstance(order_date, date) else str(order_date),
            int(payload.get("quantity") or 0), int(payload.get("order_amount") or 0),
            loaded_at,
        )
    raise ValueError(f"적재 대상 테이블을 모릅니다: {table}")


class RawDbSink:
    """확정 스키마 테이블 적재 버퍼. 일정 건수/시간마다 모아서 commit 한다.

    테이블마다 컬럼 수가 달라 버퍼를 테이블별로 나눈다. flush 는 한꺼번에 돈다 —
    한 번의 commit 안에 세 테이블이 함께 들어가야 워커가 보는 시점이 갈리지 않는다.

    ⚠️ 적재 실패는 **여기서 흡수하고 집계만** 한다. 밖으로 던지지 않는 이유:
       한 행이 잘못됐다고 나머지 재생(특히 Kafka 발행)까지 멈추면 안 되기 때문이다.
       실패 건수는 self.failed 에 쌓여 종료 요약에 함께 찍힌다.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.buffers: dict[str, list[tuple]] = {table: [] for table in TABLE_INSERTS}
        self.last_flush = time.monotonic()
        self.written = 0
        self.failed = 0

    @property
    def buffered(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    def add(self, event: dict[str, Any]) -> None:
        row = build_db_row(event)
        if row is None:  # 확정 스키마에 대응 테이블이 없는 종류(상세페이지 변경 등)
            return
        self.buffers[event["table"]].append(row)

        if self.buffered >= DB_FLUSH_ROWS or (time.monotonic() - self.last_flush) >= DB_FLUSH_SECONDS:
            self.flush()

    def flush(self) -> None:
        """버퍼를 비운다. 실패해도 버퍼는 반드시 비워지고, 불량 행만 떨어져 나간다."""
        if not self.buffered:
            self.last_flush = time.monotonic()
            return

        # 버퍼를 먼저 떼어낸다 — 실패해도 같은 행이 버퍼에 남아 다음 flush 를 계속 터뜨리는
        # 것을 원천 차단한다(그 상태가 되면 재생 전체가 조용히 멈춘다).
        pending = {table: rows for table, rows in self.buffers.items() if rows}
        self.buffers = {table: [] for table in TABLE_INSERTS}
        try:
            for table, rows in pending.items():
                self.conn.executemany(TABLE_INSERTS[table], rows)
            self.conn.commit()
            self.written += sum(len(rows) for rows in pending.values())
        except sqlite3.Error as err:
            self.conn.rollback()
            total = sum(len(rows) for rows in pending.values())
            logger.warning(f"[RAW DB] 배치 적재 실패({total}행) — 행 단위로 재시도합니다: {err}")
            for table, rows in pending.items():
                self._flush_row_by_row(table, rows)
        finally:
            self.last_flush = time.monotonic()

    def _flush_row_by_row(self, table: str, rows: list[tuple]) -> None:
        """불량 행을 골라내려고 한 줄씩 넣는다. 실패한 행의 PK 만 정확히 로그로 남는다."""
        for row in rows:
            try:
                self.conn.execute(TABLE_INSERTS[table], row)
                self.conn.commit()
                self.written += 1
            except sqlite3.Error as err:
                self.conn.rollback()
                self.failed += 1
                logger.error(f"[RAW DB] 행 적재 실패 ({table}, key={row[0]}): {err}")

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


def _to_jsonable(value: Any) -> Any:
    """CSV 셀 값을 JSON 직렬화 가능한 형태로 정리."""
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):  # numpy 스칼라 → 파이썬 기본형
        return value.item()
    return value


# §2-2 products 의 대본. 채널 쪽 사실만 들어 있고 **상품 그룹은 없다**(그건 매핑 결과라
# mapped_data 소관). 컬럼은 `raw_schema.PRODUCTS_DDL` 과 1:1 이다.
CHANNEL_PRODUCTS_FILE = "input_channel_products.csv"

# §2-3 mapped_data 의 대본. variant → 상품 그룹.
#
# ⚠️ 매핑 자체는 **백엔드(Spring Boot)가 수행해 적재한다**(2026-08-11 확정). 여기서 읽는
#    파일은 그 결과를 목으로 흉내 낸 것뿐이고, 규칙을 정하는 쪽은 producer 가 아니다.
#
# 🔴 **이 파일은 백엔드가 준다. 저장소 안에 만드는 코드가 없다**(2026-08-11 기준).
#    상품 매핑은 백엔드 소관이라 그 결과물을 받아 `data/input/` 에 두는 구조다.
#    받기 전까지는 매핑 없이 돌고, 그 상태에서 무엇이 어긋나는지는 아래 로더 docstring 참고.
#
#    ⚠️ 예전 주석은 "생성기가 `--golden-mapping-dir` 로 조인해 만들어 준다" 였는데 **틀렸다.**
#       `generate_cs_review_data.py` 는 golden 매핑을 자기 입력으로 읽을 뿐 이 파일을 내지
#       않는다. 출처가 두 갈래로 적혀 있으면 다음 사람이 없는 생성기 기능을 고치러 간다.
#       (`data/golden/golden_mapping.csv` 로 임시로 만들어 쓸 수는 있지만 — producer 는
#        golden 을 못 읽으므로(`validate_data_directory` 가드 · CLAUDE.md 9) 그 변환은
#        사람이 한 번 해서 input 쪽에 둬야 한다.)
MAPPED_DATA_FILE = "input_mapped_data.csv"


def _resolve_group(
    channel: str,
    channel_product_id: str,
    group_of: dict[tuple[str, str], str],
    unmapped: set[tuple[str, str]],
) -> str:
    """채널 상품 → 상품 그룹. 매핑에 없으면 채널 상품 ID 를 그대로 쓰고 기록해 둔다.

    빈 문자열을 돌려주지 않는다 — 호출부의 `or None` 이 빈 값을 None 으로 접기 때문에
    여기서 넘긴 대체값이 그대로 살아야 한다.
    """
    if not channel_product_id:
        return ""
    key = (channel, channel_product_id)
    resolved = group_of.get(key)
    if resolved:
        return resolved
    if group_of:  # 매핑은 있는데 이 상품만 빠진 경우 — 파일 부재와 구분해서 센다
        unmapped.add(key)
    return channel_product_id


def load_product_catalog(data_dir: Path) -> tuple[list[dict[str, str]], dict[str, str]]:
    """§2-2 products · §2-3 mapped_data 대본을 읽는다. 적재와 조인에 둘 다 쓴다.

    Returns:
        (products 행 목록, variant_row_id → product_group_id)
    """
    products: list[dict[str, str]] = []
    path = data_dir / CHANNEL_PRODUCTS_FILE
    if path.exists():
        with path.open(encoding="utf-8-sig", newline="") as f:
            products = [dict(row) for row in csv.DictReader(f)]

    group_of_variant: dict[str, str] = {}
    map_path = data_dir / MAPPED_DATA_FILE
    if map_path.exists():
        with map_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                vrid = str(row.get("variant_row_id") or "").strip()
                group = str(row.get("product_group_id") or "").strip()
                if vrid and group:
                    group_of_variant[vrid] = group

    return products, group_of_variant


def build_channel_product_map(
    products: list[dict[str, str]], group_of_variant: dict[str, str]
) -> dict[tuple[str, str], str]:
    """`products ⋈ mapped_data` 를 (channel, channel_product_id) → product_group_id 로 편다.

    운영에서 백엔드가 두 테이블에 적재해 두면 읽는 쪽이 SQL 로 하는 그 조인이다. 여기서는
    적재 **전에** 이벤트 필드를 채워야 해서 메모리에서 같은 조인을 한다 — 기준이 갈리지
    않도록 원본은 어디까지나 위 두 대본 하나씩이다.

    ⚠️ 이 매핑이 비면 **채널마다 다른 그룹으로 갈린다.** 상품 하나가 쿠팡·네이버·지그재그
       에서 서로 다른 `product_group_id` 가 되어:
         - 탐지의 채널 간 비교(편중형/전역형)가 성립하지 않는다
         - 월간 리포트의 채널 격차(JSD)도 같은 이유로 무의미해진다
         - ChromaDB 컬렉션1(상세페이지)은 `P001` 로 적재돼 있어 조회가 **전부** 빗나간다
           (2026-08-11 실측: 적중 0% → 개선안이 전부 근거없음 경로로 떨어졌다)
       전부 조용히 틀리는 실패라, 비면 아래에서 경고를 남긴다.
    """
    mapping: dict[tuple[str, str], str] = {}
    for row in products:
        group = group_of_variant.get(str(row.get("variant_row_id") or "").strip())
        channel = str(row.get("channel") or "").strip()
        channel_product_id = str(row.get("channel_product_id") or "").strip()
        if group and channel and channel_product_id:
            mapping[(channel, channel_product_id)] = group

    if not mapping:
        logger.warning(
            f"상품 매핑 없음({CHANNEL_PRODUCTS_FILE} + {MAPPED_DATA_FILE}) — 채널 상품 ID 를 "
            f"그룹 키로 대체합니다. 채널 간 비교와 상세페이지(ChromaDB) 조회가 빗나갑니다."
        )
    else:
        logger.info(
            f"[MAP] products {len(products)}행 ⋈ mapped_data {len(group_of_variant)}행 "
            f"→ 채널상품 {len(mapping)}종 / 상품그룹 {len(set(mapping.values()))}종"
        )
    return mapping


def load_and_merge_csvs(data_dir: Path, topics_filter_str: str | None) -> list[dict[str, Any]]:
    """대본 CSV 들을 읽어 이벤트 리스트로 합친다.

    ⚠️ 대상 파일을 **하나도** 못 찾으면 경고가 아니라 에러로 죽는다.
       data/ 는 gitignore 라 팀원마다 파일이 다른데, 경고 한 줄만 남기고 "0건 발행"으로
       조용히 끝나면 파일명이 틀린 건지 정상인지 구분할 수가 없다.
    """
    products, group_of_variant = load_product_catalog(data_dir)
    group_of = build_channel_product_map(products, group_of_variant)
    unmapped: set[tuple[str, str]] = set()
    merged_events: list[dict[str, Any]] = []
    topics_filter = [t.strip() for t in topics_filter_str.split(",")] if topics_filter_str else []
    missing_files: list[Path] = []
    loaded_files = 0

    for key, config in STREAMING_FILE_CONFIGS.items():
        if topics_filter and "all" not in topics_filter and key not in topics_filter:
            continue

        file_path = data_dir / config["file_name"]
        if not file_path.exists():
            missing_files.append(file_path)
            logger.warning(f"대본 파일 미존재 스킵: {file_path}")
            continue
        loaded_files += 1

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
            # 없는 파일(주문)은 파일 내 행 순번으로 만든다. cs/reviews 에서는 이 값이
            # 그대로 PK(§5-1 A안: item_id = cs.id / reviews.id)라, 같은 대본을 다시
            # 재생해도 중복 행이 쌓이지 않고 덮어써진다. orders 는 복합 PK 라 안 쓴다.
            id_column = config["id_column"]
            natural_id = sanitized_payload.get(id_column) if id_column else None
            event_id = str(natural_id) if natural_id else f"{config['id_prefix']}-{row_idx:06d}"

            text_column = config["text_column"]
            raw_text = sanitized_payload.get(text_column) if text_column else None

            merged_events.append({
                "time": event_time,
                "topic": config["topic"],
                "table": config["table"],
                "event_id": event_id,
                "source": config["source"],
                "channel": channel or None,
                "channel_product_id": channel_product_id or None,
                # ⚠️ 마스터 상품 그룹(P001…)은 답 노출 방지 설계상 input CSV 에 없다
                #    (generate_mock_data.py 참고). 그래서 매핑으로 되찾는다 —
                #    운영에서는 `products ⋈ mapped_data` 가 하는 일이다.
                #    매핑에 없으면 채널 상품 ID 를 그대로 쓰되 아래에서 건수를 남긴다.
                "product_group_id": (
                    sanitized_payload.get("product_group_id")
                    or _resolve_group(channel, channel_product_id, group_of, unmapped)
                    or None
                ),
                "raw_text": str(raw_text) if raw_text else None,
                "message_key": message_key,
                "payload": sanitized_payload,
            })

        logger.info(f"[LOAD] {file_path.name}: {len(df)}행 적재 대상 로드")

    if loaded_files == 0:
        expected = "\n  - ".join(str(p) for p in missing_files) or "(필터에 걸려 대상 없음)"
        sys.stderr.write(
            "[FATAL] 재생할 대본 파일을 하나도 찾지 못했습니다. 경로·파일명을 확인하세요.\n"
            f"  - {expected}\n"
            "  data/ 는 git 에 없으므로 scripts/generate_mock_data.py 로 먼저 생성해야 합니다.\n"
        )
        sys.exit(1)

    if unmapped:
        # 매핑 파일은 있는데 일부 상품이 빠진 경우. 그 상품만 채널별로 갈리므로
        # "일부만 조용히 틀리는" 상태가 된다 — 전량 실패보다 찾기 어렵다.
        sample = ", ".join(f"{ch}:{cpid}" for ch, cpid in sorted(unmapped)[:3])
        logger.warning(
            f"[MAP] 매핑에 없는 채널 상품 {len(unmapped)}종 — 채널 상품 ID 를 그룹 키로 대체합니다. "
            f"예: {sample}"
        )

    return merged_events


def sort_by_timestamp(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # event_id 를 2차 키로 둬서 같은 시각 이벤트의 순서도 재생마다 동일하게 고정한다
    # (워커의 (occurred_at, item_id) 커서와 같은 정렬 기준).
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


def publish(event: dict[str, Any], producer: Any | None, sink: RawDbSink | None, dry_run: bool) -> bool:
    payload = event["payload"]
    # **KST 로 찍는다.** 이 파일의 다른 시각은 전부 `to_kst_iso()` 를 거치는데 여기만
    # 호스트 로컬 오프셋이 붙어 있었다 — UTC 컨테이너에서 돌리면 같은 이벤트 안에서
    # `occurred_at`(+09:00)과 `published_at`(+00:00)의 오프셋이 갈린다.
    #
    # ⚠️ `classified_at` 과 같은 성격이다 — **일관성 확보이지 지금 깨지는 것을 고치는 게
    #    아니다.** `published_at` 은 payload 에 실려 Kafka 로 나가고 `--dry-run` 로그
    #    한 줄에 찍히는 것이 전부이고, 읽어서 비교하는 소비처는 저장소에 없다.
    #    (2026-08-13)
    payload["published_at"] = datetime.now(KST).isoformat()

    # 원본 적재를 먼저 한다 — 워커가 참조하는 정본은 이제 raw DB 쪽이다.
    # 적재 실패는 sink 안에서 흡수·집계되고 여기까지 올라오지 않는다.
    # DB 한 건이 실패했다고 Kafka 발행까지 건너뛰면 두 경로가 함께 죽는다.
    if sink is not None:
        sink.add(event)

    if dry_run:
        # --dry-run 의 정의가 "콘솔로 재생 타임라인을 검증한다"이므로 INFO 로 찍는다
        # (debug 로 두면 기본 로그 레벨이 INFO 라 아무것도 안 보인다).
        logger.info(
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


def print_summary(
    summary_counts: dict[str, int], db_written: int | None, db_failed: int = 0
) -> None:
    print("\n========== [발행 결과 요약 SUMMARY] ==========")
    total = 0
    for topic, count in summary_counts.items():
        print(f" - {topic}: {count} 건")
        total += count
    print(f" 총 발행 이벤트 수: {total} 건")
    if db_written is not None:
        print(f" raw DB 적재 행 수: {db_written} 행")
        if db_failed:
            # 조용히 넘어가면 워커가 처리할 원본이 비는 것을 눈치채지 못한다
            print(f" ⚠️ raw DB 적재 실패: {db_failed} 행 (ERROR 로그의 테이블·키 확인)")
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

    sink = None
    if not args.no_db:
        conn = open_raw_db(args.raw_db)
        # channel 마스터를 먼저 채운다 — cs·reviews·orders 의 channel_id 가 참조한다.
        seed_channels(conn, {e["channel"] for e in events if e["channel"]})
        # 그다음 상품 카탈로그 — products.channel_id 가 channel 을,
        # mapped_data.variant_row_id 가 products 를 참조한다(§2-2 → §2-3 순서).
        seed_product_catalog(conn, *load_product_catalog(data_dir))
        sink = RawDbSink(conn)

    summary_counts: dict[str, int] = {config["topic"]: 0 for config in STREAMING_FILE_CONFIGS.values()}
    prev_time: datetime | None = None

    db_written: int | None = None
    db_failed = 0
    try:
        for e in events:
            if prev_time and not args.dry_run:
                delta_seconds = (e["time"] - prev_time).total_seconds()
                if delta_seconds > 0 and args.speed > 0:
                    sleep_seconds = delta_seconds / args.speed
                    # 오래 쉬는 구간에서만 버퍼를 비운다 — 워커가 최신 행을 준실시간으로 보되,
                    # 촘촘한 구간에서는 DB_FLUSH_ROWS 배칭이 살아있게 한다.
                    if sink is not None and sleep_seconds >= FLUSH_BEFORE_SLEEP_SECONDS:
                        sink.flush()
                    time.sleep(sleep_seconds)

            if publish(e, producer, sink, args.dry_run):
                summary_counts[e["topic"]] += 1
            prev_time = e["time"]
    finally:
        if producer:
            producer.flush()
            producer.close()
        if sink is not None:
            sink.close()
            db_written = sink.written
            db_failed = sink.failed

    print_summary(summary_counts, db_written, db_failed)


if __name__ == "__main__":
    main()
