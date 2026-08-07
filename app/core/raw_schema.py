"""Raw DB 스키마 — 「Raw DB 스키마 확정 (8/7)」 §2 를 그대로 옮긴 DDL.

소유권(§1):
    main server 쓰기 : channel · products · mapped_data · orders · cs · reviews
    AI 노드 쓰기      : classified_item · classified_item_aspect ·
                       classification_failure · classification_cursor

목 파이프라인에서는 `scripts/mock_producer.py` 가 main server 자리를 대신해 원본 테이블을
채우고, `scripts/classification_worker.py` 가 AI 소유 테이블을 채운다. 읽는 쪽은
`app/reporting/monthly_aggregator.py`(월간 집계)와 이상탐지 로더다.

⚠️ 이 파일이 `scripts/` 가 아니라 `app/core/` 에 있는 이유: **쓰는 쪽과 읽는 쪽이 갈려
   있다.** 스크립트에 두면 `app/` 코드가 import 할 수 없어(폴더 규칙) 집계 쪽이 테이블·뷰
   이름을 문자열로 다시 적게 되고, 그 순간 스키마가 두 벌이 된다 — `ids.py` 가 리포팅·MQ
   양쪽에서 쓰이게 되자 core 로 옮겨진 것과 같은 이유다(5d19c51).

⚠️ `app/core/` 는 팀 계약 영역이라 변경 시 합의가 필요하다(CLAUDE.md 아키텍처 원칙 3).
   여기 담긴 것은 우리가 정한 규칙이 아니라 **확정 문서 §2 를 옮겨 적은 것**이므로, 문서가
   바뀌지 않는 한 이 파일도 바뀌지 않아야 한다.

⚠️ 예전에는 이 자리에 `raw_event` 단일 테이블이 있었다(이벤트 종류를 컬럼으로 구분).
   8/7 확정으로 문서 종류별 실테이블이 정해져서 걷어냈다. 종류마다 시각 컬럼명이 다르고
   (`cs.inquired_at` / `reviews.created_at`) 리뷰에만 `rating` 이 있는 등 컬럼이 갈려서,
   한 테이블에 몰면 NULL 허용이 늘어 제약으로 잡을 수 있는 것이 없어진다.

⚠️ sqlite 방언을 피해 표준 SQL 범위로 유지한다. 운영 Postgres 로 옮길 때 타입 표기만
   바꾸면(TEXT→VARCHAR(n), TEXT 시각→TIMESTAMPTZ, INTEGER PK→BIGSERIAL) 제약·인덱스·뷰는
   그대로 쓴다. 시각은 전부 **오프셋이 붙은 ISO 문자열**로 넣는다 — §3 이 날짜 경계를
   Asia/Seoul 로 못박았는데, 오프셋 없이 넣으면 TIMESTAMPTZ 로 옮길 때 어느 지역 시각인지
   알 수 없어 하루가 밀린다.
"""

from __future__ import annotations

# ── main server 소유 (§2-1 · §2-4 · §2-5 · §2-9) ─────────────────────────────

# §2-1 channel — 연동 채널 마스터.
# channel_id 는 문자열 자체가 PK 이고 우리 `Channel` enum 값과 그대로 일치한다.
CHANNEL_DDL = """
CREATE TABLE IF NOT EXISTS channel (
    channel_id   TEXT PRIMARY KEY,
    display_name TEXT,
    connected_at TEXT,
    status       TEXT
);
"""

# §2-4 cs — CS 문의 원문. 이상탐지·리포팅이 **분모를 세는 정본**이다.
#
# ⚠️ 분류 안 된 문의도 반드시 남는다(§2-4 운영 정책). 분모가 "그 상품·채널의 총 문의 수"라
#    분류에 실패했거나 아무 aspect 에도 안 걸린 문의가 사라지면 부정률이 왜곡된다.
#    이 테이블에서 행을 지우거나 숨기는 배치를 만들지 말 것.
#
# inquired_at  문의 발생 시각. 35일 범위조회의 조건절이라 인덱스 필수(§3).
# created_at   레코드 적재 시각. inquired_at 과 다를 수 있다(§2-4).
CS_DDL = """
CREATE TABLE IF NOT EXISTS cs (
    id                 TEXT PRIMARY KEY,
    channel_product_id TEXT,
    product_group_id   TEXT,
    channel_id         TEXT REFERENCES channel(channel_id),
    content            TEXT NOT NULL,
    inquired_at        TEXT NOT NULL,
    created_at         TEXT
);
"""

# §2-5 reviews — 리뷰 원문. cs 와 같은 구조 + rating.
#
# ⚠️ cs 와 달리 발생 시각·적재 시각을 나누지 않고 `created_at` 단일 컬럼이다(§2-5).
#    그래서 아래 `VOC_DOCUMENT_VIEW` 가 두 컬럼을 하나의 축으로 맞춘다.
REVIEWS_DDL = """
CREATE TABLE IF NOT EXISTS reviews (
    id                 TEXT PRIMARY KEY,
    channel_product_id TEXT,
    product_group_id   TEXT,
    channel_id         TEXT REFERENCES channel(channel_id),
    content            TEXT NOT NULL,
    rating             INTEGER,
    created_at         TEXT NOT NULL
);
"""

# §2-9 orders — 채널별 주문 원본. 개별 주문건이 아니라 **그 날의 합산**이라
# (channel_id, channel_product_id, order_date) 복합 PK 로 하루 한 행이다.
ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS orders (
    channel_id         TEXT NOT NULL REFERENCES channel(channel_id),
    channel_product_id TEXT NOT NULL,
    order_date         TEXT NOT NULL,
    quantity           INTEGER NOT NULL,
    order_amount       INTEGER NOT NULL,
    created_at         TEXT,
    PRIMARY KEY (channel_id, channel_product_id, order_date)
);
"""

# ── AI 노드 소유 (§2-6 · §2-7 · §2-8) ────────────────────────────────────────

# §2-6 classified_item — 문의/리뷰 1건 = 1행.
#
# ⚠️ 원문 사본을 만들지 않는다(아키텍처 확정 §6: "CS·리뷰 원문은 AI가 사본을 안 만들고
#    원본 DB에서 바로 읽음"). 채널·상품그룹·발생 시각도 cs/reviews 에 있으니 두지 않는다.
#
# ⚠️ 이 두 테이블로 **분모를 세면 안 된다**(§4 예시 쿼리 경고). aspect 가 하나도 없는
#    문의는 자식 테이블에 0행이라 여기서 세면 분모가 조용히 줄어든다. 분모는 cs/reviews 에서
#    세고 여기를 LEFT JOIN 해 분자만 가져온다.
#
# item_id 는 `cs.id` / `reviews.id` 를 그대로 재사용한다(§5-1 A안 확정) — 접두사
# INQ-/RVW- 가 달라 두 테이블을 합쳐도 충돌하지 않는다.
CLASSIFIED_ITEM_DDL = """
CREATE TABLE IF NOT EXISTS classified_item (
    item_id        TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    classified_at  TEXT,
    prompt_version TEXT
);
"""

# §2-6 classified_item_aspect — 1문의 : N aspect.
# 하나의 문의가 여러 속성에 걸릴 수 있어(explode 계약) 정규화한다. JSON 한 컬럼에 몰지
# 않는 이유는 이상탐지가 aspect 별 GROUP BY 로 집계하기 때문이다.
#   sentiment    -1 / 0 / 1
#   mixed_signal 리뷰 전용. CS 는 항상 NULL.
CLASSIFIED_ITEM_ASPECT_DDL = """
CREATE TABLE IF NOT EXISTS classified_item_aspect (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id      TEXT NOT NULL REFERENCES classified_item(item_id),
    aspect       TEXT NOT NULL,
    sentiment    INTEGER NOT NULL,
    mixed_signal INTEGER,
    UNIQUE (item_id, aspect)
);
"""

# §2-7 classification_failure — 분류 dead-letter.
#
# 이게 없으면 실패 건이 로그로만 남고 커서가 지나가 **영구 유실**된다. 그러면 분모는
# cs/reviews 그대로인데 분자(classified_item_aspect)만 빠져 부정률이 조용히 과소추정된다.
# 남겨야 ①얼마나 빠졌는지(분류 커버리지) 셀 수 있고 ②나중에 재처리할 수 있다.
#   stage    parse(스키마 변환 실패) / classify(LLM 호출·파싱 실패)
#   attempts 재처리 시도 횟수. 결정적 실패를 무한 재과금하지 않도록 상한을 건다.
# 재처리에 성공하면 그 행은 삭제한다(§2-7).
FAILURE_DDL = """
CREATE TABLE IF NOT EXISTS classification_failure (
    item_id         TEXT PRIMARY KEY,
    occurred_at     TEXT,
    stage           TEXT NOT NULL,
    error           TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 1,
    first_failed_at TEXT NOT NULL,
    last_failed_at  TEXT NOT NULL
);
"""

# §2-8 classification_cursor — 어디까지 분류했는지. Kafka 컨슈머 오프셋을 대체한다.
# `classified_item_aspect` INSERT 와 같은 트랜잭션에서 갱신해 원자성을 보장한다(§2-8).
#
# 컬럼명이 `last_inquired_at` 인데 리뷰의 시각 컬럼은 `created_at` 이다 — 확정 문서의
# 이름을 그대로 따랐다. 의미는 "마지막으로 처리한 원문의 **발생 시각**"이고, 아래 뷰의
# `occurred_at` 이 그 값이다.
CURSOR_DDL = """
CREATE TABLE IF NOT EXISTS classification_cursor (
    worker_id        TEXT PRIMARY KEY,
    last_inquired_at TEXT,
    last_item_id     TEXT,
    updated_at       TEXT NOT NULL
);
"""

# ── 읽기 모델 ────────────────────────────────────────────────────────────────

# cs ∪ reviews 통합 뷰.
#
# 분류 워커와 월간 집계는 둘 다 "원문 문서"를 하나의 타임라인으로 훑어야 하는데,
# 두 테이블은 시각 컬럼명이 다르고(inquired_at / created_at) 리뷰에만 rating 이 있다.
# 호출부마다 UNION 을 다시 쓰면 시각 컬럼을 잘못 고르는 실수가 각자 생기므로 여기서
# `occurred_at` 하나로 맞춰 둔다. §5-1 A안에 따라 `item_id` 는 원문 PK 그대로다.
#
# ⚠️ 이 뷰가 **분모의 정본**이다. 분모와 분자를 한 쿼리로 묶지 말 것 — GROUP BY 에
#    aspect 가 들어간 채로 분모까지 세면 분류 안 된 문의가 빠져 §2-4 가 금지한 상황이
#    그대로 재현된다(§4 예시 쿼리의 CTE 분리가 그 이유다). 분모는 이 뷰만 보고 세고,
#    분자는 classified_item_aspect 를 조인해 따로 센다.
VOC_DOCUMENT = "voc_document"
"""통합 뷰 이름. 조회하는 쪽은 문자열을 다시 적지 말고 이 상수를 쓴다."""

VOC_DOCUMENT_VIEW = f"""
CREATE VIEW IF NOT EXISTS {VOC_DOCUMENT} AS
    SELECT id                 AS item_id,
           'cs'               AS source,
           channel_id,
           channel_product_id,
           product_group_id,
           content,
           inquired_at        AS occurred_at
    FROM cs
    UNION ALL
    SELECT id,
           'review',
           channel_id,
           channel_product_id,
           product_group_id,
           content,
           created_at
    FROM reviews;
"""

# §3 인덱스 요약 — 조회 패턴 기준.
SOURCE_INDEXES = [
    # 35일 배치의 핵심 조건절
    "CREATE INDEX IF NOT EXISTS idx_cs_inquired_at ON cs (inquired_at);",
    "CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON reviews (created_at);",
    # 집계 GROUP BY 단위와 일치
    "CREATE INDEX IF NOT EXISTS idx_cs_group_channel ON cs (product_group_id, channel_id);",
    "CREATE INDEX IF NOT EXISTS idx_reviews_group_channel ON reviews (product_group_id, channel_id);",
]

CLASSIFIED_INDEXES = [
    # cs / reviews 와의 조인 키
    "CREATE INDEX IF NOT EXISTS idx_classified_item_aspect_item ON classified_item_aspect (item_id);",
]

# 원문 테이블 — 이게 없으면 워커가 읽을 것이 없다.
SOURCE_TABLES = ("cs", "reviews")


def create_source_tables(conn) -> None:
    """main server 소유 테이블 + 통합 뷰. 목 파이프라인에서는 프로듀서가 부른다."""
    for ddl in (CHANNEL_DDL, CS_DDL, REVIEWS_DDL, ORDERS_DDL):
        conn.execute(ddl)
    for stmt in SOURCE_INDEXES:
        conn.execute(stmt)
    conn.execute(VOC_DOCUMENT_VIEW)


def create_classified_tables(conn) -> None:
    """AI 노드 소유 테이블. 분류 워커가 부른다.

    뷰도 함께 보장한다 — 워커가 자기가 만들지 않은 DB 에 붙는 경우가 있고
    (프로듀서 없이 덤프만 받은 상태), 뷰가 없으면 조회가 통째로 실패한다.
    """
    for ddl in (
        CLASSIFIED_ITEM_DDL,
        CLASSIFIED_ITEM_ASPECT_DDL,
        FAILURE_DDL,
        CURSOR_DDL,
    ):
        conn.execute(ddl)
    for stmt in CLASSIFIED_INDEXES:
        conn.execute(stmt)
    conn.execute(VOC_DOCUMENT_VIEW)
