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

🔴 **아래 DDL 은 운영에서 안 쓰인다 — 로컬·목 파이프라인 전용이다(2026-08-16 확정).**
   운영 raw DB(Postgres `rawdb`)의 스키마는 **인프라가 노션 「RAW DB 스키마」(DDL 전문)로
   세운다.** 우리 코드는 그 스키마를 전제로 조회·삽입만 한다. 그래서 이 파일을 고쳐도
   운영에는 아무 일도 일어나지 않는다 — 반대로, 운영에 제약이 필요하면 코드가 아니라
   **문서와 인프라에 요청**해야 한다(§2-6 의 `UNIQUE (item_id, aspect)` 가 그 경우다).
   로컬 Postgres 스키마는 `docker/postgres/init/01_schema.sql` 이 세운다.

⚠️ sqlite 방언을 피해 표준 SQL 범위로 유지한다. 시각은 전부 **오프셋이 붙은 ISO 문자열**로
   넣는다 — §3 이 날짜 경계를 Asia/Seoul 로 못박았는데, 오프셋 없이 넣으면 TIMESTAMPTZ 인
   운영 컬럼으로 옮길 때 어느 지역 시각인지 알 수 없어 하루가 밀린다.
"""

from __future__ import annotations

from app.core import raw_db

# ── main server 소유 (§2-1 · §2-4 · §2-5 · §2-9) ─────────────────────────────
#
# ⚠️ 아래 `REFERENCES` 는 **sqlite 에서 기본으로 안 걸린다.** FK 는 연결마다
#    `PRAGMA foreign_keys=ON` 을 해야 켜진다(기본 OFF). 이 DDL 로 DB 를 만드는 쪽은
#    반드시 켤 것 — 안 켜면 채널 오타가 조용히 통과해서, 운영 Postgres 에 올라가서야
#    처음 터진다. 목 파이프라인에서는 `mock_producer.open_raw_db()` 와
#    `classification_worker.open_db()` 가 켠다.

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

# §2-2 products — 채널에 올라간 상품(variant) 카탈로그.
#
# 채널 쪽 사실만 담는다. **어느 상품끼리 같은 상품인지는 여기 없다** — 그 판단은 상품
# 매핑의 결과라 `mapped_data` 로 갈린다. 목 데이터의 `input_channel_products.csv` 가
# 이 테이블에 1:1 대응하고, 그 CSV 에 `product_group_id` 가 없는 것도 같은 이유다.
#
# channel_product_name  월간 리포트가 셀러에게 보여줄 표기명. 채널마다 다르다
#                       (`monthly_aggregator._fetch_product_names()` 가 최빈값을 고른다).
# fetched_at            최초 수집 시각.
# updated_at            마지막 갱신 시각. 카탈로그는 가격 변동 등으로 원본이 바뀌는데
#                       `mapped_data` 스냅샷은 따라오지 않는다(§5-3) — 두 테이블이 각각
#                       언제 기준인지 가르는 값이라 반드시 남긴다.
PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS products (
    variant_row_id       TEXT PRIMARY KEY,
    channel_id           TEXT NOT NULL REFERENCES channel(channel_id),
    channel_product_id   TEXT NOT NULL,
    channel_product_name TEXT,
    option_group_names   TEXT,
    channel_option_name  TEXT,
    sale_price           INTEGER,
    original_price       INTEGER,
    fetched_at           TEXT,
    updated_at           TEXT
);
"""

# ⚠️ `channel_product_id` 는 **PK 가 아니라 인덱스**다(§2-2). 색상·사이즈 옵션이 여러 행으로
#    붙는 구조라 같은 채널 상품 ID 를 여러 variant 가 공유한다 — 유니크로 걸면 적재가 깨진다.
PRODUCTS_CHANNEL_PRODUCT_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_products_channel_product "
    "ON products (channel_product_id);"
)

# §2-3 mapped_data — 상품 매핑 결과. variant → 상품 그룹.
#
# ⚠️ **AI 노드가 만드는 값이 아니다.** 매핑은 백엔드(Spring Boot)가 수행해 적재한다
#    (2026-08-11 확정). 목 파이프라인에서는 `mock_producer` 가 main server 자리를 대신해
#    채우지만, 규칙을 정하는 쪽은 어디까지나 백엔드다.
#
# ⚠️ 이 테이블이 비면 **채널 간 비교가 통째로 무너진다.** 상품 하나가 채널마다 다른 그룹이
#    되어 탐지의 편중형/전역형 판정도, 월간 리포트의 채널 격차도 성립하지 않는다.
#    (2026-08-11 실측: 매핑 없이 돌렸을 때 상세페이지 RAG 조회 적중률 0%)
# mapping_method      sim_embedding / rule_naming / manual — 무엇으로 묶었는지(§2-3).
# mapping_confidence  **참고용이다. 판정 기준으로 쓰지 않는다**(§2-3 명시).
# mapped_at           매핑 시점. §5-3 이 확정한 스냅샷 동기화(주 1회 매핑 재구성)의 근거
#                     컬럼이다 — `products` 원본이 나중에 바뀌어도 이 값은 따라오지 않는다.
#                     "최신 매핑을 고른다" 는 쿼리가 생기면 이 컬럼이 없이는 못 쓴다.
MAPPED_DATA_DDL = """
CREATE TABLE IF NOT EXISTS mapped_data (
    variant_row_id     TEXT PRIMARY KEY REFERENCES products(variant_row_id),
    product_group_id   TEXT NOT NULL,
    mapping_method     TEXT,
    mapping_confidence REAL,
    mapped_at          TEXT
);
"""

MAPPED_DATA_GROUP_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_mapped_data_group ON mapped_data (product_group_id);"
)

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
#
# ⚠️ 버전 3종(prompt·model·pipeline)은 **감사용 메타가 아니라 조회 조건이다.** 탐지가
#    35일(현재 7 + 과거 28)을 한 번에 읽어서, 그 사이 분류기가 바뀐 행이 섞이면 부정률
#    변화의 원인이 고객인지 라벨러인지 구분되지 않는다 — Fisher 검정은 둘을 못 가르므로
#    **분류기 개선이 그대로 고객 이상 알림으로 발화한다.** 그래서 `app/batch/daily.py` 는
#    활성 버전 행만 읽는다(`active_version_predicate`).
#    근거·적용 절차: `docs/classified_item_version_columns.md`
CLASSIFIED_ITEM_DDL = """
CREATE TABLE IF NOT EXISTS classified_item (
    item_id          TEXT PRIMARY KEY,
    source           TEXT NOT NULL,
    classified_at    TEXT,
    prompt_version   TEXT,
    model_version    TEXT,
    pipeline_version TEXT
);
"""

VERSION_COLUMNS = ("prompt_version", "model_version", "pipeline_version")
"""분류 산출물의 버전 3종. 적재·조회가 같은 목록을 봐야 한다."""


def active_version_predicate(alias: str = "ci") -> str:
    """"이 행이 활성 분류기로 만들어졌는가" SQL 술어. 파라미터는 `version_params()` 순서.

    적재(`scripts/`)와 조회(`app/`)가 **같은 술어를 써야 한다.** 각자 적으면 한쪽만
    고쳐졌을 때 조회가 0건이 되고, 그건 알림이 안 나가는 방향(미탐)이라 조용하다.

    ⚠️ **`source` 마다 프롬프트가 다르다**(CS=프롬프트1 / 리뷰=프롬프트2). 값 하나로 거를 수
       없어 CASE 로 가른다.

    ⚠️ **`=` 가 아니라 null-safe 비교다.** `=` 를 쓰면 버전을 안 남기던 시절에 적재된
       `NULL` 행이 **어느 쪽으로도 안 걸려** 조용히 빠진다. 가장 오래된, 그래서 가장
       확실히 옛것인 행들이 하필 안 잡히는 형태다. null-safe 는 NULL 을 포함해 항상
       0/1 을 돌려주므로 `NOT (...)` 로 뒤집어도 정확하다(워커의 stale 조회가 그렇게 쓴다).

    🔴 **철자가 `IS` 가 아니라 `IS NOT DISTINCT FROM` 인 이유 — 두 방언의 교집합이다.**
       sqlite 는 `IS` 를 null-safe 비교로 쓰지만 **Postgres 는 안 그렇다**(거기서 `IS` 는
       `IS NULL`·`IS TRUE` 계열 전용이라 `x IS ?` 는 구문 오류다). 반대로 sqlite 도 3.39+
       부터 `IS NOT DISTINCT FROM` 을 `IS` 의 별칭으로 받는다(호스트 3.49 · 런타임 이미지
       3.46 실측). 그래서 이 철자 하나로 양쪽이 같은 뜻이 되고, **방언 분기도 파라미터
       개수 변화도 없다** — `version_params()` 의 순서 계약이 그대로 유지된다.
       `(a = b OR (a IS NULL AND b IS NULL))` 로 풀어쓰는 방식은 `?` 가 두 배로 늘어
       그 계약을 깨므로 쓰지 않는다.
    """
    return (
        f"{alias}.prompt_version IS NOT DISTINCT FROM"
        f" (CASE {alias}.source WHEN 'cs' THEN ? ELSE ? END)"
        f" AND {alias}.model_version IS NOT DISTINCT FROM ?"
        f" AND {alias}.pipeline_version IS NOT DISTINCT FROM ?"
    )


def version_params(
    prompt_cs: str, prompt_review: str, model: str, pipeline: str
) -> tuple[str, str, str, str]:
    """`active_version_predicate()` 의 `?` 에 넣을 값. **순서가 계약이다.**

    술어와 값을 한 파일에 두는 이유: 자리 수·순서가 어긋나면 SQL 은 에러 없이 **다른 것을
    비교**한다(전부 TEXT 라 타입으로도 안 걸린다). 그 결과는 조회 0건이고, 조용하다.
    """
    return prompt_cs, prompt_review, model, pipeline

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
    # 채널 상품 단위 조회(§2-4·§2-5 Indexes). `product_group_id` 는 비정규화라 상품매핑
    # 전 구간에서는 비어 있고, 그때 상품을 가리키는 축은 이쪽뿐이다.
    #
    # ⚠️ 이 둘은 **이 PR 이 만든 누락이 아니라 원래 빠져 있던 것**이다. §3 인덱스 요약표가
    #    이상탐지 조회 패턴만 추려서(그쪽은 이 축을 안 쓴다) 거기엔 없지만, DDL 전문과
    #    §2-4·§2-5 컬럼표에는 있다. 같은 파일·같은 계약이라 함께 채운다.
    "CREATE INDEX IF NOT EXISTS idx_cs_channel_product ON cs (channel_product_id);",
    "CREATE INDEX IF NOT EXISTS idx_reviews_channel_product ON reviews (channel_product_id);",
]

CLASSIFIED_INDEXES = [
    # cs / reviews 와의 조인 키
    "CREATE INDEX IF NOT EXISTS idx_classified_item_aspect_item ON classified_item_aspect (item_id);",
    # 탐지의 활성 버전 필터와 워커의 stale 스캔이 이 세 컬럼을 **전부 등호로** 거른다
    # (`active_version_predicate`). 컬럼 순서는 술어의 비교 순서와 맞춰 둔다.
    (
        "CREATE INDEX IF NOT EXISTS idx_classified_item_versions"
        " ON classified_item (prompt_version, model_version, pipeline_version);"
    ),
]

# 원문 테이블 — 이게 없으면 워커가 읽을 것이 없다.
SOURCE_TABLES = ("cs", "reviews")


def create_source_tables(conn) -> None:
    """main server 소유 테이블 + 통합 뷰. 목 파이프라인에서는 프로듀서가 부른다.

    ⚠️ `products` 는 `channel` 다음, `mapped_data` 는 `products` 다음이어야 한다 —
       FK 가 그 방향으로 걸려 있다. 다만 **깨지는 곳은 CREATE 가 아니라 적재다.**
       sqlite 는 부모 테이블이 없어도 `CREATE TABLE ... REFERENCES` 를 그냥 통과시키고
       (실측: 예외 없음), 그 다음 INSERT 에서 `no such table: main.products` 로 터진다.
       그래서 순서가 뒤집혀도 스키마 생성 단계는 조용히 지나가고 적재에서 처음 드러난다.
    """
    for ddl in (CHANNEL_DDL, PRODUCTS_DDL, MAPPED_DATA_DDL, CS_DDL, REVIEWS_DDL, ORDERS_DDL):
        conn.execute(ddl)
    for stmt in (*SOURCE_INDEXES, PRODUCTS_CHANNEL_PRODUCT_INDEX, MAPPED_DATA_GROUP_INDEX):
        conn.execute(stmt)
    conn.execute(VOC_DOCUMENT_VIEW)


def create_classified_tables(conn) -> None:
    """AI 노드 소유 테이블. 분류 워커가 부른다.

    뷰도 함께 보장한다 — 워커가 자기가 만들지 않은 DB 에 붙는 경우가 있고
    (프로듀서 없이 덤프만 받은 상태), 뷰가 없으면 조회가 통째로 실패한다.

    ⚠️ `IF NOT EXISTS` 라 **이미 있는 테이블은 손대지 않는다.** 8/7 확정 이전 구조로
       남아 있는 DB 에 대고 부르면 조용히 통과하므로, 호출 전에 `find_legacy_tables()`
       로 먼저 걸러야 한다.
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


# 구버전 raw DB 판별용 — 테이블마다 "8/7 확정 이후에만 있는" 컬럼 하나.
#
# 확정 전 구조로 만들어진 DB 가 팀원 로컬에 남아 있다(`data/` 는 gitignore 라 각자 다르다).
# 거기에 대고 create_classified_tables() 를 불러도 `IF NOT EXISTS` 가 옛 테이블을 그대로
# 두기 때문에, 실패가 스키마 생성이 아니라 **한참 뒤 조회 단계**에서 `no such column` 으로
# 터진다 — 원인이 메시지에 안 드러난다. 그 전에 잡으려고 둔다.
#
# 🔴 **마커는 그 테이블에 가장 나중에 들어온 컬럼이어야 한다.** 판정이 "마커가 없으면
#    옛것"이라, 컬럼을 추가하고 마커를 안 옮기면 **그 사이 버전의 테이블이 전부 최신으로
#    통과한다.** 실제로 버전 컬럼 2개를 넣으면서(2026-08-12) 마커가 `prompt_version` 에
#    남아 있었고, 4컬럼 시절 테이블이 구버전으로 안 잡혔다:
#
#        find_legacy_tables()      = []                     ← 통과
#        create_classified_tables() → OperationalError: no such column: model_version
#
#    `IF NOT EXISTS` 가 옛 테이블을 그대로 두는데 인덱스는 새 컬럼을 참조해서, 가드가
#    막으려던 바로 그 자리(원인이 안 드러나는 `no such column`)로 되돌아갔다.
#    **컬럼을 추가하면 이 표도 같이 옮길 것.**
LEGACY_MARKERS: dict[str, str] = {
    # 구: raw_text·channel·aspect 를 들고 있던 단일 테이블 → 8/7 확정으로 분리
    # 이후: prompt_version 만 있던 4컬럼 → 버전 3종(2026-08-12). 마커는 마지막 것.
    "classified_item": "pipeline_version",
    "classification_failure": "item_id",      # 구: event_id PK (§2-7 이전)
    "classification_cursor": "last_inquired_at",  # 구: last_occurred_at / last_event_id (§2-8 이전)
}


def find_legacy_tables(conn) -> list[str]:
    """확정 스키마 이전 구조로 남아 있는 AI 소유 테이블 이름. 없으면 빈 리스트.

    없는 테이블은 대상이 아니다 — 컬럼 조회가 빈 결과를 주고, 그건 새로 만들면 되는
    정상 상태다. "있는데 컬럼이 옛것"인 경우만 골라낸다.

    ⚠️ 컬럼 조회를 `raw_db.table_columns()` 에 맡긴다 — sqlite 는 `PRAGMA table_info`,
       Postgres 는 `information_schema.columns` 로 방언이 완전히 갈리는 자리다. 여기서
       `PRAGMA` 를 직접 쓰면 **Postgres 에서 이 가드 자신이 구문 오류로 죽는다**(스키마가
       멀쩡한지 보려던 코드가 먼저 터지는 모양이라 원인이 메시지에 안 드러난다).
    """
    stale = []
    for table, marker in LEGACY_MARKERS.items():
        columns = raw_db.table_columns(conn, table)
        if columns and marker not in columns:
            stale.append(table)
    return stale
