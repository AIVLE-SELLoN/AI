-- AI 소유 읽기 모델·제약 — **우리 코드가 만드는 것과 같은 모양이어야 한다.**
--
-- 🔴 **이 파일의 역할이 2026-08-18 에 바뀌었다.** 예전에는 *"정본 DDL 에 없는데 우리가
--    요구하는 것 = 인프라 요청 목록"* 이었는데, 인프라(민준) 확인으로 **AI 소유 4개 테이블과
--    이 읽기 모델은 우리가 만드는 것**으로 확정됐다 — 요청 3건(뷰·UNIQUE·버전 인덱스)은
--    통째로 철회됐다. 정본은 이제 `app/core/raw_schema.create_classified_tables()` 다.
--
-- ⚠️ 그럼 왜 이 파일이 남아 있나: **게이트 테스트가 빈 컨테이너에서도 바로 돌게** 하려는
--    것이다(`tests/test_raw_db_postgres.py` 는 조회만 해서 스키마를 안 만든다).
--    `tests/test_raw_db_postgres_write.py` 는 반대로 여기 만든 것을 **지우고 우리 DDL 로
--    다시 세운다** — 안 그러면 `CREATE TABLE IF NOT EXISTS` 가 조용히 건너뛰어 우리
--    Postgres DDL 이 한 번도 안 돈다.
--
-- 🔴 **여기와 `raw_schema.py` 가 갈리면 여기가 틀린 것이다.** 고칠 일이 생기면 코드를 먼저
--    보고, 그 다음 이 파일을 맞출 것.
--
-- ⚠️ 아래 UNIQUE 는 **맨 인덱스**다. 그래서 `information_schema.table_constraints` 에는
--    안 잡힌다(실측) — `raw_db.unique_column_sets()` 가 `pg_index` 를 보는 이유이고,
--    우리 인라인 `UNIQUE (item_id, aspect)` 와 컬럼 조합이 같으므로 가드는 둘을 같게 본다.

-- ── ① cs ∪ reviews 통합 뷰 ─────────────────────────────────────────────────
--
-- 두 테이블은 시각 컬럼명이 다르고(cs.inquired_at / reviews.created_at) 리뷰에만
-- rating 이 있다. 호출부마다 UNION 을 다시 쓰면 시각 컬럼을 잘못 고르는 실수가 각자
-- 생기므로 `occurred_at` 하나로 맞춰 둔다. item_id 는 원문 PK 그대로다(§5-1 A안).
--
-- ⚠️ 이 뷰가 **분모의 정본**이다. 분모와 분자를 한 쿼리로 묶지 말 것 — GROUP BY 에
--    aspect 가 들어간 채로 분모까지 세면 분류 안 된 문의가 빠진다(§2-6 경고).
--
-- sqlite 판정의(`app/core/raw_schema.py: VOC_DOCUMENT_VIEW`) 컬럼·순서와 같아야 한다.
CREATE OR REPLACE VIEW voc_document AS
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

-- ── ② 재분류 멱등성 ────────────────────────────────────────────────────────
--
-- 워커는 2026-08-12 부터 upsert 로 재분류한다("aspect 는 지우고 다시 넣는다"). 그 설계가
-- 이 제약을 전제로 짜여 있어서, 없으면 재분류가 돌 때마다 같은 (item_id, aspect) 가
-- 중복으로 쌓인다 → 탐지 분자가 부풀고, 그건 **오탐 방향이라 시끄럽지도 않다**
-- (부정률이 올라가 알림이 더 나간다).
CREATE UNIQUE INDEX IF NOT EXISTS ux_classified_item_aspect
    ON classified_item_aspect (item_id, aspect);

-- ── ③ 활성 분류기 버전 필터용 인덱스 ───────────────────────────────────────
--
-- 탐지(`daily._ACTIVE_VERSION_PREDICATE`)와 워커의 stale 스캔이 이 세 컬럼을 전부
-- 등호로 거른다. 컬럼 순서는 술어의 비교 순서와 맞춰 둔다.
CREATE INDEX IF NOT EXISTS idx_classified_item_versions
    ON classified_item (prompt_version, model_version, pipeline_version);
