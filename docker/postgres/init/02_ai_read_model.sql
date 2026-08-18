-- 🔴 **정본 DDL 에 없는데 우리 코드가 요구하는 것 — 곧 인프라 요청 목록이다.**
--
-- 노션 「RAW DB 스키마」(DDL 전문)에는 아래 셋이 없다. 로컬에서는 이 파일이 세우지만
-- **운영 스키마는 인프라가 정본 DDL 로 세우므로, 요청하지 않으면 운영에만 없다.**
-- 셋 다 "없어도 CREATE 는 통과하고 나중에 조용히/뒤늦게 아픈" 종류라 여기 모아 둔다.
--
--   ① voc_document 뷰           없으면 탐지 배치·CS 원문 조회·월간 집계·분류 워커가
--                               `relation "voc_document" does not exist` 로 **전부 죽는다**
--   ② UNIQUE (item_id, aspect)  없으면 재분류가 같은 쌍을 중복 적재해 **탐지 분자가 부푼다**
--   ③ 버전 3종 인덱스           없으면 35일 조회가 매번 seq scan 이다(정확도 아닌 성능)
--
-- ⚠️ ①은 "인프라가 뷰를 만든다" 말고 **"우리 SQL 이 UNION 을 직접 쓴다"** 로도 닫을 수
--    있다(그러면 남의 스키마에 우리 읽기 모델을 얹지 않아도 된다). 어느 쪽이든 팀 결정이
--    필요해서 1단계에서는 정하지 않았다 — 지금은 뷰를 전제로 둔 현재 코드를 그대로 세운다.

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
