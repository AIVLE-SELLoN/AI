"""분류 워커 — raw DB 의 원문을 읽어 분류하고 classified_item 에 적재한다.

Kafka 구독 폐기:
  기존   : Kafka consumer 로 raw.* 토픽 구독 → 분류 → 로그 출력 (docker 컨테이너로 상주)
  변경후 : **raw DB 조회** → 분류 → **classified_item 테이블 적재(타임라인 순)**

  Kafka 구독을 걷어냈으므로 이 워커는 더 이상 docker compose 에 올라가지 않는다.
  브로커/컨슈머그룹/오프셋 커밋 대신, 원문을 (occurred_at, item_id) 순으로 훑는
  커서(classification_cursor)로 진행 상황을 관리한다.

「Raw DB 스키마 확정 (8/7)」 반영:
  기존   : 단일 `raw_event` 테이블을 source 컬럼으로 걸러 읽음
  변경후 : 확정 문서의 `cs` · `reviews` — 두 테이블을 합친 `voc_document` 뷰를 읽음

  `item_id` 는 `cs.id` / `reviews.id` 를 그대로 재사용한다. 접두사가
  INQ-/RVW- 로 갈려 두 테이블을 합쳐도 충돌하지 않는다. 스키마 정의는 `app/core/raw_schema.py`.

분류 실패 건은 버리지 않고 dead-letter(classification_failure)에 남긴다.
탐지의 분모를 원본 테이블에서 세고 classified_item 을 LEFT JOIN 하는 구조에서는
**분류 커버리지가 곧 분자의 정확도**다. 실패 건이 조용히 사라지면 분모는 그대로인데 분자만 비어 부정률이 과소추정된다.

실행:
  python scripts/classification_worker.py                 # 밀린 원문 전부 처리하고 종료
  python scripts/classification_worker.py --limit 50      # 시험 실행(과금 상한)
  python scripts/classification_worker.py --follow        # 프로듀서 재생을 준실시간 추종
  python scripts/classification_worker.py --retry-failed  # dead-letter 재처리(회수)
  python scripts/classification_worker.py --reclassify-stale --limit 500
                                                          # 프롬프트 교체 후 backfill
  python scripts/classification_worker.py --dry-run       # DB 없이 샘플 2건으로 추론만 확인

분류기 버전과 탐지
  적재 시 `classified_item` 에 **버전 3종**(prompt·model·pipeline)을 남기고, 탐지
  (`app/batch/daily.py`)는 **활성 버전 행만** 읽는다. 탐지가 35일(현재 7 + 과거 28)을 한 번에
  보기 때문에, 그 사이 분류기를 바꾸면 한 검정 안에 두 라벨러의 결과가 섞이고
  **분류기 개선이 고객 이상처럼 발화한다.**

  축이 셋인 이유: 프롬프트가 그대로여도 라벨러는 바뀐다. 모델(`LLM_MODEL`)을 갈아끼우거나
  후처리·폴백을 손보면 프롬프트 파일은 한 글자도 안 바뀌었는데 분포가 달라진다.

  **어느 축이든 올렸으면 `--reclassify-stale` 을 끝까지 돌려야 배치가 돈다.** 탐지는
  윈도우에 옛 버전 행이 하나라도 있으면 **중단**한다(fail-closed). 부분 backfill 상태로는
  탐지가 아예 안 도므로 "조금씩 나눠 돌리다 중간에 두는" 상태를 남기지 말 것 —
  `--limit` 으로 쪼개 돌리는 것은 되지만 마지막까지 채워야 한다.
  적재가 upsert 라 재분류 결과가 옛 결과를 덮어쓴다 — 예전 `INSERT OR IGNORE` 시절에는
  재분류를 돌려도 아무것도 안 바뀌었다.

백엔드 2종 (sqlite · Postgres)
  `RAW_DB_HOST` 가 있으면 **Postgres**, 없으면 sqlite 파일이다. 연결은
  `app.core.raw_db.connect_readwrite()` 한 곳을 경유한다 — 여기서 `sqlite3.connect` 를
  직접 부르면 `mode`·PRAGMA·오류 타입이 두 벌이 되고, 운영에서만 갈리는 조건이 생긴다.

  **이 워커가 raw DB 에 쓰는 유일한 프로세스다.** 인프라가 RW 를 전면 부여해 DB 가 더는
  `cs`·`reviews` 를 막아 주지 않으므로, 쓰기 대상이 AI 소유 4개 밖으로 나가지 않는 것은
  `tests/test_raw_db_write_scope.py` 가 지킨다.

  SQL 은 한 벌만 쓴다. `?` 바인딩은 `raw_db` 가 `%s` 로 옮기고, `INSERT OR IGNORE` 같은
  sqlite 전용 문법 대신 양쪽이 같은 뜻으로 받는 `ON CONFLICT` 를 쓴다(`raw_db.upsert_sql`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
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
from app.config import get_settings
from app.core import constants, raw_db, raw_schema
from app.core.console import force_utf8_output
from app.core.constants import KST
from app.core.exceptions import LlmParseError
from app.core.schemas import Aspect, AspectSentiment, ClassifiedItem, Sentiment, Source
from app.core.versions import CLASSIFIER_PIPELINE_VERSION

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

# 분류 대상 source. `voc_document` 뷰는 cs·reviews 만 합치므로 주문은 애초에
# 들어오지 않지만, 뷰 정의가 늘어나도 여기서 한 번 더 걸러 낸다.
CLASSIFY_SOURCES = (Source.CS.value, Source.REVIEW.value)

# 원문 통합 뷰(cs ∪ reviews). 정의는 raw_schema.VOC_DOCUMENT_VIEW.
# 두 테이블의 시각 컬럼명이 달라(inquired_at / created_at) 뷰가 occurred_at 으로 맞춰 준다.
SOURCE_VIEW = raw_schema.VOC_DOCUMENT

# 테이블 DDL 은 여기 두지 않는다 — `app/core/raw_schema.py` 가 정본이다. 프로듀서와
# 정의가 갈리면 한쪽만 고쳐지는 사고가 난다. 여기에는 이 워커만 쓰는 **쿼리**만 둔다.

FAILURE_UPSERT = """
INSERT INTO classification_failure
    (item_id, occurred_at, stage, error, attempts, first_failed_at, last_failed_at)
VALUES (?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(item_id) DO UPDATE SET
    stage          = excluded.stage,
    error          = excluded.error,
    attempts       = classification_failure.attempts + 1,
    last_failed_at = excluded.last_failed_at
"""

FAILURE_DELETE = "DELETE FROM classification_failure WHERE item_id = ?"

# 재처리 대상 조회. 시도 횟수가 상한 미만인 것만 — 결정적 실패(예: 리뷰에 허용되지 않는
# aspect)를 매번 다시 LLM 에 태우면 돈만 쓰고 결과는 같다.
#
# (occurred_at, item_id) 페이지 커서가 꼭 필요하다. 이게 없으면 재처리에 또 실패한 건이
# 다음 조회에 **다시 잡혀서**, 한 번 실행하는 동안 같은 건을 상한까지 반복 호출한다
# (1회 실행 = 건당 max_attempts 회 과금). 한 실행에서는 건당 1회만 시도한다.
FETCH_FAILED_SQL = f"""
SELECT r.item_id, r.source, r.channel_id, r.channel_product_id, r.product_group_id,
       r.content, r.occurred_at
FROM classification_failure f
JOIN {SOURCE_VIEW} r ON r.item_id = f.item_id
WHERE f.attempts < ?
  AND (f.occurred_at > ? OR (f.occurred_at = ? AND f.item_id > ?))
ORDER BY f.occurred_at, f.item_id
LIMIT ?
"""

# **upsert 여야 한다 — `INSERT OR IGNORE` 로 되돌리지 말 것.** 그 형태는 이미 있는
# item_id 를 통째로 무시해서, 재분류를 돌려도 `prompt_version` 도 결과도 옛 값 그대로
# 남는다. 그러면 지난 문서는 영원히 옛 라벨러 기준이고, 탐지가 35일(현재 7 + 과거 28)을
# 한 번에 읽으므로 **한 검정 안에 두 프롬프트 결과가 섞인다.** 부정률이 움직인 원인이
# 고객인지 라벨러 교체인지 Fisher 검정은 못 가르므로, 프롬프트 개선이 그대로 고객 이상
# 알림으로 발화한다. `--reclassify-stale` 이 이 upsert 위에서 돈다.
CLASSIFIED_ITEM_UPSERT = """
INSERT INTO classified_item
    (item_id, source, classified_at, prompt_version, model_version, pipeline_version)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(item_id) DO UPDATE SET
    source           = excluded.source,
    classified_at    = excluded.classified_at,
    prompt_version   = excluded.prompt_version,
    model_version    = excluded.model_version,
    pipeline_version = excluded.pipeline_version
"""

# 재분류 전에 **옛 aspect 를 지운다.** `classified_item_aspect` 는 UNIQUE(item_id, aspect)
# 라 갱신만으로는 **새 프롬프트가 더 이상 안 내는 aspect 가 옛 행으로 남는다**(예: v4 가
# 붙이던 `기타` 를 v5 가 안 붙여도 그 행은 그대로). 부모는 새 버전인데 자식은 두 버전이
# 섞인 상태가 되고, 분자를 세는 쪽은 그걸 구분할 방법이 없다. 지우고 다시 넣어야 "이
# 문서의 aspect 집합 = 이 프롬프트의 출력"이 지켜진다.
CLASSIFIED_ITEM_ASPECT_DELETE = "DELETE FROM classified_item_aspect WHERE item_id = ?"

# 위에서 먼저 지우므로 남는 충돌은 **한 응답 안의 중복 aspect** 뿐이다(LLM 이 같은 속성을
# 두 번 낸 경우). 그건 무시하는 게 맞아서 "이미 있으면 그대로 둔다" 를 유지한다.
#
# `INSERT OR IGNORE` 가 아니라 표준 `ON CONFLICT DO NOTHING` 인 이유: 전자는 sqlite 전용이라
# Postgres 에서 구문 오류다. 뜻은 같고 sqlite 3.24+ 가 이 철자를 받는다.
#
# **이 문장은 `UNIQUE (item_id, aspect)` 가 있어야 뜻이 선다.** 제약이 없으면 충돌 자체가
# 안 나서 같은 쌍이 그냥 두 번 들어가고, 탐지 분자가 부푼다. 그래서 그 제약이 빠진 테이블을
# `raw_schema.find_legacy_tables()` 가 구버전으로 잡는다.
CLASSIFIED_ITEM_ASPECT_INSERT = raw_db.upsert_sql(
    "classified_item_aspect",
    ("item_id", "aspect", "sentiment", "mixed_signal"),
    conflict=("item_id", "aspect"),
    update=(),
)

# (occurred_at, item_id) 복합 커서보다 큰 행만 타임라인 순으로 가져온다.
# 튜플 비교 대신 풀어 쓴 이유는 구버전 sqlite 호환(row value 는 3.15+).
FETCH_BATCH_SQL = f"""
SELECT item_id, source, channel_id, channel_product_id, product_group_id, content, occurred_at
FROM {SOURCE_VIEW}
WHERE source IN ({', '.join(['?'] * len(CLASSIFY_SOURCES))})
  AND content IS NOT NULL AND TRIM(content) <> ''
  AND (occurred_at > ? OR (occurred_at = ? AND item_id > ?))
ORDER BY occurred_at, item_id
LIMIT ?
"""

# 분류 커버리지 확인용 분모. 확정 문서가 "분류 안 된 문의도 반드시 남는다"고 못박은 대로
# 원문 테이블에서 센다 — classified_item 에서 세면 세는 대상과 확인하려는 대상이 같아진다.
COUNT_SOURCE_SQL = f"""
SELECT COUNT(*) FROM {SOURCE_VIEW}
WHERE source IN ({', '.join(['?'] * len(CLASSIFY_SOURCES))})
  AND content IS NOT NULL AND TRIM(content) <> ''
"""

# ── 활성 분류기 버전 ─────────────────────────────────────────────────────────
#
# 술어와 파라미터 순서는 `raw_schema` 가 정본이다(적재는 여기, 조회는 `app/batch/daily.py`
# — 둘이 각자 적으면 한쪽만 고쳐졌을 때 조회가 0건이 되고 그건 미탐이라 조용하다).
_STALE_PREDICATE = f"NOT ({raw_schema.active_version_predicate('c')})"


def active_version_params() -> tuple[str, str, str, str]:
    """활성 분류기 신원 — `(프롬프트CS, 프롬프트리뷰, 모델, 파이프라인)`.

    `service_module` 과 설정을 **매번 다시 읽는다**(값을 상수로 굳히지 않는다). 적재 쪽
    `save_classified_items()` 도 같은 출처를 보므로, 테스트가 버전을 monkeypatch 했을 때
    **적재와 조회가 같은 값을 본다.** 한쪽만 굳으면 stale 조회가 0건이 되고, 그건
    "재분류할 게 없다"로 조용히 통과한다.
    """
    return raw_schema.version_params(
        service_module.PROMPT_ASPECT_VERSION,
        service_module.PROMPT_SENTIMENT_VERSION,
        get_settings().llm_model,
        CLASSIFIER_PIPELINE_VERSION,
    )


# 재분류(backfill) 대상 조회 — 이미 분류됐지만 **지금 분류기로 만든 게 아닌** 문서.
#
# 신규 조회(FETCH_BATCH_SQL)로는 절대 안 잡힌다. 그쪽은 커서보다 뒤에 있는 원문만 보는데
# 이 행들은 커서가 이미 지나간 자리에 있다 — 그래서 별도 조회가 필요하다.
#
# 페이지 커서가 필요한 이유는 `--retry-failed` 와 같다 — 재분류에 **실패**한 건은 여전히
# stale 이라 다음 조회에 다시 잡히고, 한 실행 안에서 같은 건을 무한히 다시 LLM 에 태운다.
FETCH_STALE_SQL = f"""
SELECT r.item_id, r.source, r.channel_id, r.channel_product_id, r.product_group_id,
       r.content, r.occurred_at
FROM classified_item c
JOIN {SOURCE_VIEW} r ON r.item_id = c.item_id
WHERE {_STALE_PREDICATE}
  AND r.content IS NOT NULL AND TRIM(r.content) <> ''
  AND (r.occurred_at > ? OR (r.occurred_at = ? AND r.item_id > ?))
ORDER BY r.occurred_at, r.item_id
LIMIT ?
"""

# **재분류 조회와 같은 조인·같은 조건을 탄다.** 범위가 갈리면(예: 여기서만 원문 뷰 조인을
# 빼면) 원문이 사라진 행에서 `count_stale()=1` 인데 `fetch_stale_batch()=0` 이 되어, "1건
# 남았다"고 알리고 곧바로 "대상을 모두 처리했습니다"로 끝난 뒤 종료 경고가 **영원히 남는다**
# — 고치라는데 고칠 수단이 없는 경고다.
COUNT_STALE_SQL = f"""
SELECT COUNT(*)
FROM classified_item c
JOIN {SOURCE_VIEW} r ON r.item_id = c.item_id
WHERE {_STALE_PREDICATE}
  AND r.content IS NOT NULL AND TRIM(r.content) <> ''
"""

# 재분류할 수 없는 stale 행 — 원문이 사라졌거나 본문이 비어 있어 LLM 에 태울 것이 없다.
# 위 조인에서 빠지는 나머지다.
#
# 이 건수는 **탐지를 막지 않는다.** `app/batch/daily.py` 의 cutover 가드도 원문 뷰와
# 조인하므로 원문 없는 행은 애초에 안 센다. 그래서 경고 문구를 가르기만 한다 —
# "backfill 하세요"와 "backfill 로는 못 없앤다"는 사람이 할 일이 다르다.
COUNT_ORPHAN_STALE_SQL = f"""
SELECT COUNT(*)
FROM classified_item c
WHERE {_STALE_PREDICATE}
  AND NOT EXISTS (
      SELECT 1 FROM {SOURCE_VIEW} r
      WHERE r.item_id = c.item_id
        AND r.content IS NOT NULL AND TRIM(r.content) <> ''
  )
"""

# 적재된 분류 결과의 버전 분포. 섞여 있으면 탐지가 그만큼 조용히 틀어진다.
COUNT_BY_VERSION_SQL = """
SELECT source,
       COALESCE(prompt_version, '(미기록)') AS prompt,
       COALESCE(model_version, '(미기록)') AS model,
       COALESCE(pipeline_version, '(미기록)') AS pipeline,
       COUNT(*) AS n
FROM classified_item
GROUP BY source, prompt, model, pipeline
ORDER BY source, n DESC
"""


def cursor_origin(conn: raw_db.RawDbConnection) -> Any:
    """"아직 아무것도 안 읽었다" 를 뜻하는 커서 시작값. 모든 실제 시각보다 작아야 한다.

    **빈 문자열은 Postgres 에서 못 쓴다.** 커서 조건절이 `occurred_at > ?` 인데 그 컬럼이
    TIMESTAMPTZ 라, `''` 를 넘기면 비교가 실패하는 게 아니라 **`invalid input syntax for type
    timestamp with time zone` 으로 조회 자체가 죽는다.** sqlite 는 컬럼이 TEXT 라 `''` 가 모든
    ISO 문자열보다 작아서 통한다.

    이 값은 dead-letter 의 `occurred_at` 기본값이기도 하다 — `NULL` 로 두면 `f.occurred_at > ?`
    가 그 행을 **영원히 안 집어서**(NULL 비교는 항상 false) `--retry-failed` 회수가 조용히
    망가진다.
    """
    if raw_db.dialect_of(conn) == raw_db.POSTGRES:
        # TIMESTAMPTZ 하한(4713 BC)보다 훨씬 늦지만 실제 데이터보다는 확실히 이르다.
        return datetime(1, 1, 1, tzinfo=timezone.utc)
    return ""


def open_db(db_path_str: str) -> raw_db.RawDbConnection:
    """raw DB 연결 + AI 소유 테이블 보장. sqlite·Postgres 양쪽.

    원문 테이블(cs·reviews)은 main server 소유라 여기서 만들지 않는다. 목 파이프라인에서는
    mock_producer 가 그 역할이다. 없으면 아직 원문이 한 건도 적재되지
    않은 상태이므로, 잘못된 경로를 조용히 새 빈 파일로 만들어 버리지 않도록 여기서 멈춘다.

    **sqlite 파일 존재 확인은 `RAW_DB_HOST` 가 비었을 때만 뜻이 있다.** Postgres 경로에서
    `db_path` 는 안 쓰이는 값이라, 거기서 `Path.exists()` 를 보면 멀쩡한 접속을 "raw DB 없음"
    으로 막는다. 그래서 접속 문자열 유무로 먼저 가른다.

    원문 테이블 조회는 `raw_db.existing_tables()` 에 맡긴다 — `sqlite_master` 는 sqlite 에만
    있어서 Postgres 에서는 이 확인 자체가 구문 오류로 죽는다(원문이 없다는 안내가 아니라 알
    수 없는 에러가 나가는 모양이다).
    """
    dsn = raw_db.conninfo_from_settings()
    target = raw_db.describe_target(db_path_str, dsn=dsn)
    if not dsn and not Path(db_path_str).resolve().exists():
        logger.error(f"[DB ERROR] raw DB 가 없습니다: {target} (mock_producer 를 먼저 실행하세요)")
        sys.exit(1)

    # FK 는 sqlite 에서 연결마다 켜야 한다(`connect_readwrite` 가 켠다). 여기서 지키는 것은
    # classified_item_aspect.item_id → classified_item.item_id 다 — 부모 없는 aspect 행이
    # 생기면 "분류 결과는 있는데 문서가 없는" 상태가 되어 커버리지 집계가 어긋난다.
    #
    # **접속 실패를 여기서 잡는다.** 이 워커는 k8s CronJob 이라, 안 잡으면 raw traceback 으로
    # 죽고 스케줄러가 **같은 설정으로 무한 재시도**한다 — 사람이 로그를 열기 전에는 무엇이
    # 잘못됐는지 아무 데도 안 적힌다. `psycopg.Error` 는 `FileNotFoundError` 의 하위가 아니라
    # 위 파일 확인으로는 절대 안 걸린다.
    try:
        conn = raw_db.connect_readwrite(db_path_str, dsn=dsn)
    except raw_db.connection_error_types() as exc:
        logger.error(f"[DB ERROR] raw DB 에 접속하지 못했습니다: {target} - {exc}")
        sys.exit(1)

    missing = sorted(set(raw_schema.SOURCE_TABLES) - raw_db.existing_tables(conn, raw_schema.SOURCE_TABLES))
    if missing:
        logger.error(
            f"[DB ERROR] 원문 테이블이 없습니다: {', '.join(missing)} — {target} "
            "(mock_producer 를 먼저 실행하세요)"
        )
        conn.close()
        sys.exit(1)

    legacy = raw_schema.find_legacy_tables(conn)
    if legacy:
        # **자식 테이블을 항상 함께 지운다.** 부모만 지우면 **부모 없는 aspect 행이 남고**,
        # 탐지는 부모를 거쳐 읽어서 무해하지만 월간 집계는 `FROM voc_document r JOIN
        # classified_item_aspect a` 로 부모를 안 거친다 — 원문이 그대로 있으니 옛 분류기
        # 라벨이 계속 리포트에 잡힌다.
        #
        # **자식을 먼저 지운다.** `PRAGMA foreign_keys=ON` 인 세션에서 부모부터 지우면
        # `FOREIGN KEY constraint failed` 로 막힌다(실측). sqlite3 CLI 는 기본이 OFF 라 보통은 그냥
        # 돌지만, 켜 둔 셸에 붙여넣으면 안내가 통째로 실패한다 — 순서만 바꾸면 양쪽 다 된다.
        #
        # `classified_item_aspect` 는 **스스로** 구버전으로 잡힐 수도 있어(`UNIQUE (item_id,
        # aspect)` 누락) `legacy` 안에서 부모 뒤에 올 수 있다. 목록 순서에 기대지 않고 자식을
        # 항상 맨 앞으로 끌어온다.
        child = "classified_item_aspect"
        targets = [t for t in legacy if t != child]
        if child in legacy or "classified_item" in legacy:
            targets.insert(0, child)
        drops = " ".join(f"DROP TABLE IF EXISTS {t};" for t in targets)
        # 어느 CLI 로 붙는지는 백엔드마다 다르다. 안내에 sqlite3 만 적으면 Postgres 쪽
        # 사람은 실행할 수 없는 명령을 받는다.
        command = (
            f'    psql "<접속 문자열>" -c "{drops}"'
            if dsn
            else f'    sqlite3 "{db_path_str}" "{drops}"'
        )
        logger.error(
            # 이 메시지는 cp949 로 나가도 읽혀야 한다. 윈도우 stderr 는 errors=backslashreplace
            # 라 크래시까지는 안 가지만, em dash 처럼 cp949 에 없는 문자는 "\\u2014" 로 뭉개져
            # 정작 복구 안내가 안 읽힌다. 여기서는 cp949 에 있는 문자만 쓴다.
            f"[DB ERROR] 구버전 raw DB 입니다: {target}\n"
            f"  {', '.join(legacy)} 가 확정 스키마와 다른 구조입니다"
            "(컬럼 누락 또는 UNIQUE 제약 누락).\n"
            "  CREATE TABLE IF NOT EXISTS 는 옛 테이블을 그대로 두므로, 이걸 안 잡으면 "
            "'no such column' 으로 터지거나 중복 적재로 탐지 분자가 부풉니다.\n"
            "  원문(cs·reviews)은 그대로 두고 아래처럼 AI 소유 테이블을 지운 뒤 다시 실행하세요 "
            "(분류 결과는 다시 만들어야 합니다. 자식 테이블을 남기면 부모 없는 행이 "
            "월간 집계에 계속 잡힙니다):\n"
            f"{command}"
        )
        conn.close()
        sys.exit(1)

    raw_schema.create_classified_tables(conn)
    conn.commit()

    logger.info(f"[DB] 연결 완료: {target}")
    return conn


def _to_request_item(row: Any) -> ClassifyRequestItem:
    """`voc_document` 1행 → 분류 입력 1건.

    원문은 CSV 를 거의 그대로 담고 있어서 스키마 enum 과 표기가 어긋날 수 있다
    (채널 대소문자 등). 여기서만 맞춰 주고 분류 로직 자체는 건드리지 않는다.

    **분류 결과의 item_id 가 원문 PK 와 같은 값이어야 한다.** dead-letter 기록이
    `occurred_at_by_id[item_id]` 로 발생 시각을 찾는데 그 키를 채우는 것은 분류 결과의
    item_id 다. 둘이 갈라지면 occurred_at 이 "" 로 들어가고 `FETCH_FAILED_SQL` 의 페이지 커서
    정렬(f.occurred_at, f.item_id)이 깨져 `--retry-failed` 회수가 조용히 망가진다. 확정 문서의
    `item_id = cs.id / reviews.id` 가 이 등식의 근거다.
    (tests/test_classification_worker.py::test_dead_letter_records_occurred_at 이 고정)
    """
    return ClassifyRequestItem.model_validate({
        "item_id": row["item_id"],
        "source": str(row["source"]).lower(),
        "channel": str(row["channel_id"] or "").upper(),
        # product_group_id 는 확정 문서대로 원문에 비정규화돼 있어야 한다. 목 대본에는 답
        # 노출 방지 설계상 그 컬럼이 없어 채널 상품 ID 로 대신한다 — 실서비스에서는 적재
        # 시점에 이미 매핑이 끝나 있어 이 폴백이 걸릴 일이 없다.
        "product_group_id": str(
            row["product_group_id"] or row["channel_product_id"] or "PG-UNKNOWN"
        ),
        "raw_text": row["content"],
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
        reclassify_stale: bool = False,
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
        # 재분류(backfill) 모드 — 프롬프트가 바뀌어 옛 버전으로 남은 문서를 다시 태운다.
        self.reclassify_stale = reclassify_stale
        # 재처리·재분류 전용 페이지 커서(메모리에만 있음). 한 실행에서 같은 건을 두 번 부르지
        # 않으려고 둔다 — DB 의 classification_cursor 와는 무관하다. 두 모드가 각자 쓴다
        # (동시에 켤 수 없으므로 값이 섞이지는 않지만, 뜻이 다른 커서라 이름을 나눈다).
        self.retry_page_cursor: tuple[Any, str] | None = None
        self.stale_page_cursor: tuple[Any, str] | None = None
        self.total_reclassified = 0
        self.processed = 0
        self.conn: raw_db.RawDbConnection | None = None
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
        last_occurred_at, last_item_id = self.load_cursor()
        logger.info(
            f"[WORKER STARTED] db={self.db_path}, batch={self.batch_size}, follow={self.follow}, "
            f"retry_failed={self.retry_failed}, reclassify_stale={self.reclassify_stale}, "
            f"cursor=({last_occurred_at}, {last_item_id})"
        )

        try:
            if self.reclassify_stale:
                self.run_reclassify_loop()
            elif self.retry_failed:
                self.run_retry_loop()
            else:
                self.run_loop()
        finally:
            self.log_coverage()
            if self.conn:
                self.conn.close()
            self.loop.close()
            logger.info(
                f"[WORKER STOPPED] 원문 {self.total_items}건 → classified_item {self.total_rows}행 적재"
                f"{f', 실패 {self.total_failed}건' if self.total_failed else ''}"
                f"{f', 재처리 성공 {self.total_recovered}건' if self.total_recovered else ''}"
                f"{f', 재분류 {self.total_reclassified}건' if self.total_reclassified else ''}"
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
            self.retry_page_cursor = (rows[-1]["occurred_at"], rows[-1]["item_id"])
            self.process_batch(rows, advance_cursor=False)

    def run_reclassify_loop(self) -> None:
        """프롬프트 버전 backfill 전용 루프 — 옛 버전으로 남은 문서를 다시 분류한다.

        **왜 필요한가.** 탐지는 35일(현재 7 + 과거 28)을 한 번에 읽는다. 프롬프트를
        바꾸면 그 뒤로 들어오는 문서만 새 버전이라, 한동안 **과거 구간은 옛 프롬프트 ·
        현재 구간은 새 프롬프트** 결과로 검정을 한다. 부정률 변화의 원인이 고객인지
        라벨러 교체인지 구분이 안 되고, Fisher 검정은 그 둘을 못 가른다.
        `app/batch/daily.py` 는 활성 버전 행만 읽어 섞임을 막는데, 그 상태로 두면 이번엔
        과거 기준선이 비어 탐지가 아무것도 못 한다. **그 사이를 메우는 것이 이 모드다.**

        커서는 건드리지 않는다(`advance_cursor=False`) — 신규 원본 진행과 독립이고,
        여기서 미는 건 이미 지나간 구간이다.

        **비용이 든다.** 대상 1건이 곧 LLM 호출 1회다(목 데이터 96,524건 규모). `--limit` 로
        나눠 돌릴 수 있게 열어 뒀고, 중간에 끊겨도 끝난 만큼은 새 버전으로 남아 다음 실행이
        이어받는다 — 재분류된 행은 더 이상 stale 조회에 안 잡힌다.

        `--follow` 는 의미가 없어 무시한다. 대상이 고정 집합이라 다 돌면 끝이다.
        """
        total_stale = self.count_stale()
        if not total_stale:
            logger.info(
                "[DONE] 재분류할 문서가 없습니다 — 적재된 분류 결과가 전부 활성 프롬프트"
                f" 기준입니다(cs={service_module.PROMPT_ASPECT_VERSION},"
                f" review={service_module.PROMPT_SENTIMENT_VERSION})."
            )
            return

        logger.info(
            f"[RECLASSIFY] 옛 프롬프트로 남은 문서 {total_stale}건 — 대상 1건당 LLM 1회입니다."
            f"{f' 이번 실행은 {self.limit}건까지만 처리합니다.' if self.limit else ''}"
        )

        while self.is_running:
            if self.limit is not None and self.processed >= self.limit:
                logger.info(f"[LIMIT REACHED] 상한 {self.limit}건 재분류 완료 — 종료합니다.")
                return

            rows = self.fetch_stale_batch()
            if not rows:
                logger.info("[DONE] 재분류 대상을 모두 처리했습니다.")
                return

            self.processed += len(rows)
            # 재분류에 **실패**한 건은 여전히 stale 이라 다음 조회에 또 잡힌다 —
            # 커서가 없으면 한 실행 안에서 같은 건을 무한히 다시 LLM 에 태운다.
            self.stale_page_cursor = (rows[-1]["occurred_at"], rows[-1]["item_id"])
            before = self.total_items
            self.process_batch(rows, advance_cursor=False)
            self.total_reclassified += self.total_items - before

    # ── 커서 ────────────────────────────────────────────────────────────────

    def load_cursor(self) -> tuple[Any, str]:
        """(마지막으로 처리한 발생 시각, item_id).

        컬럼명이 `last_inquired_at` 인데 리뷰는 시각 컬럼이 `created_at` 이다 —
        확정 문서의 이름을 그대로 따랐고, 값은 뷰의 `occurred_at` 이다.
        """
        row = self.conn.execute(
            "SELECT last_inquired_at, last_item_id FROM classification_cursor WHERE worker_id = ?",
            (WORKER_ID,),
        ).fetchone()
        if row and row["last_inquired_at"] is not None:
            return row["last_inquired_at"], row["last_item_id"] or ""
        return cursor_origin(self.conn), ""  # 모든 실제 시각보다 작다 → 처음부터

    def save_cursor(self, occurred_at: str, item_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO classification_cursor (worker_id, last_inquired_at, last_item_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                last_inquired_at = excluded.last_inquired_at,
                last_item_id     = excluded.last_item_id,
                updated_at       = excluded.updated_at
            """,
            (WORKER_ID, occurred_at, item_id, datetime.now(KST).isoformat()),
        )

    # ── 조회 ────────────────────────────────────────────────────────────────

    def fetch_next_batch(self) -> list[Any]:
        last_occurred_at, last_item_id = self.load_cursor()
        batch_size = self.batch_size
        if self.limit is not None:
            # 상한을 넘겨서 가져오면 그만큼 LLM 을 더 부른다 — 남은 만큼만 조회한다
            batch_size = min(batch_size, self.limit - self.processed)
        params = (
            *CLASSIFY_SOURCES,
            last_occurred_at, last_occurred_at, last_item_id,
            batch_size,
        )
        return self.conn.execute(FETCH_BATCH_SQL, params).fetchall()

    def fetch_failed_batch(self) -> list[Any]:
        batch_size = self.batch_size
        if self.limit is not None:
            batch_size = min(batch_size, self.limit - self.processed)
        occurred_at, item_id = self.retry_page_cursor or (cursor_origin(self.conn), "")
        return self.conn.execute(
            FETCH_FAILED_SQL, (self.max_attempts, occurred_at, occurred_at, item_id, batch_size)
        ).fetchall()

    def fetch_stale_batch(self) -> list[Any]:
        """활성 분류기로 만들어지지 않은 분류 결과의 원문 1배치."""
        batch_size = self.batch_size
        if self.limit is not None:
            batch_size = min(batch_size, self.limit - self.processed)
        occurred_at, item_id = self.stale_page_cursor or (cursor_origin(self.conn), "")
        return self.conn.execute(
            FETCH_STALE_SQL,
            (*active_version_params(), occurred_at, occurred_at, item_id, batch_size),
        ).fetchall()

    def count_stale(self) -> int:
        """**재분류할 수 있는** 옛 분류기 행 수. `fetch_stale_batch()` 와 같은 범위다.

        0 이면 `--reclassify-stale` 이 할 일이 없다는 뜻이다. 원문이 사라져 못 고치는
        행은 여기 안 들어간다 — `count_orphan_stale()` 로 따로 센다.
        """
        return self.conn.execute(COUNT_STALE_SQL, active_version_params()).fetchone()[0]

    def count_orphan_stale(self) -> int:
        """**재분류로는 없앨 수 없는** 옛 분류기 행 수 — 태울 본문이 없는 것들이다.

        원문이 사라졌거나(목 데이터 재생성) 본문이 공백만 남은 경우다. 탐지의 cutover
        가드도 같은 조건으로 이 행들을 빼므로 배치를 세우지 않는다 — 그 불변식이
        `daily._VERSION_COUNT_SQL` 에 적혀 있다.
        """
        return self.conn.execute(
            COUNT_ORPHAN_STALE_SQL, active_version_params()
        ).fetchone()[0]

    # ── 처리 ────────────────────────────────────────────────────────────────

    def process_batch(self, rows: list[Any], *, advance_cursor: bool = True) -> None:
        """배치 1개(원문 N건)를 분류해 적재하고 커서를 전진시킨다.

        rows 는 이미 (occurred_at, item_id) 오름차순이라, 이 순서대로 INSERT 하면
        classified_item 이 타임라인 순으로 쌓인다.

        실패한 건은 **버리지 않고** dead-letter(classification_failure)에 남긴다 — 분모를
        원본 테이블에서 세는 구조에서는 분류 커버리지가 곧 분자의 정확도라, 무엇이 빠졌는지
        셀 수 없으면 부정률이 조용히 과소추정된다.

        advance_cursor=False 는 재처리 모드용 — 이미 지나간 구간을 다시 훑는 것이라
        커서를 건드리면 안 된다.
        """
        logger.info(f"[BATCH] {len(rows)}건 처리 시작 (~{rows[-1]['occurred_at']})")

        occurred_at_by_id = {row["item_id"]: row["occurred_at"] for row in rows}
        failures: list[tuple[str, str, str]] = []  # (item_id, stage, error)

        request_items: list[ClassifyRequestItem] = []
        for row in rows:
            try:
                item = _to_request_item(row)
                request_items.append(item)
            except Exception as exc:
                failures.append((row["item_id"], "parse", str(exc)))
                logger.error(f"[PARSE ERROR] item_id={row['item_id']}: {exc}")

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
            f"{f' (커서: {rows[-1]["occurred_at"]} / {rows[-1]["item_id"]})' if advance_cursor else ''}"
        )

    def classify_items(
        self, items: list[ClassifyRequestItem]
    ) -> tuple[list[ClassifiedItem], list[tuple[str, str, str]]]:
        """원문 N건을 분류한다. 실패는 **그 건에만** 국한된다.

        격리는 classify_aspect() 가 한다 (계약):
          - 반환 길이·순서가 요청과 같아 zip(items, 반환) 이 성립한다.
          - 성공은 ClassifiedItem, 실패는 **예외 객체를 그 자리에 담아** 돌려준다.
            함수 자체는 raise 하지 않는다.

        여기서 배치를 통째로 재호출하지 않는다(비용). 이 실패는 대부분 **결정적**이라 —
        리뷰에 대해 LLM 이 '파손'을 뱉으면 schemas 의 리뷰 aspect 제약(색상/사이즈/소재)에
        매번 걸린다 — 다시 부르면 성공했을 나머지 건까지 매번 다시 과금된다. 일시적 오류
        (네트워크·레이트리밋) 재시도는 llm_client 가 MAX_RETRY 로 이미 한다.

        **건별 classify_aspect([item]) 를 다시 gather(return_exceptions=True) 로 감싸지 말 것.**
        안쪽이 raise 를 안 하므로 바깥 gather 는 예외를 볼 일이 없고, outcome 이
        `[LlmParseError(...)]` 라는 **리스트**로 와서 아래 isinstance(outcome, BaseException) 이
        영원히 False 가 된다. 그러면 dead-letter 에 안 남고 예외 객체가 results 에 섞여 들어가
        persist 단계에서 AttributeError 로 배치가 통째로 터진다(=그 건은 어디에도 남지 않고
        영구 유실).

        Returns:
            (분류 성공 목록, [(item_id, "classify", 오류메시지)]) — 실패는 호출부가
            dead-letter 에 기록한다.
        """
        if not items:
            return [], []

        outcomes = self.loop.run_until_complete(self._classify_all(items))

        results: list[ClassifiedItem] = []
        failures: list[tuple[str, str, str]] = []
        call_level_failures = 0
        for item, outcome in zip(items, outcomes):
            if isinstance(outcome, BaseException):
                failures.append((item.item_id, "classify", f"{type(outcome).__name__}: {outcome}"))
                logger.error(f"[ITEM FAILED] item_id={item.item_id} 분류 실패: {outcome!s}")
                # 파싱·검증 실패는 그 원문 고유의 문제지만, 호출 자체가 실패한 것은
                # 프로세스 전역 원인(키 만료·레이트리밋 소진·네트워크)일 수 있다.
                if not isinstance(outcome, LlmParseError):
                    call_level_failures += 1
                continue
            # outcome 은 item 1건에 대응하는 ClassifiedItem 이다(리스트가 아니다)
            results.append(outcome)

        self._halt_if_batch_wide_failure(items, failures, call_level_failures)
        return results, failures

    def _halt_if_batch_wide_failure(
        self,
        items: list[ClassifyRequestItem],
        failures: list[tuple[str, str, str]],
        call_level_failures: int,
    ) -> None:
        """배치가 통째로, 그것도 호출 단계에서 죽었으면 워커를 세운다.

        이게 없으면 **시스템 장애가 "N건 개별 실패"로 위장**된다. 401 이나 레이트리밋 소진처럼
        배치 전체가 같은 이유로 죽는 상황에서, 워커는 그것을 건별 실패로 dead-letter 에 적고
        커서를 밀고 다음 배치로 간다. 96,524건이면 배치가 9,653개라 원본 전량이 dead-letter 로
        넘어간 채 **정상 종료**한다. 게다가 장애가 길어져 재처리가 DEAD_LETTER_MAX_ATTEMPTS 를
        넘기면 회수 대상에서도 빠진다.

        판정에 오류 **종류**를 같이 보는 이유: `--retry-failed` 는 이미 실패한 건만 모아
        돌리는 모드라 전량 실패가 정상이다. 거기서 건수만 보고 세우면 회수 작업이
        첫 배치에서 멈춘다. 파싱·검증 실패(LlmParseError)는 원문 고유의 결정적 실패이므로
        아무리 많아도 장애 신호가 아니다. 호출 단계 실패가 섞여 있을 때만 세운다.
        (분류 서비스 계약 docstring 이 호출부 몫으로 남겨 둔 판정이다.)
        """
        if not items or len(failures) != len(items) or not call_level_failures:
            return

        logger.error(
            f"[WORKER HALT] 배치 {len(items)}건이 전부 실패했고 그중 {call_level_failures}건이 "
            "호출 단계 실패입니다 — 개별 원문 문제가 아니라 시스템 장애로 봅니다"
            "(키 만료·레이트리밋 소진·네트워크 등). 원인을 해결한 뒤 --retry-failed 로 "
            "dead-letter 를 회수하세요. 그대로 두면 남은 배치가 전부 dead-letter 로 넘어갑니다."
        )
        self.is_running = False

    async def _classify_all(
        self, items: list[ClassifyRequestItem]
    ) -> list[ClassifiedItem | Exception]:
        """분류 결과를 그대로 받아 온다 — 격리는 classify_aspect() 가 한다.

        여기서 gather 로 한 번 더 감싸면 실패 판정이 무력화된다(위 docstring 참고).
        """
        return await classify_aspect(items)

    def persist_batch(
        self,
        classified_items: list[ClassifiedItem],
        last_row: Any,
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

        DB 오류를 그냥 위로 던지면 커서가 안 움직인 채 워커가 죽고, 재시작하면 같은 배치를
        **다시 LLM 에 태워서** 같은 자리에서 또 죽는다(무한 재과금). 그래서 잠금 경합만 잠깐
        재시도하고, 그래도 안 되면 롤백 후 워커를 정지시킨다 — 사람이 보게 만드는 쪽이 조용히
        돈을 태우는 것보다 낫다.
        """
        # 오류 타입은 백엔드마다 다르다 — 여기서 한 번 물어보고 아래 except 가 쓴다.
        # **`sqlite3.OperationalError` 를 박아 두면 안 된다.** Postgres 에서는 잠금 경합·직렬화
        # 실패가 그 타입이 아니라, 잠깐 기다리면 될 것이 "치명적" 으로 분류돼 워커가 서고 다음
        # 실행이 같은 배치를 LLM 에 다시 태운다.
        retryable = raw_db.retryable_error_types(self.conn)
        fatal = raw_db.db_error_types(self.conn)
        for attempt in range(1, DB_MAX_RETRY + 1):
            try:
                inserted = self.save_classified_items(classified_items)
                self.record_failures(failures or [], occurred_at_by_id or {})
                self.clear_failures(resolved_ids or [])
                if advance_cursor:
                    self.save_cursor(last_row["occurred_at"], last_row["item_id"])
                self.conn.commit()
                return inserted
            except retryable as exc:
                # 대개 잠금 경합(다른 프로세스가 쓰는 중) — 일시적이라 재시도 가치가 있다
                self.conn.rollback()
                logger.warning(f"[DB RETRY] 적재 실패 ({attempt}/{DB_MAX_RETRY}): {exc}")
                if attempt < DB_MAX_RETRY:
                    time.sleep(2 ** attempt)
            except fatal as exc:
                self.conn.rollback()
                logger.error(f"[DB ERROR] 적재 실패 — 재시도하지 않습니다: {exc}")
                break

        logger.error(
            "[WORKER HALT] classified_item 적재에 실패해 워커를 정지합니다. "
            f"커서는 전진하지 않았으므로 재시작하면 이 배치(~{last_row['item_id']})부터 "
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
        now = datetime.now(KST).isoformat()
        for item_id, stage, error in failures:
            self.conn.execute(
                FAILURE_UPSERT,
                (
                    item_id,
                    occurred_at_by_id.get(item_id, cursor_origin(self.conn)),
                    stage,
                    error[:500],
                    now,
                    now,
                ),
            )

    def clear_failures(self, item_ids: list[str]) -> None:
        """분류에 성공한 건을 dead-letter 에서 제거한다(재처리 회수 경로)."""
        for item_id in item_ids:
            self.conn.execute(FAILURE_DELETE, (item_id,))

    def log_coverage(self) -> None:
        """분류 커버리지를 남긴다 — 분모를 원본에서 세는 합의의 전제 확인용.

        원문(cs ∪ reviews 의 분류 대상) 대비 classified_item 에 실제로 남은 문서 수를
        비교한다. 차이는 곧 "분자에서 빠진 문서"이고, aspect 0개 정상 분류와 구분하려고
        dead-letter 대기 건수를 함께 찍는다. 미달이면 경고로 올려 로더가 집계하기 전에
        눈에 띄게 한다.
        """
        if self.conn is None:
            return
        try:
            total = self.conn.execute(COUNT_SOURCE_SQL, CLASSIFY_SOURCES).fetchone()[0]
            pending = self.conn.execute("SELECT COUNT(*) FROM classification_failure").fetchone()[0]
            exhausted = self.conn.execute(
                "SELECT COUNT(*) FROM classification_failure WHERE attempts >= ?",
                (self.max_attempts,),
            ).fetchone()[0]
        except raw_db.db_error_types(self.conn) as exc:
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

        self.log_classifier_versions()

    def log_classifier_versions(self) -> None:
        """적재된 분류 결과의 분류기 버전 분포를 남긴다.

        커버리지(몇 건이 분류됐나)와 **별개의 축**이다. 전량이 분류돼 있어도 버전이
        섞여 있으면 탐지가 **아예 안 돈다**(`daily._check_version_cutover` 가 세운다) —
        커버리지 숫자만 보면 100% 라 아무 문제 없어 보이는 상태다. 그래서 따로 찍는다.

        **고칠 수 있는 것과 없는 것을 가른다.** 원문이 사라진 stale 행은 `--reclassify-stale`
        로 없앨 수 없다. 안 가르면 "backfill 하세요" 경고가 영원히 남아 다음 사람이 시간을 쓴다.
        """
        if self.conn is None:
            return
        try:
            rows = self.conn.execute(COUNT_BY_VERSION_SQL).fetchall()
            stale = self.count_stale()
            orphan = self.count_orphan_stale()
        except raw_db.db_error_types(self.conn) as exc:
            logger.warning(f"[VERSION] 집계 실패: {exc}")
            return

        if not rows:
            return

        breakdown = " | ".join(
            f"{r['source']}:{r['prompt']}/{r['model']}/{r['pipeline']}={r['n']}" for r in rows
        )
        if not stale and not orphan:
            logger.info(f"[VERSION] {breakdown}")
            return

        prompt_cs, prompt_review, model, pipeline = active_version_params()
        lines = [
            f"[VERSION] 분류기 버전이 섞여 있습니다 — {breakdown}",
            (
                f"  활성: cs={prompt_cs}, review={prompt_review},"
                f" model={model}, pipeline={pipeline}"
            ),
        ]
        if stale:
            lines.append(
                f"  옛 버전 {stale}건이 남아 있어 **탐지 배치가 서 있습니다.** "
                "`--reclassify-stale` 로 끝까지 backfill 하세요."
            )
        if orphan:
            lines.append(
                f"  이 중 {orphan}건은 태울 본문이 없어 재분류로 없앨 수 없습니다"
                "(원문 삭제·본문 공백). 탐지도 같은 조건으로 빼므로 배치를 막지는"
                " 않습니다 — 필요하면 해당 행을 직접 정리하세요."
            )
        logger.warning("\n".join(lines))

    def save_classified_items(self, items: list[ClassifiedItem]) -> int:
        """분류 결과를 두 테이블에 적재. 반환값은 실제 INSERT 된 aspect 행 수.

        원문(raw_text)·채널·상품그룹·발생 시각을 **복사하지 않는다.** 전부 원문 테이블에
        있으므로 집계는 그쪽을 기준으로 잡고 여기를 조인한다.

        aspect 가 0개인 문의도 **부모 행은 남긴다.** 안 남기면 "분류를 시도했으나 언급된 속성이
        없었다"와 "아직 분류하지 않았다"가 구분되지 않아 커버리지 확인이 깨진다.

        **같은 item_id 를 다시 넣으면 덮어쓴다**(upsert + aspect 전체 교체) —
        `--reclassify-stale` 이 성립하려면 여기가 덮어쓰기여야 한다. 근거는 위 SQL 상수 주석.
        """
        # **KST 로 찍는다 — 호스트 시간대를 보지 않는다.** raw DB 의 시각은 전부 오프셋이 붙은
        # ISO 문자열이고 날짜 경계가 KST 다. `datetime.now(timezone.utc).astimezone()` 은 **실행
        # 호스트의 로컬 오프셋**을 붙여서, UTC 컨테이너에서 돌리면 원문(mock_producer 는 KST)과
        # 오프셋이 갈린다.
        #
        # **일관성 확보이지 지금 깨지는 것을 고치는 게 아니다.** `classified_at` 은 저장소 전체
        # 에서 **쓰기 전용**이다 — 읽거나 비교하는 곳이 0건이고, 월간 집계는 이 컬럼을 일부러
        # 안 쓴다(`monthly_aggregator` 가 `occurred_at` 으로 거른다). 소비처가 생겼을 때 한
        # 컬럼에 +09:00 과 +00:00 이 섞여 있지 않게 미리 맞춰 두는 것이다.
        classified_at = datetime.now(KST).isoformat()
        inserted = 0

        # 조회(`active_version_params`)와 **같은 출처를 본다.** 여기서 따로 읽으면 적재한 값과
        # stale 판정이 갈려, 방금 넣은 행이 곧바로 재분류 대상이 된다.
        prompt_cs, prompt_review, model_version, pipeline_version = active_version_params()

        for item in items:
            prompt_version = prompt_cs if item.source == Source.CS else prompt_review
            self.conn.execute(
                CLASSIFIED_ITEM_UPSERT,
                (
                    item.item_id,
                    item.source.value,
                    classified_at,
                    prompt_version,
                    model_version,
                    pipeline_version,
                ),
            )
            # 옛 버전이 남긴 aspect 를 먼저 걷어낸다. 신규 적재에서는 지울 게 없어 무해하다.
            self.conn.execute(CLASSIFIED_ITEM_ASPECT_DELETE, (item.item_id,))
            for row in explode_to_rows(item):
                cur = self.conn.execute(
                    CLASSIFIED_ITEM_ASPECT_INSERT,
                    (
                        row["item_id"],
                        row["aspect"],
                        row["sentiment"],
                        # `int()` 로 캐스팅하지 말 것 — Postgres 컬럼이 BOOLEAN 이라 정수를
                        # 받지 않는다. sqlite 는 bool 을 0/1 로 저장한다.
                        None if row["mixed_signal"] is None else bool(row["mixed_signal"]),
                    ),
                )
                # 앞에서 옛 행을 지웠으므로 여기서 0 이 되는 것은 **한 응답 안의 중복
                # aspect** 뿐이다(재적재로 무시된 행이 아니다 — 그건 이제 없다).
                inserted += cur.rowcount

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


def main() -> None:
    """CLI 진입점.

    **`if __name__` 블록에 인라인으로 두지 말 것.** 그러면 pytest 가 그 블록을 실행하지 않아
    `force_utf8_output()` 배선을 테스트로 잠글 수 없고, 나머지 진입점과 형태도 갈린다.
    """
    # **첫 문장이어야 한다.** `--limit` 도움말에 `—`(U+2014) 가 있어서, 아래 `parse_args()` 가
    # 그걸 먼저 찍으면 cp949 콘솔에서는 도움말만 요청해도 `UnicodeEncodeError` 로 죽는다.
    # 사유 전문은 `app/core/console.py`.
    force_utf8_output()

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
    parser.add_argument(
        "--reclassify-stale", action="store_true",
        help="프롬프트 버전이 옛것인 분류 결과를 다시 분류한다(backfill). "
             "대상 1건당 LLM 1회이므로 --limit 로 나눠 돌릴 것",
    )
    parser.add_argument("--dry-run", action="store_true", help="DB 없이 모의 데이터로 추론만 확인")
    args = parser.parse_args()

    # 두 모드는 훑는 대상이 다르다(dead-letter vs 이미 성공한 옛 버전 행). 같이 켜면
    # 어느 쪽을 돌렸는지 모른 채 한쪽만 도므로, 조용히 무시하지 않고 여기서 세운다.
    if args.retry_failed and args.reclassify_stale:
        parser.error("--retry-failed 와 --reclassify-stale 은 같이 쓸 수 없습니다.")

    worker = ClassificationWorker(
        db_path=args.db,
        batch_size=args.batch_size,
        follow=args.follow,
        poll_interval=args.poll_interval,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
        max_attempts=args.max_attempts,
        reclassify_stale=args.reclassify_stale,
    )
    worker.start()


if __name__ == "__main__":
    main()
