"""담당: 지인 (2026-08-06 인수, 원작 서영) — 일 1회 탐지 배치. **운영 진입점.**

왜 여기 있나
------------
백엔드 확정 구조상 탐지의 시작점은 **AI 노드 안에서 매일 도는 배치**다. 메인→AI 방향
큐에 '탐지 요청'이 없고(셀러 피드백 2종뿐), `/detect` 는 body 로 items(분류 결과 전량)
를 받는 API 인데 그걸 손에 든 건 분류를 수행하는 AI 노드 자신뿐이다 — 백엔드는 분류를
하지 않는다. 그래서 `POST /detect` 는 운영 경로가 아니라 **재현·디버깅 창구**로 남는다.
(2026-08-05 지인님 결선 정리)

이 모듈이 `detection/service.py` 밖에 있는 이유
    탐지 → 개선안 → CS 가이드라인 → 발행 루프를 detection 안에 넣으면 detection 이
    recommendation 을 import 하게 되어 **"각 모듈은 core 에서만 가져다 쓴다"**는 팀
    규칙이 깨진다. 그래서 양쪽 바깥에 있는 이 모듈이 둘 다 부른다.

실행 (스케줄링은 아직 안 붙인다 — 주체가 백엔드인지 AI 노드인지 미정)::

    python -m app.batch.daily --dry-run          # LLM 0회, 호출 횟수만 실측
    python -m app.batch.daily --max-alerts 3     # 비용 상한
    python -m app.batch.daily --window-end 2026-08-28
    python -m app.batch.daily --input-source golden --dry-run   # 평가·재현 (oracle)

입력은 **주입**받는 형태다 — `run_batch(load_inputs=...)`. 기본값
`load_inputs_from_db()` 는 raw DB(`cs`·`reviews`·`classified_item`)를 직접 읽는다.
목 파이프라인에서는 그 테이블을 `scripts/mock_producer.py` 와
`scripts/classification_worker.py` 가 채우므로, **둘을 먼저 돌려야 배치가 돈다.**

🔴 **이 모듈은 `data/golden/` 을 읽지 않는다.** `eval/README.md` §232("`data/golden/`
   은 `eval/` 만 읽는다 — `app/` 이 import 하면 컨닝이다")를 지키기 위해 골든 로더를
   `scripts/golden_inputs.py` 로 뺐다(2026-08-06). 평가·재현으로 배치를 돌릴 때만
   `--input-source golden` 으로 주입하고, 그때는 요약이 oracle 경고를 함께 낸다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from pydantic import ValidationError

from app.config import get_settings
from app.core import raw_schema
from app.core.console import force_utf8_output
from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS
from app.core.inquiries import build_linked_inquiries
from app.core.schemas import Channel, ClassifiedItem, DetectionAlert, Source
from app.detection.loader import check_coverage, unreliable_slots
from app.detection.service import detect_anomaly

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]

STATE_PATH = ROOT / "data" / "batch_state" / "published_alerts.json"
"""발행 기록 캐시. **`prior_alerts` 의 출처.**

alert 저장의 정본은 서비스 DB(백엔드 단독 소유)이고 AI 는 직접 접근할 수 없다. 하지만
재알림 억제(`filter_suppressed`)와 기준선 오염 방지(`_alert_days`)가 `prior_alerts` 를
필요로 하는데, 그건 **AI 가 자기가 발행한 것이라 이미 안다.** 그래서 서비스 DB 쓰기가
아니라 '자기 발행 기록 캐시'로 둔다. 백엔드 조회 API 가 생기면 이 자리만 교체한다.

⚠️ 이게 없으면 매일 배치가 첫 실행처럼 굴러서 ① 같은 알림이 억제 기간 내내 매일 나가고
   ② 더 나쁘게는 `_alert_days` 가 비어 **지속되는 이상이 과거 윈도우에 섞여 '새로운
   평소'로 굳고 알림이 스스로 꺼진다.** 매일 도는 배치에서는 며칠 안에 실제로 난다.

🔴 **컨테이너로 올릴 때 이 경로에 볼륨을 붙일 것** (지인님 지적, 2026-08-05).
   이미지 안 임시 파일로 두면 재시작마다 날아가서 **캐시가 없는 것과 같아진다** —
   위 ①② 가 그대로 재현된다. compose 예시::

       volumes:
         - ./data/batch_state:/app/data/batch_state

   `data/**` 는 .gitignore 대상이라 저장소에도 안 올라간다. 배포 시 호스트 경로를
   반드시 확보할 것.
"""

STATE_RETENTION_DAYS = CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS
"""캐시 보관 기간(일). 임의값이 아니라 두 소비처가 요구하는 범위의 합이다.

`_alert_days` 가 과거 윈도우(28일) 안의 알림 구간을 제외하고, 억제 판정은 현재
윈도우(7일) 기준으로 경과일을 센다. 그보다 짧게 자르면 그 경계에서 조용히 억제가
풀리고 기준선이 오염된다.
"""


# ── 담당자 미완성 함수 — import 폴백 ────────────────────────────
# 시그니처는 지인님 결선 정리(2026-08-05)의 예시 코드를 그대로 따른다.
# 실물이 생기면 아래 except 가 안 타므로 이 파일은 손댈 필요가 없다.
#
# ⚠️ **모듈이 없을 때만 폴백한다.** `except ImportError` 로 통째로 삼키면, 모듈은
#    올라왔는데 그 안의 의존성(예: `aio_pika`)이 없어서 나는 ImportError 까지 먹고
#    조용히 no-op 이 된다 — 요약엔 "미연결"로 찍혀서 보는 사람은 "아직 안 만들었나"
#    로 읽고, 이벤트가 하나도 안 나가는데 배치는 정상 종료한다.
#    (지인님 PR 리뷰 §6, 2026-08-06)


def _missing(exc: ImportError, module: str) -> bool:
    """그 모듈 자체가 없어서 난 ImportError 인가. 아니면 진짜 오류라 다시 던진다."""
    return exc.name == module or (exc.name or "").startswith(module + ".")


try:  # pragma: no cover - 실물이 생기면 이쪽
    from app.core.mq import (  # type: ignore[attr-defined]
        close_mq,
        new_trace_id,
        publish_anomaly_analyzed,
        publish_guideline_generated,
    )

    MQ_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - 인프라 도입 전
    if not _missing(exc, "app.core.mq"):
        raise
    MQ_AVAILABLE = False

    def new_trace_id() -> str:
        return f"trace-{uuid.uuid4().hex[:16]}"

    async def publish_anomaly_analyzed(alert: Any, rec: Any, trace_id: str) -> None:
        logger.info(
            "[MQ 미구현] ai.anomaly.analyzed 발행 생략 alert=%s", alert.alert_id
        )

    async def publish_guideline_generated(guideline: Any, trace_id: str) -> None:
        logger.info("[MQ 미구현] ai.guideline.generated 발행 생략")

    async def close_mq() -> None:
        return None


try:  # pragma: no cover
    from app.recommendation.pipeline import (
        generate_outcome_for_alert,  # type: ignore[attr-defined]
    )

    RECOMMENDATION_AVAILABLE = True
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.recommendation.pipeline"):
        raise
    RECOMMENDATION_AVAILABLE = False

    async def generate_outcome_for_alert(alert: Any, inquiries: Any) -> Any:
        logger.info("[Agent3 미연결] 개선안 생성 생략 alert=%s", alert.alert_id)
        # 아래 루프가 읽는 필드만 흉내낸다. is_evidence_gap=False 라 미연결 상태가
        # 데이터 갭으로 둔갑하지 않고 실패로 남는다 — Agent3 가 없는 건 갭이 아니다.
        return SimpleNamespace(
            recommendation=None, is_evidence_gap=False, detail="Agent3 미연결"
        )


try:  # pragma: no cover
    from app.reporting.cs_reply_service import (
        generate_guideline,  # type: ignore[attr-defined]
    )

    GUIDELINE_AVAILABLE = True
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.reporting.cs_reply_service"):
        raise
    GUIDELINE_AVAILABLE = False

    async def generate_guideline(
        alert: Any, inquiries: Any, *, product_name: str | None = None
    ) -> Any:
        logger.info("[가이드라인 미연결] 생성 생략 alert=%s", alert.alert_id)
        return None


# [2] 개선안 생성 게이트. `recommended_action == "개선안 생성"` 인 alert 만 Agent3 로
#     간다 — 큐 규약이 recommendation 을 그때만 non-null 로 못박고 있다.
#     Agent3 가 아직 안 붙은 지금도 **dry-run 호출 수 추정에 필요**하므로 폴백을 둔다
#     (조치 7종 중 개선안 생성은 1종뿐이라, 게이트 없이 세면 크게 과대추정된다).
try:  # pragma: no cover
    from app.recommendation.pipeline import should_generate
except ImportError as exc:  # pragma: no cover
    if not _missing(exc, "app.recommendation.pipeline"):
        raise

    def should_generate(alert: Any) -> bool:
        return getattr(alert.recommended_action, "value", "") == "개선안 생성"


# ── dry-run 스텁 ────────────────────────────────────────────────

STUB_CAUSE = "사진_색감_오차"
"""스텁이 돌려줄 원인 라벨. `SCOPE_LIMIT_LABELS`(실물_염색_편차·실제_원단_문제)를
피한다 — 그 라벨이면 Agent3 가 라우팅·생성을 통째로 건너뛰어 호출 수 추정이 어긋난다.

⚠️ `scripts/crosscheck_agent2_to_agent3.py` 도 이 값과 `CountingClient` 를 그대로
   가져다 쓴다. 두 도구가 같은 호출 수를 내야 하므로 복제하지 말 것."""


class CountingClient:
    """LLM 을 부르지 않고 호출 횟수만 세는 스텁. `--dry-run` 전용.

    `detect_anomaly(client=...)` 에 주입하면 [6] 원인분류가 **실제로 몇 번 불릴지**를
    돈 한 푼 안 쓰고 실측한다. 후보 수를 눈으로 세는 추정이 아니다.

    프롬프트에 박힌 cs_id 를 그대로 되돌려준다 — 개수를 맞춰야 `root_cause.total` 이
    현실적인 크기로 나오고, 그래야 `recommended_action` 이 실제와 비슷하게 산출된다.
    비우면 원인이 '미특정'으로 빠져 게이트 통과 수를 실제보다 적게 잡는다.

    돌려주는 필드는 **`diagnose_cause` 가 실제로 읽는 3개**(`cs_id`·`cause`·
    `aspect_match`)뿐이다. 프롬프트3 스키마엔 `confidence`·`evidence` 도 있지만 우리
    코드는 어느 쪽도 안 읽으므로(`app/detection/cause.py`), 스텁이 채우면 흉내낸 값이
    실측처럼 보이기만 한다. `confidence` 는 실험⑥에서 판정 기준 2개(단조증가·0.5~0.8
    분포)를 다 못 넘겨 미사용으로 확정된 필드다.
    """

    def __init__(self) -> None:
        self.calls = 0
        self.empty_extractions = 0

    async def complete_json(
        self, prompt: str, *, trace_key: str = "-", **_: object
    ) -> dict:
        import re

        self.calls += 1
        cs_ids = re.findall(r'"cs_id"\s*:\s*"([^"]+)"', prompt)
        if not cs_ids:
            self.empty_extractions += 1
        return {
            "results": [
                {
                    "cs_id": cs_id,
                    "cause": STUB_CAUSE,
                    "aspect_match": True,
                }
                for cs_id in cs_ids
            ]
        }


# ── 입력 ────────────────────────────────────────────────────────


INPUT_WINDOW_DAYS = CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS
"""DB 에서 읽어올 기간(일). 탐지가 실제로 보는 범위와 같다.

`build_baseline` 의 과거 윈도우가 **현재 윈도우 직전** 28일이라, 필요한 전부가
`[window_end - 34, window_end]` 다. 값이 `STATE_RETENTION_DAYS` 와 같지만 **사유가
다르다** — 저쪽은 캐시 보관 기간이다. 한쪽을 바꿀 일이 생겼을 때 다른 쪽이 조용히
따라가지 않도록 따로 둔다.
"""

KST = timezone(timedelta(hours=9))
"""날짜 경계의 기준 시간대. **확정 문서 §3 이 KST 로 못박았다.**

UTC 로 자르면 KST 오전 9시 이전 문의가 전날로 밀려서, 매일 도는 배치가 날짜 경계에서
매번 어긋난다. 문서의 쿼리는 `(컬럼 AT TIME ZONE 'Asia/Seoul')::date` 인데 그건
Postgres 문법이라 로컬 sqlite 에 없다 — 그래서 **절단을 파이썬에서 한다**
(`_to_kst`). 원문은 오프셋이 붙은 ISO 문자열로 저장되므로(raw_schema 모듈 docstring)
변환에 필요한 정보가 값 안에 다 있다.
"""

# 분모(원문)와 분자(분류 결과)를 **따로** 읽는다. 확정 문서 §4 는 이 둘을 CTE 로 나눠
# SQL 안에서 집계하는 형태인데, 우리는 집계를 `app/detection/aggregate.py` 가 하므로
# 행을 그대로 가져오는 두 쿼리로 나눈다. 지켜야 할 규칙은 같다 —
#   ① 분모는 원문(voc_document)에서만 센다. classified_item 에서 세면 aspect 0개인
#      문서가 통째로 빠져 부정률이 부풀려진다(§2-6 경고, loader 모듈 docstring).
#   ② `sentiment = -1` 을 WHERE 로 올리지 않는다. 올리면 LEFT JOIN 이 INNER JOIN 으로
#      퇴화해 같은 버그가 돌아온다. 부정 여부는 읽어온 뒤 파이썬이 센다.
_DOCUMENT_SQL = f"""
    SELECT item_id, source, channel_id, product_group_id, content, occurred_at
    FROM {raw_schema.VOC_DOCUMENT}
"""

_ASPECT_SQL = f"""
    SELECT ci.item_id AS item_id, a.aspect AS aspect, a.sentiment AS sentiment
    FROM classified_item ci
    JOIN {raw_schema.VOC_DOCUMENT} v ON v.item_id = ci.item_id
    LEFT JOIN classified_item_aspect a ON a.item_id = ci.item_id
"""


def _to_kst(value: str) -> datetime:
    """저장된 ISO 문자열 → KST 시각. 날짜 절단의 유일한 경로다.

    ⚠️ **문서에 넣는 `created_at` 도 이 값이어야 한다.** `build_rows` 가 `.date()` 로
       날짜를 다시 뽑는데, 여기서 거른 날짜와 그쪽이 뽑는 날짜가 다르면 윈도우 경계의
       문서가 "읽히긴 했는데 집계에선 다른 날"이 된다.
    """
    return datetime.fromisoformat(value).astimezone(KST)


def load_inputs_from_db(
    window_end: date | None = None,
    *,
    db_path: str | None = None,
) -> tuple[list[ClassifiedItem], list[dict]]:
    """(items, documents) 를 원본 DB 에서 읽는다. **기본 입력원.**

    items 는 워커가 분류해 `classified_item` 에 넣은 결과, documents 는 `cs`·`reviews`
    원문이다. 둘을 따로 읽는 이유는 **분모가 documents 에서 나와야 하기 때문**이다 —
    리뷰는 aspect 0개면 `classified_item` 에 행이 아예 없어서(`explode_to_rows` 가
    aspect 마다 1행), items 로 분모를 세면 그 문서가 통째로 빠지고 부정률이 부풀려진다
    (탐지 분모 산출 방식 §1).

    두 소스의 시각 컬럼명이 다르므로(`cs.inquired_at` / `reviews.created_at`)
    `voc_document` 뷰를 거친다 — 호출부에서 UNION 을 다시 쓰면 시각 컬럼을 잘못 고르는
    실수가 각자 생긴다(raw_schema 모듈 docstring).

    **읽기 전용으로 연다.** AI 노드는 원문 테이블에 읽기 권한만 있고(§5-2), 경로가
    틀렸을 때 sqlite 가 빈 파일을 새로 만들어 "문서 0건" 으로 조용히 통과하는 것도 막는다.

    평가·재현으로 배치를 돌리려면 `scripts/golden_inputs.load_golden_inputs` 를
    주입한다 — 골든을 `app/` 안에서 읽지 않기 위해 밖으로 뺐다(eval/README §232).

    Args:
        window_end: 현재 윈도우 마지막 날. 주면 `[window_end-34, window_end]` 35일만
            읽는다(현재 7 + 과거 28 — `build_baseline` 의 과거 윈도우가 현재 윈도우
            직전 28일이라 그 합이 필요한 전부다). None 이면 전량을 읽고 호출부가 최신
            날짜로 윈도우를 정한다 — 첫 실행·백필용이고, 매일 배치에서는 반드시 줄 것
            (현재 목 데이터 기준 128,228건 풀스캔이다).
        db_path: raw DB 경로. 기본은 `settings.raw_db_path`(테스트 주입용 인자다).

    Returns:
        items: 분자의 출처 (ClassifiedItem)
        documents: **분모의 출처** (원본 문서)

    Raises:
        FileNotFoundError: DB 파일이 없을 때. 목 파이프라인은 `scripts/mock_producer.py`
            가 먼저 돌아야 원문이 생긴다.
    """
    path = Path(db_path or get_settings().raw_db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(
            f"raw DB 가 없습니다: {path} — 목 파이프라인은 scripts/mock_producer.py 로"
            " 원문을 적재한 뒤 scripts/classification_worker.py 로 분류해야 합니다."
        )

    where, params = _window_clause(window_end)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        _require_classified_tables(conn, path)
        doc_rows = conn.execute(_DOCUMENT_SQL + where, params).fetchall()
        aspect_rows = conn.execute(
            _ASPECT_SQL + where.replace("occurred_at", "v.occurred_at"), params
        ).fetchall()
    finally:
        conn.close()

    return _build_inputs(doc_rows, aspect_rows, window_end)


def _require_classified_tables(conn: sqlite3.Connection, path: Path) -> None:
    """분류 결과 테이블이 **쓸 수 있는 상태인지** 먼저 확인한다.

    두 가지를 가른다. 둘 다 원문만 적재된 DB 에서 실제로 나는 상태다:
      - 테이블 자체가 없음 → 워커를 아직 안 돌렸다. 그냥 두면 `no such table` 이
        올라오는데, 원인이 "경로가 틀렸나"인지 "안 돌렸나"인지 안 드러난다.
      - 8/7 확정 이전 구조로 남아 있음 → `find_legacy_tables`. `IF NOT EXISTS` 가 옛
        테이블을 그대로 두기 때문에 **조회 단계에서 `no such column` 으로** 터진다
        (PR #37 에서 워커가 같은 함정을 맞았다). `data/` 는 gitignore 라 팀원마다 DB
        상태가 달라서 남아 있을 수 있다.

    조용히 빈 결과로 넘기지 않는 이유: items 가 0건이면 분자가 통째로 비어 **알림이
    한 건도 안 나오는데 배치는 정상 종료**한다. 무동작이 성공으로 보고되는 형태다.
    """
    stale = raw_schema.find_legacy_tables(conn)
    if stale:
        raise RuntimeError(
            f"raw DB 가 8/7 확정 이전 스키마입니다({', '.join(stale)}): {path} — "
            "구버전 파일을 지우고 mock_producer·classification_worker 를 다시 돌리세요."
        )
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='classified_item'"
    ).fetchone()
    if exists is None:
        raise RuntimeError(
            f"분류 결과 테이블이 없습니다: {path} — scripts/classification_worker.py 를"
            " 먼저 돌려야 분자(부정 건수)가 생깁니다."
        )


def _window_clause(window_end: date | None) -> tuple[str, tuple]:
    """35일 범위조회 조건절. 인덱스(`cs.inquired_at`·`reviews.created_at`)를 타라고 둔다.

    경계를 하루씩 넓혀 잡고 **정확한 절단은 파이썬이 한다**(`_build_inputs`). 문자열
    비교라 저장된 오프셋이 전부 같아야 정확한데, 그 전제가 깨진 행이 하나 섞여도
    경계에서 조용히 빠지지 않게 하려는 것이다. 넓힌 만큼은 뒤에서 다시 걸러진다.
    """
    if window_end is None:
        return "", ()
    start = date.fromordinal(window_end.toordinal() - INPUT_WINDOW_DAYS + 1)
    return (
        " WHERE occurred_at >= ? AND occurred_at < ?",
        (
            date.fromordinal(start.toordinal() - 1).isoformat(),
            date.fromordinal(window_end.toordinal() + 2).isoformat(),
        ),
    )


def _build_inputs(
    doc_rows: list, aspect_rows: list, window_end: date | None
) -> tuple[list[ClassifiedItem], list[dict]]:
    """조회 결과 → (items, documents). 못 쓰는 행은 버리고 건수만 경고로 남긴다.

    ⚠️ **버리는 방향은 항상 "분모에서도 뺀다" 이다.** 문서는 남기고 분류 결과만 버리면
       그 슬롯이 분류 커버리지 미달로 잡혀(`check_coverage`) 검정에서 통째로 빠지는데,
       그건 오탐이 아니라 미탐 방향이라 조용하다. 사유별 건수를 로그로 남기는 이유다.
    """
    start = (
        date.fromordinal(window_end.toordinal() - INPUT_WINDOW_DAYS + 1)
        if window_end
        else None
    )

    documents: list[dict] = []
    created_of: dict[str, datetime] = {}
    meta_of: dict[str, dict] = {}
    dropped: Counter[str] = Counter()

    for row in doc_rows:
        product = (row["product_group_id"] or "").strip()
        if not product:
            # 상품매핑이 아직 안 붙은 원문. 어느 상품의 분모인지 모르니 셀 수 없다.
            dropped["상품매핑 없음"] += 1
            continue
        try:
            channel = Channel(row["channel_id"])
            source = Source(row["source"])
            created = _to_kst(row["occurred_at"])
        except ValueError:
            dropped["채널·소스·시각 형식 오류"] += 1
            continue
        if start and not (start <= created.date() <= window_end):
            continue  # 조건절을 하루씩 넓혀 잡은 만큼 (_window_clause)

        item_id = row["item_id"]
        documents.append(
            {
                "id": item_id,
                "product": product,
                "channel": channel.value,
                "source": source.value,
                "created_at": created,
                "text": row["content"],
            }
        )
        created_of[item_id] = created
        meta_of[item_id] = {
            "source": source,
            "channel": channel,
            "product_group_id": product,
            "raw_text": row["content"],
        }

    aspects_of: dict[str, list[dict]] = defaultdict(list)
    for row in aspect_rows:
        item_id = row["item_id"]
        if item_id not in meta_of:
            continue  # 윈도우 밖이거나 위에서 버린 문서
        aspects_of.setdefault(item_id, [])
        if row["aspect"] is not None:
            # aspect 0개인 분류 결과도 항목은 만든다 — 리뷰의 정상 출력이다.
            aspects_of[item_id].append(
                {"aspect": row["aspect"], "sentiment": row["sentiment"]}
            )

    items: list[ClassifiedItem] = []
    for item_id, aspects in aspects_of.items():
        try:
            items.append(
                ClassifiedItem(
                    item_id=item_id,
                    created_at=created_of[item_id],
                    aspects=aspects,
                    **meta_of[item_id],
                )
            )
        except ValidationError:
            # 리뷰에 허용 밖 aspect 가 붙은 경우 등. 분모(문서)는 그대로 두고 분자만
            # 빠지므로 그 슬롯은 커버리지 미달로 검정에서 제외된다 — 안전한 방향이다.
            dropped["분류 결과 스키마 불일치"] += 1

    if dropped:
        logger.warning(
            "raw DB 입력에서 %d건을 제외했습니다: %s",
            sum(dropped.values()),
            dict(dropped),
        )
    logger.info(
        "raw DB 로드 documents=%d items=%d (window_end=%s)",
        len(documents),
        len(items),
        window_end,
    )
    return items, documents


# ── 발행 기록 캐시 ──────────────────────────────────────────────


def _as_date(value: Any) -> date:
    """documents 의 `created_at`(str 또는 datetime)에서 날짜만 뽑는다."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.date() if isinstance(value, datetime) else value


def _read_state(path: Path) -> list[dict]:
    """캐시 파일을 **항목 단위로 방어하며** 읽는다.

    ⚠️ 이게 유일한 상태 저장소인데 자가복구 경로가 없으면, 쓰다 만 JSON 하나로 배치가
       영구히 못 뜬다. 사람이 파일을 지워야 다시 도는데 그러면 억제·기준선이 첫 실행으로
       리셋된다 — `STATE_PATH` docstring ①② 가 경고한 그 시나리오다.
       `DetectionAlert` 에 필수 필드가 하나 추가되는 것만으로도 같은 상태가 된다
       (스키마는 전원 회의 확정 사항이라 실제로 바뀐다).
       (지인님 PR 리뷰 §3, 2026-08-06)
    """
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("발행 기록 캐시가 손상됐습니다 — 빈 상태로 진행합니다 (%s)", exc)
        return []
    if not isinstance(raw, list):
        logger.warning("발행 기록 캐시 형식이 예상과 다릅니다 — 빈 상태로 진행합니다")
        return []
    return [a for a in raw if isinstance(a, dict) and "alert_id" in a]


def _atomic_write(path: Path, text: str) -> None:
    """임시파일에 쓰고 `os.replace` 로 갈아끼운다. 도중에 죽어도 반쪽 파일이 안 남는다."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def load_prior_alerts(
    window_end: date, path: Path = STATE_PATH
) -> list[DetectionAlert]:
    """캐시에서 `prior_alerts` 를 읽는다. 없으면 빈 리스트(첫 실행).

    깨진 항목은 버리고 경고만 남긴다 — 하나 때문에 배치 전체가 멈추면 안 된다.
    """
    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    alerts: list[DetectionAlert] = []
    dropped = 0
    for item in _read_state(path):
        try:
            alert = DetectionAlert.model_validate(item)
        except ValidationError:
            dropped += 1
            continue
        if alert.window_end >= cutoff:
            alerts.append(alert)
    if dropped:
        logger.warning(
            "발행 기록 %d건이 현재 스키마와 안 맞아 건너뜁니다 (억제가 그만큼 약해집니다)",
            dropped,
        )
    return alerts


def save_published(
    published: list[DetectionAlert], window_end: date, path: Path = STATE_PATH
) -> int:
    """발행된 알림을 캐시에 누적한다. 반환값은 저장 후 총 건수.

    ⚠️ **여기 넣는 것은 "실제로 발행에 성공한 알림"뿐이다.**
       - `suppressed` 는 안 넣는다 — 정의가 "과거 **발행된** 알림"이라 억제분을 넣으면
         다음 배치가 그걸 기준으로 또 억제해 이중 억제가 된다.
       - **`--max-alerts` 로 잘린 알림, 발행이 실패한 알림도 안 넣는다.** 넣으면 다음
         배치가 그걸 직전 알림으로 보고 `RENOTIFY_BLOCK_DAYS` 만큼 억제해서 **셀러가
         그 알림을 영영 못 본다.** `resolved_alert_ids` 가 빈 집합이라 조기 해제 경로도
         없다. MQ 가 5분 죽으면 그날 알림이 7일간 침묵하는 형태다.
         (지인님 PR 리뷰 §1, 2026-08-06)

    같은 `alert_id` 는 덮어쓴다(같은 날 재실행 시 중복 누적 방지).
    """
    existing: dict[str, dict] = {a["alert_id"]: a for a in _read_state(path)}
    for alert in published:
        existing[alert.alert_id] = alert.model_dump(mode="json")

    cutoff = date.fromordinal(window_end.toordinal() - STATE_RETENTION_DAYS)
    kept = [
        a for a in existing.values() if date.fromisoformat(a["window_end"]) >= cutoff
    ]
    _atomic_write(path, json.dumps(kept, ensure_ascii=False, indent=2))
    return len(kept)


# ── 배치 본체 ───────────────────────────────────────────────────


async def run_batch(
    *,
    window_end: date | None = None,
    max_alerts: int | None = None,
    dry_run: bool = False,
    state_path: Path = STATE_PATH,
    load_inputs: Callable[..., tuple[list[ClassifiedItem], list[dict]]] | None = None,
) -> dict:
    """탐지 → 개선안 → 가이드라인 → 발행. 배치 1회.

    Args:
        window_end: 현재 윈도우 마지막 날. 없으면 입력의 최신 날짜.
        max_alerts: Agent3·가이드라인에 태울 alert 수 상한 (비용 통제).
        dry_run: LLM 을 한 번도 부르지 않고 **몇 번 부를지만 실측**한다.
        state_path: 발행 기록 캐시 경로 (테스트 주입용).
        load_inputs: 입력원. 기본은 원본 DB(`load_inputs_from_db`).
            평가·재현은 `scripts.golden_inputs.load_golden_inputs` 를 주입한다.
            **골든을 주입하면 분류 오차가 0 이라 결과가 탐지 성능이 아니다** —
            요약에 그 경고가 함께 출력된다.

    Returns:
        배치 요약 dict. 실패는 모아서 담고 **중간에 던지지 않는다.**
    """
    loader = load_inputs or load_inputs_from_db
    trace_id = new_trace_id()
    started = datetime.now()
    logger.info("배치 시작 trace_id=%s dry_run=%s", trace_id, dry_run)

    items, documents = loader(window_end)

    # ⚠️ **window_end 를 여기서 한 번만 확정한다.** 로드·탐지·저장이 같은 값을 써야 한다.
    #    읽기는 실행 시각(`date.today()`), 쓰기는 데이터 시각(`window_end`)이면, 데이터가
    #    오늘보다 STATE_RETENTION_DAYS 이상 뒤처진 상태(백필·유입 지연)에서 로드가 방금
    #    저장한 캐시를 통째로 버려 **매 배치가 첫 실행처럼 굴러간다.** 억제 모듈이 경과일을
    #    데이터 시각으로 세는 것과 같은 이유다. (지인님 PR 리뷰 §5, 2026-08-06)
    if window_end is None and documents:
        window_end = max(_as_date(d["created_at"]) for d in documents)

    # check_coverage 를 두 번 돌리지 않는다 — detect_anomaly 도 안에서 같은 계산을 한다.
    # 넘겨주면 128k 스캔이 한 번 줄고, "경고에 찍힌 슬롯 = 실제로 family 에서 빠진 슬롯"
    # 이 보장된다.
    unreliable = unreliable_slots(check_coverage(documents, items))
    if unreliable:
        logger.warning(
            "분류 커버리지 미달 %d슬롯 — 검정에서 제외됩니다", len(unreliable)
        )

    prior = load_prior_alerts(window_end or date.today(), state_path)
    logger.info(
        "입력 items=%d documents=%d prior_alerts=%d window_end=%s",
        len(items),
        len(documents),
        len(prior),
        window_end,
    )

    # ⚠️ dry-run 이어도 [6] 원인분류는 detect_anomaly 안에서 돈다. 스텁을 안 주면
    #    "LLM 0회"라고 해놓고 실제로 과금된다.
    stub = CountingClient() if dry_run else None

    alerts, suppressed = await detect_anomaly(
        items,
        documents=documents,
        window_end=window_end,
        prior_alerts=prior,
        # 백엔드가 어디서 줄지 미정 — 정해질 때까지 빈 집합 (지인님 결선 §8).
        resolved_alert_ids=set(),
        unreliable_denominators=unreliable,
        client=stub,
    )

    targets = alerts if max_alerts is None else alerts[:max_alerts]
    failures: list[dict] = []
    counts: Counter[str] = Counter()
    # 근거 0건으로 개선안을 못 만든 건수. **실패가 아니라 데이터 갭이다** — 루프 안
    # 주석 참고. failures 와 분리해 두는 이유는 종료코드에 안 실리게 하기 위해서다.
    evidence_gaps = 0
    # ⚠️ **발행에 성공한 것만** 캐시에 넣는다. save_published docstring 참고.
    delivered: list[DetectionAlert] = []

    # ⚠️ **연결을 반드시 닫는다.** app/core/mq.py 가 프로세스당 연결·채널을 재사용하는데,
    #    닫지 않고 이벤트 루프가 내려가면 connect_robust 의 재연결 태스크가 정리되지 않아
    #    "Task was destroyed but it is pending" 이 뜨고, 나중에 이 배치가 장수 프로세스에
    #    얹히거나 반복 호출되면 연결이 샌다. 루프 도중 예외가 나가도 닫히도록 finally 다.
    #    한 번도 발행하지 않았으면(dry-run 등) 연결 자체가 없어서 no-op 이다.
    #    (서영님 PR 리뷰 §1, 2026-08-07)
    try:
        for alert in targets:
            # [2] 개선안 게이트 — 조치 7종 중 '개선안 생성' 일 때만 Agent3 가 돈다.
            wants_recommendation = should_generate(alert)

            if dry_run:
                # 실제로 몇 번 부를지만 센다. 추정이 아니라 실측이다 — 게이트를 안 태우면
                # Agent3 비용이 크게 과대추정된다(조치 7종 중 1종만 해당).
                if wants_recommendation:
                    counts["개선안"] += 1
                counts["가이드라인"] += 1
                counts["발행:이상"] += 1
                continue

            # 개선안·가이드라인이 **같은 CS 원문**을 근거로 쓴다. 여기서 한 번 만들어 둘 다
            # 에게 넘긴다 — 각자 만들면 같은 매핑이 두 벌이 되고, C4(item_id ↔ cs/reviews PK)
            # 가 풀려 DB 조회로 바뀔 때 고칠 곳이 두 곳이 된다.
            inquiries = build_linked_inquiries(alert, documents)

            # ⚠️ alert 1건이 터져도 배치는 계속한다. 여기서 던지면 **이미 LLM 비용을 쓴
            #    앞쪽 알림들까지 발행되지 않고 날아간다.** 실패는 모아서 끝에 요약한다.
            rec = guideline = None
            if wants_recommendation:
                try:
                    outcome = await generate_outcome_for_alert(alert, inquiries)
                    rec = outcome.recommendation
                except Exception as exc:  # noqa: BLE001 - 배치 격리가 목적
                    counts["개선안"] += 1
                    failures.append(
                        {
                            "alert_id": alert.alert_id,
                            "stage": "개선안",
                            "error": repr(exc),
                        }
                    )
                else:
                    # ⚠️ `generate_outcome_for_alert` 는 **계약상 예외를 안 던지고** 실패를
                    #    개선안 없는 결과로 돌려준다. 위 except 만 두면 실패가 요약에도
                    #    종료코드에도 안 남아서, 개선안이 하나도 안 붙은 배치가 "성공"으로
                    #    끝난다. else 인 이유: except 와 둘 다 타면 실패 1건이 요약에 2건으로
                    #    잡혀 배치 요약의 실패 건수를 못 믿게 된다.
                    #
                    # 🔴 **개선안이 없는 사유 두 가지를 가른다 (2026-08-10).**
                    #    근거 0건(상세페이지 미등록 + CS 원문 없음)은 파이프라인이 정상인
                    #    **데이터 갭**이고 흔하다 — 그걸 실패로 세면 배치가 상시 종료코드 1로
                    #    끝나서 진짜 장애 신호가 무뎌진다. 그래서 건수로만 세고 실패엔 안 넣는다.
                    #    어떤 사유가 갭인지는 pipeline 이 정한다(`RecommendationOutcome`) —
                    #    여기서 사유를 다시 판정하면 사유가 늘 때 두 곳이 갈린다.
                    if outcome.is_evidence_gap:
                        # 라우팅 전에 걸러지므로 LLM 호출은 0회다 — 개선안 카운트에 안 넣는다.
                        evidence_gaps += 1
                    else:
                        counts["개선안"] += 1
                        if rec is None:
                            failures.append(
                                {
                                    "alert_id": alert.alert_id,
                                    "stage": "개선안",
                                    "error": outcome.detail
                                    or "생성 실패 — 사유는 app.recommendation.pipeline 로그 참고",
                                }
                            )

            try:
                guideline = await generate_guideline(alert, inquiries)
                counts["가이드라인"] += 1
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "alert_id": alert.alert_id,
                        "stage": "가이드라인",
                        "error": repr(exc),
                    }
                )

            try:
                await publish_anomaly_analyzed(alert, rec, trace_id)
                counts["발행:이상"] += 1
                delivered.append(alert)
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "alert_id": alert.alert_id,
                        "stage": "발행:이상",
                        "error": repr(exc),
                    }
                )

            if guideline is not None:
                try:
                    await publish_guideline_generated(guideline, trace_id)
                    counts["발행:가이드"] += 1
                except Exception as exc:  # noqa: BLE001
                    failures.append(
                        {
                            "alert_id": alert.alert_id,
                            "stage": "발행:가이드",
                            "error": repr(exc),
                        }
                    )

    finally:
        await close_mq()

    # 캐시는 dry-run 에서 건드리지 않는다 — 안 보낸 걸 보냈다고 기록하지 않는다.
    cached = (
        save_published(delivered, window_end, state_path)
        if (not dry_run and delivered and window_end)
        else 0
    )

    return {
        "trace_id": trace_id,
        "dry_run": dry_run,
        # 입력원을 결과에 박아둔다 — 골든(oracle)으로 돌린 숫자를 "탐지 성능"으로
        # 인용하는 사고를 막는 게 목적이다. eval/README §68 이 실험① 에 붙인 경고와
        # 같은 이유이고, 거기서는 사람이 문서에 적었지만 여기서는 코드가 매번 낸다.
        "input_source": getattr(loader, "__name__", str(loader)),
        "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
        "items": len(items),
        "documents": len(documents),
        "prior_alerts": len(prior),
        "published": len(alerts),
        # suppressed 도 정상 alert_id 를 갖고 있다. 구분 없이 세면 나중에 "발행된 건가
        # 억제된 건가"를 알 수 없으므로 따로 센다 (지인님 결선 §6-②).
        "suppressed": len(suppressed),
        "processed": len(targets),
        # 발행에 성공해 캐시에 들어간 건수. published(탐지) 와 다를 수 있고,
        # 그 차이가 곧 "다음 배치에서 다시 시도될 알림"이다.
        "delivered": len(delivered),
        "llm_calls": dict(counts),
        "cause_calls": stub.calls if stub else None,
        # 근거 0건으로 개선안을 생략한 건수. failures 와 **별개**다 — 데이터 갭이라
        # 종료코드에 안 실린다. 이 숫자가 계속 크면 상세페이지 시딩·CS 원문 조회를 볼 것.
        "no_evidence": evidence_gaps,
        "failures": failures,
        "state_cached": cached,
    }


def print_summary(summary: dict) -> None:
    print("\n" + "=" * 62)
    print(f"배치 요약  trace_id={summary['trace_id']}  {summary['elapsed_sec']}초")
    print("=" * 62)
    print(
        f"  입력          items {summary['items']} / documents {summary['documents']}"
        f"  [{summary['input_source']}]"
    )
    if summary["input_source"] == "load_golden_inputs":
        print(
            "  ⚠️ 골든 라벨(oracle) 입력 — 분류 오차 0%. 이 숫자는 탐지 성능이 아니다."
        )
    print(f"  prior_alerts  {summary['prior_alerts']}건")
    print(f"  탐지          {summary['published']}건")
    print(f"  억제          {summary['suppressed']}건  ← 발행 아님")
    print(f"  후속 처리     {summary['processed']}건")
    if not summary["dry_run"]:
        print(
            f"  발행 성공     {summary['delivered']}건  ← 캐시(prior_alerts)에 들어간 것"
        )
        missed = summary["processed"] - summary["delivered"]
        if missed > 0 or summary["published"] > summary["processed"]:
            print(
                f"  ↳ 캐시에 안 넣음 {summary['published'] - summary['delivered']}건"
                " (상한으로 잘렸거나 발행 실패) — 다음 배치에서 다시 시도됩니다"
            )
    if summary.get("no_evidence"):
        print(
            f"  개선안 생략   {summary['no_evidence']}건  ← 근거 0건(상세페이지 미등록·CS"
            " 원문 없음). 실패 아님"
        )
    if summary["dry_run"]:
        print("\n  [dry-run] LLM 호출 0회. 실제로 돌리면:")
        print(
            f"     Agent2 [6] 원인분류 : {summary['cause_calls']}회  ← 스텁이 가로채 실측"
        )
        print(f"     후속 단계           : {summary['llm_calls']}")
        print(
            "     개선안은 recommended_action=='개선안 생성' 인 alert 만 (1건당 LLM 2~4회)."
        )
    if summary["failures"]:
        print(f"\n  ⚠️ 실패 {len(summary['failures'])}건 (배치는 계속 진행됨)")
        for f in summary["failures"][:10]:
            print(f"     {f['alert_id']} [{f['stage']}] {f['error'][:80]}")
    missing = [
        name
        for name, ok in [
            ("RabbitMQ(app.core.mq)", MQ_AVAILABLE),
            ("Agent3(generate_outcome_for_alert)", RECOMMENDATION_AVAILABLE),
            ("가이드라인(generate_guideline)", GUIDELINE_AVAILABLE),
        ]
        if not ok
    ]
    if missing:
        print(f"\n  ℹ️ 미연결: {', '.join(missing)} — 해당 단계는 no-op 입니다.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--window-end", default=None, help="현재 윈도우 마지막 날 (YYYY-MM-DD)"
    )
    ap.add_argument(
        "--max-alerts",
        type=int,
        default=None,
        help="후속 처리할 alert 수 상한 (비용 통제)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="LLM 0회 — 몇 번 부를지만 실측한다"
    )
    ap.add_argument(
        "--input-source",
        choices=["db", "golden"],
        default="db",
        help="db(기본, 운영) / golden(평가·재현 전용 — scripts/golden_inputs.py)."
        " golden 은 분류 오차가 0 이라 결과가 탐지 성능이 아니다. 요약이 경고를 낸다.",
    )
    ap.add_argument(
        "--state-path",
        default=None,
        help=f"발행 기록 캐시 경로 (기본 {STATE_PATH}). 디버그 실행이 운영 캐시를"
        " 오염시키지 않게 따로 지정할 것 — 캐시가 오염되면 그 알림이 억제 기간 내내"
        " 셀러에게 안 간다.",
    )
    args = ap.parse_args()

    # 출력이 나가기 전에. 사유는 app/core/console.py 참고.
    force_utf8_output()

    loader = None
    if args.input_source == "golden":
        # app/ 안에서 골든을 읽지 않기 위해 **여기서만** 늦게 import 한다
        # (eval/README §232). 모듈 최상단에 두면 운영 import 경로에 골든이 딸려온다.
        from scripts.golden_inputs import load_golden_inputs

        loader = load_golden_inputs

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    summary = asyncio.run(
        run_batch(
            window_end=date.fromisoformat(args.window_end) if args.window_end else None,
            max_alerts=args.max_alerts,
            dry_run=args.dry_run,
            state_path=Path(args.state_path) if args.state_path else STATE_PATH,
            load_inputs=loader,
        )
    )
    print_summary(summary)

    # ⚠️ 계속 도는 것과 성공으로 보고하는 것은 다르다. 실패가 있으면 비-0 으로 끝내야
    #    cron·k8s Job 이 알아챈다 — 안 그러면 모든 알림이 발행 실패해도 성공한 배치다.
    #    (지인님 PR 리뷰 §4, 2026-08-06)
    if summary["failures"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
