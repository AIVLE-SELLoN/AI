"""담당: 지인 (Agent3) — 진입점.

router.py(HTTP)와 app/consumer.py(MQ)가 같은 함수를 쓰도록 두는 얇은 층이다.
전송 수단과 무관한 것만 여기 둔다 — CS 원문 조회와 raw DB degrade 판단이 그것이고,
파이프라인 본체는 pipeline.py 가 갖는다.
"""

from __future__ import annotations

import logging

from app.core.inquiries import fetch_linked_inquiries
from app.core.mq_consumer import RecommendationReviewed, load_hitl_context
from app.core.raw_db import connection_error_types
from app.core.schemas import DetectionAlert, Recommendation
from app.recommendation import pipeline

logger = logging.getLogger(__name__)


async def generate_recommendation(alert: DetectionAlert) -> Recommendation | None:
    """DetectionAlert → Recommendation | None (트리거 미충족 시 None).

    body 로 alert 만 받으므로 **CS 원문은 `evidence.inquiry_ids` 로 직접 조회한다**
    (`fetch_linked_inquiries`). 안 조회하면 image_guide 로 라우팅됐을 때 근거가 0건이라
    **항상 None** 이 되어 copy_draft 만 디버깅할 수 있다.

    운영 경로는 아니다 — 개선안은 탐지 배치가 `generate_outcome_for_alert(alert,
    inquiries)` 로 선생성해 `ai.anomaly.analyzed` payload 에 실어 보낸다. 이 엔드포인트는
    재현·디버깅용이다.

    ⚠️ **raw DB 를 못 읽으면 원문 없이 진행한다.** 그 환경에서 500 을 내면 DB 없이 쓰던
    copy_draft 디버깅까지 같이 막힌다. 대신 조용히 넘기지 않고 경고를 남긴다 — 근거가
    빠진 채 나온 결과를 "개선안이 안 만들어진다" 로 오해하지 않게 하려는 것이다.

    🔴 **degrade 조건은 백엔드마다 다른 타입으로 온다.** sqlite 는 파일 부재
    (`FileNotFoundError`)지만 Postgres 는 접속·스키마·권한 실패가 `psycopg.Error` 로
    오고 그건 `FileNotFoundError` 가 아니다 — `connection_error_types()` 를 안 넣으면
    이 degrade 가 sqlite 에서만 동작하고 Postgres 에서는 **500** 이 된다.
    """
    try:
        inquiries = fetch_linked_inquiries(alert)
    except (FileNotFoundError, *connection_error_types()) as exc:
        logger.warning(
            "raw DB 를 읽지 못해 CS 원문 없이 생성합니다 — image_guide 는 근거 0건으로"
            " 개선안이 안 나옵니다 (%s)",
            exc,
        )
        inquiries = []
    return await pipeline.run(alert, inquiries)


def record_hitl_outcome(alert: DetectionAlert, recommendation: Recommendation) -> None:
    """승인/반려 결과를 컬렉션2에 적재. Spring Boot가 hitl_status를 이미 결정해서
    보낸 뒤 호출 — Agent3는 상태를 저장하지 않고 RAG 학습 자료로만 반영한다.

    Raises:
        ValueError: pipeline.record_hitl_outcome 참고.
    """
    pipeline.record_hitl_outcome(alert, recommendation)


def handle_recommendation_reviewed(payload: dict) -> None:
    """`feedback.recommendation.reviewed` 1건을 컬렉션2에 적재한다.

    컨슈머(`app/core/mq_consumer.py`)가 부르지만 **core 가 이 함수를 알지는 않는다** —
    실행 진입점(`app/consumer.py`)이 `register_handler()` 로 꽂아 준다. core 가
    recommendation 을 import 하면 의존 방향이 거꾸로 뒤집히기 때문이다.

    Raises:
        ValidationError: payload 가 계약과 다름 (§8).
        HitlContextUnavailableError: 적재 재료 부족 — `load_hitl_context()` 참고.
        ValueError: alert/recommendation 이 서로 다른 건이거나 아직 대기 상태.
    """
    event = RecommendationReviewed.model_validate(payload)
    alert, recommendation = load_hitl_context(event)
    pipeline.record_hitl_outcome(alert, recommendation)
