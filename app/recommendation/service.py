"""담당: 지인 (Agent3) — 진입점.

이 파일은 얇게 유지. 지금은 pipeline.py(순수 함수 파이프라인)를 직접 호출한다.
LangGraph 이식 후에는 generate_recommendation()이 pipeline.run() 대신
graph.ainvoke()를 호출하도록 바뀔 예정이며, router.py는 이 함수들의 시그니처가
유지되는 한 수정할 필요가 없다.
"""

from __future__ import annotations

from app.core.mq_consumer import RecommendationReviewed, load_hitl_context
from app.core.schemas import DetectionAlert, Recommendation
from app.recommendation import pipeline


async def generate_recommendation(alert: DetectionAlert) -> Recommendation | None:
    """DetectionAlert → Recommendation | None (트리거 미충족 시 None).

    ⚠️ **CS 원문을 안 넘긴다 — 그래서 image_guide 로 라우팅되면 개선안이 안 나온다.**
    원문은 `alert.evidence.inquiry_ids` 로 조회해야 하는데(`app/core/inquiries.py`),
    이 REST 엔드포인트는 body 로 alert 만 받아서 조회 입력이 없다. 근거가 0건이면
    `run()` 이 None 을 돌려주므로(§4-3), image_guide 케이스는 여기서 항상 None 이다.

    운영 경로가 아니라서 그대로 둔다 — 개선안은 탐지 배치가 `generate_for_alert(alert,
    inquiries)` 로 선생성해 `ai.anomaly.analyzed` payload 에 실어 보낸다. 이 엔드포인트는
    재현·디버깅용이다. **copy_draft 케이스 디버깅에만 쓸 것.**

    # TODO: LangGraph 이식 후 pipeline.run(alert) → graph.ainvoke(초기 상태) 로 교체.
    """
    return await pipeline.run(alert)


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
