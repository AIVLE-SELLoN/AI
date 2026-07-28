"""담당: 지인 (Agent3) — 진입점.

이 파일은 얇게 유지. 지금은 pipeline.py(순수 함수 파이프라인)를 직접 호출한다.
LangGraph 이식 후에는 generate_recommendation()이 pipeline.run() 대신
graph.ainvoke()를 호출하도록 바뀔 예정이며, router.py는 이 함수들의 시그니처가
유지되는 한 수정할 필요가 없다.
"""

from __future__ import annotations

from app.core.schemas import DetectionAlert, Recommendation
from app.recommendation import pipeline


async def generate_recommendation(alert: DetectionAlert) -> Recommendation | None:
    """DetectionAlert → Recommendation | None (트리거 미충족 시 None).

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
