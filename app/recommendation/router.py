"""담당: 지인 (Agent3) — 개선안 생성.

완료 기준: AnomalyResult → 개선안 JSON.
           인용검증·재시도 루프·근거없음 경로 작동.

Agent3는 상태를 저장하지 않는다 — Recommendation을 만들어서 돌려줄 뿐, 승인·반려
상태의 소유자는 Spring Boot다(graph.py HITL 메모). 그래서 /hitl 엔드포인트는
"이미 결정된 결과"를 alert·recommendation 통째로 받아서 컬렉션2 학습 자료로만
쌓는다 — recommendation_id로 다시 조회하는 게 아니다(그런 저장소 자체가 없음).
"""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.core.schemas import DetectionAlert, Recommendation
from app.recommendation import service

router = APIRouter(prefix="/api/v1", tags=["recommendation"])


@router.get("/recommendations/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "recommendation", "owner": "지인", "status": "ok"}


class GenerateRecommendationRequest(BaseModel):
    alert: DetectionAlert


class GenerateRecommendationResponse(BaseModel):
    recommendation: Recommendation | None
    """None이면 트리거 미충족(alert.recommended_action != "개선안 생성")."""


@router.post("/recommendations/generate", response_model=GenerateRecommendationResponse)
async def generate_recommendation(
    request: GenerateRecommendationRequest,
) -> GenerateRecommendationResponse:
    """DetectionAlert → Recommendation(개선안 JSON + 확신도). 트리거 미충족 시 recommendation=None."""
    recommendation = await service.generate_recommendation(request.alert)
    return GenerateRecommendationResponse(recommendation=recommendation)


class ProcessHitlRequest(BaseModel):
    alert: DetectionAlert
    recommendation: Recommendation
    """hitl_status/hitl_feedback은 호출부(Spring Boot)가 셀러 결정을 반영해 이미
    채워서 보낸다. Agent3는 그 값을 판단하지 않고 그대로 받는다."""


class ProcessHitlResponse(BaseModel):
    recorded: bool


@router.post("/recommendations/hitl", response_model=ProcessHitlResponse)
async def process_hitl(request: ProcessHitlRequest) -> ProcessHitlResponse:
    """승인/반려 결과를 컬렉션2(과거·반려 사례)에 적재(§4-2).

    다음 유사 케이스의 개선안 생성 때 "이런 개선안은 반려/승인됐었다"를 참고
    자료로 쓴다. hitl_status가 아직 PENDING이거나 alert/recommendation이 서로
    다른 건이면 400.
    """
    try:
        service.record_hitl_outcome(request.alert, request.recommendation)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ProcessHitlResponse(recorded=True)
