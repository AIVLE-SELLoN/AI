"""담당: 지인 (Agent3) — 개선안 생성 HTTP 경계.

Agent3는 상태를 저장하지 않는다 — Recommendation을 만들어서 돌려줄 뿐, 승인·반려
상태의 소유자는 Spring Boot다. 그래서 /hitl 엔드포인트는 "이미 결정된 결과"를
alert·recommendation 통째로 받아서 컬렉션2 학습 자료로만 쌓는다 —
recommendation_id로 다시 조회하는 게 아니다(그런 저장소 자체가 없음).
"""

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.core.exceptions import VectorDbError
from app.core.schemas import DetectionAlert, Recommendation
from app.recommendation import service

VECTORDB_UNAVAILABLE_DETAIL = "벡터DB 조회·적재에 실패했습니다. 잠시 후 다시 시도하세요."
"""`VectorDbError` 를 503 으로 내보낼 때의 문구.

임베딩이 OpenAI 를 타면서 레이트리밋·인증 실패가 이 경로로 올라올 수 있게 됐다.
안 잡으면 500 + 스택트레이스라 호출자가 "우리 잘못인가 일시 장애인가"를 못 가린다.

⚠️ **여기서 잡는 건 `VectorDbError` 하나뿐이다.** 상위 `AiServiceError` 를 통째로
503 에 매핑하면 설정 오류(`MqConfigError`)까지 "잠시 후 재시도"가 되어 거짓 안내가
된다. 예외별 상태코드 매핑은 라우터 3개(개선안·탐지·리포팅)에 걸친 결정이라 팀 합의가
필요하고, 그 전까지는 각 라우터가 자기 예외만 책임진다.
"""

router = APIRouter(prefix="/api/v1", tags=["recommendation"])


@router.get("/recommendations/ping")
async def ping() -> dict[str, str]:
    """모듈 4개가 한 앱으로 붙어 뜨는지 보는 부팅 확인(tests/test_app_boot.py)."""
    return {"module": "recommendation", "owner": "지인", "status": "ok"}


class GenerateRecommendationRequest(BaseModel):
    alert: DetectionAlert


class GenerateRecommendationResponse(BaseModel):
    recommendation: Recommendation | None
    """None인 경우 둘 — ① 트리거 미충족(recommended_action != "개선안 생성")
    ② 근거 0건. 이 엔드포인트는 CS 원문을 안 받아서 image_guide 는 항상 ②다
    (`service.generate_recommendation` docstring 참고)."""


@router.post("/recommendations/generate", response_model=GenerateRecommendationResponse)
async def generate_recommendation(
    request: GenerateRecommendationRequest,
) -> GenerateRecommendationResponse:
    """DetectionAlert → Recommendation(개선안 JSON + 확신도). 트리거 미충족 시 recommendation=None."""
    try:
        recommendation = await service.generate_recommendation(request.alert)
    except VectorDbError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=VECTORDB_UNAVAILABLE_DETAIL,
        ) from exc
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
    except VectorDbError as exc:
        # 적재도 문서를 임베딩하므로 이 경로로도 공급자 오류가 올라온다.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=VECTORDB_UNAVAILABLE_DETAIL,
        ) from exc
    return ProcessHitlResponse(recorded=True)
