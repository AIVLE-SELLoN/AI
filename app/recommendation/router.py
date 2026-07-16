"""담당: 지인 (Agent3) — 개선안 생성.

완료 기준: AnomalyResult → 개선안 JSON.
           인용검증·재시도 루프·근거없음 경로 작동.
"""

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/api/v1", tags=["recommendation"])


@router.get("/recommendations/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "recommendation", "owner": "지인", "status": "ok"}


@router.post("/recommendations/generate")
async def generate_recommendation():
    """AnomalyResult → Recommendation (개선안 JSON + 확신도).

    TODO(지인): schemas.py 확정 후 구현.
      - 요청/응답 타입을 GenerateRecommendationRequest / ~Response 로 교체
      - service.generate_recommendation() 호출 → graph.py 실행
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="미구현 — schemas.py 확정 후 작업 예정",
    )
