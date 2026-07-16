"""담당: 현진 (Agent1) — aspect·감성 분류.

완료 기준: 원문 리스트 → ClassifiedItem 리스트.
           fixture 100건 분류 정확도 측정치 첨부.
"""

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/api/v1", tags=["classification"])


@router.get("/classify/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "classification", "owner": "현진", "status": "ok"}


@router.post("/classify")
async def classify():
    """CS/리뷰 원문 → ClassifiedItem 리스트.

    TODO(현진): schemas.py 확정 후 구현.
      - 요청/응답 타입을 ClassifyRequest / ClassifyResponse 로 교체
      - service.classify_aspect() 호출
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="미구현 — schemas.py 확정 후 작업 예정",
    )
