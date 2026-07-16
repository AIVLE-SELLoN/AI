"""담당: 서영 (Agent2) — 이상탐지 + 원인분류.

완료 기준: ClassifiedItem → AnomalyResult.
           진양성·위양성함정 케이스 통과.
"""

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/api/v1", tags=["detection"])


@router.get("/detect/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "detection", "owner": "서영", "status": "ok"}


@router.post("/detect")
async def detect():
    """ClassifiedItem 집합 → AnomalyResult (편중형/전역형 + 원인라벨).

    TODO(서영): schemas.py 확정 후 구현.
      - 요청/응답 타입을 DetectRequest / DetectResponse 로 교체
      - service.detect_anomaly() 호출
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="미구현 — schemas.py 확정 후 작업 예정",
    )
