"""담당: 서영 (Agent2) — 이상탐지 + 원인분류.

완료 기준: ClassifiedItem → DetectionAlert.
           진양성·위양성함정 케이스 통과.

라우터는 얇게 — 판정 로직은 전부 service.py 아래 단계별 모듈에 있다.
"""

from fastapi import APIRouter

from app.detection.service import DetectRequest, DetectResponse, detect_anomaly

router = APIRouter(prefix="/api/v1", tags=["detection"])


@router.get("/detect/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "detection", "owner": "서영", "status": "ok"}


@router.post("/detect", response_model=DetectResponse)
async def detect(request: DetectRequest) -> DetectResponse:
    """ClassifiedItem 집합 → DetectionAlert (편중형/전역형 + 원인라벨).

    ⚠️ BH-FDR 은 배치 연산이라 **한 번에 윈도우 전체를 넘겨야 한다.** 상품 하나만
       따로 호출하면 다중검정 보정의 family 가 그 상품으로 좁아져 컷오프가 달라진다
       (로직 §8). 상품별 분할 호출 금지.
    """
    alerts, suppressed = await detect_anomaly(
        request.items,
        window_end=request.window_end,
        prior_alerts=request.prior_alerts,
        resolved_alert_ids=set(request.resolved_alert_ids),
    )
    return DetectResponse(alerts=alerts, suppressed=suppressed)
