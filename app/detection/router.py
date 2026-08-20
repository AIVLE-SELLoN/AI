"""담당: 서영 (Agent2) — 이상탐지 + 원인분류 REST 창구.

라우터는 얇게 — 판정 로직은 전부 service.py 아래 단계별 모듈에 있다.
"""

import logging

from fastapi import APIRouter, HTTPException

from app.core.schemas import Source
from app.detection.service import DetectRequest, DetectResponse, detect_anomaly

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["detection"])


@router.get("/detect/ping")
async def ping() -> dict[str, str]:
    """앱 부팅·라우터 등록 확인용 (tests/test_app_boot.py)."""
    return {"module": "detection", "owner": "서영", "status": "ok"}


@router.post("/detect", response_model=DetectResponse)
async def detect(request: DetectRequest) -> DetectResponse:
    """ClassifiedItem 집합 → DetectionAlert (편중형/전역형 + 원인라벨).

    운영 진입점이 아니다 — 매일 도는 배치(`app/batch/daily.py`)가 `detect_anomaly()` 를
    직접 부른다. 이 API 는 "이 케이스만 넣으면 왜 알림이 안 뜨지?"를 보는 재현·디버깅 창구다.

    BH-FDR family 는 상품별이라, 그 상품의 aspect×채널×source 슬롯을 일부만 보내면 family
    가 줄어 컷오프가 달라진다. 상품 하나의 완전한 윈도우 입력을 넘길 것.
    """
    # `documents: []` 는 거절한다. 빈 리스트를 그대로 넘기면 build_rows 가 0행을 내고
    # detect_anomaly 의 `if not rows` 에 걸려 조용히 빈 응답이 나간다.
    if request.documents is not None and not request.documents:
        raise HTTPException(
            status_code=422,
            detail=(
                "documents 가 빈 배열입니다. 분모의 출처라 이대로면 알림이 0건으로 나갑니다. "
                "items 기준 분모로 돌리려면 documents 필드를 아예 생략하세요."
            ),
        )

    if request.documents is None and any(
        i.source == Source.REVIEW for i in request.items
    ):
        # documents 없이는 분류가 누락된 원문이 있는지 확인할 수 없고, 누락되면 분모가
        # 줄어 부정률이 부풀 수 있다(오탐 방향).
        logger.warning(
            "documents 없이 리뷰가 섞인 요청 — 원문 대비 부모 분류 레코드 coverage를 "
            "검증할 수 없어 운영 경로와 결과가 달라질 수 있습니다. documents 를 함께 "
            "보내세요."
        )

    alerts, suppressed = await detect_anomaly(
        request.items,
        documents=[d.model_dump() for d in request.documents]
        if request.documents
        else None,
        window_end=request.window_end,
        prior_alerts=request.prior_alerts,
        resolved_alert_ids=set(request.resolved_alert_ids),
    )
    return DetectResponse(alerts=alerts, suppressed=suppressed)
