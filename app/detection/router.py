"""담당: 서영 (Agent2) — 이상탐지 + 원인분류.

완료 기준: ClassifiedItem → DetectionAlert.
           진양성·위양성함정 케이스 통과.

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
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "detection", "owner": "서영", "status": "ok"}


@router.post("/detect", response_model=DetectResponse)
async def detect(request: DetectRequest) -> DetectResponse:
    """ClassifiedItem 집합 → DetectionAlert (편중형/전역형 + 원인라벨).

    ⚠️ **운영 진입점이 아니다.** 매일 도는 배치(`app/batch/daily.py`)가 `detect_anomaly()`
       를 직접 부른다. 이 API 는 "이 케이스만 넣으면 왜 알림이 안 뜨지?"를 보는
       재현·디버깅 창구다 (2026-08-05 지인님 결선 정리).

    ⚠️ BH-FDR 은 배치 연산이라 **한 번에 윈도우 전체를 넘겨야 한다.** 상품 하나만
       따로 호출하면 다중검정 보정의 family 가 그 상품으로 좁아져 컷오프가 달라진다
       (로직 §8). 상품별 분할 호출 금지.
    """
    # ⚠️ `documents: []` 는 거절한다. 빈 리스트를 그대로 넘기면 build_rows 가 0행을 내고
    #    detect_anomaly 의 `if not rows` 에 걸려 **조용히 빈 응답**이 나간다. 재현 창구인데
    #    "왜 아무것도 안 뜨지?"가 정확히 이 형태다. (지인님 PR 리뷰 §8, 2026-08-06)
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
        # 정상 빈 배열 리뷰도 부모 item 으로 들어오지만, documents 없이는 분류 자체가
        # 누락된 원문이 있는지 확인할 수 없다. 누락 시 분모가 줄어 부정률이 부풀 수 있다.
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
