"""담당: 현진 (Agent1) — aspect·감성 분류.

완료 기준: 원문 리스트 → ClassifiedItem 리스트.
           fixture 100건 분류 정확도 측정치 첨부.

🔴 스코프 확인 필요: 분류 워커 명세 §1은 Kafka 컨슈머(폴링→배치→오프셋 커밋) 구조를
   그리고 있음. 이 라우터는 README 표의 POST /api/v1/classify(요청-즉시응답 방식)를
   우선 구현한 것 — Kafka 워커와 이 REST 엔드포인트가 둘 다 필요한지, 이 라우터가
   나중에 대체되는지 팀 확인 필요.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.classification.service import classify_aspect
from app.core.exceptions import LlmCallError, LlmParseError
from app.core.schemas import Channel, ClassifiedItem, Source

router = APIRouter(prefix="/api/v1", tags=["classification"])


class ClassifyRequestItem(BaseModel):
    """분류 대상 원문 1건. Kafka 메시지 필드와 1:1 대응 예정(워커 연동 시)."""

    item_id: str
    source: Source
    channel: Channel
    product_group_id: str
    raw_text: str
    created_at: datetime


class ClassifyRequest(BaseModel):
    items: list[ClassifyRequestItem]


class ClassifyResponse(BaseModel):
    results: list[ClassifiedItem]


@router.get("/classify/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "classification", "owner": "현진", "status": "ok"}


@router.post("/classify", response_model=ClassifyResponse)
async def classify(request: ClassifyRequest) -> ClassifyResponse:
    """CS/리뷰 원문 → ClassifiedItem 리스트.

    service.classify_aspect()가 내부적으로 asyncio.gather()를 써서 여러 건을
    동시에 처리(분류 워커 명세 §1 "LLM 배치 분류 추론" 대응).
    하나라도 실패하면 전체 502 처리 — 부분 성공 허용 여부는 아직 미정.
    """
    try:
        results = await classify_aspect(request.items)
    except (LlmCallError, LlmParseError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=f"분류 실패: {exc}"
        ) from exc

    return ClassifyResponse(results=results)