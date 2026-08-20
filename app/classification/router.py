"""담당: 현진 (Agent1) — aspect·감성 분류 REST 진입점.

원문 리스트 → ClassifiedItem 리스트. 요청-즉시응답 경로다.
배치 경로는 scripts/classification_worker.py 가 따로 맡는다 — 같은
classify_aspect() 를 부르고 결과를 raw DB 에 적재한다.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from app.classification.service import ClassifyRequestItem, classify_aspect
from app.core.schemas import ClassifiedItem

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["classification"])


class ClassifyRequest(BaseModel):
    items: list[ClassifyRequestItem]


class ClassifyErrorItem(BaseModel):
    """부분 실패 1건. item_id 로 요청의 어느 항목이 실패했는지 매칭.

    error 는 예외 타입명만 담는다 — LlmParseError 메시지엔 LLM 원본 응답이 통째로
    실릴 수 있어, 그대로 내보내면 내부 프롬프트·데이터가 외부로 샌다. 상세는 서버
    로그에만 남긴다.
    """

    item_id: str
    error: str


class ClassifyResponse(BaseModel):
    results: list[ClassifiedItem]
    errors: list[ClassifyErrorItem] = []


@router.get("/classify/ping")
async def ping() -> dict[str, str]:
    """헬스체크 — 앱 하나에 네 모듈이 함께 떠 있는지 본다."""
    return {"module": "classification", "owner": "현진", "status": "ok"}


@router.post("/classify", response_model=ClassifyResponse)
async def classify(request: ClassifyRequest) -> ClassifyResponse:
    """CS/리뷰 원문 → ClassifiedItem 리스트.

    부분 성공을 허용한다 — 1건의 파싱 실패가 요청 전체를 502 로 만들지 않도록
    성공·실패를 나눠 200 으로 응답하고, 실패는 item_id 와 함께 errors 에 담아
    클라이언트가 그 항목만 재시도하게 한다. 실패가 예외가 아니라 반환값으로 오는
    계약은 service.classify_aspect() 참고.
    """
    raw_results = await classify_aspect(request.items)

    results: list[ClassifiedItem] = []
    errors: list[ClassifyErrorItem] = []
    for item, r in zip(request.items, raw_results):
        if isinstance(r, Exception):
            logger.warning(f"classify_partial_failure item_id={item.item_id}: {r}")
            errors.append(ClassifyErrorItem(item_id=item.item_id, error=type(r).__name__))
        else:
            results.append(r)

    return ClassifyResponse(results=results, errors=errors)