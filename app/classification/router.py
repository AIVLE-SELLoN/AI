"""담당: 현진 (Agent1) — aspect·감성 분류.

완료 기준: 원문 리스트 → ClassifiedItem 리스트.
           fixture 100건 분류 정확도 측정치 첨부.

🔴 스코프 확인 필요: 분류 워커 명세 §1은 Kafka 컨슈머(폴링→배치→오프셋 커밋) 구조를
   그리고 있음. 이 라우터는 README 표의 POST /api/v1/classify(요청-즉시응답 방식)를
   우선 구현한 것 — Kafka 워커와 이 REST 엔드포인트가 둘 다 필요한지, 이 라우터가
   나중에 대체되는지 팀 확인 필요.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.classification.service import ClassifyRequestItem, classify_aspect
from app.core.schemas import ClassifiedItem

router = APIRouter(prefix="/api/v1", tags=["classification"])


class ClassifyRequest(BaseModel):
    items: list[ClassifyRequestItem]


class ClassifyErrorItem(BaseModel):
    """부분 실패 1건. item_id로 요청의 어느 항목이 실패했는지 매칭."""

    item_id: str
    error: str


class ClassifyResponse(BaseModel):
    results: list[ClassifiedItem]
    errors: list[ClassifyErrorItem] = []


@router.get("/classify/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "classification", "owner": "현진", "status": "ok"}


@router.post("/classify", response_model=ClassifyResponse)
async def classify(request: ClassifyRequest) -> ClassifyResponse:
    """CS/리뷰 원문 → ClassifiedItem 리스트.

    service.classify_aspect()가 내부적으로 asyncio.gather()를 써서 여러 건을
    동시에 처리(분류 워커 명세 §1 "LLM 배치 분류 추론" 대응).

    ⚠️ 부분 성공 허용(서영님↔현진 합의, 2026-08-04): classify_aspect()가 이제
    실패 시 raise하지 않고 그 자리에 예외 객체를 담아 반환한다(계약 — service.py
    참고). 1건(예: 환각으로 인한 파싱 실패)이 요청 전체를 502로 만드는 게 과하다고
    판단해, 성공/실패를 나눠 200으로 응답하고 실패는 item_id와 함께 errors에
    담는다. 클라이언트가 errors만 보고 그 항목만 재시도할 수 있게.
    """
    raw_results = await classify_aspect(request.items)

    results: list[ClassifiedItem] = []
    errors: list[ClassifyErrorItem] = []
    for item, r in zip(request.items, raw_results):
        if isinstance(r, Exception):
            errors.append(ClassifyErrorItem(item_id=item.item_id, error=str(r)))
        else:
            results.append(r)

    return ClassifyResponse(results=results, errors=errors)