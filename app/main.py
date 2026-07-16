"""FastAPI 앱 생성 + 라우터 등록만. 비즈니스 로직은 각 모듈 service.py 로.

실행:
    uvicorn app.main:app --reload
문서:
    http://localhost:8000/docs
"""

import logging

from fastapi import FastAPI

from app.classification.router import router as classification_router
from app.config import get_settings
from app.detection.router import router as detection_router
from app.recommendation.router import router as recommendation_router
from app.reporting.router import router as reporting_router

settings = get_settings()
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(
    title="SELLoN AI Service",
    description="분류 → 이상탐지 → 개선안 생성 파이프라인 + 리포팅",
    version="0.1.0",
)

# 4명의 모듈이 앱 1개로 뜨는지 확인하는 지점.
app.include_router(classification_router)
app.include_router(detection_router)
app.include_router(recommendation_router)
app.include_router(reporting_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    """k8s liveness/readiness probe 용."""
    return {"status": "ok"}
