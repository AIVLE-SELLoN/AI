"""FastAPI 앱 생성 + 라우터 등록만. 비즈니스 로직은 각 모듈 service.py 로.

실행:
    uvicorn app.main:app --reload
문서:
    http://localhost:8000/docs

종료 코드는 `app/core/exit_codes.py` 가 정본이다 — 설정 문제면 **2** 로 끝낸다.

**설정 오류 처리는 import 시점에 일어난다.** 여기엔 `main()` 이 없고 uvicorn 이 이 모듈을
import 하므로 부팅 실패를 알릴 지점이 그것뿐이다. 종료코드가 OS 까지 나가는 것은 배포 형태
(`uvicorn app.main:app`)에서 확인했다(실측). `--reload`·`--workers` 는 부모가 감독 프로세스라
자식이 죽어도 부모가 안 끝나는데, uvicorn 쪽 동작이고 Dockerfile `CMD` 에 두 플래그가 없어
운영 경로도 아니다. 그 경우에도 **사유 한 줄은 그대로 찍힌다.**
"""

from fastapi import FastAPI

from app.classification.router import router as classification_router
from app.core.logging_setup import configure_logging_or_exit
from app.detection.router import router as detection_router
from app.recommendation.router import router as recommendation_router
from app.reporting.router import router as reporting_router

configure_logging_or_exit("서버")

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
