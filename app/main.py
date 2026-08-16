"""FastAPI 앱 생성 + 라우터 등록만. 비즈니스 로직은 각 모듈 service.py 로.

실행:
    uvicorn app.main:app --reload
문서:
    http://localhost:8000/docs

종료 코드는 `app/core/exit_codes.py` 가 정본이다 — 설정 문제면 **2** 로 끝낸다.
"""

import logging
import sys

from fastapi import FastAPI

from app.classification.router import router as classification_router
from app.config import get_settings
from app.core.exit_codes import EXIT_CONFIG_ERROR
from app.detection.router import router as detection_router
from app.recommendation.router import router as recommendation_router
from app.reporting.router import router as reporting_router


def _configure_logging() -> None:
    """설정을 읽고 로깅을 건다. **실패하면 한 줄 남기고 exit 2 로 끝낸다.**

    🔴 **왜 try 가 필요한가 — 주 배포물이 CrashLoopBackOff 로 죽던 자리다.**
       예전엔 이 두 줄이 모듈 최상단에 맨몸으로 있어서 `LOG_LEVEL=info`(소문자) 하나가
       미포착 예외로 나갔다. 종료코드가 **1**(일시적 오류)이 되는데, k8s 는 그걸 보고
       재시작하고 **같은 환경변수를 다시 읽어 또 죽는다.** 원인은 raw traceback 으로만
       남아 로그 수집기에서 여러 줄로 흩어진다. `app/consumer.py` 가 PR #91 에서 고친
       것과 같은 결함이고, 이쪽이 Dockerfile `CMD` 가 띄우는 **주 배포물**이다.

    ⚠️ **`ValueError` 만 잡는다.** 이 분기가 덮는 실패는 둘인데 둘 다 `ValueError`
       하위라 이걸로 정확히 덮인다 — `logging.basicConfig(level=...)` 의 잘못된 레벨과
       `get_settings()` 의 pydantic `ValidationError`. 더 넓히면 진짜 예상 밖 오류까지
       "설정 문제" 로 보고하게 된다.

    ⚠️ **`logger` 가 아니라 `print` 다.** 두 갈래 중 `get_settings()` 쪽에서 터지면
       로깅이 아직 아무것도 설정되지 않았다. 두 갈래가 같은 모양으로 나가게 통일한다
       (`app/consumer.py` 와 같은 판단).

    🔴 **메시지에 `—`·`⚠️`·`ℹ️`·`❌` 를 넣지 말 것 — 한글과 ASCII 만.**
       이 파일은 `force_utf8_output()` 을 **일부러 안 부른다**(스트림 소유자가 uvicorn
       이라서. `tests/test_console_encoding.py` 가 그 결정을 단언으로 고정하고 있다).
       그래서 cp949 콘솔에서 그 문자를 찍으면 `UnicodeEncodeError` 가 **이 except 안에서**
       터져 exit 1 로 되돌아간다 — 고치려던 것이 그대로 되살아난다. 한글 음절은 cp949 에
       있어서 안전하다(실측).

    ⚠️ **종료코드가 실제로 나가는 건 배포 형태(`uvicorn app.main:app`)에서다**(실측).
       `--reload`·`--workers` 는 부모가 감독 프로세스라 자식이 죽어도 부모가 안 끝난다 —
       uvicorn 쪽 동작이라 우리가 어쩌지 못하고, 그 형태는 운영에 안 쓴다(Dockerfile
       `CMD` 에 두 플래그가 없다). 그 경우에도 **아래 한 줄 진단은 그대로 찍힌다.**
    """
    try:
        settings = get_settings()
        logging.basicConfig(
            level=settings.log_level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    except ValueError as exc:
        # `{exc}` 를 쓴다(`{exc!r}` 아님) — pydantic `ValidationError` 의 repr 은 여러
        # 줄이라, 한 줄로 남기려는 이 처리의 목적을 되살려 놓는다.
        first_line = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        print(f"설정을 읽지 못해 서버를 띄우지 못했습니다: {first_line}", file=sys.stderr)
        sys.exit(EXIT_CONFIG_ERROR)


_configure_logging()

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
