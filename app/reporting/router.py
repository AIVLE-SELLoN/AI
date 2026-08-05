"""담당: 용준 — 리포팅 (차별화 기능).

월간 리포트 + CS 가이드라인 생성. 파이프라인 밖 독립 기능이라
classification/detection/recommendation 을 import 하지 않고 core/ 만 쓴다(문서 2장).

응답은 문서 생성 스키마 §3-2 의 `GenerationCallback` 이다.
⚠️ 지금은 콜백을 **응답 본문으로 돌려준다**. 문서상 계약은 FastAPI 가 Spring Boot 의
   `POST /api/v1/internal/reports/complete` 로 푸시하는 것이지만, 그 URL 은
   `app/config.py`(공유 영역)에 환경변수로 들어가야 해서 팀 합의 후 붙인다.
   붙일 때는 `app/reporting/callback.py` 에 전송 함수를 추가하면 된다.
"""

from fastapi import APIRouter, HTTPException
from fastapi import status as status_code

from app.core.schemas import (
    CallbackStatus,
    CSGuidelineInput,
    GenerationCallback,
    MonthlyReportInput,
    MonthlyReportOutput,
)
from app.reporting.callback import build_hold_notice
from app.reporting.cs_reply_service import generate_cs_reply_pipeline
from app.reporting.monthly_report_service import generate_monthly_report_output

router = APIRouter(prefix="/api/v1", tags=["reporting"])


@router.get("/reports/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "reporting", "owner": "용준", "status": "ok"}


@router.post("/reports", response_model=MonthlyReportOutput)
async def create_report(input_data: MonthlyReportInput) -> MonthlyReportOutput:
    """월간 리포트 **단건 생성**(문장까지). 미리보기·디버그용이다.

    ⚠️ PDF 는 여기서 만들지 않는다. 운영 산출물은 월 1개 합본이라
    `scripts/generate_monthly_reports.py` 배치가 전 상품을 모아 만든다(2026-08-03 확정).

    표본 부족(total_voc_count < 10)이면 LLM 을 태우지 않고 409 로 알린다 —
    실패가 아니라 정상적인 보류 상태다(§4-3).
    """
    output, status, errors = await generate_monthly_report_output(input_data)
    if status == CallbackStatus.HOLD_INSUFFICIENT_DATA:
        raise HTTPException(status_code=status_code.HTTP_409_CONFLICT, detail=build_hold_notice())
    if output is None:
        raise HTTPException(
            status_code=status_code.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "생성 결과가 검증을 통과하지 못했습니다", "errors": errors},
        )
    return output


@router.post("/replies", response_model=GenerationCallback)
async def create_reply(input_data: CSGuidelineInput) -> GenerationCallback:
    """CS 가이드라인(답변 초안) 생성.

    `verdict='정상'` 인 알림은 스키마 단계에서 422 로 거른다 — 생성 대상이 아니다(§2-1).
    """
    result = await generate_cs_reply_pipeline(input_data)
    return result.callback
