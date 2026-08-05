"""생성 완료 콜백 조립 — 문서 생성 스키마 §3-2.

FastAPI 가 문서를 만든 뒤 Spring Boot(`POST /api/v1/internal/reports/complete`)로
결과를 알리는 계약이다. 두 파이프라인(월간 리포트·CS 가이드라인)이 같은 형태를 쓰므로
여기서 조립만 담당한다.

⚠️ 아직 **아웃바운드 HTTP 푸시는 하지 않는다.** Spring Boot 콜백 URL 은 환경변수
   (`app/config.py`)로 들어가야 하는데 그건 공유 영역이라 팀 합의가 필요하다.
   지금은 라우터가 이 객체를 응답 본문으로 그대로 돌려주고, 연동 시점에 여기에
   전송 함수를 붙이면 된다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core import constants
from app.core.schemas import (
    CallbackStatus,
    CSGuidelineInput,
    CSGuidelineOutput,
    GenerationCallback,
    MonthlyReportOutput,
    PdfS3Meta,
)


@dataclass
class GenerationResult:
    """파이프라인 반환값.

    output 은 성공했을 때만 채워진다(HOLD·FAILED 면 None). 호출부가 둘 다 필요해서
    콜백만 돌려주지 않고 묶어서 준다 — 콜백의 source_payload 에서 다시 파싱해 쓰는 건
    타입이 사라져서 불편하다.
    """

    output: MonthlyReportOutput | CSGuidelineOutput | None
    callback: GenerationCallback


def build_monthly_callback(
    *,
    status: CallbackStatus,
    report_id: str,
    pdf_s3_meta: PdfS3Meta | None = None,
    notice_message: str | None = None,
    validation_report: dict[str, Any] | None = None,
) -> GenerationCallback:
    """월간 리포트 콜백 조립. **월 1건**만 나간다(PDF 가 월 1개 합본이라서).

    입력·출력을 인자로 받지 않는다 — 월간은 `source_payload` 를 보내지 않으므로 콜백에
    담을 것이 없다(합본이라 상품별 정보도 들어가지 않는다).
    """
    return GenerationCallback(
        report_id=report_id,
        guideline_id=None,
        status=status,
        pdf_s3_meta=pdf_s3_meta if status == CallbackStatus.SUCCESS else None,
        notice_message=notice_message,
        # ⚠️ 월간은 source_payload 를 보내지 않는다 (2026-08-03 확정).
        #    PDF 만 S3 영구 버킷에 올리고 링크로 열람하는 구조라 DB 에 데이터를 쌓지
        #    않기로 했다. 원본을 실어 보내면 저장하지도 않을 데이터가 큐에 흐른다.
        source_payload=None,
        validation_report=validation_report,
    )


def build_guideline_callback(
    input_data: CSGuidelineInput,
    output: CSGuidelineOutput | None,
    *,
    status: CallbackStatus,
    guideline_id: str,
    pdf_s3_meta: PdfS3Meta | None = None,
    notice_message: str | None = None,
    validation_report: dict[str, Any] | None = None,
) -> GenerationCallback:
    """CS 가이드라인 콜백 조립."""
    return GenerationCallback(
        report_id=None,
        guideline_id=guideline_id,
        status=status,
        pdf_s3_meta=pdf_s3_meta if status == CallbackStatus.SUCCESS else None,
        notice_message=notice_message,
        source_payload=_build_source_payload(input_data, output)
        if status == CallbackStatus.SUCCESS
        else None,
        validation_report=validation_report,
    )


def _build_source_payload(input_data: Any, output: Any) -> dict[str, Any]:
    """입력 JSON + 출력 JSON 원본. **CS 가이드라인 전용**이다(§3-2).

    PostgreSQL JSONB 컬럼에 적재되며, PDF 를 다시 만들어야 할 때의 유일한 원천이다.
    mode="json" 으로 덤프하는 이유: datetime·date·Enum 이 그대로 남으면 JSONB 직렬화가
    터진다.

    월간 리포트는 이 경로를 타지 않는다 — PDF 자체가 정본이라 원본 데이터를 보관하지 않는다.
    """
    return {
        "input": input_data.model_dump(mode="json"),
        "output": output.model_dump(mode="json") if output is not None else None,
    }


def build_hold_notice() -> str:
    """표본 부족 보류 시의 고정 안내 문구(§4-3). LLM 이 쓰는 문장이 아니다."""
    return constants.HOLD_INSUFFICIENT_DATA_NOTICE
