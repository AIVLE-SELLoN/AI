from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from app.core import constants
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import CSGuidelineInput, CSGuidelineOutput
from app.reporting.cs_reply_validator import validate_cs_guideline
from app.reporting.pdf_compiler import ReportType, compile_report_to_pdf
from app.reporting.s3_uploader import upload_pdf_to_s3

logger = logging.getLogger("CSReplyService")


def create_fallback_cs_guideline(input_data: CSGuidelineInput) -> CSGuidelineOutput:
    """LLM 파싱 또는 검증 재시도 실패 시 실행되는 방어적 Fallback 가이드라인"""
    logger.warning(f"[FALLBACK TRIGGERED] alert_id={input_data.alert_id}")
    today_compact = datetime.now(UTC).strftime("%Y%m%d")

    return CSGuidelineOutput(
        guideline_id=f"GD-{today_compact}-{input_data.product_group_id}",
        alert_id=input_data.alert_id,
        summary={
            "issue_title": f"{input_data.product_group_id} ({input_data.channel.value}) {input_data.main_aspect.value} 대응 가이드",
            "risk_level": "WARNING",
            "key_metric_text": f"부정 비율 변동폭 Δ{int(input_data.stats.delta * 100)}%p 감지",
        },
        root_cause_summary=f"최다 원인: {input_data.root_cause.label if input_data.root_cause else '미특정'}",
        standard_guideline={
            "core_message": "상세페이지 정보 재점검 및 불만 고객 대상 즉시 무상 교환/반품 처리",
            "draft_reply": "안녕하세요 고객님, 수령하신 제품에 대해 불편을 드려 사과드립니다. 본 문의건은 무상 반품 및 교환 처리를 즉시 도와드리겠습니다.",
            "key_talking_points": [
                "1. 고객 불만 사항 정중히 경청 및 정형화된 사과 표현 전달",
                "2. 단순 변심 귀책 적용 금지 및 무상 수거 접수 진행",
            ],
        },
        ops_action_guide="운영팀 상세페이지 이미지 및 사이즈 스펙 재점검 수행 필요",
        inquiry_specific_guides=[
            {
                "item_id": inquiry.item_id,
                "recommended_point": "고객 수령 제품 무상 수거 및 교환/반품 안내",
            }
            for inquiry in input_data.linked_inquiries[:3]
        ],
        pdf_s3_meta=None,
    )


async def generate_cs_reply_pipeline(
    input_data: CSGuidelineInput,
) -> CSGuidelineOutput:
    """CS 가이드라인 생성 -> 팩트체크 검증 -> PDF 컴파일 -> S3 업로드 파이프라인"""
    client = get_llm_client()
    prompt_template = load_prompt("reporting", "cs_reply_v1")

    retry_count = 0
    feedback_context = ""
    today_compact = datetime.now(UTC).strftime("%Y%m%d")

    linked_inquiries_payload = [
        {
            "item_id": item.item_id,
            "raw_text": item.raw_text,
            "created_at": item.created_at.isoformat(),
        }
        for item in input_data.linked_inquiries
    ]

    stats_summary = (
        f"현재 부정률: {int(input_data.stats.cur_rate * 100)}%, "
        f"직전 부정률: {int(input_data.stats.past_rate * 100)}%, "
        f"변동폭: Δ{int(input_data.stats.delta * 100)}%p (총 문의 {input_data.stats.cur_total}건)"
    )

    rc_label = input_data.root_cause.label if input_data.root_cause else "미특정"
    rc_count = input_data.root_cause.count if input_data.root_cause else 0
    rc_total = input_data.root_cause.total if input_data.root_cause else 0
    root_cause_summary = f"{rc_label} ({rc_count}/{rc_total}건)"

    final_output: CSGuidelineOutput | None = None

    while retry_count <= constants.MAX_RETRY:
        replacements = {
            "{today_compact}": today_compact,
            "{alert_id}": input_data.alert_id,
            "{detected_at}": input_data.detected_at.isoformat(),
            "{product_group_id}": input_data.product_group_id,
            "{channel}": input_data.channel.value,
            "{main_aspect}": input_data.main_aspect.value,
            "{recommended_action}": input_data.recommended_action.value,
            "{stats_summary}": stats_summary,
            "{root_cause_summary}": root_cause_summary,
            "{linked_inquiries_json}": json.dumps(
                linked_inquiries_payload, ensure_ascii=False, indent=2
            ),
            "{validation_feedback}": feedback_context,
        }

        formatted_prompt = str(prompt_template)
        for key, value in replacements.items():
            formatted_prompt = formatted_prompt.replace(key, value)

        trace_key = f"alert_id={input_data.alert_id}|retry={retry_count}"
        response_json = await client.complete_json(
            prompt=formatted_prompt, trace_key=trace_key
        )

        try:
            output = CSGuidelineOutput.model_validate(response_json)
            is_valid, validation_errors = validate_cs_guideline(input_data, output)

            if is_valid:
                logger.info(f"[VALIDATION SUCCESS] {trace_key}")
                final_output = output
                break

            retry_count += 1
            feedback_context = (
                "\n[이전 생성 검증 실패 원인]:\n" + "\n".join(validation_errors)
            )
            logger.warning(
                f"[RETRY REQUIRED] {trace_key} | errors={validation_errors}"
            )

        except Exception as e:  # noqa: BLE001
            retry_count += 1
            feedback_context = f"\n[JSON 파싱/스키마 오류]: {e!s}"
            logger.error(f"[SCHEMA ERROR] {trace_key} | error={e!s}")

    if final_output is None:
        final_output = create_fallback_cs_guideline(input_data)

    try:
        pdf_bytes = compile_report_to_pdf(
            report_type=ReportType.CS_GUIDELINE,
            context={
                "guideline": final_output.model_dump(),
                "input": input_data.model_dump(),
            },
        )

        pdf_s3_meta = await upload_pdf_to_s3(
            pdf_bytes=pdf_bytes,
            report_type="cs_guidelines",
            product_group_id=input_data.product_group_id,
            identifier=input_data.alert_id,
        )

        final_output.pdf_s3_meta = pdf_s3_meta
        logger.info(f"[S3 UPLOAD SUCCESS] key={pdf_s3_meta.s3_full_key}")

    except Exception as e:  # noqa: BLE001
        logger.error(
            f"[PDF/S3 FAILED] alert_id={input_data.alert_id} | error={e!s}"
        )
        final_output.pdf_s3_meta = None

    return final_output