from __future__ import annotations

import json
import logging

from app.core import constants
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import MonthlyReportInput, MonthlyReportOutput
from app.reporting.monthly_report_validator import validate_monthly_report
from app.reporting.pdf_compiler import ReportType, compile_report_to_pdf
from app.reporting.s3_uploader import upload_pdf_to_s3

logger = logging.getLogger("MonthlyReportService")


def create_fallback_monthly_report(input_data: MonthlyReportInput) -> MonthlyReportOutput:
    """월간 보고서 연산 실패 시 반환되는 Fallback 객체"""
    logger.warning(f"[FALLBACK TRIGGERED] report_month={input_data.report_month}")

    return MonthlyReportOutput(
        report_id=f"REP-{input_data.report_month}-{input_data.product_group_id}",
        product_group_id=input_data.product_group_id,
        report_month=input_data.report_month,
        aspect_summaries=[
            {
                "aspect": stat.aspect,
                "summary_text": f"{stat.aspect.value} 속성 부정 비율 {int(stat.negative_ratio * 100)}% (변동폭 Δ{int(stat.drift_rate * 100)}%p)",
            }
            for stat in input_data.aspect_stats
        ],
        channel_divergence_cause={
            "cause_title": f"{input_data.channel_divergence.comparison_pair} 채널간 평판 격차 감지",
            "cause_description": f"JSD 점수 {input_data.channel_divergence.jsd_score:.2f} 기록. 특정 채널 중심 부정 의견 편중 모니터링 필요.",
        },
        cause_analysis_results=[
            f"1. 월간 총 VOC {input_data.total_voc_count}건 중 주요 위험 속성 점검 필요",
            "2. 다채널 수집 데이터 기준 채널별 상품 이미지 및 옵션 표기 불일치 가능성 확인",
        ],
        recommended_actions=[
            "1. 전 채널 상세페이지 상품 스펙 표기 표준화 진행",
            "2. 주요 위험 속성 불만 문의에 대한 전용 CS 응대 매뉴얼 배포",
        ],
        pdf_s3_meta=None,
    )


async def generate_monthly_report_pipeline(
    input_data: MonthlyReportInput,
) -> MonthlyReportOutput:
    """월간 보고서 생성 -> 팩트체크 검증 -> PDF 컴파일 -> S3 업로드 파이프라인"""
    client = get_llm_client()
    prompt_template = load_prompt("reporting", "monthly_report_v2")

    retry_count = 0
    feedback_context = ""

    aspect_stats_payload = [
        {
            "aspect": stat.aspect.value,
            "total_count": stat.total_count,
            "positive_ratio": stat.positive_ratio,
            "neutral_ratio": stat.neutral_ratio,
            "negative_ratio": stat.negative_ratio,
            "drift_rate": stat.drift_rate,
            "status": stat.status.value,
            "cause_distributions": [
                {
                    "cause_label": cd.cause_label,
                    "count": cd.count,
                    "ratio": cd.ratio,
                    "sample_evidences": cd.sample_evidences,
                }
                for cd in stat.cause_distributions
            ],
        }
        for stat in input_data.aspect_stats
    ]

    final_output: MonthlyReportOutput | None = None

    while retry_count <= constants.MAX_RETRY:
        replacements = {
            "{report_month}": input_data.report_month,
            "{start_date}": input_data.start_date.isoformat(),
            "{end_date}": input_data.end_date.isoformat(),
            "{product_group_id}": input_data.product_group_id,
            "{product_name}": input_data.product_name,
            "{total_voc_count}": str(input_data.total_voc_count),
            "{aspect_stats_json}": json.dumps(aspect_stats_payload, ensure_ascii=False, indent=2),
            "{comparison_pair}": input_data.channel_divergence.comparison_pair,
            "{jsd_score}": str(input_data.channel_divergence.jsd_score),
            "{is_crisis}": str(input_data.channel_divergence.is_crisis),
            "{validation_feedback}": feedback_context,
        }

        formatted_prompt = str(prompt_template)
        for key, value in replacements.items():
            formatted_prompt = formatted_prompt.replace(key, value)

        trace_key = f"report_{input_data.report_month}_{input_data.product_group_id}|retry={retry_count}"
        response_json = await client.complete_json(prompt=formatted_prompt, trace_key=trace_key)

        try:
            output = MonthlyReportOutput.model_validate(response_json)
            is_valid, validation_errors = validate_monthly_report(input_data, output)

            if is_valid:
                logger.info(f"[VALIDATION SUCCESS] {trace_key}")
                final_output = output
                break

            retry_count += 1
            feedback_context = "\n[이전 생성 검증 실패 원인]:\n" + "\n".join(validation_errors)
            logger.warning(f"[RETRY REQUIRED] {trace_key} | errors={validation_errors}")

        except Exception as e:  # noqa: BLE001
            retry_count += 1
            feedback_context = f"\n[JSON 파싱/스키마 오류]: {e!s}"
            logger.error(f"[SCHEMA ERROR] {trace_key} | error={e!s}")

    if final_output is None:
        final_output = create_fallback_monthly_report(input_data)

    try:
        pdf_bytes = compile_report_to_pdf(
            report_type=ReportType.MONTHLY_REPORT,
            context={"report": final_output.model_dump(), "input": input_data.model_dump()},
        )

        pdf_s3_meta = await upload_pdf_to_s3(
            pdf_bytes=pdf_bytes,
            report_type="monthly",
            product_group_id=input_data.product_group_id,
            identifier=input_data.report_month,
        )

        final_output.pdf_s3_meta = pdf_s3_meta
        logger.info(f"[S3 UPLOAD SUCCESS] key={pdf_s3_meta.s3_full_key}")

    except Exception as e:  # noqa: BLE001
        logger.error(f"[PDF/S3 FAILED] report_month={input_data.report_month} | error={e!s}")
        final_output.pdf_s3_meta = None

    return final_output