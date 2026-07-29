from __future__ import annotations

import logging

from app.core.schemas import MonthlyReportInput, MonthlyReportOutput

logger = logging.getLogger("MonthlyReportValidator")


def validate_monthly_report(
    input_data: MonthlyReportInput,
    output_data: MonthlyReportOutput,
) -> tuple[bool, list[str]]:
    """월간 보고서 LLM 생성 결과물과 정량 입력 데이터 간 정합성을 검증한다."""
    validation_errors: list[str] = []

    # 1. 마스터 식별자 일치 검증
    if output_data.product_group_id != input_data.product_group_id:
        validation_errors.append(
            f"product_group_id 불일치: 입력({input_data.product_group_id}) != 출력({output_data.product_group_id})"
        )

    if output_data.report_month != input_data.report_month:
        validation_errors.append(
            f"report_month 불일치: 입력({input_data.report_month}) != 출력({output_data.report_month})"
        )

    # 2. 분석 속성 커버리지 검증
    input_aspects = {stat.aspect for stat in input_data.aspect_stats}
    output_aspects = {summary.aspect for summary in output_data.aspect_summaries}

    missing_aspects = input_aspects - output_aspects
    if missing_aspects:
        missing_str = ", ".join([a.value for a in missing_aspects])
        validation_errors.append(
            f"속성 요약 누락 발생: 입력 속성 중 [{missing_str}] 항목이 요약에서 누락되었습니다."
        )

    # 3. 채널 분열 사유 필드 검증
    if not output_data.channel_divergence_cause.cause_title.strip():
        validation_errors.append("채널 분열 사유 제목(cause_title)이 비어 있습니다.")

    if not output_data.channel_divergence_cause.cause_description.strip():
        validation_errors.append("채널 분열 사유 설명(cause_description)이 비어 있습니다.")

    # 4. 분석 결과 및 권장 조치 리스트 최소 건수 검증
    if not output_data.cause_analysis_results:
        validation_errors.append("핵심 원인 분석 결과(cause_analysis_results) 목록이 비어 있습니다.")

    if not output_data.recommended_actions:
        validation_errors.append("운영/CS 권장 조치사항(recommended_actions) 목록이 비어 있습니다.")

    is_valid = len(validation_errors) == 0

    if not is_valid:
        logger.warning(
            f"[VALIDATION FAILED] report_month={input_data.report_month} | "
            f"product_group_id={input_data.product_group_id} | "
            f"errors={validation_errors}"
        )

    return is_valid, validation_errors