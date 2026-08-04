"""월간 보고서 출력 검증 — 문서 생성 스키마 §4-4.

계층 분담:
  - **구조**(미정의 키·배열 길이·문자열 길이·enum 도메인) → `MonthlyReportOutput` 스키마가 담당.
  - **도메인**(비율 합·status 경계·게이트 null 정합·s3_full_key 조합) → 입력 스키마 validator 가 담당.
  - **그라운딩**(수치 팩트체크·단계 라벨 대조·금지 표현) → **이 모듈**.

즉 여기서는 "스키마를 통과한 JSON 이 입력 데이터와 말이 맞는가"만 본다.
반환값이 (통과여부, 사유목록)인 이유: 사유를 그대로 프롬프트에 되먹여 재생성시키기 때문이다.
"""

from __future__ import annotations

import logging

from app.core.constants import SEVERITY_STAGE_LABEL
from app.core.schemas import MonthlyReportInput, MonthlyReportOutput
from app.reporting.grounding import (
    UNIT_COUNT,
    UNIT_PERCENT,
    UNIT_PERCENT_POINT,
    UNIT_SCORE,
    check_numbers_grounded,
    find_forbidden_expressions,
    ratio_to_percent,
)

logger = logging.getLogger("MonthlyReportValidator")


def build_allowed_numbers(input_data: MonthlyReportInput) -> dict[str, set[float]]:
    """입력 데이터에서 "출력에 등장해도 되는 수치" 집합을 만든다.

    LLM 은 여기 있는 수치만 문장에 쓸 수 있다. 반올림 표기(12.5% → 13%)를 허용하려고
    소수 원본과 반올림값을 같이 넣어 둔다.
    """
    percents: set[float] = set()
    percent_points: set[float] = set()
    counts: set[float] = {float(input_data.total_voc_count)}
    scores: set[float] = set()

    for dist in input_data.aspect_distributions:
        for ratio in (dist.positive_ratio, dist.neutral_ratio, dist.negative_ratio):
            value = ratio_to_percent(ratio)
            percents.update({value, round(value)})
        counts.add(float(dist.total_count))

    for drift in input_data.sentiment_drifts:
        value = ratio_to_percent(drift.drift_rate)
        # 부호 없이 "8%p 상승" 으로 쓰는 경우가 많아 절댓값도 허용한다
        percent_points.update({value, round(value), abs(value), round(abs(value))})

    for pair in input_data.channel_divergence.pairs:
        counts.add(float(pair.sample_size))
        for score in (pair.jsd_score, pair.jsd_baseline):
            if score is not None:
                scores.update({round(score, 4), round(score, 2)})

    return {
        UNIT_PERCENT: percents,
        UNIT_PERCENT_POINT: percent_points,
        UNIT_COUNT: counts,
        UNIT_SCORE: scores,
    }


def _validate_stage_label(
    input_data: MonthlyReportInput,
    output_data: MonthlyReportOutput,
) -> list[str]:
    """cause_title 이 worst_pair 의 severity 단계 라벨을 정확히 담고 있는지 검사(§1-2).

    전 쌍이 보류(severity=null)면 단계 자체가 없으므로 검사하지 않는다.
    """
    divergence = input_data.channel_divergence
    worst = next(
        (p for p in divergence.pairs if p.comparison_pair == divergence.worst_pair), None
    )
    if worst is None or worst.severity is None:
        return []

    required = SEVERITY_STAGE_LABEL[worst.severity.value]
    title = output_data.channel_divergence_cause.cause_title
    errors: list[str] = []

    if required not in title:
        errors.append(
            f"단계 라벨 누락: cause_title 에 '{required}' 가 그대로 포함돼야 합니다 (현재: '{title}')"
        )

    for key, label in SEVERITY_STAGE_LABEL.items():
        if key != worst.severity.value and label in title:
            errors.append(
                f"단계 라벨 혼입: cause_title 에 다른 단계 '{label}' 가 섞였습니다 (기대: '{required}')"
            )

    return errors


def validate_monthly_report(
    input_data: MonthlyReportInput,
    output_data: MonthlyReportOutput,
) -> tuple[bool, list[str]]:
    """월간 보고서 LLM 출력의 그라운딩 검증. (통과여부, 사유목록)."""
    errors: list[str] = []

    # 1. 식별자 일치 — 입력값 그대로 되돌려줘야 한다(§1-2)
    if output_data.master_product_code != input_data.master_product_code:
        errors.append(
            f"master_product_code 불일치: 입력({input_data.master_product_code}) "
            f"!= 출력({output_data.master_product_code})"
        )
    if output_data.report_month != input_data.report_month:
        errors.append(
            f"report_month 불일치: 입력({input_data.report_month}) != 출력({output_data.report_month})"
        )

    # 2. 속성 커버리지 — aspect 집합이 입력과 같아야 한다
    input_aspects = {d.aspect for d in input_data.aspect_distributions}
    output_aspects = {s.aspect for s in output_data.aspect_summaries}
    if missing := input_aspects - output_aspects:
        errors.append(
            f"속성 요약 누락: [{', '.join(a.value for a in missing)}] 가 aspect_summaries 에 없습니다."
        )
    if extra := output_aspects - input_aspects:
        errors.append(
            f"입력에 없는 속성 생성: [{', '.join(a.value for a in extra)}]"
        )

    # 3. 단계 라벨 대조 (§1-2)
    errors.extend(_validate_stage_label(input_data, output_data))

    # 4. 수치 팩트체크 + 금지 표현 — §4-4 가 지정한 대상 필드에만 적용
    allowed = build_allowed_numbers(input_data)
    targets: list[tuple[str, str]] = [
        (f"aspect_summaries[{s.aspect.value}].summary_text", s.summary_text)
        for s in output_data.aspect_summaries
    ]
    targets.append(
        ("channel_divergence_cause.cause_title", output_data.channel_divergence_cause.cause_title)
    )
    targets.append(
        (
            "channel_divergence_cause.cause_description",
            output_data.channel_divergence_cause.cause_description,
        )
    )
    targets.extend(
        (f"cause_analysis_results[{i}]", text)
        for i, text in enumerate(output_data.cause_analysis_results)
    )

    for field_name, text in targets:
        errors.extend(check_numbers_grounded(text, allowed, field_name=field_name))
        errors.extend(find_forbidden_expressions(text, field_name=field_name))

    # recommended_actions 는 수치 팩트체크 대상이 아니지만(조치 문장이라 수치가 없다),
    # 금지 표현은 어느 필드에도 나오면 안 된다.
    for i, text in enumerate(output_data.recommended_actions):
        errors.extend(find_forbidden_expressions(text, field_name=f"recommended_actions[{i}]"))

    is_valid = not errors
    if not is_valid:
        logger.warning(
            f"[VALIDATION FAILED] report_month={input_data.report_month} | "
            f"master_product_code={input_data.master_product_code} | errors={errors}"
        )
    return is_valid, errors
