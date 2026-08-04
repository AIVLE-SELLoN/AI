"""CS 가이드라인 출력 검증 — 문서 생성 스키마 §4-4.

계층 분담은 monthly_report_validator 와 같다 — 구조·도메인은 스키마가, 그라운딩은 여기가.

CS 쪽에서 특히 중요한 두 가지:
  - **cs_id 포함관계** — 맞춤 가이드가 실제로 존재하는 문의만 가리켜야 한다.
    없는 문의에 대한 응대 지침이 나가면 상담원이 헛짚는다.
  - **제외 필드** — `standard_guideline.*` / `ops_action_guide` /
    `inquiry_specific_guides[].recommended_point` 는 수치 팩트체크에서 빼야 한다.
    보상 정책 상수(무상 교환 기간 등 입력에 없는 숫자)가 정당하게 들어가는 자리다.
    단, 금지 표현(p값·FDR)은 어느 필드에서도 허용하지 않는다.
"""

from __future__ import annotations

import logging

from app.core import constants
from app.core.schemas import CSGuidelineInput, CSGuidelineOutput
from app.reporting.grounding import (
    UNIT_COUNT,
    UNIT_PERCENT,
    UNIT_PERCENT_POINT,
    UNIT_SCORE,
    check_numbers_grounded,
    find_forbidden_expressions,
    ratio_to_percent,
)
from app.reporting.ids import build_guideline_id

logger = logging.getLogger("CSGuidelineValidator")


def build_allowed_numbers(input_data: CSGuidelineInput) -> dict[str, set[float]]:
    """입력에서 "출력에 등장해도 되는 수치" 집합을 만든다."""
    stats = input_data.stats
    percents: set[float] = set()
    for ratio in (stats.cur_rate, stats.past_rate):
        value = ratio_to_percent(ratio)
        percents.update({value, round(value)})

    delta_percent = ratio_to_percent(stats.delta)
    percent_points: set[float] = {
        delta_percent,
        round(delta_percent),
        abs(delta_percent),
        round(abs(delta_percent)),
    }

    counts: set[float] = {float(stats.cur_total), float(len(input_data.linked_inquiries))}

    if input_data.root_cause is not None:
        counts.update({float(input_data.root_cause.count), float(input_data.root_cause.total)})
        if input_data.root_cause.total > 0:
            share = ratio_to_percent(input_data.root_cause.count / input_data.root_cause.total)
            percents.update({share, round(share)})

    return {
        UNIT_PERCENT: percents,
        UNIT_PERCENT_POINT: percent_points,
        UNIT_COUNT: counts,
        UNIT_SCORE: set(),  # CS 가이드라인에는 단위 없는 점수가 없다
    }


def validate_cs_guideline(
    input_data: CSGuidelineInput,
    generated_output: CSGuidelineOutput,
) -> tuple[bool, list[str]]:
    """CS 가이드라인 LLM 출력의 그라운딩 검증. (통과여부, 사유목록)."""
    errors: list[str] = []

    # 1. 식별자 일치 — alert_id 와 그로부터 파생되는 guideline_id 둘 다 본다.
    #    가이드라인은 알림과 1:1 이라, ID 가 어긋나면 백엔드 upsert 에서 다른 알림의
    #    가이드라인을 덮어쓰게 된다.
    if generated_output.alert_id != input_data.alert_id:
        errors.append(
            f"alert_id 불일치: 입력({input_data.alert_id}) != 출력({generated_output.alert_id})"
        )

    expected_guideline_id = build_guideline_id(input_data.alert_id)
    if generated_output.guideline_id != expected_guideline_id:
        errors.append(
            f"guideline_id 불일치: 기대({expected_guideline_id}) "
            f"!= 출력({generated_output.guideline_id})"
        )

    # 2. cs_id 포함관계 — 맞춤 가이드는 linked_inquiries 안의 문의만 가리켜야 한다
    allowed_item_ids = {inquiry.item_id for inquiry in input_data.linked_inquiries}
    for guide in generated_output.inquiry_specific_guides:
        if guide.item_id not in allowed_item_ids:
            errors.append(
                f"Grounding 오류: 인용된 item_id '{guide.item_id}' 가 입력 CS 문의 목록에 없습니다."
            )

    # 3. root_cause 가 없으면 대체 문구가 반드시 들어가야 한다(§2-2)
    if input_data.root_cause is None:
        if constants.ROOT_CAUSE_UNSPECIFIED_TEXT not in generated_output.root_cause_summary:
            errors.append(
                f"root_cause 가 null 이면 root_cause_summary 에 "
                f"'{constants.ROOT_CAUSE_UNSPECIFIED_TEXT}' 가 포함돼야 합니다."
            )
    elif input_data.root_cause.label not in generated_output.root_cause_summary:
        errors.append(
            f"root_cause_summary 에 최다 원인 라벨 '{input_data.root_cause.label}' 이 없습니다."
        )

    # 4. 수치 팩트체크 — §4-4 대상 필드에만 적용
    allowed = build_allowed_numbers(input_data)
    factcheck_targets: list[tuple[str, str]] = [
        ("summary.key_metric_text", generated_output.summary.key_metric_text),
        ("root_cause_summary", generated_output.root_cause_summary),
    ]
    for field_name, text in factcheck_targets:
        errors.extend(check_numbers_grounded(text, allowed, field_name=field_name))

    # 5. 금지 표현 — 제외 필드 포함 전 필드 검사
    std = generated_output.standard_guideline
    all_texts: list[tuple[str, str]] = [
        *factcheck_targets,
        ("summary.issue_title", generated_output.summary.issue_title),
        ("standard_guideline.core_message", std.core_message),
        ("standard_guideline.draft_reply", std.draft_reply),
        *(
            (f"standard_guideline.key_talking_points[{i}]", point)
            for i, point in enumerate(std.key_talking_points)
        ),
        ("ops_action_guide", generated_output.ops_action_guide),
        *(
            (f"inquiry_specific_guides[{g.item_id}].recommended_point", g.recommended_point)
            for g in generated_output.inquiry_specific_guides
        ),
    ]
    for field_name, text in all_texts:
        errors.extend(find_forbidden_expressions(text, field_name=field_name))

    is_valid = not errors
    if not is_valid:
        logger.warning(
            f"[VALIDATION FAILED] alert_id={input_data.alert_id} | errors={errors}"
        )
    return is_valid, errors
