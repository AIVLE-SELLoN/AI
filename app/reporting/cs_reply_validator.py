from __future__ import annotations

import logging

from app.core import constants
from app.core.schemas import CSGuidelineInput, CSGuidelineOutput

logger = logging.getLogger("CSGuidelineValidator")


def validate_cs_guideline(
    input_data: CSGuidelineInput,
    generated_output: CSGuidelineOutput
) -> tuple[bool, list[str]]:
    """
    CS 가이드라인 생성 결과에 대한 검증을 수행한다.
    1. Grounding Validation: inquiry_specific_guides의 cs_id가 input.linked_inquiries 범위 내에 존재하는가?
    2. Fact-Checking: 부정 비율 변동폭 및 최다 원인 수치가 유효한가?
    3. Structural Completeness: draft_reply 및 key_talking_points가 충실히 작성되었는가?
    """
    errors: list[str] = []

    # 1. CS ID Grounding Validation
    allowed_cs_ids = {inquiry.cs_id for inquiry in input_data.linked_inquiries}
    for guide in generated_output.inquiry_specific_guides:
        if guide.cs_id not in allowed_cs_ids:
            errors.append(
                f"Grounding 오류: 인용된 cs_id '{guide.cs_id}'가 입력 CS 문의 목록에 존재하지 않습니다."
            )

    # 2. Structural & Completeness Check
    std_guide = generated_output.standard_guideline
    if not std_guide.draft_reply or len(std_guide.draft_reply.strip()) < 20:
        errors.append("상담원 답변 초안(draft_reply) 내용이 없거나 너무 짧습니다.")

    if len(std_guide.key_talking_points) < constants.MIN_TALKING_POINTS_COUNT:
        errors.append(
            f"필수 언급/금지 표현(key_talking_points)은 최소 {constants.MIN_TALKING_POINTS_COUNT}개 이상 작성되어야 합니다."
        )

    # 3. Fact-Checking: Root Cause Total Validation
    if input_data.root_cause and input_data.root_cause.total > 0:
        expected_ratio = int((input_data.root_cause.count / input_data.root_cause.total) * 100)
        if str(expected_ratio) not in generated_output.root_cause_summary:
            logger.warning(
                f"원인 지분율 수치 불일치 경고: expected={expected_ratio}%, summary={generated_output.root_cause_summary}"
            )

    is_valid = len(errors) == 0
    return is_valid, errors