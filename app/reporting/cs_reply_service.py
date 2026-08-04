"""CS 가이드라인 생성 파이프라인 — 문서 생성 스키마 §2·§3·§4.

흐름은 월간 리포트와 같다(생성 → 검증 → 재시도 → PDF/S3 → 콜백). 다른 점 두 가지:
  - 표본 부족 보류(HOLD)가 없다. CS 가이드라인은 이미 발화한 알림 1건에 대응하는
    문서라, 문의가 적어도 상담원은 답을 해야 한다.
  - 그라운딩 대상이 수치뿐 아니라 **cs_id 포함관계**다(없는 문의를 가리키면 반려).
  - 출력 데이터를 DB 에 적재한다 — 콜백의 source_payload(입력+출력 JSON)가 필수이며,
    그 원본으로 PDF 를 언제든 재컴파일할 수 있다(월간과 달리 PDF 가 정본이 아니다).

⚠️ 월간 리포트와 마찬가지로 fallback 생성물을 성공처럼 내보내지 않는다.
   검증을 통과 못 하면 FAILED_VALIDATION 으로 운영자에게 넘긴다(§4-3).
"""

from __future__ import annotations

import json
import logging
from string import Template

from app.core import constants
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import CallbackStatus, CSGuidelineInput, CSGuidelineOutput
from app.reporting.callback import GenerationResult, build_guideline_callback
from app.reporting.cs_reply_validator import validate_cs_guideline
from app.reporting.pdf_compiler import ReportType, compile_report_to_pdf
from app.reporting.s3_uploader import (
    REPORT_TYPE_GUIDELINE,
    PdfSizeExceededError,
    upload_pdf_to_s3,
)

logger = logging.getLogger("CSReplyService")

# v3: 지시문 압축 + 문의 목록을 JSON → 파이프 표로 바꾼 토큰 절감판.
PROMPT_VERSION = "cs_reply_v3"


def build_guideline_id(input_data: CSGuidelineInput) -> str:
    """GD-{탐지일 YYYYMMDD}-{마스터 상품 그룹}. 예: GD-20260528-P001

    생성 시각이 아니라 **탐지 시각**을 쓴다 — 재생성해도 같은 알림이면 같은 ID 가 나와야
    Spring Boot 쪽에서 중복 문서를 구분할 수 있다.
    """
    return f"GD-{input_data.detected_at.strftime('%Y%m%d')}-{input_data.product_group_id}"


def _build_stats_summary(input_data: CSGuidelineInput) -> str:
    """프롬프트에 넣을 지표 요약 문장.

    p_value·bh_significant 는 넣지 않는다 — §4-4 금지 표현이라 모델에게 보여주지 않는다.
    """
    stats = input_data.stats
    return (
        f"현재 부정률 {stats.cur_rate * 100:.0f}%, 직전 부정률 {stats.past_rate * 100:.0f}%, "
        f"변동폭 {stats.delta * 100:.0f}%p (현재 윈도우 총 문의 {stats.cur_total}건)"
    )


def _build_root_cause_summary(input_data: CSGuidelineInput) -> str:
    """최다 원인 요약. 원인이 없으면 고정 대체 문구를 넘겨 출력에도 그대로 남게 한다."""
    root_cause = input_data.root_cause
    if root_cause is None:
        return constants.ROOT_CAUSE_UNSPECIFIED_TEXT

    share = (root_cause.count / root_cause.total * 100) if root_cause.total else 0.0
    return f"{root_cause.label} {root_cause.count}건 / 전체 {root_cause.total}건 ({share:.0f}%)"


def _build_inquiry_table(input_data: CSGuidelineInput) -> str:
    """문의 목록을 `문의ID|원문` 표로 (v3 이상에서 사용).

    토큰이 가장 많이 걸리는 자리다 — 문의가 수십 건이면 JSON 은 건마다
    `{"item_id": ..., "raw_text": ..., "created_at": ...}` 키를 되풀이한다.
    created_at 은 뺐다. 출력 어느 필드도 문의 시각을 쓰지 않아 순수 낭비였다.
    파이프·줄바꿈은 표가 깨지지 않게 치환한다.
    """
    rows = []
    for item in input_data.linked_inquiries:
        text = item.raw_text.replace("|", "/").replace("\n", " ").strip()
        rows.append(f"{item.item_id}|{text}")
    return "\n".join(rows)


def build_prompt(
    input_data: CSGuidelineInput,
    *,
    feedback: str = "",
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """완성된 프롬프트 문자열. 파이프라인과 eval 이 같은 함수를 쓰도록 밖으로 뺐다.

    구버전(v2)이 쓰는 `linked_inquiries_json` 도 같이 넘긴다 — 버전 비교 실험이
    깨지지 않게. Template 은 안 쓰는 키를 무시하므로 토큰에는 영향이 없다.
    """
    template = Template(load_prompt("reporting", prompt_version))
    linked_inquiries_json = json.dumps(
        [
            {
                "item_id": item.item_id,
                "raw_text": item.raw_text,
                "created_at": item.created_at.isoformat(),
            }
            for item in input_data.linked_inquiries
        ],
        ensure_ascii=False,
    )
    return template.substitute(
        inquiry_table=_build_inquiry_table(input_data),
        alert_id=input_data.alert_id,
        detected_at=input_data.detected_at.isoformat(),
        product_group_id=input_data.product_group_id,
        product_name=input_data.product_name or "미상",
        channel=input_data.channel.value,
        main_aspect=input_data.main_aspect.value,
        verdict=input_data.verdict.value,
        recommended_action=input_data.recommended_action.value,
        detection_confidence=input_data.detection_confidence.value,
        stats_summary=_build_stats_summary(input_data),
        root_cause_summary=_build_root_cause_summary(input_data),
        linked_inquiries_json=linked_inquiries_json,
        validation_feedback=feedback,
    )


async def generate_cs_reply_pipeline(
    input_data: CSGuidelineInput,
) -> GenerationResult:
    """CS 가이드라인 생성 → 검증 → PDF → S3 → 콜백."""
    guideline_id = build_guideline_id(input_data)
    trace_base = f"alert_id={input_data.alert_id}"

    client = get_llm_client()

    feedback_context = ""
    last_errors: list[str] = []
    final_output: CSGuidelineOutput | None = None

    for attempt in range(1 + constants.MAX_RETRY):
        prompt = build_prompt(input_data, feedback=feedback_context)

        trace_key = f"{trace_base}|attempt={attempt + 1}"
        try:
            response_json = await client.complete_json(prompt=prompt, trace_key=trace_key)
            output = CSGuidelineOutput.model_validate(response_json)
        except Exception as exc:  # noqa: BLE001 — 호출·파싱·스키마 오류를 재시도로 흡수
            last_errors = [f"JSON 파싱/스키마 오류: {exc!s}"]
            feedback_context = f"\n[이전 시도 실패 — 아래를 고쳐 다시 작성하세요]\n{last_errors[0]}"
            logger.warning(f"[SCHEMA ERROR] {trace_key} | {exc!s}")
            continue

        is_valid, errors = validate_cs_guideline(input_data, output)
        if is_valid:
            logger.info(f"[VALIDATION SUCCESS] {trace_key}")
            final_output = output
            break

        last_errors = errors
        feedback_context = "\n[이전 시도 검증 실패 — 아래를 고쳐 다시 작성하세요]\n" + "\n".join(errors)
        logger.warning(f"[RETRY REQUIRED] {trace_key} | errors={errors}")

    if final_output is None:
        logger.error(f"[FAILED_VALIDATION] {trace_base} | 최종 실패 사유={last_errors}")
        return GenerationResult(
            output=None,
            callback=build_guideline_callback(
                input_data,
                None,
                status=CallbackStatus.FAILED_VALIDATION,
                guideline_id=guideline_id,
                notice_message="CS 가이드라인 생성 결과가 검증을 통과하지 못했습니다. 운영자 확인이 필요합니다.",
                validation_report={"attempts": 1 + constants.MAX_RETRY, "errors": last_errors},
            ),
        )

    try:
        pdf_bytes = compile_report_to_pdf(
            report_type=ReportType.CS_GUIDELINE,
            context={
                "guideline": final_output.model_dump(mode="json"),
                "input": input_data.model_dump(mode="json"),
            },
        )
        pdf_s3_meta = await upload_pdf_to_s3(
            pdf_bytes=pdf_bytes,
            report_type=REPORT_TYPE_GUIDELINE,  # → 임시 버킷 (DB 원본으로 재컴파일 가능)
            product_group_id=input_data.product_group_id,
            identifier=input_data.alert_id,
        )
    except PdfSizeExceededError as exc:
        logger.error(f"[FAILED_SIZE_EXCEEDED] {trace_base} | {exc!s}")
        return GenerationResult(
            output=final_output,
            callback=build_guideline_callback(
                input_data,
                final_output,
                status=CallbackStatus.FAILED_SIZE_EXCEEDED,
                guideline_id=guideline_id,
                notice_message="가이드라인 파일 용량이 상한을 초과해 발송이 중단되었습니다.",
            ),
        )
    except Exception:  # PDF/S3 계층 오류는 유형을 가리지 않고 FAILED_ERROR 로 수렴시킨다
        logger.exception(f"[FAILED_ERROR] {trace_base}")
        return GenerationResult(
            output=final_output,
            callback=build_guideline_callback(
                input_data,
                final_output,
                status=CallbackStatus.FAILED_ERROR,
                guideline_id=guideline_id,
                notice_message="가이드라인 파일 생성 중 오류가 발생했습니다.",
            ),
        )

    final_output.pdf_s3_meta = pdf_s3_meta
    logger.info(f"[SUCCESS] {trace_base} | s3_key={pdf_s3_meta.s3_full_key}")

    return GenerationResult(
        output=final_output,
        callback=build_guideline_callback(
            input_data,
            final_output,
            status=CallbackStatus.SUCCESS,
            guideline_id=guideline_id,
            pdf_s3_meta=pdf_s3_meta,
        ),
    )
