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
from app.core.ids import build_guideline_id as _build_guideline_id
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import (
    CallbackStatus,
    CSGuidelineInput,
    CSGuidelineOutput,
    CSGuidelineRootCause,
    CSGuidelineStatsInput,
    DetectionAlert,
    GenerationCallback,
    LinkedCSInquiry,
)
from app.reporting.callback import GenerationResult, build_guideline_callback
from app.reporting.cs_reply_validator import validate_cs_guideline
from app.reporting.pdf_compiler import ReportType, compile_report_to_pdf
from app.reporting.s3_uploader import (
    REPORT_TYPE_GUIDELINE,
    PdfSizeExceededError,
    S3NotConfiguredError,
    upload_pdf_to_s3,
)

logger = logging.getLogger("CSReplyService")

# v3: 지시문 압축 + 문의 목록을 JSON → 파이프 표로 바꾼 토큰 절감판.
# v4: guideline_id 를 서버가 계산해 주입 — 모델이 만들면 알림별 유일성이 깨진다.
PROMPT_VERSION = "cs_reply_v4"


def build_guideline_id(input_data: CSGuidelineInput) -> str:
    """alert_id 에서 파생한 가이드라인 ID. 규칙 본문은 `ids.build_guideline_id` 참고."""
    return _build_guideline_id(input_data.alert_id)


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
        # 서버가 계산한 ID 를 그대로 주입한다 — 모델이 만들면 알림별 유일성이 깨진다
        guideline_id=build_guideline_id(input_data),
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
            report_type=REPORT_TYPE_GUIDELINE,  # → cs-guideline 프리픽스 (7일 보존)
            # CS 는 대상 "기간"이 없으므로 탐지 연월을 경로 기준으로 쓴다.
            # 생성 시각이 아니라 탐지 시각이라 재생성해도 같은 폴더에 떨어진다.
            period=input_data.detected_at.strftime("%Y-%m"),
            # CS 는 알림마다 1건씩 나오므로 표시용 파일명에 alert_id 를 붙인다.
            # 없으면 같은 달 가이드라인이 전부 같은 이름이 되어 목록에서 구분이 안 된다.
            source_id=input_data.alert_id,
        )
    except S3NotConfiguredError as exc:
        # 업로드하지 않은 파일을 성공으로 보고하지 않는다(스텁 상태에서의 안전장치).
        logger.error(f"[FAILED_ERROR] {trace_base} | {exc!s}")
        return GenerationResult(
            output=None,
            callback=build_guideline_callback(
                input_data,
                final_output,
                status=CallbackStatus.FAILED_ERROR,
                guideline_id=guideline_id,
                notice_message="S3 업로드가 아직 구성되지 않아 문서를 저장하지 못했습니다.",
            ),
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


# ── 배치 진입점 ──────────────────────────────────────────────────────────


def is_guideline_target(alert: DetectionAlert) -> bool:
    """이 알림이 CS 가이드라인 생성 대상인가.

    `evidence.inquiry_ids` 가 비어 있으면 대상이 아니다. **이건 오류가 아니라 정상
    상태다** — 원인 분류([6])는 스코프 안(색상·사이즈·소재) 알림만 타므로, 파손·오배송
    같은 스코프 밖 알림은 언제나 이 목록이 비어 있다(`app/detection/alert.py` 참고).
    답변할 문의가 없는데 상담 가이드라인을 만들 수는 없다.

    ⚠️ 여기서 걸러 내지 않으면 스코프 밖 알림이 뜰 때마다 배치 요약에 "가이드라인 실패"가
       쌓인다. 정상 동작이 실패로 보이기 시작하면 요약을 아무도 안 읽게 되고, 그때부터는
       **진짜 실패도 같이 묻힌다**.

    판정 유형(`GUIDELINE_EXCLUDED_VERDICTS`)과 중복 판정은 하지 않는다 — 그건
    `CSGuidelineInput` 의 검증기가 이미 본다.
    """
    return bool(alert.evidence.inquiry_ids)


def build_guideline_input(
    alert: DetectionAlert,
    inquiries: list[LinkedCSInquiry],
    *,
    product_name: str | None = None,
) -> CSGuidelineInput:
    """`DetectionAlert` + CS 원문 → 가이드라인 입력.

    탐지 쪽 모델을 그대로 못 넘기는 이유는 두 군데가 좁아지기 때문이다:
      - `DetectionStats.source` 는 가이드라인 입력에 없다. CS·리뷰를 종합한 뒤의
        알림이라 "이 지표가 어느 쪽에서 왔는지"는 문서에 쓰지 않는다(§4-4).
      - `RootCause.consistent` 도 없다. 원인 일관성은 탐지가 판정에 쓰는 값이지
        상담원에게 보여 줄 내용이 아니다.
    남은 필드는 이름이 같아 그대로 옮긴다.

    Raises:
        ValueError: 알림은 문의를 가리키는데 넘어온 `inquiries` 가 비었을 때. 원문 조회가
            전부 실패했다는 뜻이라 근거 없는 가이드라인이 나가지 않도록 막는다
            (`build_linked_inquiries` 는 원문 없는 ID 를 버리고 경고만 남긴다).
            애초에 가리키는 문의가 없는 알림은 여기까지 오면 안 된다 —
            `is_guideline_target()` 로 먼저 거른다.
    """
    if not inquiries:
        raise ValueError(
            f"alert_id={alert.alert_id}: evidence.inquiry_ids "
            f"{alert.evidence.inquiry_ids} 에 해당하는 CS 원문을 하나도 찾지 못해 "
            "가이드라인을 만들 수 없습니다"
        )

    return CSGuidelineInput(
        alert_id=alert.alert_id,
        detected_at=alert.detected_at,
        product_group_id=alert.product_group_id,
        product_name=product_name,
        channel=alert.channel,
        main_aspect=alert.main_aspect,
        verdict=alert.verdict,
        recommended_action=alert.recommended_action,
        detection_confidence=alert.detection_confidence,
        stats=CSGuidelineStatsInput(
            cur_rate=alert.stats.cur_rate,
            past_rate=alert.stats.past_rate,
            delta=alert.stats.delta,
            cur_total=alert.stats.cur_total,
            p_value=alert.stats.p_value,
            bh_significant=alert.stats.bh_significant,
        ),
        root_cause=(
            None
            if alert.root_cause is None
            else CSGuidelineRootCause(
                label=alert.root_cause.label,
                count=alert.root_cause.count,
                total=alert.root_cause.total,
            )
        ),
        linked_inquiries=inquiries,
    )


async def generate_guideline(
    alert: DetectionAlert,
    inquiries: list[LinkedCSInquiry],
    *,
    product_name: str | None = None,
) -> GenerationCallback | None:
    """일간 배치(`app/batch/daily.py`)가 부르는 진입점.

    `generate_cs_reply_pipeline` 을 감싸기만 한다. 배치가 파이프라인을 직접 못 부르는
    이유는 입력·출력 양쪽이 다르기 때문이다 — 배치는 탐지 알림을 들고 있고, 결과를
    `publish_guideline_generated(callback, trace_id)` 로 바로 넘긴다. 그래서 여기서
    `CSGuidelineInput` 을 조립하고 `GenerationResult` 에서 콜백만 꺼내 돌려준다.

    반환값 세 가지를 구분한다:
      콜백(SUCCESS)      정상 생성. 배치가 그대로 발행한다.
      콜백(FAILED_*)     생성은 시도했으나 실패. **None 이 아니다** — 배치가
                         `if guideline is not None` 로 발행 여부를 가르는데 실패를
                         None 으로 돌려주면 백엔드가 "생성 중"에서 영영 못 벗어난다.
      None               애초에 생성 대상이 아님(`is_guideline_target()` 참고).
                         실패가 아니라 게이트라 발행할 것도, 요약에 남길 것도 없다.

    Raises:
        ValueError: 대상 알림인데 원문 조회가 전부 실패한 경우. 이건 진짜 실패라
            배치 요약에 남아야 한다.
    """
    if not is_guideline_target(alert):
        logger.info(
            f"[SKIP] alert_id={alert.alert_id} | 가리키는 CS 문의가 없어 가이드라인 대상이 "
            f"아닙니다 (main_aspect={alert.main_aspect.value}, scope_in={alert.scope_in})"
        )
        return None

    input_data = build_guideline_input(alert, inquiries, product_name=product_name)
    result = await generate_cs_reply_pipeline(input_data)
    return result.callback
