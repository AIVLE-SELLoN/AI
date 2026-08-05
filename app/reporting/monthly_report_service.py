"""월간 리포트 생성 파이프라인 — 문서 생성 스키마 §1·§3·§4.

⚠️ **PDF 는 월 1개 합본이고, 그게 유일한 산출물이다** (2026-08-03 확정).
   데이터를 DB 에 적재하지 않고, S3 에 올린 합본 PDF 의 링크만 메시지 큐로 보낸다.
   UI 는 **첫 페이지(표지)만 화면에 띄우고** 전체는 presigned URL 로 내려받는다.
   따라서
     - 콜백에 `source_payload` 를 싣지 않는다(저장하지 않을 데이터를 큐에 흘리지 않는다).
     - 콜백은 **월 1건**이다. 상품별로 나가지 않는다.
     - PDF 안에 수치 표·차트까지 모두 들어가야 한다(pdf_compiler 가 자립 문서를 만든다).

흐름(두 단계로 갈라진다):

    [상품별] generate_monthly_report_output()
        [보류 게이트] total_voc_count < 10 → LLM 미호출, HOLD
            ↓
        [생성] LLM → MonthlyReportOutput 파싱
            ↓
        [검증] 실패 시 사유를 프롬프트에 되먹여 재시도 (총 1 + MAX_RETRY 회)
            ↓ 3회 실패 → FAILED_VALIDATION (그 상품만 빠지고 배치는 계속된다)

    [월 단위] compile_and_upload_monthly_book()
        성공한 상품들을 모아 합본 PDF → S3 → SUCCESS 콜백 1건
        용량 초과면 FAILED_SIZE_EXCEEDED, 그 외 예외는 FAILED_ERROR

⚠️ fallback 생성물을 만들어 성공처럼 내보내지 않는다. 검증을 통과 못 한 문서를
   셀러에게 자동 발송하면 틀린 수치가 그대로 나가기 때문이다(§4-3 FAILED_VALIDATION 은
   "운영자 알림, 자동 발송 중단"이다).
"""

from __future__ import annotations

import json
import logging
from string import Template
from typing import Any

from app.core import constants
from app.core.constants import SEVERITY_STAGE_LABEL
from app.core.llm_client import get_llm_client
from app.core.prompts import load_prompt
from app.core.schemas import CallbackStatus, MonthlyReportInput, MonthlyReportOutput
from app.reporting.callback import GenerationResult, build_monthly_callback
from app.reporting.monthly_report_validator import validate_monthly_report
from app.reporting.pdf_compiler import build_book_context, compile_monthly_book
from app.reporting.s3_uploader import (
    REPORT_TYPE_MONTHLY,
    PdfSizeExceededError,
    S3NotConfiguredError,
    upload_pdf_to_s3,
)

logger = logging.getLogger("MonthlyReportService")

# 프롬프트 버전 — 매직스트링 방지. 교체 시 이 한 줄만 바꾼다(구버전 파일은 남겨둔다).
# v4: 지시문 압축 + 데이터를 JSON → 파이프 표로 바꾼 토큰 절감판.
# v5: 채널쌍별 원인·조치(channel_pair_analyses) 생성 추가 — 리포트가 게이지마다 따로 보여준다.
PROMPT_VERSION = "monthly_report_v5"


def build_report_id(input_data: MonthlyReportInput) -> str:
    """상품별 추적용 ID. 예: RPT-202607-P001 — 로그·배치 결과에서만 쓴다."""
    return f"RPT-{input_data.report_month.replace('-', '')}-{input_data.product_group_id}"


def build_book_report_id(report_month: str) -> str:
    """월간 합본의 **멱등 키**. 예: RPT-202607

    PDF 가 월 1개라 콜백·이벤트도 월 1건이다. 상품 코드가 들어가지 않는다.
    같은 달을 다시 돌려도 같은 ID 라 메인이 upsert 하면 된다.
    """
    return f"RPT-{report_month.replace('-', '')}"


def _resolve_stage_label(input_data: MonthlyReportInput) -> str:
    """worst_pair 의 severity 단계 라벨. 전 쌍 보류면 안정 단계로 안내한다.

    프롬프트에 이 문자열을 그대로 넣어야 cause_title 이 §1-2 대조를 통과한다.
    """
    divergence = input_data.channel_divergence
    worst = next(
        (p for p in divergence.pairs if p.comparison_pair == divergence.worst_pair), None
    )
    if worst is None or worst.severity is None:
        return SEVERITY_STAGE_LABEL["SAFE"]
    return SEVERITY_STAGE_LABEL[worst.severity.value]


def _fmt(value: float) -> str:
    """표에 넣을 짧은 수 표기. 정수면 소수점을 떼서 토큰을 아낀다(20.0 → 20)."""
    return f"{value:g}"


def _build_prompt_tables(input_data: MonthlyReportInput) -> dict[str, str]:
    """입력 모델 → 파이프 구분 표 문자열 (v4 이상에서 사용).

    JSON 대신 표를 쓰는 이유는 토큰이다. JSON 은 행마다 키 이름을 되풀이해서
    (aspect/total_count/positive_ratio…) 3행이면 키만 20회 넘게 실린다.

    비율을 0.5 가 아니라 50(%) 로 **미리 환산해서** 넣는 것도 의도된 것이다.
    모델이 소수→퍼센트를 직접 계산하면 반올림이 어긋나 수치 팩트체크에서 반려되기
    쉬운데, 계산된 값을 그대로 주면 옮겨 적기만 하면 된다(토큰·정확도 양쪽 이득).
    """
    aspect_rows = []
    drift_by_aspect = {d.aspect: d for d in input_data.sentiment_drifts}
    for dist in input_data.aspect_distributions:
        drift = drift_by_aspect.get(dist.aspect)
        aspect_rows.append(
            "|".join(
                [
                    dist.aspect.value,
                    str(dist.total_count),
                    _fmt(dist.positive_ratio * 100),
                    _fmt(dist.neutral_ratio * 100),
                    _fmt(dist.negative_ratio * 100),
                    f"{drift.drift_rate * 100:+g}" if drift else "-",
                    drift.status.value if drift else "-",
                ]
            )
        )

    pair_rows = []
    for pair in input_data.channel_divergence.pairs:
        if pair.hold_reason is not None:
            pair_rows.append(f"{pair.comparison_pair}|{pair.sample_size}|-|보류")
        else:
            pair_rows.append(
                f"{pair.comparison_pair}|{pair.sample_size}|"
                f"{_fmt(round(pair.jsd_score, 2))}|{pair.severity.value}"
            )

    return {
        "aspect_table": "\n".join(aspect_rows),
        "pair_table": "\n".join(pair_rows),
        "worst_pair": input_data.channel_divergence.worst_pair,
    }


def _build_prompt_payloads(input_data: MonthlyReportInput) -> dict[str, str]:
    """입력 모델 → 프롬프트에 넣을 JSON 문자열들 (v3 이하 호환용).

    v4 부터는 _build_prompt_tables() 의 표를 쓰지만, `--compare monthly_report_v3,v4`
    처럼 구버전을 다시 돌리는 실험이 깨지지 않도록 두 형태를 모두 넘긴다.
    Template.substitute 는 쓰지 않는 키를 무시하므로, 실제 프롬프트에 실리는 것은
    그 버전이 참조하는 쪽뿐이다(안 쓰는 형식은 토큰을 먹지 않는다).

    p_value 는 어느 형식에도 넣지 않는다 — §4-4 금지 표현이라 애초에 모델에게 보여주지
    않는다(보여주면 문장에 옮겨 적고 검증에서 반려되는 낭비가 생긴다).
    """
    aspect_distributions = [
        {
            "aspect": d.aspect.value,
            "total_count": d.total_count,
            "positive_ratio": d.positive_ratio,
            "neutral_ratio": d.neutral_ratio,
            "negative_ratio": d.negative_ratio,
        }
        for d in input_data.aspect_distributions
    ]
    sentiment_drifts = [
        {
            "aspect": d.aspect.value,
            "drift_rate": d.drift_rate,
            "status": d.status.value,
            "baseline_recalculated": d.baseline_recalculated,
        }
        for d in input_data.sentiment_drifts
    ]
    divergence = input_data.channel_divergence
    channel_divergence = {
        "worst_pair": divergence.worst_pair,
        "pairs": [
            {
                "comparison_pair": p.comparison_pair,
                "sample_size": p.sample_size,
                "jsd_score": p.jsd_score,
                "severity": p.severity.value if p.severity else None,
                "hold_reason": p.hold_reason.value if p.hold_reason else None,
            }
            for p in divergence.pairs
        ],
    }

    return {
        "aspect_distributions_json": json.dumps(aspect_distributions, ensure_ascii=False),
        "sentiment_drifts_json": json.dumps(sentiment_drifts, ensure_ascii=False),
        "channel_divergence_json": json.dumps(channel_divergence, ensure_ascii=False),
    }


def build_prompt(
    input_data: MonthlyReportInput,
    *,
    feedback: str = "",
    prompt_version: str = PROMPT_VERSION,
) -> str:
    """완성된 프롬프트 문자열. 파이프라인과 eval 이 같은 함수를 쓰도록 밖으로 뺐다.

    ⚠️ str.format() 금지(프롬프트의 JSON 중괄호와 충돌) — Template + $플레이스홀더를 쓴다.
    """
    template = Template(load_prompt("reporting", prompt_version))
    return template.substitute(
        report_month=input_data.report_month,
        start_date=input_data.start_date.isoformat(),
        end_date=input_data.end_date.isoformat(),
        master_product_code=input_data.product_group_id,
        product_name=input_data.product_name,
        total_voc_count=str(input_data.total_voc_count),
        stage_label=_resolve_stage_label(input_data),
        validation_feedback=feedback,
        **_build_prompt_tables(input_data),
        **_build_prompt_payloads(input_data),
    )


async def generate_monthly_report_output(
    input_data: MonthlyReportInput,
) -> tuple[MonthlyReportOutput | None, CallbackStatus, list[str]]:
    """상품 1건의 문장 생성 + 검증. **PDF·S3·콜백은 여기서 하지 않는다.**

    월간 PDF 가 상품별이 아니라 **월 1개 합본**으로 바뀌면서(2026-08-03 확정) 파이프라인이
    둘로 갈라졌다:
        [상품별] 이 함수 — LLM 생성·검증까지만. 산출물은 MonthlyReportOutput.
        [월 단위] compile_and_upload_monthly_book() — 합본 PDF·S3·콜백 1건.

    Returns:
        (출력, 상태, 실패 사유). 상태는 SUCCESS / HOLD_INSUFFICIENT_DATA /
        FAILED_VALIDATION 중 하나다.
    """
    report_id = build_report_id(input_data)
    trace_base = f"report_id={report_id}"

    # [보류 게이트] 표본이 모자라면 LLM 을 아예 태우지 않는다(§4-3)
    if input_data.total_voc_count < constants.MIN_VOC_COUNT_FOR_REPORT:
        logger.info(
            f"[HOLD] {trace_base} | total_voc_count={input_data.total_voc_count} "
            f"< {constants.MIN_VOC_COUNT_FOR_REPORT}"
        )
        return None, CallbackStatus.HOLD_INSUFFICIENT_DATA, []

    client = get_llm_client()
    feedback_context = ""
    last_errors: list[str] = []

    for attempt in range(1 + constants.MAX_RETRY):
        prompt = build_prompt(input_data, feedback=feedback_context)
        trace_key = f"{trace_base}|attempt={attempt + 1}"
        try:
            response_json = await client.complete_json(prompt=prompt, trace_key=trace_key)
            output = MonthlyReportOutput.model_validate(response_json)
        except Exception as exc:  # noqa: BLE001 — 호출·파싱·스키마 오류를 재시도로 흡수
            last_errors = [f"JSON 파싱/스키마 오류: {exc!s}"]
            feedback_context = (
                "\n[이전 시도 실패 — 아래를 고쳐 다시 작성하세요]\n" + last_errors[0]
            )
            logger.warning(f"[SCHEMA ERROR] {trace_key} | {exc!s}")
            continue

        is_valid, errors = validate_monthly_report(input_data, output)
        if is_valid:
            logger.info(f"[VALIDATION SUCCESS] {trace_key}")
            return output, CallbackStatus.SUCCESS, []

        last_errors = errors
        feedback_context = "\n[이전 시도 검증 실패 — 아래를 고쳐 다시 작성하세요]\n" + "\n".join(
            errors
        )
        logger.warning(f"[RETRY REQUIRED] {trace_key} | errors={errors}")

    logger.error(f"[FAILED_VALIDATION] {trace_base} | 최종 실패 사유={last_errors}")
    return None, CallbackStatus.FAILED_VALIDATION, last_errors


def _build_excluded_notice(
    held_products: list[str] | None,
    failed_products: list[str] | None,
) -> str | None:
    """합본에서 빠진 상품 안내. 표지를 없앴으므로(2026-08-04) 이 정보는 콜백으로 나간다.

    보류와 실패를 **한 문장에 섞지 않는다** — VOC 500건짜리 상품이 '표본 부족'으로
    안내되면 셀러가 데이터가 없다고 오해한다.
    """
    parts = []
    if held_products:
        parts.append(
            f"표본 부족으로 보류된 상품 {len(held_products)}개: {', '.join(held_products)} "
            f"— VOC {constants.MIN_VOC_COUNT_FOR_REPORT}건 미만이라 분석하지 않았습니다."
        )
    if failed_products:
        parts.append(
            f"생성에 실패해 이번 호에서 빠진 상품 {len(failed_products)}개: "
            f"{', '.join(failed_products)} — 데이터는 정상이며 운영자가 확인 중입니다."
        )
    return " ".join(parts) or None


async def compile_and_upload_monthly_book(
    report_month: str,
    items: list[dict[str, Any]],
    *,
    held_products: list[str] | None = None,
    failed_products: list[str] | None = None,
) -> GenerationResult:
    """전 상품을 합친 **월 1개 PDF** 를 만들어 S3 에 올리고 콜백 1건을 낸다.

    items 원소는 `{"input": MonthlyReportInput, "report": MonthlyReportOutput}`.
    첫 페이지(표지)만 화면에 띄우고 전체는 presigned URL 로 내려받는 구조라
    (2026-08-03 확정), 표지에 전사 요약과 상품 목록이 들어간다.

    ⚠️ 상품 하나가 실패해도 합본은 나간다 — 나머지 상품의 리포트까지 막을 이유가 없다.
       빠진 상품은 표지에 **보류(표본 부족)와 실패(검증 미통과)를 구분해서** 표기한다.
       둘을 합치면 데이터가 멀쩡한 상품이 '표본 부족'으로 잘못 안내된다.
    """
    report_id = build_book_report_id(report_month)
    trace_base = f"report_id={report_id}"

    if not items:
        logger.error(f"[FAILED_ERROR] {trace_base} | 합본에 넣을 상품이 하나도 없습니다")
        return GenerationResult(
            output=None,
            callback=build_monthly_callback(
                status=CallbackStatus.FAILED_ERROR,
                report_id=report_id,
                notice_message="생성에 성공한 상품이 없어 월간 보고서를 만들지 못했습니다.",
            ),
        )

    try:
        context = build_book_context(
            report_month,
            [
                {
                    "input": item["input"].model_dump(mode="json"),
                    "report": item["report"].model_dump(mode="json"),
                }
                for item in items
            ],
        )
        pdf_bytes = compile_monthly_book(context)
        pdf_s3_meta = await upload_pdf_to_s3(
            pdf_bytes=pdf_bytes,
            report_type=REPORT_TYPE_MONTHLY,  # → monthly-report 프리픽스 (6개월 보존)
            # 경로의 {yyyy}/{mm} 와 파일명의 {yyyyMM} 은 **보고 대상 월**이다.
            # 업로드 시각(1일 새벽)을 쓰면 7월 리포트가 2026/08 폴더로 들어간다.
            period=report_month,
        )
    except S3NotConfiguredError as exc:
        # 업로드하지 않은 파일을 성공으로 보고하지 않는다(스텁 상태에서의 안전장치).
        logger.error(f"[FAILED_ERROR] {trace_base} | {exc!s}")
        return GenerationResult(
            output=None,
            callback=build_monthly_callback(
                status=CallbackStatus.FAILED_ERROR,
                report_id=report_id,
                notice_message="S3 업로드가 아직 구성되지 않아 문서를 저장하지 못했습니다.",
            ),
        )
    except PdfSizeExceededError as exc:
        logger.error(f"[FAILED_SIZE_EXCEEDED] {trace_base} | {exc!s}")
        return GenerationResult(
            output=None,
            callback=build_monthly_callback(
                status=CallbackStatus.FAILED_SIZE_EXCEEDED,
                report_id=report_id,
                notice_message="월간 보고서 파일 용량이 상한을 초과해 발송이 중단되었습니다.",
            ),
        )
    except Exception:  # PDF/S3 계층 오류는 유형을 가리지 않고 FAILED_ERROR 로 수렴시킨다
        logger.exception(f"[FAILED_ERROR] {trace_base}")
        return GenerationResult(
            output=None,
            callback=build_monthly_callback(
                status=CallbackStatus.FAILED_ERROR,
                report_id=report_id,
                notice_message="월간 보고서 파일 생성 중 오류가 발생했습니다.",
            ),
        )

    logger.info(
        f"[SUCCESS] {trace_base} | 상품 {len(items)}개 · {pdf_s3_meta.file_size_bytes / 1024:.0f}KB "
        f"· s3_key={pdf_s3_meta.s3_full_key}"
    )
    return GenerationResult(
        output=None,  # 합본은 단일 output 이 없다 — 본문은 PDF 안에만 있다
        callback=build_monthly_callback(
            status=CallbackStatus.SUCCESS,
            report_id=report_id,
            pdf_s3_meta=pdf_s3_meta,
            notice_message=_build_excluded_notice(held_products, failed_products),
        ),
    )
