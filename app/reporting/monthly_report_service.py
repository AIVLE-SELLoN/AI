"""월간 리포트 생성 파이프라인.

PDF 는 월 1개 합본이고 그게 유일한 산출물이다. 데이터를 DB 에 적재하지 않고 S3 에
올린 합본 PDF 의 링크만 메시지 큐로 보낸다. UI 는 첫 페이지(표지)만 화면에 띄우고
전체는 presigned URL 로 내려받는다. 따라서:
  - 콜백에 `source_payload` 를 싣지 않는다(저장하지 않을 데이터를 큐에 흘리지 않는다).
  - 콜백은 월 1건이다. 상품별로 나가지 않는다.
  - PDF 안에 수치 표·차트까지 다 들어가야 한다(pdf_compiler 가 자립 문서를 만든다).

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

fallback 생성물을 만들어 성공처럼 내보내지 않는다. 검증을 통과 못 한 문서를 셀러에게
자동 발송하면 틀린 수치가 그대로 나간다 — FAILED_VALIDATION 은 "운영자 알림, 자동
발송 중단"이다.
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

# 프롬프트 버전 — 교체 시 이 한 줄만 바꾼다(구버전 파일은 남겨둔다).
# v6 이 글자 수 상한을 명시한다(항목당 80자, cause_title 40자). 레이아웃이 상품 1건 =
# 1페이지라 문장이 길어지면 그 상품만 두 장으로 갈린다. 스키마 max_length(200자)는
# 계약이라 그대로 두고 프롬프트로 실제 출력 길이를 잡는다.
#
# 🔴 **v7 은 「출력 형식」 예시에서 실제처럼 보이는 값을 걷어낸다.** v6 예시가
#    `8%p` · `50%` · `450건` · `P001` 을 리터럴로 담고 있었는데, gpt-4o-mini 가 그걸
#    데이터로 오인해 그대로 옮겨 적었다 — 검증기의 수치 팩트체크가 전량 반려하고,
#    예시가 고정이라 **재시도 3회가 모두 같은 값을 냈다**(2026-07 실행 실측: 42건 중
#    9건이 FAILED_VALIDATION, 실패 사유가 저 네 값에 집중).
#    `P001` 이 특히 분명한 증거였다 — P003 요청에 P001 이 돌아왔다.
#    v7 은 숫자 자리를 `<속성표의 부정%>` 같은 자리표시자로 바꾸고, 상품코드·연월은
#    `$master_product_code`·`$report_month` 로 두어 **예시가 곧 정답**이 되게 했다.
#    → 실패 9건 → 2건 (42건 중, 2026-07 재실행 실측).
#
# v8 은 그 2건을 닫는다. 사유가 수치가 아니라 **스키마 위반**이었다:
#   `List should have at least 3 items after validation, not 1` (aspect_summaries)
# `aspect_summaries` 는 min_length=max_length=3(색상·사이즈·소재 각 1건)인데 v6·v7 의
# 출력 예시에는 **항목이 1개뿐**이라, 모델이 예시의 형태를 따라 1건만 냈다. 규칙 4번은
# "3개 속성 각 1건" 이라고 적혀 있었지만 **예시가 규칙을 이긴다.**
# v8 은 예시에 세 항목을 다 넣고 규칙 4번에도 개수를 못박았다.
#
# ⚠️ 같은 함정을 다시 만들지 않으려면 **스키마의 고정 개수 제약과 예시 항목 수를 함께**
#    봐야 한다. 확인해 둔 나머지: cause_analysis_results·recommended_actions 는 min 1
#    (예시 2개로 맞춤), channel_pair_analyses 는 min 없음, 중첩 리스트는 min 1 이다.
PROMPT_VERSION = "monthly_report_v8"


def build_report_id(input_data: MonthlyReportInput) -> str:
    """상품별 추적용 ID. 예: RPT-202607-P001 — 로그·배치 결과에서만 쓴다."""
    return f"RPT-{input_data.report_month.replace('-', '')}-{input_data.product_group_id}"


def build_book_report_id(report_month: str) -> str:
    """월간 합본의 멱등 키. 예: RPT-202607

    PDF 가 월 1개라 콜백·이벤트도 월 1건이고 상품 코드가 들어가지 않는다. 같은 달을
    다시 돌려도 같은 ID 라 메인이 upsert 하면 된다.
    """
    return f"RPT-{report_month.replace('-', '')}"


def _resolve_stage_label(input_data: MonthlyReportInput) -> str:
    """worst_pair 의 severity 단계 라벨. 전 쌍 보류면 안정 단계로 안내한다.

    프롬프트에 이 문자열을 그대로 넣어야 cause_title 이 검증기 대조를 통과한다.
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

    p_value 는 어느 형식에도 넣지 않는다 — 금지 표현이라 모델에게 보여주지 않는다
    (보여주면 문장에 옮겨 적고 검증에서 반려되는 낭비가 생긴다).
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

    str.format() 금지(프롬프트의 JSON 중괄호와 충돌) — Template + $플레이스홀더를 쓴다.
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
    """상품 1건의 문장 생성 + 검증. PDF·S3·콜백은 여기서 하지 않는다.

    PDF 가 월 1개 합본이라 파이프라인이 둘로 갈라져 있다:
        [상품별] 이 함수 — LLM 생성·검증까지만. 산출물은 MonthlyReportOutput.
        [월 단위] compile_and_upload_monthly_book() — 합본 PDF·S3·콜백 1건.

    Returns:
        (출력, 상태, 실패 사유). 상태는 SUCCESS / HOLD_INSUFFICIENT_DATA /
        FAILED_VALIDATION 중 하나다.
    """
    report_id = build_report_id(input_data)
    trace_base = f"report_id={report_id}"

    # [보류 게이트] 표본이 모자라면 LLM 을 아예 태우지 않는다
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


def _label(input_data: MonthlyReportInput) -> str:
    """안내 문구에 쓸 상품 표기. `미디 원피스(P001)` — 이름이 없으면 코드만."""
    return (
        f"{input_data.product_name}({input_data.product_group_id})"
        if input_data.product_name
        else input_data.product_group_id
    )


def _summarize(labels: list[str], budget: int) -> str:
    """앞에서 몇 개만 나열하고 나머지는 "외 N개" 로 접는다. 기준은 개수가 아니라 길이다.

    **`len(반환값) <= budget` 을 보장한다.** 호출부가 이 보장 위에서 전체 길이를 계산하므로
    깨뜨리면 안 된다. "외 N개" 꼬리도 예산 **안에** 들어간다 — 밖에 두면 접힐 때마다
    예산을 넘긴다.

    개수로 자르면 상품명이 길 때 상한이 안 지켜진다. `product_name` 은 커머스 노출명
    (`products.channel_product_name`)이라 원래 길다 — 38자짜리 이름이면 5개만 나열해도
    구절 하나가 200자를 넘는다. 목 데이터 이름이 7자라 로컬에서는 안 밟힌다(실측: 개수
    상한이면 223자, 실제 노출명이면 368자).

    최소 1개는 반드시 나열한다 — 하나도 없으면 셀러가 무엇인지 알 수 없다. 다만 그
    하나가 예산보다 길면 이름 자체를 자른다. 노출명에 길이 제한이 없어 안 자르면 이름
    하나가 상한을 통째로 무너뜨린다.
    """
    if not labels or budget <= 0:
        return ""

    full = ", ".join(labels)
    if len(full) <= budget:
        return full

    # 접어야 한다. 꼬리 자릿수는 접힌 개수에 따라 변하니 **최악**(전부 접힘)으로 잡아
    # 자리를 먼저 빼둔다 — 실제 꼬리는 이보다 짧거나 같으므로 예산을 넘길 수 없다.
    room = budget - len(f" 외 {len(labels)}개")
    if room < 1:
        # 꼬리조차 못 담는 예산. 호출부 계산상 여기까지 오지 않지만, `len <= budget` 은
        # 호출부 산술이 기대는 불변식이라 어떤 입력에서도 지킨다. 이름을 못 실으므로
        # 개수만 알린다.
        summary = f"{len(labels)}개"
        return summary[:budget]

    shown: list[str] = []
    used = 0
    for label in labels:
        if not shown:
            # 첫 항목은 반드시 하나 싣는다 — room 보다 길면 잘라서라도.
            text = _truncate_label(label, room)
            shown.append(text)
            used = len(text)  # 자른 **뒤** 길이를 센다
            continue
        cost = len(label) + 2  # ", " 구분자
        if used + cost > room:
            break
        shown.append(label)
        used += cost

    # 접힌 게 없으면 꼬리를 붙이지 않는다. 여기까지 왔다는 건 "전부 나열하면 예산을
    # 넘는다"는 뜻이지 "여러 건 중 일부만 실었다"는 뜻이 아니다 — 긴 이름 1건을 잘라
    # 실은 경우가 그렇다. 그때 꼬리를 붙이면 "상품 1개: …이름… 외 0개" 가 셀러 화면에
    # 나간다. 잘린 것과 접힌 것은 다르다.
    folded = len(labels) - len(shown)
    if not folded:
        return ", ".join(shown)
    return f"{', '.join(shown)} 외 {folded}개"


def _truncate_label(label: str, room: int) -> str:
    """`room` 글자에 맞춰 자른다. `이름(P001)` 꼴이면 코드를 남기고 이름만 줄인다.

    오른쪽부터 자르면 끝에 붙은 상품 코드가 먼저 날아간다. 셀러가 관리 화면에서 상품을
    특정하는 값은 노출명이 아니라 코드라, 같은 예산이면 코드를 남기는 쪽이 정보가 많다.
    코드까지 넣을 자리도 없으면 그때는 통째로 자른다.
    """
    if len(label) <= room:
        return label
    if room <= 1:
        return label[:room]  # "…" 를 넣을 자리도 없다

    if label.endswith(")") and "(" in label:
        code = label[label.rindex("(") :]  # "(P001)"
        keep = room - len(code) - 1  # "…" 한 자리
        if keep >= 1:
            return f"{label[:keep]}…{code}"

    return label[: room - 1] + "…"


def _build_excluded_notice(
    held_inputs: list[MonthlyReportInput] | None,
    failed_products: list[str] | None,
) -> str | None:
    """합본에서 빠진 상품 안내. 표지가 없으므로 이 정보는 콜백으로 나간다.

    보류와 실패를 **한 문장에 섞지 않는다** — VOC 500건짜리 상품이 '표본 부족'으로
    안내되면 셀러가 데이터가 없다고 오해한다.

    보류 상품은 지면에도 페이지가 생기지만 이 문구는 별개로 유지한다 — 메인 화면은
    PDF 를 열지 않고도 빠진 상품을 알아야 한다.

    상한은 조립된 최종 문자열에 건다(`NOTICE_MAX_CHARS`). 구절마다 따로 예산을 주면
    구절 수·고정 문구 길이만큼 천장이 같이 올라가 상한이 안 지켜진다 — 구절당 70자로
    주면 실측 천장이 260자다. 그래서 고정 문구가 쓸 자리를 먼저 빼고 남은 것을 구절들이
    나눠 쓴다.

           총길이 = 고정 문구 + 구분자 + Σ(나열)  ≤  고정 문구 + 구분자 + n × 나열예산
                                                 ≤  NOTICE_MAX_CHARS

       `_summarize` 가 `len <= budget` 을 보장하므로 이 부등식이 성립한다.
    """
    # (머리말, 나열할 라벨, 꼬리말) — 머리말·꼬리말이 고정 문구다.
    sections: list[tuple[str, list[str], str]] = []
    if held_inputs:
        sections.append((
            f"표본 부족으로 보류된 상품 {len(held_inputs)}개: ",
            [_label(i) for i in held_inputs],
            f" — VOC {constants.MIN_VOC_COUNT_FOR_REPORT}건 미만이라 분석하지 않았습니다.",
        ))
    if failed_products:
        sections.append((
            f"생성에 실패해 이번 호에서 빠진 상품 {len(failed_products)}개: ",
            list(failed_products),
            " — 데이터는 정상이며 운영자가 확인 중입니다.",
        ))
    if not sections:
        return None

    # 고정 문구는 못 줄인다(개수 자릿수까지 포함해 여기서 확정된다). 남은 것을 나눠 쓴다.
    fixed = sum(len(head) + len(tail) for head, _, tail in sections) + (len(sections) - 1)
    listing_budget = (constants.NOTICE_MAX_CHARS - fixed) // len(sections)

    return " ".join(
        f"{head}{_summarize(labels, listing_budget)}{tail}" for head, labels, tail in sections
    )


async def compile_and_upload_monthly_book(
    report_month: str,
    items: list[dict[str, Any]],
    *,
    held_inputs: list[MonthlyReportInput] | None = None,
    failed_products: list[str] | None = None,
) -> GenerationResult:
    """전 상품을 합친 월 1개 PDF 를 만들어 S3 에 올리고 콜백 1건을 낸다.

    items       `{"input": MonthlyReportInput, "report": MonthlyReportOutput}` 목록.
    held_inputs 표본 부족으로 보류된 상품의 **입력**. 지면에 보류 페이지를 만들고
                콜백 안내 문구도 여기서 만든다.

    보류 상품도 지면에 페이지를 만든다(사유는 `pdf_compiler.build_book_context`).
    콜백 안내는 메인 화면용으로 그대로 둔다.

    보류는 `held_inputs`(입력 객체), 실패는 `failed_products`(코드 문자열)로 받는다 —
    보류는 지면에 상품명·VOC 건수를 찍어야 해 입력이 통째로 필요하고, 실패는 안내
    문구에만 쓰여 코드면 충분하다.

    상품 하나가 실패해도 합본은 나간다 — 나머지 상품의 리포트까지 막을 이유가 없다.
    보류(표본 부족)와 실패(검증 미통과)는 구분해서 표기한다. 둘을 합치면 데이터가
    멀쩡한 상품이 '표본 부족'으로 잘못 안내된다.
    """
    report_id = build_book_report_id(report_month)
    trace_base = f"report_id={report_id}"

    if not items:
        # 수록 상품이 0개면 PDF 를 만들지 않는다(계약: FAILED_ERROR). 보류 페이지만
        # 있는 PDF 를 SUCCESS 로 내보내면 메인이 "링크 저장 + 메일 발송" 을 타는데,
        # 분석이 한 건도 없는 문서가 셀러에게 나간다.
        #
        # 다만 왜 하나도 없는지는 반드시 실어 보낸다. 전 상품이 표본 부족인 상황(신규
        # 고객사 등)에서 "생성에 실패했다"만 가면 데이터 파이프라인 고장과 구분이 안
        # 된다 — 보류는 정상 동작이다.
        logger.error(
            f"[FAILED_ERROR] {trace_base} | 합본에 넣을 상품이 하나도 없습니다 "
            f"(보류 {len(held_inputs or [])}건)"
        )
        excluded = _build_excluded_notice(held_inputs, failed_products)
        return GenerationResult(
            output=None,
            callback=build_monthly_callback(
                status=CallbackStatus.FAILED_ERROR,
                report_id=report_id,
                notice_message=" ".join(
                    filter(None, ["생성에 성공한 상품이 없어 월간 보고서를 만들지 못했습니다.", excluded])
                ),
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
            held=[i.model_dump(mode="json") for i in (held_inputs or [])],
        )
        pdf_bytes = compile_monthly_book(context)
        pdf_s3_meta = await upload_pdf_to_s3(
            pdf_bytes=pdf_bytes,
            report_type=REPORT_TYPE_MONTHLY,  # → monthly-report 프리픽스 (6개월 보존)
            # 경로의 {yyyy}/{mm} 와 파일명의 {yyyyMM} 은 보고 대상 월이다.
            # 업로드 시각(1일 새벽)을 쓰면 7월 리포트가 2026/08 폴더로 들어간다.
            period=report_month,
        )
    except S3NotConfiguredError as exc:
        # 업로드하지 않은 파일을 성공으로 보고하지 않는다.
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
            notice_message=_build_excluded_notice(held_inputs, failed_products),
        ),
    )
