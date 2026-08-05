"""리포팅 검증 로직 테스트 — 문서 생성 스키마 §4-4.

여기서 재는 것은 **검증기가 반려해야 할 것을 반려하는가**다(비용 0, LLM 미호출).
생성물의 품질(문장이 좋은가)은 `eval/run_reporting_eval.py` 소관이다.

구성:
  1. 픽스처 — 스키마를 통과하는 정상 입력/출력 한 벌
  2. cs_reply_validator  — 통과 케이스 + 반려 케이스 5종
  3. monthly_report_validator — 통과 케이스 + 반려 케이스 6종
  4. 파이프라인 — HOLD 게이트·재시도·FAILED_VALIDATION·SUCCESS 콜백 (LLM 은 mock)
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from jinja2 import BaseLoader, Environment, StrictUndefined, select_autoescape

from app.core import constants
from app.core.schemas import (
    Aspect,
    CallbackStatus,
    Channel,
    CSGuidelineInput,
    CSGuidelineOutput,
    DetectionConfidence,
    GenerationCallback,
    HoldReason,
    MonthlyReportInput,
    MonthlyReportOutput,
    PdfS3Meta,
    RecommendedAction,
    Severity,
    Verdict,
)
from app.reporting import cs_reply_service, monthly_report_service
from app.reporting.callback import build_monthly_callback
from app.reporting.cs_reply_service import generate_cs_reply_pipeline
from app.reporting.cs_reply_validator import validate_cs_guideline
from app.reporting.ids import build_guideline_id
from app.reporting.metrics_calculator import (
    build_channel_divergence_pair,
    calculate_jsd_bits,
    check_divergence_gate,
    decide_severity,
    permutation_test_jsd,
)
from app.reporting.monthly_report_service import (
    build_book_report_id,
    compile_and_upload_monthly_book,
    generate_monthly_report_output,
)
from app.reporting.monthly_report_validator import validate_monthly_report
from app.reporting.pdf_compiler import (
    MONTHLY_BOOK_HTML,
    build_book_context,
)
from app.reporting.s3_uploader import (
    REPORT_TYPE_GUIDELINE,
    REPORT_TYPE_MONTHLY,
    S3NotConfiguredError,
    _build_original_file_name,
    build_object_path,
    resolve_storage_policy,
    upload_pdf_to_s3,
)

# ── 1. 픽스처 ────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def mock_infrastructure() -> Generator[None, None, None]:
    """PDF 컴파일(weasyprint)과 S3 업로드를 자동 mock. 테스트는 네트워크·GTK 를 타지 않는다."""
    dummy_s3_meta = PdfS3Meta(
        s3_bucket_name="mock-bucket",
        s3_file_path="reports/mock/",
        original_file_name="mock_report.pdf",
        new_file_name="mock_report_123.pdf",
        s3_full_key="reports/mock/mock_report_123.pdf",
        created_at=datetime.now(UTC),
        file_size_bytes=2048,
        presigned_url="https://mock-s3.amazonaws.com/mock_report_123.pdf",
    )
    with (
        patch("app.reporting.cs_reply_service.compile_report_to_pdf", return_value=b"%PDF-MOCK"),
        patch(
            "app.reporting.cs_reply_service.upload_pdf_to_s3",
            new_callable=AsyncMock,
            return_value=dummy_s3_meta,
        ),
        # 월간은 합본 컴파일러를 쓴다(상품별 PDF 는 더 이상 만들지 않는다)
        patch(
            "app.reporting.monthly_report_service.compile_monthly_book", return_value=b"%PDF-BOOK"
        ),
        patch(
            "app.reporting.monthly_report_service.upload_pdf_to_s3",
            new_callable=AsyncMock,
            return_value=dummy_s3_meta,
        ),
    ):
        yield


@pytest.fixture
def cs_input() -> CSGuidelineInput:
    """부정률 5% → 13% (변동 8%p), 최다 원인 사진_색감_오차 18/26건(69%)."""
    return CSGuidelineInput(
        alert_id="ALT-20260528-P001-COUPANG",
        detected_at=datetime(2026, 5, 28, 9, 0, tzinfo=UTC),
        product_group_id="P001",
        product_name="미디 원피스",
        channel=Channel.COUPANG,
        main_aspect=Aspect.COLOR,
        verdict=Verdict.BIASED,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        detection_confidence=DetectionConfidence.HIGH,
        stats={
            "cur_rate": 0.13,
            "past_rate": 0.05,
            "delta": 0.08,
            "cur_total": 200,
            "p_value": 0.002,
            "bh_significant": True,
        },
        root_cause={"label": "사진_색감_오차", "count": 18, "total": 26},
        linked_inquiries=[
            {
                "item_id": "INQ-000001",
                "raw_text": "색이 사진이랑 너무 달라요",
                "created_at": datetime(2026, 5, 27, 10, 0, tzinfo=UTC),
            }
        ],
    )


@pytest.fixture
def cs_output() -> CSGuidelineOutput:
    """cs_input 과 정합한 정상 출력."""
    return CSGuidelineOutput(
        guideline_id="GD-20260528-P001-COUPANG",
        alert_id="ALT-20260528-P001-COUPANG",
        summary={
            "issue_title": "쿠팡 색상 불만 급증 대응 가이드",
            "risk_level": "WARNING",
            "key_metric_text": "색상 부정 비율이 5%에서 13%로 8%p 상승했습니다 (문의 200건 기준).",
        },
        root_cause_summary="사진_색감_오차 18건 / 전체 26건 (69%)",
        standard_guideline={
            "core_message": "촬영 조명 차이로 실물 색상이 다르게 보일 수 있음을 안내하고 무상 교환을 접수합니다.",
            "draft_reply": "안녕하세요 고객님, 색상 차이로 불편을 드려 죄송합니다. 무상 반품 및 교환을 도와드리겠습니다.",
            "key_talking_points": ["조명 차이 정중히 안내", "고객 과실 암시 표현 금지"],
        },
        ops_action_guide="쿠팡 대표 이미지의 색보정 상태를 점검하고 원본 기준으로 재등록하세요.",
        inquiry_specific_guides=[
            {"item_id": "INQ-000001", "recommended_point": "사과 후 무상 회수 접수를 우선 안내하세요."}
        ],
    )


@pytest.fixture
def monthly_input() -> MonthlyReportInput:
    """색상 부정 50%(드리프트 +8%p, RISK), worst_pair 는 CRISIS 단계."""
    return MonthlyReportInput(
        report_month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 31),
        product_group_id="P001",
        product_name="미디 원피스",
        total_voc_count=450,
        aspect_distributions=[
            {
                "aspect": "색상",
                "total_count": 200,
                "positive_ratio": 0.2,
                "neutral_ratio": 0.3,
                "negative_ratio": 0.5,
            },
            {
                "aspect": "사이즈",
                "total_count": 150,
                "positive_ratio": 0.5,
                "neutral_ratio": 0.3,
                "negative_ratio": 0.2,
            },
            {
                "aspect": "소재",
                "total_count": 100,
                "positive_ratio": 0.6,
                "neutral_ratio": 0.2,
                "negative_ratio": 0.2,
            },
        ],
        sentiment_drifts=[
            {"aspect": "색상", "drift_rate": 0.08, "status": "RISK"},
            {"aspect": "사이즈", "drift_rate": 0.01, "status": "NORMAL"},
            {"aspect": "소재", "drift_rate": -0.02, "status": "NORMAL"},
        ],
        channel_divergence={
            "calculated_at": datetime(2026, 8, 1, tzinfo=UTC),
            "worst_pair": "COUPANG_VS_NAVER",
            "is_crisis": True,
            "pairs": [
                {
                    "comparison_pair": "COUPANG_VS_NAVER",
                    "sample_size": 120,
                    "jsd_score": 0.42,
                    "jsd_baseline": 0.18,
                    "p_value": 0.001,
                    "bh_significant": True,
                    "is_crisis": True,
                    "severity": "CRISIS",
                },
                {
                    "comparison_pair": "COUPANG_VS_ZIGZAG",
                    "sample_size": 12,
                    "hold_reason": "INSUFFICIENT_SAMPLE",
                },
            ],
        },
        recommended_id="REC-202607-P001",
    )


@pytest.fixture
def monthly_output() -> MonthlyReportOutput:
    """monthly_input 과 정합한 정상 출력. cause_title 에 CRISIS 단계 라벨('위험 단계') 포함."""
    return MonthlyReportOutput(
        report_id="RPT-202607-P001",
        product_group_id="P001",
        report_month="2026-07",
        aspect_summaries=[
            {"aspect": "색상", "summary_text": "부정 의견이 전월 대비 8%p 올라 50%를 기록했습니다."},
            {"aspect": "사이즈", "summary_text": "부정 비율 20%로 전월과 유사한 수준을 유지했습니다."},
            {"aspect": "소재", "summary_text": "부정 비율 20%로 안정적인 흐름을 보였습니다."},
        ],
        channel_divergence_cause={
            "cause_title": "쿠팡-네이버 채널 평판 격차 위험 단계",
            "cause_description": "쿠팡 채널의 색상 불만 비중이 뚜렷하게 높아 이미지 운영 점검이 필요합니다.",
        },
        channel_pair_analyses=[
            {
                "comparison_pair": "COUPANG_VS_NAVER",
                "cause_analysis": ["쿠팡 채널의 색상 부정 의견 비중이 네이버보다 높습니다."],
                "recommended_actions": ["쿠팡 대표 이미지를 원본 색상 기준으로 교체하세요."],
            },
            {
                # 보류 쌍 — 수치를 붙이지 않는다(§보류 시 판정 문구 금지)
                "comparison_pair": "COUPANG_VS_ZIGZAG",
                "cause_analysis": ["표본이 부족해 두 채널 간 차이를 판단하지 않았습니다."],
                "recommended_actions": ["지그재그 채널 리뷰 수집량을 늘린 뒤 재확인하세요."],
            },
        ],
        cause_analysis_results=["색상 속성 부정 의견이 전체 450건 중 가장 큰 비중을 차지했습니다."],
        recommended_actions=["쿠팡 대표 이미지를 원본 색상 기준으로 교체하세요."],
    )


def test_monthly_validator_requires_analysis_for_every_pair(
    monthly_input: MonthlyReportInput, monthly_output: MonthlyReportOutput
) -> None:
    """쌍별 분석이 비면 반려 — PDF 게이지 바로 아래가 통째로 빈칸이 된다."""
    output = monthly_output.model_copy(update={"channel_pair_analyses": []})
    passed, errors = validate_monthly_report(monthly_input, output)
    assert passed is False
    assert any("채널쌍 분석 누락" in e for e in errors)


def test_monthly_validator_rejects_unknown_pair(
    monthly_input: MonthlyReportInput, monthly_output: MonthlyReportOutput
) -> None:
    """입력에 없는 채널쌍 분석은 반려 — 없는 게이지 자리에 붙을 문장이다."""
    ghost = monthly_output.channel_pair_analyses[0].model_copy(
        update={"comparison_pair": "NAVER_VS_ZIGZAG"}
    )
    output = monthly_output.model_copy(update={"channel_pair_analyses": [ghost]})
    passed, errors = validate_monthly_report(monthly_input, output)
    assert passed is False
    assert any("입력에 없는 채널쌍" in e for e in errors)


def test_monthly_validator_factchecks_pair_analysis(
    monthly_input: MonthlyReportInput, monthly_output: MonthlyReportOutput
) -> None:
    """쌍별 원인 문장의 수치도 팩트체크 대상이다.

    ⚠️ 쌍을 **빼지 않고** 하나만 오염시킨다. 쌍을 떨어뜨리면 "채널쌍 분석 누락" 이 함께
       올라와, 팩트체크를 통째로 지워도 passed=False 라서 테스트가 통과해 버린다.
    """
    analyses = [a.model_copy(deep=True) for a in monthly_output.channel_pair_analyses]
    analyses[0] = analyses[0].model_copy(
        update={"cause_analysis": ["쿠팡 색상 부정 의견이 전체 9999건으로 집계됐습니다."]}
    )
    output = monthly_output.model_copy(update={"channel_pair_analyses": analyses})

    passed, errors = validate_monthly_report(monthly_input, output)
    assert passed is False
    assert any("수치 팩트체크 실패" in e for e in errors)
    assert any("channel_pair_analyses[COUPANG_VS_NAVER].cause_analysis[0]" in e for e in errors)
    # 쌍은 그대로 두었으므로 커버리지 반려는 섞이지 않아야 한다
    assert not any("채널쌍 분석 누락" in e for e in errors)


# ── 2. cs_reply_validator ────────────────────────────────────────────────


def test_cs_validator_accepts_grounded_output(cs_input, cs_output) -> None:
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert is_valid, errors


def test_cs_validator_rejects_unknown_item_id(cs_input, cs_output) -> None:
    """linked_inquiries 에 없는 문의를 가리키면 반려 — 상담원이 헛짚는 것을 막는다."""
    cs_output.inquiry_specific_guides[0].item_id = "INQ-999999"
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any("INQ-999999" in e for e in errors)


def test_cs_validator_rejects_hallucinated_number(cs_input, cs_output) -> None:
    """입력에 없는 수치(27%)를 지어내면 반려."""
    cs_output.summary.key_metric_text = "색상 부정 비율이 27%로 급등했습니다."
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any("수치 팩트체크 실패" in e for e in errors)


@pytest.mark.parametrize(
    "leak_text",
    [
        "부정률 상승이 통계적으로 유의합니다 (p = 0.002).",
        "BH-FDR 보정 결과 유의한 상승입니다.",
        "유의확률 기준으로 이상이 확인됐습니다.",
    ],
)
def test_cs_validator_rejects_forbidden_expressions(cs_input, cs_output, leak_text: str) -> None:
    """p값·FDR 같은 통계 용어가 셀러 문서에 새면 반려(§4-4)."""
    cs_output.summary.key_metric_text = leak_text
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any("금지 표현" in e for e in errors)


def test_cs_validator_requires_root_cause_label(cs_input, cs_output) -> None:
    """root_cause 가 있으면 그 라벨이 요약에 그대로 있어야 한다."""
    cs_output.root_cause_summary = "여러 원인이 섞여 있습니다 18건 / 전체 26건 (69%)"
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any("사진_색감_오차" in e for e in errors)


def test_cs_validator_requires_unspecified_text_when_no_root_cause(cs_input, cs_output) -> None:
    """root_cause 가 null 이면 대체 문구가 반드시 들어가야 한다(§2-2)."""
    cs_input.root_cause = None
    cs_input.recommended_action = RecommendedAction.OPERATION_CHECK
    cs_output.root_cause_summary = "사진_색감_오차가 주요 원인입니다."
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any(constants.ROOT_CAUSE_UNSPECIFIED_TEXT in e for e in errors)

    cs_output.root_cause_summary = f"{constants.ROOT_CAUSE_UNSPECIFIED_TEXT} 상태로 집계됐습니다."
    is_valid, _ = validate_cs_guideline(cs_input, cs_output)
    assert is_valid


def test_cs_validator_skips_factcheck_on_excluded_fields(cs_input, cs_output) -> None:
    """정책 상수가 들어가는 제외 필드는 수치 팩트체크 대상이 아니다(§4-4)."""
    cs_output.standard_guideline.core_message = "수령 후 7일 이내 무상 교환이 가능하며 30% 할인 쿠폰을 제공합니다."
    cs_output.ops_action_guide = "14일 내 재촬영을 완료하세요."
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert is_valid, errors


def test_guideline_id_is_unique_per_alert(cs_input) -> None:
    """알림이 다르면 ID 도 달라야 한다 — 같으면 백엔드 upsert 가 앞의 가이드를 덮어쓴다.

    탐지는 (상품, aspect, 채널) 단위로 발화하므로 같은 날 같은 상품에 알림이 여러 건
    나오는 것이 정상이다. 예전 규칙(GD-{탐지일}-{상품})은 이 경우를 구분하지 못했다.
    """
    coupang = cs_input.model_copy(deep=True)
    naver = cs_input.model_copy(deep=True)
    naver.alert_id = "ALT-20260528-P001-NAVER"
    naver.channel = Channel.NAVER
    naver.main_aspect = Aspect.SIZE

    assert cs_reply_service.build_guideline_id(coupang) != cs_reply_service.build_guideline_id(naver)
    assert cs_reply_service.build_guideline_id(coupang) == "GD-20260528-P001-COUPANG"
    assert cs_reply_service.build_guideline_id(naver) == "GD-20260528-P001-NAVER"
    # 같은 알림이면 몇 번을 만들어도 같은 ID (재생성 멱등)
    assert cs_reply_service.build_guideline_id(coupang) == cs_reply_service.build_guideline_id(
        cs_input.model_copy(deep=True)
    )


def test_cs_validator_rejects_wrong_guideline_id(cs_input, cs_output) -> None:
    """모델이 ID 를 임의로 만들면 반려한다(서버 계산값과 1:1 이어야 한다)."""
    cs_output.guideline_id = "GD-20260528-P001"  # 채널이 빠진 옛 형식
    is_valid, errors = validate_cs_guideline(cs_input, cs_output)
    assert not is_valid
    assert any("guideline_id 불일치" in e for e in errors)


# ── 3. monthly_report_validator ──────────────────────────────────────────


def test_monthly_validator_accepts_grounded_output(monthly_input, monthly_output) -> None:
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert is_valid, errors


def test_monthly_validator_rejects_identifier_mismatch(monthly_input, monthly_output) -> None:
    monthly_output.product_group_id = "P999"
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("product_group_id" in e for e in errors)


def test_monthly_validator_rejects_missing_aspect(monthly_input, monthly_output) -> None:
    """입력 3속성 중 하나라도 요약에서 빠지면 반려."""
    monthly_output.aspect_summaries[2].aspect = Aspect.COLOR
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("소재" in e for e in errors)


def test_monthly_validator_rejects_missing_stage_label(monthly_input, monthly_output) -> None:
    """worst_pair 가 CRISIS 면 cause_title 에 '위험 단계' 가 그대로 있어야 한다(§1-2)."""
    monthly_output.channel_divergence_cause.cause_title = "쿠팡-네이버 채널 평판 격차 발생"
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("단계 라벨 누락" in e for e in errors)


def test_monthly_validator_rejects_mixed_stage_label(monthly_input, monthly_output) -> None:
    """다른 단계 라벨이 섞이면 반려 — 게이지 색과 문구가 어긋난다."""
    monthly_output.channel_divergence_cause.cause_title = "위험 단계이나 일부는 안정 단계입니다"
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("단계 라벨 혼입" in e for e in errors)


def test_monthly_validator_rejects_hallucinated_number(monthly_input, monthly_output) -> None:
    monthly_output.aspect_summaries[0].summary_text = "부정 의견이 전월 대비 33%p 올랐습니다."
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("수치 팩트체크 실패" in e for e in errors)


def test_monthly_validator_rejects_forbidden_expression(monthly_input, monthly_output) -> None:
    monthly_output.channel_divergence_cause.cause_description = (
        "채널 간 차이가 FDR 보정 후에도 유지됩니다."
    )
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert not is_valid
    assert any("금지 표현" in e for e in errors)


def test_monthly_validator_allows_rounded_numbers(monthly_input, monthly_output) -> None:
    """반올림 표기(8%p → 8%p, 50% → 50%)는 허용 오차 안이라 통과해야 한다."""
    monthly_output.aspect_summaries[0].summary_text = (
        "색상 부정 비율 50%, 변동폭 8%p 로 200건이 접수됐습니다."
    )
    is_valid, errors = validate_monthly_report(monthly_input, monthly_output)
    assert is_valid, errors


# ── 4. 파이프라인 (LLM mock) ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_monthly_holds_on_insufficient_voc(monthly_input) -> None:
    """표본 부족이면 LLM 을 아예 호출하지 않고 HOLD 를 돌려준다(§4-3)."""
    monthly_input.total_voc_count = constants.MIN_VOC_COUNT_FOR_REPORT - 1

    with patch("app.reporting.monthly_report_service.get_llm_client") as mock_get_client:
        output, status, errors = await generate_monthly_report_output(monthly_input)

    mock_get_client.assert_not_called()
    assert output is None
    assert status == CallbackStatus.HOLD_INSUFFICIENT_DATA
    assert not errors


@pytest.mark.asyncio
async def test_monthly_output_success(monthly_input, monthly_output) -> None:
    with patch("app.reporting.monthly_report_service.get_llm_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = monthly_output.model_dump(mode="json")
        mock_get_client.return_value = mock_client

        output, status, _ = await generate_monthly_report_output(monthly_input)

    assert status == CallbackStatus.SUCCESS
    assert output.report_id == "RPT-202607-P001"
    # 상품별 단계에서는 PDF 를 만들지 않는다(합본에서 한 번만 만든다)
    assert output.pdf_s3_meta is None


@pytest.mark.asyncio
async def test_monthly_retries_then_fails_validation(monthly_input, monthly_output) -> None:
    """검증 실패가 계속되면 재시도 소진 후 FAILED_VALIDATION — 그 상품만 빠진다."""
    bad = monthly_output.model_copy(deep=True)
    bad.aspect_summaries[0].summary_text = "부정 의견이 전월 대비 33%p 올랐습니다."

    with patch("app.reporting.monthly_report_service.get_llm_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = bad.model_dump(mode="json")
        mock_get_client.return_value = mock_client

        output, status, errors = await generate_monthly_report_output(monthly_input)

    assert output is None
    assert status == CallbackStatus.FAILED_VALIDATION
    assert mock_client.complete_json.await_count == 1 + constants.MAX_RETRY
    assert errors


@pytest.mark.asyncio
async def test_monthly_recovers_on_second_attempt(monthly_input, monthly_output) -> None:
    """1차 실패 → 피드백 반영 → 2차 통과 경로가 실제로 동작하는지."""
    bad = monthly_output.model_copy(deep=True)
    bad.aspect_summaries[0].summary_text = "부정 의견이 전월 대비 33%p 올랐습니다."

    with patch("app.reporting.monthly_report_service.get_llm_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.complete_json.side_effect = [
            bad.model_dump(mode="json"),
            monthly_output.model_dump(mode="json"),
        ]
        mock_get_client.return_value = mock_client

        output, status, _ = await generate_monthly_report_output(monthly_input)

    assert status == CallbackStatus.SUCCESS
    assert output is not None
    assert mock_client.complete_json.await_count == 2
    second_prompt = mock_client.complete_json.await_args_list[1].kwargs["prompt"]
    assert "이전 시도 검증 실패" in second_prompt


@pytest.mark.asyncio
async def test_monthly_book_is_one_object_per_month(monthly_input, monthly_output) -> None:
    """PDF·S3·콜백이 상품별이 아니라 **월 1건**인지(2026-08-03 확정)."""
    items = [{"input": monthly_input, "report": monthly_output} for _ in range(3)]

    with (
        patch(
            "app.reporting.monthly_report_service.compile_monthly_book", return_value=b"%PDF-BOOK"
        ) as mock_compile,
        patch(
            "app.reporting.monthly_report_service.upload_pdf_to_s3", new_callable=AsyncMock
        ) as mock_upload,
    ):
        mock_upload.return_value = PdfS3Meta(
            s3_bucket_name="sellon-reports",
            s3_file_path="reports/monthly/2026/08/",
            original_file_name="monthly_2026-07.pdf",
            new_file_name="monthly_ALL_2026-07_20260801_a1b2.pdf",
            s3_full_key="reports/monthly/2026/08/monthly_ALL_2026-07_20260801_a1b2.pdf",
            created_at=datetime.now(UTC),
            file_size_bytes=497000,
        )
        result = await compile_and_upload_monthly_book(
            "2026-07", items, held_products=["P099"]
        )

    # 상품이 3개여도 PDF 는 한 번만 만들고 한 번만 올린다
    assert mock_compile.call_count == 1
    assert mock_upload.await_count == 1
    # 월 1개 합본이라 상품 구분이 없다 — 경로 기준은 보고 대상 월이다
    assert mock_upload.await_args.kwargs["period"] == "2026-07"
    assert result.callback.status == CallbackStatus.SUCCESS
    assert result.callback.report_id == build_book_report_id("2026-07")  # RPT-202607
    assert result.callback.source_payload is None


@pytest.mark.asyncio
async def test_monthly_book_fails_when_no_product_survived() -> None:
    """수록할 상품이 하나도 없으면 산출물이 없으므로 FAILED_ERROR."""
    result = await compile_and_upload_monthly_book("2026-07", [])
    assert result.callback.status == CallbackStatus.FAILED_ERROR
    assert result.callback.pdf_s3_meta is None
    assert result.callback.notice_message


@pytest.mark.asyncio
async def test_cs_pipeline_success(cs_input, cs_output) -> None:
    with patch("app.reporting.cs_reply_service.get_llm_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = cs_output.model_dump(mode="json")
        mock_get_client.return_value = mock_client

        result = await generate_cs_reply_pipeline(cs_input)

    assert result.callback.status == CallbackStatus.SUCCESS
    assert result.callback.guideline_id == "GD-20260528-P001-COUPANG"
    assert result.callback.report_id is None
    assert result.output.inquiry_specific_guides[0].item_id == "INQ-000001"
    # CS 는 데이터를 DB 에 적재한다 → 재컴파일 원본(입력+출력)이 반드시 실려야 한다
    assert result.callback.source_payload["input"]["alert_id"] == cs_input.alert_id
    assert result.callback.source_payload["output"]["guideline_id"] == "GD-20260528-P001-COUPANG"


# 인프라 문서의 {company_id} 자리 — 실제 값은 고객사 PK/UUID 다.
_COMPANY_ID = "c0ffee00-0000-4000-8000-000000000000"


def test_object_path_follows_infra_rule() -> None:
    """인프라 「S3 파일 구조 규칙 정의」 경로·파일명 규칙 (2026-08-05 확정).

        reports/companies/{company_id}/{report_type}/{yyyy}/{mm}/
        {report_type}_{yyyyMM}_{uuid4}.pdf
    """
    path, name = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)
    assert path == f"reports/companies/{_COMPANY_ID}/monthly-report/2026/07/"
    assert name.startswith("monthly-report_202607_")
    assert name.endswith(".pdf")
    # uuid4 가 붙어 같은 달을 다시 올려도 이전 객체를 덮어쓰지 않는다
    _, name2 = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)
    assert name != name2

    cs_path, cs_name = build_object_path(REPORT_TYPE_GUIDELINE, "2026-05", _COMPANY_ID)
    assert cs_path == f"reports/companies/{_COMPANY_ID}/cs-guideline/2026/05/"
    assert cs_name.startswith("cs-guideline_202605_")


def test_object_path_rejects_bad_period() -> None:
    """period 는 YYYY-MM 이어야 한다 — 폴더의 연월이 곧 파일명의 연월이다."""
    for bad in ("2026/07", "202607", "2026-7", ""):
        with pytest.raises(ValueError, match="period"):
            build_object_path(REPORT_TYPE_MONTHLY, bad, _COMPANY_ID)


@pytest.mark.asyncio
async def test_upload_path_uses_report_period_not_upload_time() -> None:
    """경로의 {yyyy}/{mm} 는 **보고 대상 월**이다.

    업로드 시각을 쓰면 8/1 새벽에 올린 7월 리포트가 2026/08 폴더에 들어가면서
    폴더의 연월과 파일명(`…_202607_…`)의 연월이 어긋난다.
    """
    with patch("app.reporting.s3_uploader.S3_ENABLED", True):
        meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )

    assert "/2026/07/" in meta.s3_file_path
    assert "_202607_" in meta.new_file_name
    assert meta.s3_full_key == meta.s3_file_path + meta.new_file_name


@pytest.mark.asyncio
async def test_upload_refuses_without_company_id() -> None:
    """company_id 를 모르면 올리지 않는다 — 경로가 회사 단위로 갈린다."""
    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        patch("app.reporting.s3_uploader.S3_DEFAULT_COMPANY_ID", ""),
        pytest.raises(S3NotConfiguredError, match="company_id"),
    ):
        await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07"
        )


@pytest.mark.asyncio
async def test_cs_original_file_name_differs_by_alert(cs_input, cs_output) -> None:
    """같은 달 CS 가이드라인이라도 알림이 다르면 **표시용 파일명**이 달라야 한다.

    `original_file_name` 은 메인이 목록에 표시할 때 쓰는 이름이다. CS 는 알림마다 1건씩
    나오므로 `{yyyyMM}` 만으로는 5월 가이드라인이 전부 `cs-guideline_202605.pdf` 가 되어
    목록이 도배된다(저장 자체는 `new_file_name` 의 uuid4 로 안전하다).

    ⚠️ 업로더를 mock 하지 않고 **실제 함수**를 태운다 — mock 하면 서비스가 source_id 를
       넘기는지, 업로더가 그걸 이름에 붙이는지 둘 다 검증되지 않는다.
    """
    naver_input = cs_input.model_copy(update={"alert_id": "ALT-20260528-P001-NAVER"})

    async def _run(input_data: CSGuidelineInput):
        output = cs_output.model_copy(
            update={
                "alert_id": input_data.alert_id,
                "guideline_id": build_guideline_id(input_data.alert_id),
            }
        )
        with patch("app.reporting.cs_reply_service.get_llm_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_client.complete_json.return_value = output.model_dump(mode="json")
            mock_get_client.return_value = mock_client
            return await generate_cs_reply_pipeline(input_data)

    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        patch("app.reporting.s3_uploader.S3_DEFAULT_COMPANY_ID", _COMPANY_ID),
        patch("app.reporting.cs_reply_service.upload_pdf_to_s3", upload_pdf_to_s3),
    ):
        coupang = await _run(cs_input)
        naver = await _run(naver_input)

    first = coupang.callback.pdf_s3_meta.original_file_name
    second = naver.callback.pdf_s3_meta.original_file_name
    assert first != second
    assert cs_input.alert_id in first
    assert naver_input.alert_id in second


def test_monthly_original_file_name_has_no_source_id() -> None:
    """월간은 월 1건이라 source_id 를 붙이지 않는다 — 달만으로 유일하다."""
    assert _build_original_file_name("monthly-report", "2026-07", None) == (
        "monthly-report_202607.pdf"
    )
    assert _build_original_file_name("cs-guideline", "2026-05", "ALT-1") == (
        "cs-guideline_202605_ALT-1.pdf"
    )


def test_storage_policy_differs_by_document_type() -> None:
    """월간 6개월 / CS 1일 자동 삭제 (S3 Lifecycle).

    버킷은 **하나**이고 문서 종류는 프리픽스로 가른다(인프라 2026-08-05) — Lifecycle
    규칙이 프리픽스 단위로 걸리기 때문이다.
    """
    monthly = resolve_storage_policy(REPORT_TYPE_MONTHLY)
    guideline = resolve_storage_policy(REPORT_TYPE_GUIDELINE)

    assert monthly.bucket_name == guideline.bucket_name  # 버킷 1개
    assert (monthly.prefix, guideline.prefix) == ("monthly-report", "cs-guideline")
    assert monthly.retention_hours == constants.MONTHLY_RETENTION_DAYS * 24
    assert guideline.retention_hours == constants.GUIDELINE_RETENTION_HOURS
    # 월간은 원본을 보관하지 않아 만료되면 재생성이 불가능하다
    assert monthly.recompilable is False
    assert guideline.recompilable is True
    # 링크가 객체보다 오래 살면 "받을 수 있다"는 잘못된 안내가 된다
    assert monthly.presigned_ttl_hours <= monthly.retention_hours
    assert guideline.presigned_ttl_hours <= guideline.retention_hours
    # 링크 수명은 문서 종류와 무관하게 24시간 고정 (인프라 §5)
    assert monthly.presigned_ttl_hours == constants.PRESIGNED_URL_TTL_HOURS
    assert guideline.presigned_ttl_hours == constants.PRESIGNED_URL_TTL_HOURS
    # 등록되지 않은 종류는 6개월 프리픽스에 쌓지 않는다
    assert resolve_storage_policy("unknown").retention_hours == guideline.retention_hours


@pytest.mark.asyncio
async def test_upload_refuses_when_s3_not_configured() -> None:
    """S3 미구성 상태에서는 성공을 반환하지 않는다 — 죽은 링크가 나가는 것을 막는다."""
    with patch("app.reporting.s3_uploader.S3_ENABLED", False), pytest.raises(S3NotConfiguredError):
        await upload_pdf_to_s3(
            pdf_bytes=b"%PDF", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id="c0ffee00-0000-4000-8000-000000000000",
        )


@pytest.mark.asyncio
async def test_upload_sets_object_expiry_by_policy() -> None:
    """업로드 결과에 자동 삭제 시각(다운로드 기한)이 정책대로 박혀야 한다."""
    before = datetime.now(UTC)
    with patch("app.reporting.s3_uploader.S3_ENABLED", True):
        monthly_meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )
        guideline_meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_GUIDELINE, period="2026-05",
            company_id=_COMPANY_ID,
        )

    monthly_days = (monthly_meta.object_expires_at - before).total_seconds() / 86400
    guideline_hours = (guideline_meta.object_expires_at - before).total_seconds() / 3600
    assert round(monthly_days) == constants.MONTHLY_RETENTION_DAYS
    assert round(guideline_hours) == constants.GUIDELINE_RETENTION_HOURS
    assert monthly_meta.presigned_expires_at <= monthly_meta.object_expires_at
    assert guideline_meta.presigned_expires_at <= guideline_meta.object_expires_at


def test_schema_rejects_link_outliving_object() -> None:
    """객체보다 오래 사는 링크는 스키마가 거부한다 — 지워진 파일을 안내하게 된다."""
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="presigned_expires_at"):
        PdfS3Meta(
            s3_bucket_name="sellon-temp-reports",
            s3_file_path="reports/cs_guidelines/2026/05/",
            original_file_name="a.pdf",
            new_file_name="b.pdf",
            s3_full_key="reports/cs_guidelines/2026/05/b.pdf",
            created_at=now,
            file_size_bytes=1024,
            presigned_expires_at=now + timedelta(days=7),
            object_expires_at=now + timedelta(hours=24),
        )


@pytest.mark.asyncio
async def test_monthly_callback_rejects_source_payload_requirement(monthly_input, monthly_output) -> None:
    """스키마가 월간 SUCCESS 콜백에 source_payload 를 요구하지 않아야 한다."""
    meta = PdfS3Meta(
        s3_bucket_name="sellon-reports",
        s3_file_path="reports/monthly/2026/07/",
        original_file_name="monthly.pdf",
        new_file_name="monthly_1.pdf",
        s3_full_key="reports/monthly/2026/07/monthly_1.pdf",
        created_at=datetime.now(UTC),
        file_size_bytes=482913,
    )
    callback = build_monthly_callback(
        status=CallbackStatus.SUCCESS, report_id="RPT-202607-P001", pdf_s3_meta=meta,
    )
    assert callback.source_payload is None
    assert callback.pdf_s3_meta.s3_bucket_name == "sellon-reports"

    # 반대로 CS 가이드라인은 source_payload 가 없으면 스키마가 거부해야 한다
    with pytest.raises(ValueError, match="source_payload"):
        GenerationCallback(
            guideline_id="GD-20260528-P001-COUPANG",
            status=CallbackStatus.SUCCESS,
            pdf_s3_meta=meta,
        )


def test_book_template_renders_without_undefined_vars(monthly_input, monthly_output) -> None:
    """합본 템플릿이 필요한 컨텍스트를 전부 받는지 — 렌더링까지 해봐야 잡힌다.

    파이프라인 테스트는 compile_monthly_book 을 mock 하므로 템플릿 변수 누락을 못 잡는다.
    실제로 `pair_label` 을 {% with %} 바인딩에 안 넘겨 합본만 터진 적이 있다.
    """
    context = build_book_context(
        "2026-07",
        [
            {
                "input": monthly_input.model_dump(mode="json"),
                "report": monthly_output.model_dump(mode="json"),
            }
        ],
    )
    env = Environment(
        loader=BaseLoader(),
        autoescape=select_autoescape(["html"]),
        undefined=StrictUndefined,  # 정의 안 된 변수를 즉시 에러로
    )
    html = env.from_string(MONTHLY_BOOK_HTML).render(**context)

    assert "쿠팡 vs 네이버" in html  # 채널 라벨이 한글로 치환됐는지
    assert "원인 분석 결과" in html  # 채널쌍 카드 안으로 옮겨간 항목
    assert "권장 조치 사항" in html
    assert "CRITICAL RISKS" not in html  # 삭제된 카드
    assert "STABLE" not in html  # 삭제된 상태 배지


@pytest.mark.asyncio
async def test_book_notice_separates_held_and_failed(monthly_input, monthly_output) -> None:
    """보류(표본 부족)와 실패(검증 미통과)를 콜백 안내 문구에서 구분한다.

    표지 페이지를 없앤 뒤(2026-08-04) 이 안내는 notice_message 로만 나간다. 둘을 합쳐
    보내면 VOC 500건인 상품이 "VOC 10건 미만이라 분석하지 않았다"고 잘못 안내된다.
    """
    items = [{"input": monthly_input, "report": monthly_output}]
    with (
        patch("app.reporting.monthly_report_service.compile_monthly_book", return_value=b"%PDF-"),
        patch(
            "app.reporting.monthly_report_service.upload_pdf_to_s3", new_callable=AsyncMock
        ) as mock_upload,
    ):
        mock_upload.return_value = PdfS3Meta(
            s3_bucket_name="sellon-reports",
            s3_file_path="reports/monthly/2026/08/",
            original_file_name="monthly_2026-07.pdf",
            new_file_name="monthly_ALL_2026-07_20260801_a1b2.pdf",
            s3_full_key="reports/monthly/2026/08/monthly_ALL_2026-07_20260801_a1b2.pdf",
            created_at=datetime.now(UTC),
            file_size_bytes=497000,
        )
        result = await compile_and_upload_monthly_book(
            "2026-07", items, held_products=["P090"], failed_products=["P001"]
        )

    notice = result.callback.notice_message
    assert notice is not None
    held_part, failed_part = notice.split("생성에 실패해")
    assert "P090" in held_part
    assert "P001" in failed_part
    assert "미만" not in failed_part  # 실패 상품에 표본 부족이라 적지 않는다


def test_book_has_no_cover_page(monthly_input, monthly_output) -> None:
    """총합 요약(표지) 페이지는 만들지 않는다 — 첫 페이지가 곧 첫 상품 리포트다."""
    context = build_book_context(
        "2026-07",
        [
            {
                "input": monthly_input.model_dump(mode="json"),
                "report": monthly_output.model_dump(mode="json"),
            }
        ],
    )
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    html = env.from_string(MONTHLY_BOOK_HTML).render(**context)

    assert "월간 CS·품질 분석 보고서" not in html  # 표지 제목
    assert "상품별 요약" not in html  # 표지 표
    assert html.index(monthly_input.product_name) < len(html)  # 첫 상품이 곧바로 나온다


# ── 5. 프롬프트 압축 (토큰 절감이 데이터를 흘리지 않는지) ────────────────


def test_monthly_prompt_carries_all_data(monthly_input) -> None:
    """표로 압축해도 속성·채널쌍·단계 라벨이 하나도 빠지지 않아야 한다."""
    prompt = monthly_report_service.build_prompt(monthly_input)

    for dist in monthly_input.aspect_distributions:
        assert dist.aspect.value in prompt
    for pair in monthly_input.channel_divergence.pairs:
        assert pair.comparison_pair in prompt
    # 비율은 %로 미리 환산해서 넣는다(모델이 0.5→50% 계산하다 틀리는 걸 막는 목적)
    assert "|20|30|50|" in prompt
    assert "위험 단계" in prompt  # worst_pair 가 CRISIS
    assert str(monthly_input.total_voc_count) in prompt
    # p값은 §4-4 금지 표현이라 애초에 모델에게 보여주지 않는다
    assert "0.001" not in prompt


def test_cs_prompt_carries_all_inquiries(cs_input) -> None:
    """문의 표에 모든 item_id 와 원문이 실려야 한다."""
    prompt = cs_reply_service.build_prompt(cs_input)

    for inquiry in cs_input.linked_inquiries:
        assert inquiry.item_id in prompt
        assert inquiry.raw_text in prompt
    assert cs_input.root_cause.label in prompt
    assert "0.002" not in prompt  # p값 미노출


def test_cs_prompt_sanitizes_table_breakers(cs_input) -> None:
    """원문에 파이프·줄바꿈이 있어도 표가 깨지지 않아야 한다(행 수 유지)."""
    cs_input.linked_inquiries[0].raw_text = "색상이 다름 | 사이즈도\n작아요"
    prompt = cs_reply_service.build_prompt(cs_input)

    table = prompt.split("[문의] 문의ID|원문\n")[1].split("\n\n")[0]
    assert len(table.splitlines()) == len(cs_input.linked_inquiries)
    assert "색상이 다름 / 사이즈도 작아요" in prompt


def test_compact_prompt_is_smaller_than_previous_version(monthly_input, cs_input) -> None:
    """토큰 절감판이 실제로 더 짧은지. 구버전 파일은 비교 실험용으로 남겨둔다."""
    assert len(monthly_report_service.build_prompt(monthly_input, prompt_version="monthly_report_v4")) < len(
        monthly_report_service.build_prompt(monthly_input, prompt_version="monthly_report_v3")
    )
    assert len(cs_reply_service.build_prompt(cs_input, prompt_version="cs_reply_v3")) < len(
        cs_reply_service.build_prompt(cs_input, prompt_version="cs_reply_v2")
    )


# ── 6. metrics_calculator (§4-2 판정식) ──────────────────────────────────


def test_jsd_bits_range() -> None:
    """같은 분포면 0, 완전히 갈리면 1 (bits 기준이라 상한이 1.0)."""
    assert calculate_jsd_bits([50, 30, 20], [50, 30, 20]) == pytest.approx(0.0, abs=1e-9)
    assert calculate_jsd_bits([100, 0, 0], [0, 0, 100]) == pytest.approx(1.0, abs=1e-9)


def test_divergence_gate() -> None:
    """min(n_A, n_B) ≥ 1 AND N ≥ 30 을 못 넘으면 보류 사유가 나온다."""
    assert check_divergence_gate(0, 50) == HoldReason.EMPTY_CHANNEL
    assert check_divergence_gate(10, 15) == HoldReason.INSUFFICIENT_SAMPLE
    assert check_divergence_gate(60, 60) is None


@pytest.mark.parametrize(
    ("excess", "significant", "expected_severity", "expected_crisis"),
    [
        (0.05, True, Severity.SAFE, False),      # δ_min 미만
        (0.15, True, Severity.CAUTION, True),    # δ_min ~ 2δ_min
        (0.25, True, Severity.CRISIS, True),     # 2δ_min 이상
        (0.25, False, Severity.SAFE, False),     # 유의하지 않으면 excess 가 커도 SAFE
    ],
)
def test_decide_severity(excess, significant, expected_severity, expected_crisis) -> None:
    """§4-2 판정식. 다중검정을 통과 못 한 차이는 우연으로 보고 SAFE 로 떨어뜨린다."""
    baseline = 0.10
    severity, is_crisis = decide_severity(
        baseline + excess, baseline, bh_significant=significant
    )
    assert severity == expected_severity
    assert is_crisis == expected_crisis


def test_hold_pair_has_all_judgement_fields_null() -> None:
    """게이트 미충족 쌍은 판정 6개 값이 전부 null 이어야 한다(반쪽 상태 금지)."""
    pair, p_value = build_channel_divergence_pair(
        "COUPANG_VS_ZIGZAG", [5, 2, 1], [3, 1, 0], n_permutations=50
    )
    assert pair.hold_reason == HoldReason.INSUFFICIENT_SAMPLE
    assert p_value is None
    assert (pair.jsd_score, pair.jsd_baseline, pair.p_value) == (None, None, None)
    assert (pair.bh_significant, pair.is_crisis, pair.severity) == (None, None, None)


def test_permutation_test_detects_real_divergence() -> None:
    """분포가 확연히 다른 두 채널은 순열검정에서 낮은 p값이 나와야 한다."""
    jsd, baseline, p_value = permutation_test_jsd(
        [80, 10, 10], [10, 10, 80], n_permutations=200, seed=42
    )
    assert jsd > baseline  # 관측값이 귀무 기댓값보다 커야 excess 가 양수가 된다
    assert p_value < 0.05


@pytest.mark.asyncio
async def test_cs_pipeline_fails_validation_on_ungrounded_id(cs_input, cs_output) -> None:
    bad = cs_output.model_copy(deep=True)
    bad.inquiry_specific_guides[0].item_id = "INQ-999999"

    with patch("app.reporting.cs_reply_service.get_llm_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = bad.model_dump(mode="json")
        mock_get_client.return_value = mock_client

        result = await generate_cs_reply_pipeline(cs_input)

    assert result.output is None
    assert result.callback.status == CallbackStatus.FAILED_VALIDATION
    assert result.callback.source_payload is None
