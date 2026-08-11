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

import re
import shutil
import tempfile
from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from jinja2 import BaseLoader, Environment, StrictUndefined, select_autoescape

from app.core import constants
from app.core.ids import build_guideline_id
from app.core.schemas import (
    Aspect,
    CallbackStatus,
    Channel,
    CSGuidelineInput,
    CSGuidelineOutput,
    DetectionConfidence,
    Evidence,
    GenerationCallback,
    HoldReason,
    LinkedCSInquiry,
    MonthlyReportInput,
    MonthlyReportOutput,
    PdfS3Meta,
    RecommendedAction,
    Severity,
    Source,
    Verdict,
)
from app.reporting import cs_reply_service, monthly_report_service, s3_uploader
from app.reporting.callback import build_monthly_callback
from app.reporting.cs_reply_service import (
    build_guideline_input,
    generate_cs_reply_pipeline,
    generate_guideline,
    is_guideline_target,
)
from app.reporting.cs_reply_validator import validate_cs_guideline
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
    S3UploadError,
    _build_original_file_name,
    build_object_path,
    resolve_storage_policy,
    upload_pdf_to_s3,
)


@pytest.fixture
def short_mirror_dir():
    """MAX_PATH 여유가 있는 짧은 임시 폴더.

    pytest 의 `tmp_path` 는 경로가 깊어, 실제 버킷명(48자) + S3 키(약 110자)를 얹으면
    Windows MAX_PATH(260자)를 넘긴다. 미러는 개발 편의 장치라 그때 경고만 남기고
    넘어가므로(생성 자체는 보호된다), 레이아웃 검증에는 짧은 뿌리를 쓴다.
    """
    directory = Path(tempfile.mkdtemp(prefix="m", dir=tempfile.gettempdir()))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture(autouse=True)
def block_real_s3():
    """테스트가 **실제 AWS 를 호출하지 않게** 막고, 자격증명도 가짜로 고정한다.

    `upload_pdf_to_s3` 는 이제 boto3 로 진짜 올린다. 막지 않으면 테스트가 네트워크를 타고,
    자격증명이 있으면 개발 버킷에 쓰레기 객체를 쌓는다.

    ⚠️ 키까지 여기서 patch 하는 이유: 키 존재 검사가 `_get_s3_client()` 호출보다 **한 칸
       위**에 있어서, 클라이언트를 막는 것만으로는 그 전에 걸린다. 그러면 `.env` 에 키가
       있는 사람만 통과하고 없는 사람은 9개가 `S3NotConfiguredError` 로 죽는다 — CI 가
       따로 없어 **각자 로컬이 곧 CI** 인데 "PR 전 pytest 통과" 게이트가 사람마다 달라진다.
       AWS 에 닿지도 않는 로컬 미러 테스트까지 "정적 액세스 키가 없습니다" 로 죽어서
       원인 파악도 어렵다.

       가짜 값이어도 되는 건 위에서 실제 서명 경로를 이미 가로챘기 때문이다. 반대로 키를
       채우라고 안내하는 쪽은, 빈 문자열 검사 하나를 맞추려고 **실제 정적 키를 팀에
       배포하는** 모순이 된다.
    """
    client = MagicMock()
    client.generate_presigned_url.return_value = "https://example.test/signed"
    real_factory = s3_uploader._get_s3_client  # 클라이언트 생성 규칙 자체를 검증할 때 쓴다
    with (
        patch("app.reporting.s3_uploader._get_s3_client", return_value=client) as factory,
        patch("app.reporting.s3_uploader.AWS_ACCESS_KEY_ID", "AKIATEST"),
        patch("app.reporting.s3_uploader.AWS_SECRET_ACCESS_KEY", "secret"),
    ):
        factory.client = client
        factory.real = real_factory
        yield factory


# 인프라 문서의 {company_id} 자리 — 실제 값은 고객사 PK/UUID 다.
_COMPANY_ID = "c0ffee00-0000-4000-8000-000000000000"


# ── 1. 픽스처 ────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def mock_infrastructure() -> Generator[None, None, None]:
    """PDF 컴파일(weasyprint)과 S3 업로드를 자동 mock. 테스트는 네트워크·GTK 를 타지 않는다."""
    dummy_s3_meta = PdfS3Meta(
        company_id=_COMPANY_ID,
        company_name="주식회사 셀론",
        s3_bucket_name="mock-bucket",
        s3_file_path="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/",
        original_file_name="mock_report.pdf",
        new_file_name="mock_report_123.pdf",
        s3_full_key="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/mock_report_123.pdf",
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
        # 업로드를 mock 했으면 그 **사전 점검**도 같이 통과시켜야 한다. 점검이 LLM 호출
        # 앞으로 당겨져 있어서(비용 0 으로 거르려고), 여기를 빼면 파이프라인이 생성도
        # 해보기 전에 FAILED_ERROR 로 끝난다.
        patch(
            "app.reporting.cs_reply_service.ensure_s3_ready",
            return_value=_COMPANY_ID,
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


def _held_input(
    product_group_id: str, *, voc: int = 4, name: str | None = None
) -> MonthlyReportInput:
    """표본 부족으로 보류될 상품의 입력. 지면의 보류 페이지가 이 값을 찍는다.

    채널쌍은 전부 보류로 둔다 — VOC 가 10건도 안 되는 상품은 채널쌍 판정도 설 수 없다.

    ⚠️ 기본 이름은 13자라 **안내 문구 길이 검증에는 못 쓴다.** 실제 `channel_product_name`
       은 커머스 노출명이라 훨씬 길다. 길이를 재는 테스트는 `name` 으로 실제 길이를 준다.
    """
    return MonthlyReportInput(
        report_month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 31),
        product_group_id=product_group_id,
        product_name=name if name is not None else f"보류상품 {product_group_id}",
        total_voc_count=voc,
        aspect_distributions=[
            {"aspect": a, "total_count": 1, "positive_ratio": 0.2,
             "neutral_ratio": 0.3, "negative_ratio": 0.5}
            for a in ("색상", "사이즈", "소재")
        ],
        sentiment_drifts=[
            {"aspect": a, "drift_rate": 0.0, "status": "NORMAL"}
            for a in ("색상", "사이즈", "소재")
        ],
        channel_divergence={
            "calculated_at": datetime(2026, 8, 1, tzinfo=UTC),
            "worst_pair": "COUPANG_VS_NAVER",
            "is_crisis": None,
            "pairs": [
                {"comparison_pair": "COUPANG_VS_NAVER", "sample_size": voc,
                 "hold_reason": "INSUFFICIENT_SAMPLE"}
            ],
        },
        recommended_id=f"REC-202607-{product_group_id}",
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
            company_id=_COMPANY_ID,
        s3_bucket_name="sellon-reports",
            s3_file_path="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/",
            original_file_name="monthly_2026-07.pdf",
            new_file_name="monthly_ALL_2026-07_20260801_a1b2.pdf",
            s3_full_key="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/monthly_ALL_2026-07_20260801_a1b2.pdf",
            created_at=datetime.now(UTC),
            file_size_bytes=497000,
        )
        result = await compile_and_upload_monthly_book(
            "2026-07", items, held_inputs=[_held_input("P099")]
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



def test_object_path_follows_infra_rule() -> None:
    """인프라 「S3 파일 구조 규칙 정의」 경로·파일명 규칙 (2026-08-05 확정).

        reports/{report_type}/{company_id}/{yyyy}/{mm}/
        {report_type}_{yyyyMM}_{uuid4}.pdf
    """
    path, name = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)
    assert path == f"reports/monthly-report/{_COMPANY_ID}/2026/07/"
    assert name.startswith("monthly-report_202607_")
    assert name.endswith(".pdf")
    # uuid4 가 붙어 같은 달을 다시 올려도 이전 객체를 덮어쓰지 않는다
    _, name2 = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)
    assert name != name2

    cs_path, cs_name = build_object_path(REPORT_TYPE_GUIDELINE, "2026-05", _COMPANY_ID)
    assert cs_path == f"reports/cs-guideline/{_COMPANY_ID}/2026/05/"
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


@pytest.mark.asyncio
async def test_upload_carries_company_metadata() -> None:
    """회사 구분을 **메타데이터로** 실어 보낸다 (2026-08-06 확정).

    경로가 `reports/{report_type}/{company_id}/…` 로 회사 단위로 갈리는데 그 값이 어느 입력
    스키마에도 없어, 산출물만 보고는 어느 회사 것인지 알 수 없었다. `company_id` 를 실어
    메인이 **S3 키를 파싱하지 않고** 바로 알 수 있게 한다.
    """
    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        patch("app.reporting.s3_uploader.S3_DEFAULT_COMPANY_NAME", "주식회사 셀론"),
    ):
        meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )

    assert meta.company_id == _COMPANY_ID
    assert meta.company_name == "주식회사 셀론"
    # 경로에 박힌 회사 구간과 같은 값이어야 한다
    assert f"/{_COMPANY_ID}/" in meta.s3_file_path


def test_company_name_is_not_used_in_path() -> None:
    """회사명은 **경로에 쓰지 않는다** — 이름이 바뀌면 경로가 갈라져 이전 산출물을 못 찾는다."""
    path, _ = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)

    assert "주식회사" not in path
    assert path == f"reports/monthly-report/{_COMPANY_ID}/2026/07/"


def test_schema_rejects_company_id_mismatched_with_path() -> None:
    """메타의 company_id 와 경로의 회사 구간이 다르면 거부한다.

    둘이 어긋나면 메인이 무엇을 믿어야 할지 알 수 없다 — 조용히 남의 회사 것으로 분류된다.
    """
    with pytest.raises(ValueError, match="company_id"):
        PdfS3Meta(
            company_id="other-company",
            s3_bucket_name="sellon-reports",
            s3_file_path=f"reports/monthly-report/{_COMPANY_ID}/2026/07/",
            original_file_name="monthly-report_202607.pdf",
            new_file_name="a.pdf",
            s3_full_key=f"reports/monthly-report/{_COMPANY_ID}/2026/07/a.pdf",
            created_at=datetime.now(UTC),
            file_size_bytes=1024,
        )


@pytest.mark.asyncio
async def test_local_mirror_reproduces_bucket_layout(short_mirror_dir) -> None:
    """로컬 미러는 **버킷 키와 똑같은 경로**로 떨어진다.

    스텁은 원래 바이트를 버려서 경로 규칙이 맞는지 산출물로 확인할 방법이 없다. 미러가
    있으면 boto3 연동 전에 리허설이 된다 — 규칙이 깨지면 트리 모양으로 바로 드러난다.
    """
    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        patch("app.reporting.s3_uploader.S3_LOCAL_MIRROR_DIR", str(short_mirror_dir)),
    ):
        meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )

    mirrored = short_mirror_dir / meta.s3_bucket_name / meta.s3_full_key
    assert mirrored.is_file()
    assert mirrored.read_bytes() == b"%PDF-MOCK"
    # 인프라 규칙 그대로 — 회사/문서종류/연/월 순으로 갈린다
    parts = mirrored.relative_to(short_mirror_dir).parts
    # ⚠️ report_type 이 company_id 보다 **위**다 — Lifecycle 이 리터럴 prefix 완전 일치만
    #    지원해서, 회사가 위면 문서 종류별로 규칙을 걸 수 없다.
    assert parts[1:6] == ("reports", "monthly-report", _COMPANY_ID, "2026", "07")


@pytest.mark.asyncio
async def test_local_mirror_is_off_by_default() -> None:
    """미러가 꺼져 있으면 파일을 쓰지 않는다 — 운영 기본 동작은 그대로다."""
    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        patch("app.reporting.s3_uploader.S3_LOCAL_MIRROR_DIR", ""),
        patch("app.reporting.s3_uploader.Path") as mock_path,
    ):
        await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )

    mock_path.assert_not_called()


def test_extension_is_not_a_separate_field() -> None:
    """확장자는 파일명에만 있고 별도 컬럼으로 두지 않는다 (인프라 §4).

    "확장자는 파일명에 `.pdf` 로 고정 포함(이미지와 다르게 DB 별도 컬럼에 저장하지 않음)"
    이 규칙이다. 같은 값을 두 군데 들고 다니면 둘이 어긋날 수 있다.
    """
    assert "file_extension" not in PdfS3Meta.model_fields

    _, new_file_name = build_object_path(REPORT_TYPE_MONTHLY, "2026-07", _COMPANY_ID)
    assert new_file_name.endswith(".pdf")


@pytest.mark.asyncio
async def test_presigned_link_never_outlives_object() -> None:
    """링크(7일)가 객체보다 오래 살지 않는다.

    CS 는 객체도 7일이라 두 시각이 같고, 월간은 객체 6개월이라 링크가 먼저 만료된다.
    링크가 더 길면 "받을 수 있다"고 안내해 놓고 실제로는 사라진 파일을 가리키게 된다.
    """
    with patch("app.reporting.s3_uploader.S3_ENABLED", True):
        monthly = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )
        guideline = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF", report_type=REPORT_TYPE_GUIDELINE, period="2026-07",
            company_id=_COMPANY_ID, source_id="ALT-1",
        )

    assert monthly.presigned_expires_at < monthly.object_expires_at
    assert guideline.presigned_expires_at == guideline.object_expires_at


@pytest.mark.asyncio
async def test_upload_puts_object_then_signs(block_real_s3) -> None:
    """올린 **뒤에** 서명한다. 반대면 업로드 실패인데 링크만 나가 셀러가 404 를 본다."""
    client = block_real_s3.client
    with patch("app.reporting.s3_uploader.S3_ENABLED", True):
        meta = await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )

    put = client.put_object.call_args.kwargs
    assert put["Bucket"] == meta.s3_bucket_name
    assert put["Key"] == meta.s3_full_key
    assert put["Body"] == b"%PDF-MOCK"
    # 없으면 S3 가 binary/octet-stream 으로 저장해 브라우저가 뷰어 대신 다운로드를 띄운다
    assert put["ContentType"] == "application/pdf"

    sign = client.generate_presigned_url.call_args
    assert sign.args[0] == "get_object"
    assert sign.kwargs["Params"] == {"Bucket": meta.s3_bucket_name, "Key": meta.s3_full_key}
    # 7일 = SigV4 상한
    assert sign.kwargs["ExpiresIn"] == 7 * 24 * 3600
    assert meta.presigned_url == "https://example.test/signed"


@pytest.mark.asyncio
async def test_upload_failure_raises_instead_of_reporting_success(block_real_s3) -> None:
    """업로드가 실패하면 예외다 — 올리지 못한 파일을 성공으로 보고하지 않는다."""
    from botocore.exceptions import ClientError

    block_real_s3.client.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
    )
    with (
        patch("app.reporting.s3_uploader.S3_ENABLED", True),
        pytest.raises(S3UploadError, match="업로드·서명 실패"),
    ):
        await upload_pdf_to_s3(
            pdf_bytes=b"%PDF-MOCK", report_type=REPORT_TYPE_MONTHLY, period="2026-07",
            company_id=_COMPANY_ID,
        )


def test_s3_client_signs_with_static_keys_and_sigv4(block_real_s3) -> None:
    """정적 키 + SigV4 로 클라이언트를 만든다.

    ⚠️ 기본 자격증명 체인(IAM Role)을 쓰면 임시 크리덴셜 만료 시 URL 도 같이 죽어 7일이
       유지되지 않는다. SigV4 도 못 박아야 한다 — 구버전 서명은 7일을 못 버틴다.
    """
    with (
        patch("app.reporting.s3_uploader.AWS_ACCESS_KEY_ID", "AKIATEST"),
        patch("app.reporting.s3_uploader.AWS_SECRET_ACCESS_KEY", "secret"),
        patch("boto3.client") as mock_client,
    ):
        block_real_s3.real()

    kwargs = mock_client.call_args.kwargs
    assert kwargs["aws_access_key_id"] == "AKIATEST"
    assert kwargs["aws_secret_access_key"] == "secret"
    assert kwargs["region_name"] == s3_uploader.S3_REGION
    assert kwargs["config"].signature_version == "s3v4"


def test_storage_policy_differs_by_document_type() -> None:
    """월간 6개월 / CS 7일 자동 삭제 (S3 Lifecycle).

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
    # 링크 수명은 문서 종류와 무관하게 **7일 고정** (인프라 §5) — SigV4 상한이기도 하다
    assert monthly.presigned_ttl_hours == constants.PRESIGNED_URL_TTL_HOURS
    assert guideline.presigned_ttl_hours == constants.PRESIGNED_URL_TTL_HOURS
    assert constants.PRESIGNED_URL_TTL_HOURS == 7 * 24
    # CS 는 운영 MD 승인(사람 단계) 뒤에 메일이 나가므로 하루로는 부족하다 —
    # 승인 대기 중에 객체가 사라지면 발송할 것이 없어진다.
    assert guideline.retention_hours == 7 * 24
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
            company_id=_COMPANY_ID,
        s3_bucket_name="sellon-temp-reports",
            s3_file_path="reports/cs-guideline/c0ffee00-0000-4000-8000-000000000000/2026/05/",
            original_file_name="a.pdf",
            new_file_name="b.pdf",
            s3_full_key="reports/cs-guideline/c0ffee00-0000-4000-8000-000000000000/2026/05/b.pdf",
            created_at=now,
            file_size_bytes=1024,
            presigned_expires_at=now + timedelta(days=7),
            object_expires_at=now + timedelta(hours=24),
        )


@pytest.mark.asyncio
async def test_monthly_callback_rejects_source_payload_requirement(monthly_input, monthly_output) -> None:
    """스키마가 월간 SUCCESS 콜백에 source_payload 를 요구하지 않아야 한다."""
    meta = PdfS3Meta(
        company_id=_COMPANY_ID,
        s3_bucket_name="sellon-reports",
        s3_file_path="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/07/",
        original_file_name="monthly.pdf",
        new_file_name="monthly_1.pdf",
        s3_full_key="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/07/monthly_1.pdf",
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
            company_id=_COMPANY_ID,
        s3_bucket_name="sellon-reports",
            s3_file_path="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/",
            original_file_name="monthly_2026-07.pdf",
            new_file_name="monthly_ALL_2026-07_20260801_a1b2.pdf",
            s3_full_key="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/08/monthly_ALL_2026-07_20260801_a1b2.pdf",
            created_at=datetime.now(UTC),
            file_size_bytes=497000,
        )
        result = await compile_and_upload_monthly_book(
            "2026-07", items, held_inputs=[_held_input("P090")], failed_products=["P001"]
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

    table = prompt.split("[원문] ID|출처|내용\n")[1].split("\n\n")[0]
    assert len(table.splitlines()) == len(cs_input.linked_inquiries)
    assert "색상이 다름 / 사이즈도 작아요" in prompt


def test_cs_prompt_labels_review_rows(cs_input) -> None:
    """🔴 리뷰 원문은 표에 **리뷰**로 표기된다 — 접두사 추측에 맡기지 않는다.

    리뷰를 CS 가이드라인 근거로 쓰는 것이 확정 정책이다(2026-08-11). 그런데 리뷰는
    **공개 답글**이라 응대가 다르다 — 답글로는 반품·교환을 접수할 수 없다. 표에 출처가
    없으면 모델이 전부 1:1 문의로 답해서 **지키지 못할 약속**("무상 교환·반품을
    도와드리겠습니다")이 리뷰 답글로 나간다.

    `RVW-` 접두사로 추측시키지 않는 이유: ID 규칙이 바뀌면 조용히 틀린다.
    """
    cs_input.linked_inquiries[0].source = Source.CS
    cs_input.linked_inquiries.append(
        LinkedCSInquiry(
            item_id="RVW-000002",
            raw_text="실물 색이 더 어둡네요",
            created_at=datetime(2026, 5, 27, 11, 0, tzinfo=UTC),
            source=Source.REVIEW,
        )
    )

    table = cs_reply_service._build_inquiry_table(cs_input)
    rows = {line.split("|")[0]: line.split("|")[1] for line in table.splitlines()}

    assert rows["INQ-000001"] == "문의"
    assert rows["RVW-000002"] == "리뷰"


def test_unknown_source_is_treated_as_inquiry(cs_input) -> None:
    """⚠️ 출처 미상(None)은 **문의**로 본다 — 어긋났을 때 덜 나쁜 쪽이다.

    `build_linked_inquiries` 는 `source` 값이 이상하거나 키가 없으면 None 을 넣는다
    (2026-08-11, PR #58). 그때 "리뷰 답글" 톤으로 쓰면 **답변을 기다리는 고객에게
    "고객센터로 연락 주세요" 가 나간다.** 반대(리뷰에 문의 답변 톤)는 어색할 뿐이지만
    이쪽은 응대 자체가 어긋나므로, 모르면 문의 쪽으로 기운다.
    """
    cs_input.linked_inquiries[0].source = None

    table = cs_reply_service._build_inquiry_table(cs_input)

    assert table.splitlines()[0].split("|")[1] == "문의"


def test_old_prompt_versions_keep_the_two_column_table(cs_input) -> None:
    """⚠️ 구버전에는 출처 열을 넣지 않는다 — 버전 비교 실험의 조건이 달라진다.

    v4 의 헤더는 `[문의] 문의ID|원문` 이라 2열이다. 3열을 주면 자기가 선언하지 않은 열을
    받게 되고, 구버전을 남겨 둔 이유(정량 비교 — CLAUDE.md 4)가 무너진다. 예전에 잰
    토큰·정확도와 지금 수치를 나란히 놓을 수 없게 된다.
    """
    cs_input.linked_inquiries[0].source = Source.REVIEW

    v4 = cs_reply_service.build_prompt(cs_input, prompt_version="cs_reply_v4")
    v5 = cs_reply_service.build_prompt(cs_input)

    v4_row = v4.split("[문의] 문의ID|원문\n")[1].splitlines()[0]
    v5_row = v5.split("[원문] ID|출처|내용\n")[1].splitlines()[0]

    assert v4_row.count("|") == 1, f"v4 표에 열이 늘었다: {v4_row}"
    assert v5_row.count("|") == 2, f"v5 표에 출처 열이 없다: {v5_row}"
    assert "|리뷰|" in v5_row and "리뷰" not in v4_row


def test_v5_tells_the_model_reviews_cannot_accept_returns(cs_input) -> None:
    """v5 프롬프트가 리뷰 답글의 **조치 한계**를 지시한다.

    이게 v5 를 만든 이유다. v4 는 `draft_reply` 를 "사과 → 원인 설명 → 즉시 조치(무상
    교환·반품)" 로 못박아서, 리뷰에 그대로 쓰면 답글로는 못 하는 일을 약속한다.
    """
    prompt = cs_reply_service.build_prompt(cs_input)

    assert "리뷰 답글" in prompt
    assert "고객센터" in prompt, "답글로 접수가 안 된다면 어디로 유도할지 알려줘야 한다"

    v4 = cs_reply_service.build_prompt(cs_input, prompt_version="cs_reply_v4")
    assert "리뷰" not in v4, "v4 는 리뷰를 모른다 — 그래서 v5 를 만들었다(구버전은 보존)"


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


# ── 배치 진입점 (app/batch/daily.py ↔ generate_guideline) ────────────────


def _inquiries(*ids: str) -> list[LinkedCSInquiry]:
    return [
        LinkedCSInquiry(item_id=i, raw_text="색이 사진과 달라요", created_at=datetime(2026, 5, 20, tzinfo=UTC))
        for i in ids
    ]


def test_guideline_input_narrows_detection_models(biased_alert) -> None:
    """DetectionAlert → CSGuidelineInput 매핑에서 두 필드가 떨어져 나간다.

    `DetectionStats.source` 와 `RootCause.consistent` 는 가이드라인 입력에 없다. 탐지가
    판정에 쓰는 값이지 상담원에게 보여 줄 내용이 아니다(§4-4). 나머지는 그대로 옮긴다.
    """
    result = build_guideline_input(
        biased_alert, _inquiries("INQ-000412", "INQ-000415"), product_name="미디 원피스"
    )

    assert result.alert_id == biased_alert.alert_id
    assert result.product_group_id == biased_alert.product_group_id
    assert result.product_name == "미디 원피스"
    assert result.stats.cur_rate == biased_alert.stats.cur_rate
    assert result.stats.cur_total == biased_alert.stats.cur_total
    assert not hasattr(result.stats, "source")
    assert result.root_cause is not None
    assert result.root_cause.label == biased_alert.root_cause.label
    assert not hasattr(result.root_cause, "consistent")
    assert [i.item_id for i in result.linked_inquiries] == ["INQ-000412", "INQ-000415"]


def test_out_of_scope_alert_is_not_a_guideline_target(biased_alert) -> None:
    """가리키는 문의가 없는 알림은 **대상이 아니다** — 실패가 아니다.

    ⚠️ 원인 분류([6])는 스코프 안(색상·사이즈·소재) 알림만 타므로 파손·오배송 알림은
       `evidence.inquiry_ids` 가 언제나 비어 있다. 이걸 실패로 처리하면 스코프 밖 알림이
       뜰 때마다 배치 요약에 실패가 쌓이고, 정상 동작에 묻혀 진짜 실패를 놓친다.
    """
    out_of_scope = biased_alert.model_copy(
        update={"evidence": Evidence(inquiry_ids=[]), "scope_in": False}
    )

    assert is_guideline_target(biased_alert) is True
    assert is_guideline_target(out_of_scope) is False


@pytest.mark.asyncio
async def test_generate_guideline_skips_non_target_without_llm(biased_alert) -> None:
    """대상이 아니면 None 을 돌려주고 **LLM 을 부르지 않는다**(비용)."""
    out_of_scope = biased_alert.model_copy(update={"evidence": Evidence(inquiry_ids=[])})

    with patch.object(cs_reply_service, "get_llm_client") as client:
        result = await generate_guideline(out_of_scope, [])

    assert result is None
    client.assert_not_called()


@pytest.mark.asyncio
async def test_generate_guideline_raises_when_source_texts_are_missing(biased_alert) -> None:
    """대상인데 원문 조회가 전부 실패하면 예외 — 근거 없는 가이드라인을 만들지 않는다.

    `build_linked_inquiries`(지인)가 원문 없는 ID 를 버리고 경고만 남기므로, 여기까지
    빈 리스트로 오면 조회가 통째로 실패했다는 뜻이다. 조용히 넘어가면 배치 요약에
    안 남는다.
    """
    with pytest.raises(ValueError, match="CS 원문을 하나도 찾지 못해"):
        await generate_guideline(biased_alert, [])


@pytest.mark.asyncio
async def test_generate_guideline_returns_publishable_callback(biased_alert) -> None:
    """성공 콜백이 `publish_guideline_generated` 가 요구하는 모양이다.

    발행기(`app/core/mq.py`)는 ①`guideline_id` 가 채워져 있어야 하고(없으면 월간 리포트로
    보고 ValueError) ②`alert_id` 를 `source_payload["input"]["alert_id"]` 에서 읽는다.
    둘 중 하나라도 빠지면 배치가 발행 단계에서 터진다.
    """
    key = f"reports/cs-guideline/{_COMPANY_ID}/2026/05/cs-guideline_202605_a1b2.pdf"
    callback = GenerationCallback(
        report_id=None,
        guideline_id=build_guideline_id(biased_alert.alert_id),
        status=CallbackStatus.SUCCESS,
        pdf_s3_meta=PdfS3Meta(
            company_id=_COMPANY_ID,
            s3_bucket_name="mock-bucket",
            s3_file_path=f"reports/cs-guideline/{_COMPANY_ID}/2026/05/",
            original_file_name=f"cs-guideline_202605_{biased_alert.alert_id}.pdf",
            new_file_name="cs-guideline_202605_a1b2.pdf",
            s3_full_key=key,
            created_at=datetime.now(UTC),
            file_size_bytes=2048,
            presigned_url="https://mock-s3.amazonaws.com/cs-guideline.pdf",
        ),
        source_payload={"input": {"alert_id": biased_alert.alert_id}, "output": {}},
    )

    async def _pipeline(input_data):
        assert input_data.alert_id == biased_alert.alert_id
        return type("R", (), {"output": None, "callback": callback})()

    with patch.object(cs_reply_service, "generate_cs_reply_pipeline", _pipeline):
        result = await generate_guideline(biased_alert, _inquiries("INQ-000412"))

    assert result is callback
    assert result.guideline_id is not None
    assert result.report_id is None
    assert result.source_payload["input"]["alert_id"] == biased_alert.alert_id


@pytest.mark.asyncio
async def test_unconfigured_s3_fails_before_paying_for_the_llm(cs_input) -> None:
    """S3 가 구성 안 됐으면 **LLM 을 부르기 전에** 끝낸다.

    ⚠️ 업로드는 `LLM 호출 → PDF 컴파일 → S3` 의 마지막 단계다. 점검이 거기 있으면
       알림 1건마다 LLM 값을 다 지불하고 FAILED_ERROR 만 돌아온다. 가이드라인은 개선안과
       달리 발화한 알림 **거의 전부**에 대해 생성되므로 건수가 그대로 비용이다.
       결론은 같고 비용만 0 이어야 한다.
    """
    with (
        patch.object(
            cs_reply_service,
            "ensure_s3_ready",
            side_effect=S3NotConfiguredError("S3_COMPANY_ID 미설정"),
        ),
        patch.object(cs_reply_service, "get_llm_client") as llm,
        patch.object(cs_reply_service, "compile_report_to_pdf") as pdf,
    ):
        result = await generate_cs_reply_pipeline(cs_input)

    llm.assert_not_called()
    pdf.assert_not_called()
    assert result.output is None
    assert result.callback.status == CallbackStatus.FAILED_ERROR
    # 실패도 guideline_id 를 달고 나가야 백엔드가 "생성 중"에서 벗어난다
    assert result.callback.guideline_id is not None


def test_upload_precheck_is_reusable_before_generation() -> None:
    """`ensure_s3_ready` 는 PDF 바이트 없이 부를 수 있다 — 돈 쓰기 전에 부르라고 뺀 함수다."""
    with (
        patch.object(s3_uploader, "S3_ENABLED", True),
        patch.object(s3_uploader, "S3_DEFAULT_COMPANY_ID", _COMPANY_ID),
        patch.object(s3_uploader, "AWS_ACCESS_KEY_ID", "AKIATEST"),
        patch.object(s3_uploader, "AWS_SECRET_ACCESS_KEY", "secret"),
    ):
        assert s3_uploader.ensure_s3_ready() == _COMPANY_ID
        # 인자로 준 값이 환경변수보다 우선한다
        assert s3_uploader.ensure_s3_ready("other-company") == "other-company"

    with (
        patch.object(s3_uploader, "S3_ENABLED", False),
        pytest.raises(S3NotConfiguredError, match="S3_ENABLED=false"),
    ):
        s3_uploader.ensure_s3_ready(_COMPANY_ID)


# ── 보류 상품 지면 노출 (2026-08-09) ──────────────────────────────────────


def test_held_product_gets_its_own_page(monthly_input, monthly_output) -> None:
    """보류 상품도 지면에 남는다 — 사유가 PDF 안에서 읽혀야 한다.

    ⚠️ 예전에는 합본에서 통째로 빼고 콜백 notice_message 로만 알렸다. 표지도 목차도 없는
       구조라 **PDF 만 받아보는 사람은 자기 상품이 왜 없는지 알 방법이 없었다** —
       "빠졌다"는 사실 자체가 문서 어디에도 안 보인다.
    """
    context = build_book_context(
        "2026-07",
        [{"input": monthly_input.model_dump(mode="json"),
          "report": monthly_output.model_dump(mode="json")}],
        held=[_held_input("P090", voc=4).model_dump(mode="json")],
    )

    assert len(context["items"]) == 1
    assert len(context["held"]) == 1
    assert context["held"][0]["product_group_id"] == "P090"
    assert context["hold_notice"] == constants.HOLD_IN_BOOK_NOTICE

    from jinja2 import BaseLoader, Environment, select_autoescape

    from app.reporting.pdf_compiler import MONTHLY_BOOK_HTML

    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html", "xml"]))
    html = env.from_string(MONTHLY_BOOK_HTML).render(**context)

    assert "보류상품 P090" in html
    assert "리포트 생성 보류" in html
    assert constants.HOLD_IN_BOOK_NOTICE in html
    # 보류 페이지도 상품 페이지와 같은 단위로 쪽이 나뉜다
    assert html.count('class="product-page') == 2


def test_hold_notice_says_under_not_at_or_under() -> None:
    """문구가 **미만**이어야 한다 — '이하'로 쓰면 정확히 10건인 상품을 잘못 안내한다.

    보류 조건은 `total_voc_count < MIN_VOC_COUNT_FOR_REPORT` 라, 10건짜리 상품은
    리포트가 정상 생성된다. 그 상품에 "10건 이하라 보류" 라고 적으면 문서가 거짓말을 한다.
    """
    assert f"{constants.MIN_VOC_COUNT_FOR_REPORT}건 미만" in constants.HOLD_IN_BOOK_NOTICE
    assert "이하" not in constants.HOLD_IN_BOOK_NOTICE


def test_callback_notice_still_lists_held_products(monthly_input) -> None:
    """지면에 넣었다고 콜백 안내를 없애지 않는다 — 메인 화면은 PDF 를 열지 않는다."""
    from app.reporting.monthly_report_service import _build_excluded_notice

    notice = _build_excluded_notice([_held_input("P090")], None)

    assert notice is not None
    assert "P090" in notice
    assert "보류상품 P090" in notice  # 상품명도 같이 — 코드만 있으면 셀러가 못 알아본다


@pytest.mark.asyncio
async def test_all_held_reports_why_not_just_that_it_failed() -> None:
    """전 상품이 보류면 PDF 는 안 나가지만 **사유는 실어 보낸다**.

    수록 0개 → FAILED_ERROR 는 계약이다(§5 status 표). 보류 페이지만 있는 PDF 를 SUCCESS
    로 내보내면 메인이 "PDF 첨부 메일 발송"을 타서 분석이 한 건도 없는 문서가 셀러에게 간다.

    ⚠️ 다만 "생성에 실패했다"만 가면 **데이터 파이프라인 고장과 구분이 안 된다** —
       전 상품 표본 부족(신규 고객사 등)은 정상 동작이다. 어느 상품이 왜 빠졌는지 실어야 한다.
    """
    result = await compile_and_upload_monthly_book(
        "2026-07", [], held_inputs=[_held_input("P090"), _held_input("P091")]
    )

    assert result.callback.status == CallbackStatus.FAILED_ERROR
    assert result.callback.pdf_s3_meta is None
    notice = result.callback.notice_message
    assert "P090" in notice and "P091" in notice
    assert "표본 부족" in notice


# 실제 `products.channel_product_name` 을 흉내 낸 이름. `_fetch_product_names()` 가 커머스
# 노출명을 **자르지 않고 그대로** 싣기 때문에 문구에도 이 길이가 그대로 들어온다.
#
# ⚠️ 픽스처 기본 이름(`보류상품 P000`, 13자)으로 재면 상한이 안 걸려도 통과한다 —
#    개수 상한만 있던 시절 목 이름으로는 223자라 통과했지만 이 이름으로는 368자였다
#    (2026-08-09 리뷰 실측). 길이 검증은 반드시 **긴 이름**으로 한다.
_REAL_LENGTH_NAME = "2026 신상 봄가을 여성 미디 원피스 데일리 롱 A라인 5color"

# SEO 키워드가 붙은 노출명. 커머스 노출명은 이 정도 길이가 흔하다.
#
# ⚠️ 38자와 82자는 **서로 다른 것을 잰다.** 38자 이름은 라벨이 45자라 두 번째가 예산에
#    안 들어가 45자에서 멈추지만, 긴 이름은 예산 끝까지 잘려 들어와 **예산을 꽉 채운다.**
#    그래서 38자로만 재면 천장을 못 본다 — 구절당 70자를 주던 시절 실측으로
#    38자는 226자였는데 82자는 256자로 255를 넘겼다(2026-08-10 리뷰).
_KEYWORD_STUFFED_NAME = (
    "2026 신상 봄가을 여성 미디 원피스 데일리 롱 A라인 5color "
    "빅사이즈 하객룩 데이트룩 오피스룩 무료배송 당일출고 인기상품"
)

# 나열 예산을 **확실히 넘는** 이름. 자르기 분기를 타게 하는 것이 목적이다.
#
# ⚠️ `_KEYWORD_STUFFED_NAME` 은 72자 = 라벨 78자로, 보류1+실패1 일 때의 예산(78자)과
#    **정확히 같아서** 접히지 않고 지나간다. 경계에 딱 걸친 값만 쓰면 자르기 경로가
#    한 번도 실행되지 않는다(2026-08-10 리뷰). 그래서 여유 있게 넘는 이름을 따로 둔다.
_OVER_BUDGET_NAME = _KEYWORD_STUFFED_NAME + " 리뷰이벤트 사은품증정 한정수량"


def test_notice_length_is_bounded_even_with_real_product_names() -> None:
    """안내 문구 길이가 상품 수에도, **상품명 길이에도** 비례해 자라지 않는다.

    ⚠️ `notice_message` 에는 스키마 max_length 가 없어 **우리 쪽에서 안 걸린다.** 전부
       나열하면 보류 42건에 631자, 보류+실패 동시면 742자였다(2026-08-09 실측).
       백엔드 컬럼이 짧으면 조용히 잘리거나 INSERT 가 터지고, 어느 쪽이든 셀러는 자기
       상품이 왜 빠졌는지 못 본다.

    ⚠️ 상한을 **개수**로 걸면 안 된다. 상품명이 길면 5개만 나열해도 넘는다 — 실측으로
       목 이름(7자) 223자 / 실제 노출명(38자) **368자**(2026-08-09).
    """
    from app.core import constants
    from app.reporting.monthly_report_service import _build_excluded_notice

    def held(n: int, name: str) -> list:
        return [_held_input(f"P{i:03d}", name=name) for i in range(n)]

    many = _build_excluded_notice(held(42, _REAL_LENGTH_NAME), None)
    far_more = _build_excluded_notice(held(504, _REAL_LENGTH_NAME), None)

    # 전부 나열하면 42건 × 45자 = 1,890자다. 상한이 걸려 그 근처도 안 간다.
    assert len(many) <= constants.NOTICE_MAX_CHARS, f"{len(many)}자 — 나열 예산이 안 걸렸다"

    # 포화 뒤에는 상품이 12배로 늘어도 길이가 그대로다. 늘어나는 건 개수 자릿수뿐이라
    # 한 자릿수당 몇 글자에 그친다 — 상품 수에 **비례**하지 않는다는 뜻이다.
    assert abs(len(far_more) - len(many)) <= 10, (
        f"상품 수에 비례해 자라고 있다: 42건 {len(many)}자 → 504건 {len(far_more)}자"
    )

    # 접혀도 셈이 맞아야 한다: 나열한 것 + "외 N개" = 총 개수.
    # ⚠️ "외 41개" 처럼 접힌 수를 박아 두면 예산이 바뀔 때마다 깨진다 — 몇 개가 들어가는지는
    #    상한과 이름 길이에 달렸고, 계약은 "합이 맞는다" 쪽이다.
    shown = many.count("(P")
    folded = int(re.search(r"외 (\d+)개", many).group(1))
    assert shown + folded == 42, f"나열 {shown} + 접힘 {folded} 가 총 42 와 안 맞는다"
    assert "42개" in many  # 총 개수는 그대로 알린다


def test_notice_length_is_bounded_at_every_scale() -> None:
    """상품 수·이름 길이를 **천장까지** 밀어도 `NOTICE_MAX_CHARS` 를 넘지 않는다.

    ⚠️ 한 조합(42/42)만 재면 못 잡는다. 구절당 예산을 주던 시절 42/42 는 252자로 통과했지만
       150/150 은 256자, 자릿수를 최대로 올리면 260자였다(2026-08-10 리뷰 실측).

    두 축이 동시에 길이를 밀어올린다:
      · **이름 길이** — 예산을 넘는 이름은 예산 끝까지 잘려 들어와 예산을 꽉 채운다.
      · **개수 자릿수** — 양쪽 구절의 "상품 N개"·"외 N개" 네 곳에 들어간다. 2자리→3자리면
        +4자다. 카탈로그가 504개라 보류 3자리는 정상 범위다.
    """
    from app.core import constants
    from app.reporting.monthly_report_service import _build_excluded_notice

    names = (
        "원피스",
        _REAL_LENGTH_NAME,
        _KEYWORD_STUFFED_NAME,
        _OVER_BUDGET_NAME,
        "초장문 상품명 " * 30,
    )
    # ⚠️ (1, 1) 이 빠지면 안 된다. 구절이 **둘**이라야 예산이 반으로 갈려 긴 이름 1건이
    #    잘리는 경로에 닿는다 — (1, 0) 은 구절이 하나라 예산이 넉넉해서 접히지도 않는다.
    #    그 경로에서 "외 0개" 가 새어 나갔다(2026-08-10 리뷰).
    counts = ((1, 0), (0, 1), (1, 1), (3, 3), (42, 42), (150, 150), (504, 504), (99_999, 99_999))

    for name in names:
        for n_held, n_failed in counts:
            notice = _build_excluded_notice(
                [_held_input(f"P{i:03d}", name=name) for i in range(n_held)],
                [f"P{i:03d}" for i in range(n_failed)],
            )
            where = f"이름 {len(name)}자 · 보류 {n_held} · 실패 {n_failed}"
            assert len(notice) <= constants.NOTICE_MAX_CHARS, (
                f"{where} 에서 {len(notice)}자 — 상한 {constants.NOTICE_MAX_CHARS} 초과"
            )
            # 길이만 재면 문구가 이상해도 통과한다. 내용도 본다.
            assert "외 0개" not in notice, f"{where} 에서 '외 0개' 가 나갔다: {notice}"


def test_single_held_product_does_not_say_folded_zero() -> None:
    """보류 1건이면 "외 0개" 가 붙지 않는다 — 잘린 것과 접힌 것은 다르다.

    ⚠️ 이름이 예산보다 길면 자르기 분기를 타는데, 거기서 꼬리를 무조건 붙이면
       `len(labels) - len(shown)` 이 0 이라 **"상품 1개: …이름… 외 0개"** 가 셀러 화면에
       나간다(2026-08-10 리뷰 실측). 1개라고 알린 바로 뒤에 "외 0개" 가 붙는 문구다.

    구절이 **둘**이라야 예산이 반으로 갈려 이 경로에 닿는다 — 보류만 있으면 예산이
    넉넉해서 접히지 않는다. 그래서 실패도 1건 같이 넣는다.
    """
    from app.reporting.monthly_report_service import _build_excluded_notice

    notice = _build_excluded_notice(
        [_held_input("P001", name=_OVER_BUDGET_NAME)], ["P900"]
    )

    assert "외 0개" not in notice, f"1건인데 '외 0개' 가 붙었다: {notice}"
    assert "보류된 상품 1개" in notice


def test_truncated_label_keeps_the_product_code() -> None:
    """이름을 자를 때 상품 코드는 남긴다.

    오른쪽부터 자르면 끝에 붙은 `(P001)` 이 가장 먼저 날아간다. 셀러가 관리 화면에서
    상품을 특정하는 값은 노출명이 아니라 **코드**라, 같은 예산이면 코드를 남기는 쪽이
    정보가 더 많다(2026-08-10 리뷰).
    """
    from app.reporting.monthly_report_service import _build_excluded_notice

    notice = _build_excluded_notice(
        [_held_input("P001", name=_OVER_BUDGET_NAME)], ["P900"]
    )

    assert "…" in notice, "예산을 넘는 이름인데 잘리지 않았다"
    assert "(P001)" in notice, f"이름을 자르면서 상품 코드까지 날아갔다: {notice}"


def test_notice_stays_bounded_when_one_name_is_absurdly_long() -> None:
    """이름 **하나**가 예산보다 길어도 상한이 무너지지 않는다.

    최소 1개는 나열해야 셀러가 무엇인지 아는데, 노출명에는 길이 제한이 없어서 그 하나가
    상한을 통째로 무너뜨릴 수 있다. `_summarize` 가 이름 자체를 자르는 이유다.
    """
    from app.core import constants
    from app.reporting.monthly_report_service import _build_excluded_notice

    absurd = "초장문 상품명 " * 30  # 240자
    held = [_held_input(f"P{i:03d}", name=absurd) for i in range(42)]

    worst = _build_excluded_notice(held, [f"P{i:03d}" for i in range(42)])

    assert len(worst) <= constants.NOTICE_MAX_CHARS, f"이름 하나가 상한을 넘겼다: {len(worst)}자"
    assert "…" in worst, "긴 이름이 잘리지 않았다"
    assert "42개" in worst  # 잘려도 총 개수는 알린다


def test_notice_lists_every_product_when_budget_allows() -> None:
    """예산 안이면 전부 나열한다 — 몇 개 안 될 때까지 접으면 정보만 잃는다.

    ⚠️ 개수를 하드코딩하지 않는다. 상수에서 파생시켜야 `NOTICE_MAX_CHARS` 를 낮췄을 때
       이 테스트가 "왜 접혔는지" 를 알려준다(2026-08-10 지적).
    """
    from app.core import constants
    from app.reporting.monthly_report_service import _build_excluded_notice

    # 상한의 1/4 만 쓰는 짧은 목록 — 고정 문구를 빼고도 남으므로 접힐 이유가 없다
    name = "원피스"
    label_len = len(name) + len("(P000)") + len(", ")
    count = max(1, constants.NOTICE_MAX_CHARS // 4 // label_len)

    notice = _build_excluded_notice(
        [_held_input(f"P{i:03d}", name=name) for i in range(count)], None
    )

    assert "외 " not in notice, (
        f"상한({constants.NOTICE_MAX_CHARS}자)의 1/4 인 {count}건인데 접혔다: {notice}"
    )
    for i in range(count):
        assert f"P{i:03d}" in notice
