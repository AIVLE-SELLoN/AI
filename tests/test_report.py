from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, patch

import pytest

from app.core.schemas import (
    Aspect,
    AspectStatus,
    Channel,
    CSGuidelineInput,
    CSGuidelineRootCause,
    CSGuidelineStats,
    LinkedCSInquiry,
    MonthlyAspectStat,
    MonthlyChannelDivergenceInput,
    MonthlyReportInput,
    PdfS3Meta,
    RecommendedAction,
)
from app.reporting.cs_reply_service import generate_cs_reply_pipeline
from app.reporting.monthly_report_service import generate_monthly_report_pipeline

# ── 1. 인프라 모킹 자동 적용 Fixture ───────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_infrastructure() -> Generator[None, None, None]:
    """모든 테스트 실행 시 S3 업로드 및 PDF 컴파일을 자동으로 Mocking한다."""
    dummy_s3_meta = PdfS3Meta(
        s3_bucket_name="mock-bucket",
        s3_file_path="reports/mock/",
        original_file_name="mock_report.pdf",
        new_file_name="mock_report_123.pdf",
        s3_full_key="reports/mock/mock_report_123.pdf",
        file_extension="pdf",
        file_size_bytes=2048,
        presigned_url="https://mock-s3.amazonaws.com/mock_report_123.pdf",
    )

    with (
        patch("app.reporting.cs_reply_service.compile_report_to_pdf", return_value=b"%PDF-1.4-MOCK-BYTES"),
        patch("app.reporting.cs_reply_service.upload_pdf_to_s3", new_callable=AsyncMock, return_value=dummy_s3_meta),
        patch("app.reporting.monthly_report_service.compile_report_to_pdf", return_value=b"%PDF-1.4-MOCK-BYTES"),
        patch("app.reporting.monthly_report_service.upload_pdf_to_s3", new_callable=AsyncMock, return_value=dummy_s3_meta),
    ):
        yield


# ── 2. 입력 데이터 Fixture ───────────────────────────────────────────────────

@pytest.fixture
def sample_cs_input() -> CSGuidelineInput:
    default_action = getattr(RecommendedAction, "EXCHANGE", next(iter(RecommendedAction)))

    return CSGuidelineInput(
        alert_id="ALT-20260728-001",
        detected_at=datetime(2026, 7, 28, 10, 0, 0, tzinfo=UTC),
        product_group_id="PG-SHIRT-01",
        channel=Channel.COUPANG,
        main_aspect=Aspect.SIZE,
        recommended_action=default_action,
        stats=CSGuidelineStats(
            cur_rate=0.25,
            past_rate=0.10,
            delta=0.15,
            cur_total=100,
        ),
        root_cause=CSGuidelineRootCause(
            label="사이즈 작음",
            count=15,
            total=25,
        ),
        linked_inquiries=[
            LinkedCSInquiry(
                item_id="CS-ITEM-001",
                raw_text="상세페이지보다 옷이 너무 작게 나왔습니다. 교환 원합니다.",
                created_at=datetime(2026, 7, 28, 9, 30, 0, tzinfo=UTC),
            )
        ],
    )


@pytest.fixture
def sample_monthly_input() -> MonthlyReportInput:
    return MonthlyReportInput(
        report_month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 31),
        product_group_id="PG-SHIRT-01",
        product_name="오버핏 린넨 셔츠",
        total_voc_count=450,
        aspect_stats=[
            MonthlyAspectStat(
                aspect=Aspect.SIZE,
                total_count=150,
                positive_ratio=0.6,
                neutral_ratio=0.1,
                negative_ratio=0.3,
                drift_rate=0.12,
                status=AspectStatus.RISK,
                cause_distributions=[],
            )
        ],
        channel_divergence=MonthlyChannelDivergenceInput(
            comparison_pair="COUPANG_VS_NAVER",
            jsd_score=0.18,
            is_crisis=False,
        ),
        linked_alert_ids=["ALT-20260728-001"],
    )


# ── 3. 파이프라인 비즈니스 로직 단위 테스트 ───────────────────────────────────

@pytest.mark.asyncio
async def test_generate_cs_reply_pipeline_success(
    sample_cs_input: CSGuidelineInput,
) -> None:
    """LLM 완료 및 검증 통과 시 CS 가이드라인 파이프라인 정합성 검증"""
    mock_llm_response = {
        "guideline_id": "GD-20260728-PG-SHIRT-01",
        "alert_id": "ALT-20260728-001",
        "summary": {
            "issue_title": "PG-SHIRT-01 (COUPANG) SIZE 대응 가이드",
            "risk_level": "WARNING",
            "key_metric_text": "부정 비율 변동폭 Δ15%p 감지",
        },
        "root_cause_summary": "최다 원인: 사이즈 작음 (15/25건)",
        "standard_guideline": {
            "core_message": "상세페이지 사이즈 안내 수정 및 무상 교환 처리",
            "draft_reply": "고객님, 사이즈 불편에 대해 사과드립니다. 무상 교환 접수를 도와드리겠습니다.",
            "key_talking_points": ["1. 사이즈 스펙 오차 정중히 설명", "2. 무상 교환 수거 안내"],
        },
        "ops_action_guide": "운영팀 실측 실리콘 스펙 재측정 및 상세페이지 갱신 요청",
        "inquiry_specific_guides": [
            {
                "item_id": "CS-ITEM-001",
                "recommended_point": "치수 오차에 따른 무상 회수 및 교환 절차 전담 안내",
            }
        ],
        "pdf_s3_meta": None,
    }

    with (
        patch("app.reporting.cs_reply_service.get_llm_client") as mock_get_client,
        patch("app.reporting.cs_reply_service.validate_cs_guideline", return_value=(True, [])),
    ):
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = mock_llm_response
        mock_get_client.return_value = mock_client

        output = await generate_cs_reply_pipeline(sample_cs_input)

        assert output.guideline_id == "GD-20260728-PG-SHIRT-01"
        assert output.alert_id == sample_cs_input.alert_id
        assert output.inquiry_specific_guides[0].item_id == "CS-ITEM-001"
        assert output.pdf_s3_meta is not None
        assert output.pdf_s3_meta.s3_bucket_name == "mock-bucket"


@pytest.mark.asyncio
async def test_generate_monthly_report_pipeline_success(
    sample_monthly_input: MonthlyReportInput,
) -> None:
    """월간 보고서 생성 파이프라인 정합성 검증"""
    mock_llm_response = {
        "report_id": "REP-2026-07-PG-SHIRT-01",
        "product_group_id": "PG-SHIRT-01",
        "report_month": "2026-07",
        "aspect_summaries": [
            {
                "aspect": "SIZE",
                "summary_text": "사이즈 관련 부정 의견 비율이 전월 대비 12%p 증가함.",
            }
        ],
        "channel_divergence_cause": {
            "cause_title": "쿠팡 대 네이버 평판 이격 발생",
            "cause_description": "쿠팡 채널 중심의 표기 스펙 불만집중 모니터링 필요.",
        },
        "cause_analysis_results": [
            "1. 린넨 소재 특성상 수축률 미반영에 따른 치수 불만",
        ],
        "recommended_actions": [
            "1. 상세페이지 내 세탁 후 수축률 가이드 추가",
        ],
        "pdf_s3_meta": None,
    }

    with (
        patch("app.reporting.monthly_report_service.get_llm_client") as mock_get_client,
        patch("app.reporting.monthly_report_service.validate_monthly_report", return_value=(True, [])),
    ):
        mock_client = AsyncMock()
        mock_client.complete_json.return_value = mock_llm_response
        mock_get_client.return_value = mock_client

        output = await generate_monthly_report_pipeline(sample_monthly_input)

        assert output.report_id == "REP-2026-07-PG-SHIRT-01"
        assert output.report_month == "2026-07"
        assert output.pdf_s3_meta is not None
        assert output.pdf_s3_meta.presigned_url == "https://mock-s3.amazonaws.com/mock_report_123.pdf"