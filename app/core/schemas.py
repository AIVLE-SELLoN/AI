"""단계 간 입출력 Pydantic 모델 = 팀 계약서.

정본: docs/schemas.md §3·§4·§7, docs/detection_schema.md §3, docs/recommenation_schema.md §3.
각 컴포넌트는 이 모듈만 import한다 (서로의 폴더에 의존하지 않는다).
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field, model_validator, ConfigDict

# ── 공통 Enum (schemas.md §3) ────────────────────────────────────


class Channel(str, Enum):
    COUPANG = "COUPANG"
    NAVER = "NAVER"
    ZIGZAG = "ZIGZAG"
    ALL = "ALL"


class Aspect(str, Enum):
    COLOR = "색상"
    SIZE = "사이즈"
    MATERIAL = "소재"
    DAMAGE = "파손"
    MISDELIVERY = "오배송"
    ETC = "기타"


class Sentiment(int, Enum):
    NEGATIVE = -1
    NEUTRAL = 0
    POSITIVE = 1


class Verdict(str, Enum):
    NORMAL = "정상"
    BIASED = "편중형"
    GLOBAL = "전역형"
    TENTATIVE_GLOBAL = "잠정 전역형"
    INDETERMINATE = "구분불가"


class Source(str, Enum):
    CS = "cs"
    REVIEW = "review"


REVIEW_ALLOWED_ASPECTS = frozenset({Aspect.COLOR, Aspect.SIZE, Aspect.MATERIAL})


# ── ClassifiedItem (schemas.md §4) ───────────────────────────────


class AspectSentiment(BaseModel):
    aspect: Aspect
    sentiment: Sentiment
    mixed_signal: bool | None = None


class ClassifiedItem(BaseModel):
    item_id: str
    source: Source
    channel: Channel
    product_group_id: str
    raw_text: str
    aspects: list[AspectSentiment]
    created_at: datetime

    @model_validator(mode="after")
    def _validate_review_aspects(self) -> ClassifiedItem:
        if self.source == Source.REVIEW:
            invalid = [a.aspect for a in self.aspects if a.aspect not in REVIEW_ALLOWED_ASPECTS]
            if invalid:
                raise ValueError(
                    f"source=='review'이면 aspect는 색상/사이즈/소재만 허용됩니다: {invalid}"
                )
        return self


# ── DetectionAlert 전용 Enum (detection_schema.md §3) ────────────


class RecommendedAction(str, Enum):
    GENERATE_RECOMMENDATION = "개선안 생성"
    CHANNEL_OPERATION_CHECK = "채널 운영 요소 점검 권장"
    LOGISTICS_CHECK = "물류 점검 권장"
    OPERATION_CHECK = "운영 점검 권장"
    PRODUCT_CHECK = "상품 자체 점검 권장"
    SCOPE_UNDETERMINED = "편중·전역 구분 불가(채널 표본 부족)"
    OTHER_TYPE_CHECK = "기타 유형"


class DetectionConfidence(str, Enum):
    HIGH = "높음"
    MEDIUM = "중간"
    LOW = "낮음"
    NOT_APPLICABLE = "해당없음"


# ── DetectionAlert (detection_schema.md §3) ──────────────────────


class SubAspectAction(BaseModel):
    aspect: Aspect
    delta: float
    recommended_action: RecommendedAction


class DetectionStats(BaseModel):
    source: Source
    cur_rate: float
    past_rate: float
    delta: float
    p_value: float
    bh_significant: bool
    cur_total: int


class SourceSignals(BaseModel):
    cs: bool | None
    review: bool | None
    interpretation: str


class RootCause(BaseModel):
    label: str
    count: int
    total: int
    consistent: bool


class Evidence(BaseModel):
    inquiry_ids: list[str]
    linked_change_id: str | None = None


class DetectionAlert(BaseModel):
    alert_id: str
    detected_at: datetime
    updates_alert_id: str | None = None

    product_group_id: str
    channel: Channel
    window_start: date
    window_end: date

    verdict: Verdict
    significant_channels: list[Channel] = Field(default_factory=list)
    excluded_channels: list[Channel] = Field(default_factory=list)

    main_aspect: Aspect
    sub_aspects: list[SubAspectAction] = Field(default_factory=list)

    stats: DetectionStats
    source_signals: SourceSignals

    root_cause: RootCause | None = None

    detection_confidence: DetectionConfidence
    scope_in: bool
    recommended_action: RecommendedAction

    evidence: Evidence


# ── Recommendation 전용 Enum (recommenation_schema.md §3) ────────


class ProposalType(str, Enum):
    COPY_DRAFT = "copy_draft"
    IMAGE_GUIDE = "image_guide"


class RecommendationConfidence(str, Enum):
    HIGH = "높음"
    MEDIUM = "중간"
    LOW = "낮음"


class HitlStatus(str, Enum):
    PENDING = "대기"
    APPROVED = "승인"
    REJECTED = "반려"
    EDITED_APPROVED = "수정후승인"


class RejectionReasonCode(str, Enum):
    INSUFFICIENT_GROUNDS = "근거부족"
    ALREADY_HANDLED = "이미조치함"
    DIFFERENT_CAUSE = "원인다름"
    OTHER = "기타"


# ── Recommendation (recommenation_schema.md §3) ──────────────────


class Proposal(BaseModel):
    type: ProposalType
    target_field: str
    current_text: str
    proposed_text: str
    rationale: str
    detailpage_grounded: bool


class Citation(BaseModel):
    inquiry_id: str
    quote: str


class EvaluatorChecks(BaseModel):
    grounding: bool
    consistency: bool
    actionability: bool


class Evaluator(BaseModel):
    passed: bool
    attempts: int = Field(ge=1, le=3)
    checks: EvaluatorChecks
    failure_reason: str | None = None


class RejectionReason(BaseModel):
    reason_code: RejectionReasonCode | None = None
    reason_text: str | None = None


class HitlFeedback(BaseModel):
    processed_at: datetime
    processed_by: str
    rejection_reason: RejectionReason | None = None
    edited_text: str | None = None


class Recommendation(BaseModel):
    recommendation_id: str
    alert_id: str
    created_at: datetime

    proposal: Proposal | None = None

    citations: list[Citation] = Field(default_factory=list)

    evaluator: Evaluator

    similar_case: str | None = None

    recommendation_confidence: RecommendationConfidence | None = None
    confidence_reason: str | None = None
    capped_by_detection: bool = False

    hitl_status: HitlStatus = HitlStatus.PENDING
    hitl_feedback: HitlFeedback | None = None


# ── 모델 간 교차검증 함수 (schemas.md §7) ─────────────────────────


def validate_citations_grounded(recommendation: Recommendation, alert: DetectionAlert) -> None:
    """citations[].inquiry_id ⊆ alert.evidence.inquiry_ids 인지 검증."""
    allowed = set(alert.evidence.inquiry_ids)
    invalid = [c.inquiry_id for c in recommendation.citations if c.inquiry_id not in allowed]
    if invalid:
        raise ValueError(
            f"citations가 evidence.inquiry_ids 밖의 문의를 인용했습니다: {invalid}"
        )

# ──  reporting 생성 공통 Enum 및 S3 메타데이터 스키마 ────────────────────────────

class AspectStatus(str, Enum):
    STABLE = "STABLE"
    RISK = "RISK"


class PdfS3Meta(BaseModel):
    s3_bucket_name: str = Field(..., description="S3 버킷명")
    s3_file_path: str = Field(..., description="S3 디렉토리 경로")
    original_file_name: str = Field(..., description="원본 / 표시용 파일명")
    new_file_name: str = Field(..., description="S3 저장용 고유 파일명")
    s3_full_key: str = Field(..., alias="s3_full_kdy", description="S3 객체 전체 키")
    file_extension: str = Field("pdf", description="파일 확장자")
    file_size_bytes: int = Field(..., ge=0, description="PDF 파일 용량 (bytes)")
    presigned_url: str | None = Field(None, description="다운로드용 1시간 유효 Presigned URL")

    model_config = ConfigDict(populate_by_name=True)


# ── 월간 리포트 스키마 (Monthly Report) ───────────────────────────


class CauseDistribution(BaseModel):
    cause_label: str = Field(..., description="원인 분류 라벨 (예: '사진_색감_오차')")
    count: int = Field(..., ge=0, description="발생 건수")
    ratio: float = Field(..., ge=0.0, le=100.0, description="속성 내 지분율 (%)")
    sample_evidences: list[str] = Field(default_factory=list, description="대표 고객 인용 문구 목록")


class MonthlyAspectStat(BaseModel):
    aspect: Aspect = Field(..., description="속성 구분 ('색상', '사이즈', '소재' 등)")
    total_count: int = Field(..., ge=0, description="속성별 전체 피드백 수")
    positive_ratio: float = Field(..., ge=0.0, le=1.0, description="긍정 비율")
    neutral_ratio: float = Field(..., ge=0.0, le=1.0, description="중립 비율")
    negative_ratio: float = Field(..., ge=0.0, le=1.0, description="부정 비율")
    drift_rate: float = Field(..., description="ΔP_neg 전월 대비 부정 비율 변동폭")
    status: AspectStatus = Field(..., description="위험 상태 판정 (STABLE / RISK)")
    cause_distributions: list[CauseDistribution] = Field(
        default_factory=list,
        description="속성 내 세부 원인 지분율 집계 (LLM 요약 근거)"
    )


class MonthlyChannelDivergenceInput(BaseModel):
    comparison_pair: str = Field(..., description="대조 채널 쌍 (예: 'COUPANG_VS_NAVER')")
    jsd_score: float = Field(..., description="월간 평균 M_JSD 실측 수치")
    is_crisis: bool = Field(..., description="오퍼레이션 장애 여부 (M_JSD >= 0.5)")


class MonthlyReportInput(BaseModel):
    report_month: str = Field(..., pattern=r"^\d{4}-\d{2}$", description="보고서 대상 연월 (YYYY-MM)")
    start_date: date = Field(..., description="전월 시작일 (YYYY-MM-01)")
    end_date: date = Field(..., description="전월 종료일 (YYYY-MM-DD)")
    product_group_id: str = Field(..., description="마스터 상품 그룹 고유 ID")
    product_name: str = Field(..., description="상품명")
    total_voc_count: int = Field(..., ge=0, description="월간 총 VOC 처리량")
    aspect_stats: list[MonthlyAspectStat] = Field(..., description="속성별 통계 지표 및 원인 분포")
    channel_divergence: MonthlyChannelDivergenceInput = Field(..., description="다채널 분열 정량 수치")
    linked_alert_ids: list[str] = Field(default_factory=list, description="월간 발생된 DetectionAlert ID 리스트")


class MonthlyAspectSummary(BaseModel):
    aspect: Aspect = Field(..., description="속성 구분")
    summary_text: str = Field(..., description="속성별 AI 감성 요약 문구")


class MonthlyChannelDivergenceCause(BaseModel):
    cause_title: str = Field(..., description="채널간 평판 격차 핵심 요약 제목")
    cause_description: str = Field(..., description="다채널 분열 발생 세부 원인 분석")


class MonthlyReportOutput(BaseModel):
    report_id: str = Field(..., description="월간 보고서 고유 ID")
    product_group_id: str = Field(..., description="마스터 상품 그룹 고유 ID")
    report_month: str = Field(..., pattern=r"^\d{4}-\d{2}$", description="보고서 연월 (YYYY-MM)")
    aspect_summaries: list[MonthlyAspectSummary] = Field(..., description="속성별 요약 문구 목록")
    channel_divergence_cause: MonthlyChannelDivergenceCause = Field(..., description="채널 분열 사유 분석")
    cause_analysis_results: list[str] = Field(..., description="핵심 원인 분석 결과 목록")
    recommended_actions: list[str] = Field(..., description="운영/CS 권장 조치 사항 목록")
    pdf_s3_meta: PdfS3Meta | None = Field(None, description="LLM 노드가 적재한 S3 PDF 메타데이터")


# ── CS 가이드라인 스키마 (CS Guideline) ───────────────────────────


class CSGuidelineStats(BaseModel):
    cur_rate: float = Field(..., ge=0.0, le=1.0, description="현재 윈도우 부정 비율")
    past_rate: float = Field(..., ge=0.0, le=1.0, description="직전 윈도우 부정 비율")
    delta: float = Field(..., description="부정 비율 변동폭 Delta")
    cur_total: int = Field(..., ge=0, description="현재 윈도우 총 문의 건수")


class CSGuidelineRootCause(BaseModel):
    label: str = Field(..., description="분류기 집계 최다 원인 명칭")
    count: int = Field(..., ge=0, description="최다 원인 해당 건수")
    total: int = Field(..., ge=0, description="원인 분석 대상 전체 건수")


class LinkedCSInquiry(BaseModel):
    item_id: str = Field(..., description="CS 문의 고유 ID")
    raw_text: str = Field(..., description="고객 원문 텍스트")
    created_at: datetime = Field(..., description="CS 접수 일시")


class CSGuidelineInput(BaseModel):
    alert_id: str = Field(..., description="이상 탐지 알림 고유 ID")
    detected_at: datetime = Field(..., description="이상 탐지 완료 일시")
    product_group_id: str = Field(..., description="마스터 상품 그룹 고유 ID")
    channel: Channel = Field(..., description="이상 감지 채널")
    main_aspect: Aspect = Field(..., description="이상 감지 주요 속성")
    recommended_action: RecommendedAction = Field(..., description="이상 탐지 엔진 결정 권장 액션")
    stats: CSGuidelineStats = Field(..., description="이상 탐지 정량 통계")
    root_cause: CSGuidelineRootCause | None = Field(None, description="세부 원인 집계 데이터")
    linked_inquiries: list[LinkedCSInquiry] = Field(..., description="기반 DB 조인 CS 문의 리스트")


class CSGuidelineSummary(BaseModel):
    issue_title: str = Field(..., description="가이드라인 이슈 타이틀")
    risk_level: str = Field(..., description="위험 등급 (WARNING / CRITICAL)")
    key_metric_text: str = Field(..., description="지표 변동 추이 요약 문구")


class StandardGuideline(BaseModel):
    core_message: str = Field(..., description="CS 핵심 안내 매뉴얼")
    draft_reply: str = Field(..., description="표준 권장 답변 초안")
    key_talking_points: list[str] = Field(..., description="필수 언급 및 금지 표현 리스트")


class InquirySpecificGuide(BaseModel):
    item_id: str = Field(..., description="대상 CS 문의 ID")
    recommended_point: str = Field(..., description="해당 문의 맞춤 응대 가이드")


class CSGuidelineOutput(BaseModel):
    guideline_id: str = Field(..., description="가이드라인 고유 ID")
    alert_id: str = Field(..., description="원본 탐지 알림 ID")
    summary: CSGuidelineSummary = Field(..., description="가이드라인 요약 정보")
    root_cause_summary: str = Field(..., description="최다 원인 지분율 요약")
    standard_guideline: StandardGuideline = Field(..., description="표준 응대 가이드 세트")
    ops_action_guide: str = Field(..., description="운영팀 조치 가이드")
    inquiry_specific_guides: list[InquirySpecificGuide] = Field(..., description="개별 CS 문의별 맞춤 가이드 목록")
    pdf_s3_meta: PdfS3Meta | None = Field(None, description="생성된 PDF의 S3 적재 메타데이터")