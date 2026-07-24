"""단계 간 입출력 Pydantic 모델 = 팀 계약서.

정본: docs/schemas.md §3·§4·§7, docs/detection_schema.md §3, docs/recommenation_schema.md §3.
각 컴포넌트는 이 모듈만 import한다 (서로의 폴더에 의존하지 않는다).
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field, model_validator

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
