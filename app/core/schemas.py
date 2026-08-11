"""단계 간 입출력 Pydantic 모델 = 팀 계약서.

정본: docs/schemas.md §3·§4·§7, docs/detection_schema.md §3, docs/recommenation_schema.md §3.
각 컴포넌트는 이 모듈만 import한다 (서로의 폴더에 의존하지 않는다).
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.core.constants import (
    DRIFT_RISK_THRESHOLD,
    MAX_CHANNEL_PAIRS,
    MAX_PDF_SIZE_BYTES,
    MONTHLY_ASPECT_COUNT,
    RATIO_SUM_TOLERANCE,
)

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


class ChannelRate(BaseModel):
    """탐지 시점의 채널별 현재 윈도우 부정률 스냅샷.

    ``total`` 은 그 비율의 분모(= 그 채널의 현재 윈도우 총문의, aspect 무관)다.
    ``stats.cur_total`` 은 **대표 채널 1개분**이라 채널별 분모를 대체하지 못한다.

    ⚠️ **``None`` 과 ``0`` 은 뜻이 다르다.** ``None`` 은 이 필드가 생기기 전(2026-08-11)
    발행돼 백엔드에 저장된 구버전 알림뿐이고, ``0`` 은 그 채널에 문서가 아예 없었다는
    관측 결과다(``rate=None``·``excluded=True`` 와 세트). **신규 발행은 관측이 없어도
    ``0`` 을 싣는다** — 기본값 ``None`` 이 그대로 나가면 백엔드가 둘을 못 가린다.

    🔴 **선택 필드인 이유는 백엔드 사정만이 아니다 — 필수로 만들면 우리 엔드포인트가
    깨진다.** ``POST /recommendations/hitl`` 의 ``ProcessHitlRequest.alert`` 가
    ``DetectionAlert`` 라, Spring Boot 가 저장해둔 알림이 그대로 되돌아온다. 필수 필드면
    구버전 알림에 대한 HITL 요청이 전부 422 가 되고, 그건 **컬렉션2 축적 경로가 막히는
    것**이다.
    """

    channel: Channel
    rate: float | None
    excluded: bool
    total: int | None = None


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
    channel_rates: list[ChannelRate] = Field(default_factory=list)

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

# ── 문서 생성 스키마 (확정) — 노션 "문서 생성 스키마 (확정)" 정본 반영 ──────────
#
# 범위: CS 가이드라인 문서 · 월간 리포트 대시보드 및 보고서 문서 · S3 적재.
# 대체 대상: 「문서 생성 스키마 (1)」(deprecated).
#
# 표기 규칙
#   - Alias 는 동일 필드를 가리키는 다른 표기로, 파서가 양쪽을 모두 수용한다
#     (`populate_by_name=True` + `alias=` 조합 — 정본 필드명으로도, alias 로도 들어온다).
#   - 문서에서 굵게 표시된 제약 조건은 검증 실패 시 반려 사유가 되므로,
#     모델 단독으로 판정 가능한 것은 여기서 validator 로 막고,
#     두 모델을 맞대봐야 하는 것은 맨 아래 교차검증 함수로 뺐다.
#
# 임계값·고정 문구는 컨벤션(매직넘버 금지)대로 constants.py 에 있고 상단에서 import 한다:
#   MONTHLY_ASPECT_COUNT / MAX_CHANNEL_PAIRS / DRIFT_RISK_THRESHOLD / RATIO_SUM_TOLERANCE /
#   MAX_PDF_SIZE_BYTES / SEVERITY_STAGE_LABEL / HOLD_INSUFFICIENT_DATA_NOTICE
#   (마지막 둘은 검증기·콜백에서 쓰므로 app.core.constants 에서 직접 가져다 쓸 것)
#
# 아래 두 집합은 이 파일의 Enum 에서 파생되는 값이라 여기 남긴다 —
# constants.py 로 옮기면 constants → schemas 역방향 import 가 생겨 순환한다.

# 월간 리포트가 다루는 aspect (§4-1 "Aspect (월간 연산)").
# CS 탐지용 6종(Aspect enum 전체)과 달리 3종으로 제한된다.
MONTHLY_ASPECTS = frozenset({Aspect.COLOR, Aspect.SIZE, Aspect.MATERIAL})

# 생성 대상이 아닌 판정 (§2-1 verdict: "정상 은 생성 대상 아님").
GUIDELINE_EXCLUDED_VERDICTS = frozenset({Verdict.NORMAL})


# ── 부록 §4-1 Enum 목록 ──────────────────────────────────────────────────
#
# Aspect / Channel / Verdict / DetectionConfidence / RecommendedAction 은
# 이 파일 상단(탐지 계약)에 이미 정의돼 있으므로 재정의하지 않고 그대로 쓴다.


class DriftStatus(str, Enum):
    """속성별 감정 드리프트 상태 — 경고 박스 스타일 분기용."""

    NORMAL = "NORMAL"
    RISK = "RISK"


class Severity(str, Enum):
    """채널 분열 게이지 문구 단계 (§4-2 판정식 산출값)."""

    SAFE = "SAFE"
    CAUTION = "CAUTION"
    CRISIS = "CRISIS"


class RiskLevel(str, Enum):
    """CS 가이드라인 위험 등급."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    NORMAL = "NORMAL"


class HoldReason(str, Enum):
    """채널쌍 판정 보류 사유 (게이트 미충족)."""

    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    EMPTY_CHANNEL = "EMPTY_CHANNEL"


class CallbackStatus(str, Enum):
    """생성 완료 콜백 상태 코드 (§4-3)."""

    SUCCESS = "SUCCESS"
    HOLD_INSUFFICIENT_DATA = "HOLD_INSUFFICIENT_DATA"
    FAILED_VALIDATION = "FAILED_VALIDATION"
    FAILED_SIZE_EXCEEDED = "FAILED_SIZE_EXCEEDED"
    FAILED_ERROR = "FAILED_ERROR"


# ── §3-1. S3 PDF 메타데이터 (PdfS3Meta) ──────────────────────────────────


class PdfS3Meta(BaseModel):
    """생성된 파일의 S3 적재 메타데이터. 월간 리포트·CS 가이드라인이 공유한다.

    📌 **파일 산출물은 종류를 불문하고 아래 4종을 반드시 실어 보낸다** (2026-08-03 확정):
       `original_file_name`(원본 파일명) · `new_file_name`(버킷 저장 파일명) ·
       `created_at`(생성 일자) · `file_size_bytes`(파일 크기).
       메인이 파일을 다시 찾거나 목록에 표시할 때 필요한 최소 집합이라 optional 로 두지
       않는다. 앞으로 PDF 외 형식(엑셀·CSV 등)이 늘어도 같은 4종을 유지한다.

    📌 **확장자는 별도 컬럼으로 두지 않는다** (인프라 §4, 2026-08-06).
       "확장자는 파일명에 `.pdf` 로 고정 포함(이미지와 다르게 DB 별도 컬럼에 저장하지
       않음)"이 규칙이다. 예전에는 `file_extension="pdf"` 필드가 있었는데, 파일명에
       이미 들어 있는 값을 한 번 더 들고 다니면 둘이 어긋날 수 있다.

    📌 **회사 구분은 메타데이터로 실어 보낸다** (2026-08-06 확정).
       S3 경로가 `reports/{report_type}/{company_id}/…` 로 회사 단위로 갈리는데, 그 값이
       어느 입력 스키마에도 없어 산출물만 보고는 어느 회사 것인지 알 수 없었다.
       `company_id` 를 필수로 실어 메인이 **S3 키를 파싱하지 않고** 바로 알 수 있게 한다.

       ⚠️ 경로에는 `company_id`(불변 식별자)만 쓴다. `company_name` 은 표시용이다 —
          회사명이 바뀌면 경로가 갈라져 이전 산출물을 못 찾게 된다.

    ⚠️ 보존 정책은 문서 종류별로 다르다 (2026-08-03 확정). 삭제는 S3 Lifecycle 이 한다:
      - **월간 리포트**: PDF 가 **유일한 산출물**이다(DB 에 데이터를 적재하지 않는다).
        생성 후 **6개월** 뒤 자동 삭제되며, 원본이 없으므로 **만료 = 영구 소실**이다.
      - **CS 가이드라인**: 출력 데이터가 DB 에 적재되어 재컴파일이 가능하다 →
        업로드 후 **7일** 뒤 자동 삭제 (2026-08-06 확정, 기존 24시간에서 연장).
        메일 발송이 **운영 MD 승인 뒤에** 일어나므로, 승인 대기 중에 객체가 사라지면
        발송할 것이 없어진다.

    두 시각은 의미가 다르다:
      - `object_expires_at`   S3 가 객체를 지우는 시각 = **다운로드 가능 기한**
      - `presigned_expires_at` 발급된 **링크**의 만료. 만료되면 `s3_full_key` 로 재발급한다
                              (presigned URL 은 SigV4 상 최대 7일이라 6개월 링크는 불가).
    링크가 객체보다 오래 살 수는 없으므로 아래 validator 가 그 조합을 거부한다.
    """

    company_id: str = Field(
        ...,
        description=(
            "[회사] 경로에 쓰인 고객사 식별자 "
            "(s3_file_path 의 reports/{report_type}/ 다음 구간)"
        ),
    )
    company_name: str | None = Field(
        None, description="[회사] 표시용 고객사명. 경로에는 쓰지 않는다(이름은 바뀔 수 있다)"
    )
    s3_bucket_name: str = Field(..., description="S3 버킷명 (월간 6개월 / CS 7일 보존)")
    s3_file_path: str = Field(..., description="S3 디렉토리 경로 (trailing slash 포함)")
    original_file_name: str = Field(..., description="[필수 4종] 원본·표시용 파일명")
    new_file_name: str = Field(..., description="[필수 4종] 버킷에 저장한 파일명")
    created_at: datetime = Field(..., description="[필수 4종] 파일 생성(업로드) 일자")
    file_size_bytes: int = Field(
        ...,
        ge=0,
        le=MAX_PDF_SIZE_BYTES,
        description="[필수 4종] 파일 크기 (bytes, 최대 10MB)",
    )
    s3_full_key: str = Field(..., description="S3 객체 전체 키 (= s3_file_path + new_file_name)")
    presigned_url: str | None = Field(None, description="다운로드·미리보기용 URL")
    presigned_expires_at: datetime | None = Field(
        None, description="URL 만료 시각 (만료 후 s3_full_key 로 재발급)"
    )
    object_expires_at: datetime | None = Field(
        None, description="S3 Lifecycle 자동 삭제 시각 = 다운로드 가능 기한 (월간 6개월/CS 7일)"
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _validate_full_key(self) -> PdfS3Meta:
        # 경로에 박힌 회사 구간과 company_id 가 다르면, 메인이 둘 중 뭘 믿어야 할지 모른다.
        # 경로는 reports/{report_type}/{company_id}/{yyyy}/{mm}/ 순이다(2026-08-06).
        #
        # ⚠️ **자리를 지정해서 본다**(3번째 구간). 부분 문자열로 찾으면 company_id 가 "07"
        #    같은 짧은 값일 때 연월 구간("/2026/07/")과 우연히 맞아 통과한다. 지금 실제
        #    값은 UUID 라 안 걸리지만, 검사가 우연에 기대고 있으면 검사가 아니다.
        segments = self.s3_file_path.strip("/").split("/")
        if segments and segments[0] == "reports":
            company_segment = segments[2] if len(segments) > 2 else None
            if company_segment != self.company_id:
                raise ValueError(
                    f"company_id 가 s3_file_path 의 회사 구간과 다릅니다: "
                    f"{self.company_id!r} != {company_segment!r} ({self.s3_file_path!r})"
                )

        expected = f"{self.s3_file_path}{self.new_file_name}"
        if self.s3_full_key != expected:
            raise ValueError(
                f"s3_full_key 는 s3_file_path + new_file_name 이어야 합니다: "
                f"{self.s3_full_key!r} != {expected!r}"
            )

        # 객체가 사라진 뒤에도 살아있는 링크는 "받을 수 있다"는 잘못된 안내가 된다.
        if (
            self.presigned_expires_at is not None
            and self.object_expires_at is not None
            and self.presigned_expires_at > self.object_expires_at
        ):
            raise ValueError(
                f"presigned_expires_at 은 object_expires_at 을 넘을 수 없습니다: "
                f"{self.presigned_expires_at.isoformat()} > {self.object_expires_at.isoformat()}"
            )
        return self


# ── §1-1. 월간 보고서 입력 (MonthlyReportInput) ──────────────────────────
#
# 생성 주체는 FastAPI(reporting 노드)다. DB 조회·계산 후 자체 구성한다.


class MonthlyAspectDistribution(BaseModel):
    """속성별 감성 분포. 분모는 total_count 로 통일한다."""

    aspect: Aspect = Field(..., description="속성 구분 (색상/사이즈/소재)")
    total_count: int = Field(..., ge=0, description="속성별 전체 피드백 수")
    positive_ratio: float = Field(..., ge=0.0, le=1.0, description="긍정 비율")
    neutral_ratio: float = Field(..., ge=0.0, le=1.0, description="중립 비율")
    negative_ratio: float = Field(..., ge=0.0, le=1.0, description="부정 비율")

    @model_validator(mode="after")
    def _validate_ratio_sum(self) -> MonthlyAspectDistribution:
        if self.aspect not in MONTHLY_ASPECTS:
            raise ValueError(f"월간 연산 대상 aspect 가 아닙니다(색상/사이즈/소재): {self.aspect}")
        total = self.positive_ratio + self.neutral_ratio + self.negative_ratio

        # 관측이 0건이면 비율도 전부 0 이다. 합을 1.00 으로 맞추려고 중립 100% 로
        # 채우면 "관측이 없다"가 "전부 중립이다"로 바뀌어, LLM 이 없는 관측을 있는 것처럼
        # 서술하게 된다(§4-4 수치 팩트체크로도 못 걸러낸다).
        if self.total_count == 0:
            if total > RATIO_SUM_TOLERANCE:
                raise ValueError(
                    f"total_count=0 이면 세 비율도 0 이어야 합니다: {total}"
                )
            return self

        if abs(total - 1.0) > RATIO_SUM_TOLERANCE:
            raise ValueError(
                f"세 비율의 합은 1.00(±{RATIO_SUM_TOLERANCE})이어야 합니다: {total}"
            )
        return self


class MonthlySentimentDrift(BaseModel):
    """속성별 감정 드리프트 (전월 대비 부정 비율 변동)."""

    aspect: Aspect = Field(..., description="속성 구분 (색상/사이즈/소재)")
    drift_rate: float = Field(
        ..., ge=-1.0, le=1.0,
        description="ΔP_neg = negative_ratio(t) − negative_ratio(t−1)",
    )
    status: DriftStatus = Field(..., description="경고 박스 스타일 분기 (RISK iff drift_rate ≥ 0.03)")
    baseline_recalculated: bool = Field(
        False, description="t−1 값을 동일 분모로 재계산했는지 여부"
    )

    @model_validator(mode="after")
    def _validate_status(self) -> MonthlySentimentDrift:
        if self.aspect not in MONTHLY_ASPECTS:
            raise ValueError(f"월간 연산 대상 aspect 가 아닙니다(색상/사이즈/소재): {self.aspect}")
        expected = DriftStatus.RISK if self.drift_rate >= DRIFT_RISK_THRESHOLD else DriftStatus.NORMAL
        if self.status != expected:
            raise ValueError(
                f"status 는 drift_rate ≥ {DRIFT_RISK_THRESHOLD} 일 때만 RISK 입니다: "
                f"drift_rate={self.drift_rate}, status={self.status}"
            )
        return self


class ChannelDivergencePair(BaseModel):
    """채널쌍 1건의 분열 정량 수치.

    게이트(min(n_A, n_B) ≥ 1 AND N ≥ 30) 미충족이면 판정 6개 값이 전부 null 이 되고
    hold_reason 이 세팅된다. 그 반대(일부만 null)인 반쪽 상태는 허용하지 않는다.
    """

    comparison_pair: str = Field(
        ..., pattern=r"^[A-Z]+_VS_[A-Z]+$", description="대조 채널 쌍 (예: COUPANG_VS_NAVER)"
    )
    sample_size: int = Field(..., ge=0, description="두 채널 부정 문서 합")
    jsd_score: float | None = Field(
        None, ge=0.0, le=1.0, description="월간 일별 M_JSD 평균 실측값 (log₂, bits)"
    )
    jsd_baseline: float | None = Field(
        None, ge=0.0, le=1.0, description="귀무 기댓값 E[JSD|H0] (bits), AI 노드 산출"
    )
    p_value: float | None = Field(
        None, ge=0.0, le=1.0, description="순열검정 p값 — ⚠️ 화면·LLM 노출 금지"
    )
    bh_significant: bool | None = Field(None, description="다중검정 통과 여부 (BH-FDR q=0.05)")
    is_crisis: bool | None = Field(None, description="내부 판정값 (§4-2 판정식)")
    severity: Severity | None = Field(None, description="게이지 문구 단계 근거")
    hold_reason: HoldReason | None = Field(None, description="표본 부족 사유")

    # hold_reason 설정 시 전부 null 이어야 하는 판정 6개 값
    _GATED_FIELDS = ("jsd_score", "jsd_baseline", "p_value", "bh_significant", "is_crisis", "severity")

    @model_validator(mode="after")
    def _validate_gate_consistency(self) -> ChannelDivergencePair:
        gated = {name: getattr(self, name) for name in self._GATED_FIELDS}
        nulls = [name for name, value in gated.items() if value is None]

        if self.hold_reason is not None:
            if nulls != list(self._GATED_FIELDS):
                raise ValueError(
                    f"hold_reason 이 설정되면 판정 6개 값이 전부 null 이어야 합니다"
                    f"(비어있지 않음: {[n for n in self._GATED_FIELDS if n not in nulls]})"
                )
        elif nulls:
            raise ValueError(
                f"hold_reason 이 없으면 판정 6개 값이 전부 non-null 이어야 합니다(누락: {nulls})"
            )
        return self


class MonthlyChannelDivergenceInput(BaseModel):
    """다채널 분열 정량 수치 (채널쌍 전수 + 롤업)."""

    calculated_at: datetime = Field(..., description="집계 기준 시각")
    worst_pair: str = Field(..., description="pairs[] 중 jsd_score 최댓값 쌍 — 게이지 상단 라벨")
    is_crisis: bool | None = Field(
        None, description="내부 판정값 (pairs[] 롤업, 전 쌍 보류면 null)"
    )
    pairs: list[ChannelDivergencePair] = Field(
        ...,
        min_length=1,
        max_length=MAX_CHANNEL_PAIRS,
        description="채널쌍 전수 (comparison_pair 중복 불가)",
    )

    @model_validator(mode="after")
    def _validate_pairs(self) -> MonthlyChannelDivergenceInput:
        labels = [p.comparison_pair for p in self.pairs]
        if len(labels) != len(set(labels)):
            raise ValueError(f"comparison_pair 는 중복될 수 없습니다: {labels}")

        # worst_pair 는 **가장 위험한 쌍**(severity 등급 → excess 순)이다.
        # ⚠️ jsd_score 최댓값으로 고르면 안 된다. severity 는 excess(= jsd − baseline)와
        #    유의성으로 정해지고 baseline 은 쌍마다 다르므로(표본이 작을수록 크다),
        #    jsd 가 가장 큰 쌍이 SAFE 인데 다른 쌍이 CRISIS 인 상황이 생긴다. 그때
        #    리포트 제목에는 "안정 단계"가 박히고 is_crisis=true 로 나간다.
        judged = [p for p in self.pairs if p.severity is not None]
        if judged:
            severity_rank = {Severity.CRISIS: 3, Severity.CAUTION: 2, Severity.SAFE: 1}
            worst = max(
                judged,
                key=lambda p: (severity_rank[p.severity], p.jsd_score - p.jsd_baseline),
            )
            if self.worst_pair != worst.comparison_pair:
                raise ValueError(
                    f"worst_pair 는 가장 위험한 쌍(severity → excess)이어야 합니다: "
                    f"{self.worst_pair!r} != {worst.comparison_pair!r}"
                )
        elif self.worst_pair not in labels:
            raise ValueError(f"worst_pair 가 pairs[] 안에 없습니다: {self.worst_pair!r}")

        # 롤업: 전 쌍이 보류면 null, 아니면 판정된 쌍 중 하나라도 위기면 true
        judged = [p.is_crisis for p in self.pairs if p.is_crisis is not None]
        expected = any(judged) if judged else None
        if self.is_crisis != expected:
            raise ValueError(
                f"is_crisis 는 pairs[] 롤업값이어야 합니다(전 쌍 보류면 null): "
                f"{self.is_crisis} != {expected}"
            )
        return self


class MonthlyReportInput(BaseModel):
    """월간 보고서 입력. FastAPI(reporting 노드)가 DB 조회·계산 후 자체 구성한다."""

    report_month: str = Field(
        ..., pattern=r"^\d{4}-\d{2}$", description="보고서 대상 연월 (YYYY-MM)"
    )
    start_date: date = Field(..., description="전월 시작일 (YYYY-MM-01)")
    end_date: date = Field(..., description="전월 종료일 (YYYY-MM-DD, 말일)")
    # 정본 필드명은 프로젝트 런타임 식별자인 product_group_id 로 통일한다
    # (CSGuidelineInput 과 방향이 반대이면 join·직렬화에서 계속 헷갈린다).
    product_group_id: str = Field(
        ..., alias="master_product_code", description="마스터 상품 그룹 고유 ID"
    )
    product_name: str = Field(..., description="상품명")
    total_voc_count: int = Field(
        ..., ge=0, description="월간 총 VOC 처리량 (< 10 이면 보류 처리)"
    )
    aspect_distributions: list[MonthlyAspectDistribution] = Field(
        ...,
        min_length=MONTHLY_ASPECT_COUNT,
        max_length=MONTHLY_ASPECT_COUNT,
        description="속성별 감성 분포 (색상·사이즈·소재 3건)",
    )
    sentiment_drifts: list[MonthlySentimentDrift] = Field(
        ...,
        min_length=MONTHLY_ASPECT_COUNT,
        max_length=MONTHLY_ASPECT_COUNT,
        description="속성별 감정 드리프트 (aspect 집합이 aspect_distributions 와 동일)",
    )
    channel_divergence: MonthlyChannelDivergenceInput = Field(
        ..., description="다채널 분열 정량 수치"
    )
    recommended_id: str = Field(..., description="권장 조치 사항 고유 ID")

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _validate_period_and_aspects(self) -> MonthlyReportInput:
        # 모듈 상단 import 를 건드리지 않기 위해 여기서만 calendar 를 쓴다(말일 계산 전용).
        import calendar

        year, month = int(self.report_month[:4]), int(self.report_month[5:7])
        last_day = calendar.monthrange(year, month)[1]
        if (self.start_date.year, self.start_date.month, self.start_date.day) != (year, month, 1):
            raise ValueError(
                f"start_date 는 report_month 의 1일이어야 합니다: {self.start_date} (report_month={self.report_month})"
            )
        if (self.end_date.year, self.end_date.month, self.end_date.day) != (year, month, last_day):
            raise ValueError(
                f"end_date 는 report_month 의 말일이어야 합니다: {self.end_date} (말일={last_day}일)"
            )

        dist_aspects = {d.aspect for d in self.aspect_distributions}
        drift_aspects = {d.aspect for d in self.sentiment_drifts}
        if len(dist_aspects) != MONTHLY_ASPECT_COUNT:
            raise ValueError(f"aspect_distributions 의 aspect 가 중복됩니다: {dist_aspects}")
        if dist_aspects != drift_aspects:
            raise ValueError(
                f"sentiment_drifts 의 aspect 집합은 aspect_distributions 와 같아야 합니다: "
                f"{drift_aspects} != {dist_aspects}"
            )
        return self


# ── §1-2. 월간 보고서 출력 (MonthlyReportOutput) ─────────────────────────
#
# LLM 생성 구간이다. 백엔드는 이 JSON 을 콜백으로 수신해 JSONB 로 영구 저장한다.


class MonthlyAspectSummary(BaseModel):
    aspect: Aspect = Field(..., description="속성 구분 (색상/사이즈/소재)")
    summary_text: str = Field(
        ..., min_length=10, max_length=200,
        description="속성별 AI 감성 요약 문구 (수치 그라운딩 대상)",
    )


class MonthlyChannelDivergenceCause(BaseModel):
    cause_title: str = Field(
        ..., min_length=5, max_length=60,
        description="채널간 격차 핵심 요약 제목 (severity 단계 라벨 필수 포함)",
    )
    cause_description: str = Field(
        ..., min_length=10, max_length=200, alias="cause",
        description="다채널 분열 발생 세부 원인 분석",
    )

    model_config = ConfigDict(populate_by_name=True)


class ChannelPairAnalysis(BaseModel):
    """채널쌍 1개에 대한 원인·조치. 리포트가 쌍마다 따로 보여준다(2026-08-04 화면 확정).

    보고서 전체 단위(`cause_analysis_results`/`recommended_actions`)와 별개다 —
    전자는 "이 상품 전체"의 결론이고, 이건 "이 채널쌍" 한정이다. 화면에서 게이지 바로
    아래 붙기 때문에 쌍과 어긋나면 곧바로 오독이 된다.
    """

    comparison_pair: str = Field(
        ..., pattern=r"^[A-Z]+_VS_[A-Z]+$", description="대상 채널쌍 (입력과 동일 표기)"
    )
    cause_analysis: list[str] = Field(
        ..., min_length=1, max_length=2, description="이 채널쌍의 원인 분석 (단문)"
    )
    recommended_actions: list[str] = Field(
        ..., min_length=1, max_length=2, description="이 채널쌍의 권장 조치 (단문)"
    )


class MonthlyReportOutput(BaseModel):
    """월간 보고서 출력 (LLM 생성 구간)."""

    report_id: str = Field(..., description="월간 보고서 고유 ID (예: RPT-202608-P001)")
    product_group_id: str = Field(
        ..., alias="master_product_code", description="마스터 상품 그룹 고유 ID (입력값과 일치)"
    )
    report_month: str = Field(
        ..., pattern=r"^\d{4}-\d{2}$", description="보고서 연월 (입력값과 일치)"
    )
    aspect_summaries: list[MonthlyAspectSummary] = Field(
        ...,
        min_length=MONTHLY_ASPECT_COUNT,
        max_length=MONTHLY_ASPECT_COUNT,
        description="속성별 요약 문구 목록 (aspect 집합이 입력과 동일)",
    )
    channel_divergence_cause: MonthlyChannelDivergenceCause = Field(
        ..., description="채널 분열 사유 분석 (전체 요약 1건)"
    )
    channel_pair_analyses: list[ChannelPairAnalysis] = Field(
        default_factory=list,
        max_length=MAX_CHANNEL_PAIRS,
        description="채널쌍별 원인·조치 (입력 pairs 와 1:1). 리포트에서 게이지 아래 표시",
    )
    cause_analysis_results: list[str] = Field(
        ..., min_length=1, max_length=5, description="핵심 원인 분석 결과 목록 (단문 문장 리스트)"
    )
    recommended_actions: list[str] = Field(
        ..., min_length=1, max_length=5, description="운영·CS 권장 조치 사항 목록 (단문 문장 리스트)"
    )
    pdf_s3_meta: PdfS3Meta | None = Field(None, description="생성된 PDF 의 S3 적재 메타데이터")

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _validate_aspects(self) -> MonthlyReportOutput:
        pair_labels = [a.comparison_pair for a in self.channel_pair_analyses]
        if len(pair_labels) != len(set(pair_labels)):
            raise ValueError(f"channel_pair_analyses 의 comparison_pair 가 중복됩니다: {pair_labels}")

        aspects = {s.aspect for s in self.aspect_summaries}
        if len(aspects) != MONTHLY_ASPECT_COUNT:
            raise ValueError(f"aspect_summaries 의 aspect 가 중복됩니다: {aspects}")
        invalid = aspects - MONTHLY_ASPECTS
        if invalid:
            raise ValueError(f"월간 연산 대상 aspect 가 아닙니다(색상/사이즈/소재): {invalid}")
        return self


# ── §1-3. 대시보드 요약 (DashboardMonthlySummary) ────────────────────────
#
# GET /api/v1/dashboard/monthly-summary?month=YYYY-MM
# ⚠️ 백엔드가 직접 집계하는 응답 형태로, AI 계약 밖이다. 계약 참조용으로만 둔다.


class DashboardMonthlySummary(BaseModel):
    report_month: str = Field(..., pattern=r"^\d{4}-\d{2}$", description="조회 대상 연월")
    total_voc_count: int = Field(..., ge=0, description="카드① 총 VOC 처리량 (전 상품 합계)")
    total_voc_ratio: float = Field(..., ge=-1.0, description="카드① 전월 대비 증감률")
    brand_sentiment_ratio: float = Field(..., ge=0.0, le=1.0, description="카드② 브랜드 감성 지수")
    brand_sentiment_rank_3m: int = Field(..., ge=1, le=3, description="카드② 최근 3개월 내 순위")


# ── §2-1. CS 가이드라인 입력 (CSGuidelineInput) ──────────────────────────


class CSGuidelineStatsInput(BaseModel):
    """이상탐지 정량 통계."""

    cur_rate: float = Field(..., ge=0.0, le=1.0, description="현재 윈도우 부정 비율")
    past_rate: float = Field(..., ge=0.0, le=1.0, description="직전 윈도우 부정 비율")
    delta: float = Field(..., description="부정 비율 변동폭 (= cur_rate − past_rate)")
    cur_total: int = Field(..., ge=0, description="현재 윈도우 총 문의 건수")
    p_value: float | None = Field(
        None, ge=0.0, le=1.0,
        description="순열검정 p값 — 템플릿 고정 렌더 전용, 프롬프트 제외",
    )
    bh_significant: bool | None = Field(None, description="다중검정 통과 여부 (BH-FDR q=0.05)")

    @model_validator(mode="after")
    def _validate_delta(self) -> CSGuidelineStatsInput:
        expected = self.cur_rate - self.past_rate
        if abs(self.delta - expected) > RATIO_SUM_TOLERANCE:
            raise ValueError(f"delta 는 cur_rate − past_rate 여야 합니다: {self.delta} != {expected}")
        return self


class CSGuidelineRootCause(BaseModel):
    """세부 원인 집계 데이터."""

    label: str = Field(..., description="원인 분류기 도출 최다 원인 명칭")
    count: int = Field(..., ge=0, description="최다 원인 해당 건수")
    total: int = Field(..., ge=0, description="원인 분석 대상 전체 건수")

    @model_validator(mode="after")
    def _validate_count(self) -> CSGuidelineRootCause:
        if self.count > self.total:
            raise ValueError(f"count 는 total 이하여야 합니다: {self.count} > {self.total}")
        return self


class LinkedCSInquiry(BaseModel):
    """evidence.inquiry_ids 기반 DB 조인 CS 문의."""

    item_id: str = Field(..., alias="cs_id", description="CS 문의 고유 ID")
    raw_text: str = Field(..., description="고객 작성 문의 원문")
    created_at: datetime = Field(..., description="CS 접수 일시")
    source: Source | None = Field(
        None,
        description=(
            "원문 출처(cs=문의 / review=리뷰). 리뷰 소스 알림은 이 목록이 리뷰 원문이다"
            " — 리뷰도 근거로 쓰는 것이 확정 정책(2026-08-11). None 은 출처 미상"
        ),
    )

    model_config = ConfigDict(populate_by_name=True)


class CSGuidelineInput(BaseModel):
    """CS 가이드라인 입력. DetectionAlert 와 1:1 로 매칭된다(alert_id)."""

    alert_id: str = Field(..., description="이상 탐지 알림 고유 ID (예: ALT-20260528-P001-COUPANG)")
    detected_at: datetime = Field(..., description="이상 탐지 완료 일시")
    product_group_id: str = Field(
        ..., alias="master_product_code", description="마스터 상품 그룹 고유 ID"
    )
    product_name: str | None = Field(None, description="상품명")
    channel: Channel = Field(..., description="이상 감지 채널")
    main_aspect: Aspect = Field(..., description="주 이상 감지 속성 (6종 전체 허용)")
    verdict: Verdict = Field(..., description="판정 유형 (정상은 생성 대상 아님)")
    recommended_action: RecommendedAction = Field(..., description="이상탐지 엔진 결정 권장 액션")
    detection_confidence: DetectionConfidence = Field(..., description="탐지 확신도")
    stats: CSGuidelineStatsInput = Field(..., alias="status", description="이상탐지 정량 통계")
    root_cause: CSGuidelineRootCause | None = Field(
        None, description="세부 원인 집계 데이터 (null 이면 recommended_action != '개선안 생성')"
    )
    linked_inquiries: list[LinkedCSInquiry] = Field(
        ..., min_length=1, description="evidence.inquiry_ids 기반 DB 조인 CS 문의 리스트"
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _validate_generation_target(self) -> CSGuidelineInput:
        if self.verdict in GUIDELINE_EXCLUDED_VERDICTS:
            raise ValueError(f"'{self.verdict.value}' 판정은 가이드라인 생성 대상이 아닙니다")
        if self.root_cause is None and self.recommended_action == RecommendedAction.GENERATE_RECOMMENDATION:
            raise ValueError(
                "recommended_action 이 '개선안 생성'이면 root_cause 가 있어야 합니다"
            )
        ids = [i.item_id for i in self.linked_inquiries]
        if len(ids) != len(set(ids)):
            raise ValueError(f"linked_inquiries 의 item_id 가 중복됩니다: {ids}")
        return self


# ── §2-2. CS 가이드라인 출력 (CSGuidelineOutput) ─────────────────────────


class CSGuidelineSummary(BaseModel):
    issue_title: str = Field(..., min_length=5, max_length=80, description="가이드라인 이슈 타이틀")
    risk_level: RiskLevel = Field(..., description="위험 등급")
    key_metric_text: str = Field(
        ..., min_length=5, max_length=200,
        description="지표 변동 추이 요약 문구 (수치 그라운딩 대상)",
    )


class StandardGuideline(BaseModel):
    core_message: str = Field(..., min_length=10, max_length=400, description="CS팀 핵심 안내 매뉴얼")
    draft_reply: str = Field(..., min_length=10, max_length=1200, description="표준 권장 답변 초안")
    key_talking_points: list[str] = Field(
        ..., min_length=1, max_length=6, description="필수 언급 및 금지 표현 리스트"
    )


class InquirySpecificGuide(BaseModel):
    item_id: str = Field(..., description="대상 CS 문의 ID (⊆ linked_inquiries[].item_id)")
    recommended_point: str = Field(
        ..., min_length=5, max_length=300, description="해당 문의 맞춤 응대 가이드"
    )


class CSGuidelineOutput(BaseModel):
    """CS 가이드라인 출력 (LLM 생성 구간)."""

    guideline_id: str = Field(..., description="가이드라인 고유 ID (예: GD-20260528-P001)")
    alert_id: str = Field(..., description="원본 탐지 알림 ID (입력값과 일치)")
    summary: CSGuidelineSummary = Field(..., description="가이드라인 요약 정보")
    root_cause_summary: str = Field(
        ..., min_length=5, max_length=200,
        description="최다 원인 지분율 요약 (수치 그라운딩 대상)",
    )
    standard_guideline: StandardGuideline = Field(..., description="표준 응대 가이드 세트")
    ops_action_guide: str = Field(
        ..., min_length=5, max_length=400, description="예외 보상 및 조치 지침"
    )
    inquiry_specific_guides: list[InquirySpecificGuide] = Field(
        ..., min_length=1, description="개별 CS 문의별 맞춤 가이드 목록"
    )
    pdf_s3_meta: PdfS3Meta | None = Field(None, description="생성된 PDF 의 S3 적재 메타데이터")

    @model_validator(mode="after")
    def _validate_guide_ids(self) -> CSGuidelineOutput:
        ids = [g.item_id for g in self.inquiry_specific_guides]
        if len(ids) != len(set(ids)):
            raise ValueError(f"inquiry_specific_guides 의 item_id 가 중복됩니다: {ids}")
        return self


# ── §3-2. 생성 완료 콜백 (GenerationCallback) ────────────────────────────
#
# POST /api/v1/internal/reports/complete · FastAPI → Spring Boot
#
# ⚠️ 산출물 적재 방식이 문서 종류별로 다르다 (2026-08-03 확정):
#
#   월간 리포트  — **PDF 만** S3(영구 버킷)에 적재하고 링크만 보낸다. UI 는 그 링크를
#                 PDF 뷰어로 띄운다. 데이터를 DB 에 적재하지 않으므로 source_payload 는
#                 **보내지 않는다**(용량이 커서 데이터를 따로 쌓지 않기로 한 결정).
#                 → PDF 자체가 정본이라 문서 안에 수치 표까지 모두 들어가야 한다.
#   CS 가이드라인 — 출력 데이터를 DB 에 적재하고 PDF 도 S3 에 함께 올린다.
#                 source_payload(입력+출력 JSON)가 **필수**이며, 이 원본으로 PDF 를
#                 언제든 재컴파일할 수 있다.


class GenerationCallback(BaseModel):
    report_id: str | None = Field(
        None, description="월간 보고서 산출물 ID (guideline_id 와 배타적 — 정확히 하나)"
    )
    guideline_id: str | None = Field(None, description="CS 가이드라인 산출물 ID (상동)")
    status: CallbackStatus = Field(..., description="처리 결과 상태 코드")
    pdf_s3_meta: PdfS3Meta | None = Field(
        None, description="S3 적재 메타데이터 (SUCCESS 일 때만 non-null)"
    )
    notice_message: str | None = Field(
        None, description="사용자 안내 문구 (비 SUCCESS 일 때 필수)"
    )
    source_payload: dict | None = Field(
        None,
        description="입력+출력 JSON 원본 (CS 가이드라인 SUCCESS 시 필수 / 월간은 미전송)",
    )
    validation_report: dict | None = Field(None, description="검증 실패 내역")

    @model_validator(mode="after")
    def _validate_exclusive_and_status(self) -> GenerationCallback:
        if (self.report_id is None) == (self.guideline_id is None):
            raise ValueError("report_id 와 guideline_id 중 정확히 하나만 있어야 합니다")

        if self.status == CallbackStatus.SUCCESS:
            if self.pdf_s3_meta is None:
                raise ValueError("status=SUCCESS 이면 pdf_s3_meta 가 필요합니다")
            # CS 가이드라인만 원본 데이터를 적재한다. 월간은 PDF 가 정본이라 원본을
            # 보내지 않으며, 여기서 요구하면 그대로 발행이 막힌다.
            if self.guideline_id is not None and self.source_payload is None:
                raise ValueError(
                    "CS 가이드라인은 status=SUCCESS 이면 source_payload 가 필요합니다"
                )
        else:
            if self.pdf_s3_meta is not None:
                raise ValueError(f"status={self.status.value} 이면 pdf_s3_meta 는 null 이어야 합니다")
            if not self.notice_message:
                raise ValueError(f"status={self.status.value} 이면 notice_message 가 필요합니다")
        return self


# ── §4-4. 교차검증 ──────────────────────────────────────────────────────
#
# 입력↔출력을 맞대보는 그라운딩 검증(식별자 일치·단계 라벨 대조·cs_id 포함관계)은
# `app/reporting/{monthly_report_validator,cs_reply_validator}.py` 가 **유일한 구현**이다.
# 예전에는 같은 규칙이 여기에도 있었는데, 아무도 호출하지 않으면서 규칙만 두 벌이 되어
# 한쪽만 고치기 쉬운 상태였다. 검증기 쪽은 반려 사유 목록을 돌려줘 재시도 프롬프트에
# 그대로 되먹일 수 있으므로 그쪽으로 일원화했다.
