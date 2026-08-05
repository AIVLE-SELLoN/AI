"""PDF S3 적재 — 인프라 「S3 파일 구조 규칙 정의」 + 문서 생성 스키마 §3-1.

🚧 **아직 스텁이다.** 실제 업로드(boto3)를 하지 않고 메타데이터만 만든다.
   AWS 버킷·자격증명이 준비되기 전이라 의도적으로 비워 뒀다.
   그래서 `S3_ENABLED` 가 꺼져 있으면 **성공을 반환하지 않고 예외를 던진다** —
   업로드하지 않은 파일을 "적재 완료"로 보고하면 백엔드 연동 순간 셀러 화면에
   죽은 링크가 뜨고, 월간은 재생성 경로도 없다.
   실제 구현을 붙일 때는 이 docstring 과 S3_ENABLED 기본값을 함께 정리할 것.

── 인프라 확정 규칙 (2026-08-05) ──────────────────────────────────────────

**버킷은 하나다.** 문서 종류는 프리픽스로 가른다 — Lifecycle 이 프리픽스 단위로
걸리기 때문이다. (예전에는 `sellon-reports` / `sellon-temp-reports` 두 개로 나눴다.)

    reports/companies/{company_id}/monthly-report/{yyyy}/{mm}/{filename}.pdf
    reports/companies/{company_id}/cs-guideline/{yyyy}/{mm}/{filename}.pdf

파일명: `{report_type}_{yyyyMM}_{uuid4}.pdf`
    monthly-report_202607_a3f4c9e2-b7d1-4f2a-9e6c-2a5f8b3d1c7a.pdf

⚠️ `{yyyy}/{mm}` 은 **업로드 시각이 아니라 보고 대상 기간**이다. 인프라 경로 예시가
   `monthly-report/2026/07/monthly-report_202607_….pdf` 로 폴더와 파일명의 연월을
   맞춰 놨다. 업로드 시각을 쓰면 8/1 새벽에 올린 7월 리포트가 `2026/08` 폴더에
   `…_202607_….pdf` 로 들어가 폴더와 파일명이 어긋난다.

보존(삭제는 **S3 Lifecycle** 이 하고, 코드는 같은 값으로 `object_expires_at` 만 알린다):

  monthly-report  회사/연/월 기준 누적 보관 → **6개월** 뒤 자동 삭제.
                  **재생성 경로가 없다**(2026-08-05 확정). 올려두고 Lifecycle 이 지울
                  때까지 두는 것이 전부다 — 월간은 DB 에 데이터를 적재하지 않아 PDF 가
                  유일한 산출물이므로 **만료 = 영구 소실**이다.
                  (인프라 문서 §6 의 "필요 시 원본 데이터를 DB 기준으로 재생성" 은
                   월간에 해당하지 않는다. 재생성할 원본이 없다.)
  cs-guideline    Pre-signed URL 만료(24h)와 맞물려 짧게 → **1일** 뒤 자동 삭제.
                  **원본(출력 JSON)을 DB(JSONB)에 보관해 두었다가 재생성 요청이 오면
                  다시 만든다**(2026-08-05 확정) — 재생성 플로우를 타는 유일한 문서다.

Pre-signed URL 은 객체 수명과 별개인 "링크의 만료"이며 **발급 시점 기준 24시간 고정**
이다(문서 종류 무관). 메일 발송 때마다 새로 발급하고 재사용하지 않는다.
**링크가 객체보다 오래 살 수는 없다** — 그런 조합은 스키마가 거부한다.
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.core import constants
from app.core.schemas import PdfS3Meta

# 실제 업로드 구현이 붙기 전까지는 꺼둔다. 켜면 아래 upload 가 메타데이터만 만들고
# 성공을 반환하므로, boto3 연동이 끝난 뒤에만 기본값을 True 로 바꿀 것.
S3_ENABLED = os.getenv("S3_ENABLED", "false").lower() in ("1", "true", "yes")

# 버킷은 **하나**다. 문서 종류는 프리픽스로 가르고 Lifecycle 도 프리픽스 단위로 건다.
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "sellon-reports")

# 고객사 식별자(PK/UUID). 경로가 회사 단위로 갈리므로 업로드에 반드시 필요하다.
# ⚠️ 아직 어떤 입력 스키마에도 없다(MonthlyReportInput·CSGuidelineInput 모두).
#    지금은 단일 테넌트 가정으로 환경변수에서 받는다. 멀티테넌트가 되면 요청마다
#    달라지므로 **입력 스키마에 넣어 호출부가 넘겨야 한다** — 팀 합의 대상.
S3_DEFAULT_COMPANY_ID = os.getenv("S3_COMPANY_ID", "")

# 인프라 문서가 정한 프리픽스 이름이자 파일명 접두어. 값이 바뀌면 Lifecycle 규칙이
# 걸린 프리픽스와 어긋나 객체가 영영 안 지워지거나 일찍 지워진다.
REPORT_TYPE_MONTHLY = "monthly-report"
REPORT_TYPE_GUIDELINE = "cs-guideline"

_PERIOD_PATTERN = re.compile(r"^\d{4}-\d{2}$")


@dataclass(frozen=True)
class StoragePolicy:
    """문서 종류별 적재 정책. S3 Lifecycle 설정과 짝이 맞아야 한다."""

    bucket_name: str
    prefix: str  # 버킷 내 문서 종류 구분 = Lifecycle 규칙이 걸리는 단위
    presigned_ttl_hours: int
    retention_hours: int
    recompilable: bool  # 만료 후 원본으로 다시 만들 수 있는가

    @property
    def retention_label(self) -> str:
        if self.retention_hours % 24 == 0 and self.retention_hours >= 24:
            return f"{self.retention_hours // 24}일"
        return f"{self.retention_hours}시간"


_STORAGE_POLICY: dict[str, StoragePolicy] = {
    REPORT_TYPE_MONTHLY: StoragePolicy(
        bucket_name=S3_BUCKET_NAME,
        prefix=REPORT_TYPE_MONTHLY,
        presigned_ttl_hours=constants.PRESIGNED_URL_TTL_HOURS,
        retention_hours=constants.MONTHLY_RETENTION_DAYS * 24,
        recompilable=False,  # 재생성 경로 없음 — 만료되면 끝이다(2026-08-05 확정)
    ),
    REPORT_TYPE_GUIDELINE: StoragePolicy(
        bucket_name=S3_BUCKET_NAME,
        prefix=REPORT_TYPE_GUIDELINE,
        presigned_ttl_hours=constants.PRESIGNED_URL_TTL_HOURS,
        retention_hours=constants.GUIDELINE_RETENTION_HOURS,
        recompilable=True,  # 재생성 요청 시 source_payload 로 재컴파일한다
    ),
}


class S3NotConfiguredError(Exception):
    """S3 업로드가 아직 구현·구성되지 않았을 때. 호출부가 FAILED_ERROR 로 변환한다.

    "업로드한 척"보다 실패가 낫다 — 죽은 링크가 셀러에게 나가는 것을 막는다.
    """


class PdfSizeExceededError(Exception):
    """PDF 가 10MB 상한을 넘었을 때. 호출부가 FAILED_SIZE_EXCEEDED 로 변환한다.

    S3 업로드·메일 발송 트랜잭션 **이전에** 차단하려고 예외로 올린다(§4-3).
    """


def resolve_storage_policy(report_type: str) -> StoragePolicy:
    """report_type → 적재 정책.

    등록되지 않은 종류는 짧은 보존(1일) 쪽으로 보낸다 — 6개월 프리픽스에 정체 불명의
    객체가 쌓이는 것보다 하루 뒤 사라지는 쪽이 안전하다.
    """
    return _STORAGE_POLICY.get(report_type, _STORAGE_POLICY[REPORT_TYPE_GUIDELINE])


def build_object_path(report_type: str, period: str, company_id: str) -> tuple[str, str]:
    """인프라 규칙에 맞는 (디렉토리 경로, 파일명) 을 만든다.

        reports/companies/{company_id}/{report_type}/{yyyy}/{mm}/
        {report_type}_{yyyyMM}_{uuid4}.pdf

    period 는 **보고 대상 기간**(YYYY-MM)이다. 업로드 시각이 아니다 — 폴더의 연월과
    파일명의 연월이 어긋나면 인프라가 정한 경로 규칙을 깨뜨린다.
    """
    if not _PERIOD_PATTERN.match(period):
        raise ValueError(f"period 는 YYYY-MM 형식이어야 합니다: {period!r}")

    year, month = period.split("-")
    policy = resolve_storage_policy(report_type)
    s3_file_path = f"reports/companies/{company_id}/{policy.prefix}/{year}/{month}/"
    new_file_name = f"{policy.prefix}_{year}{month}_{uuid.uuid4()}.pdf"
    return s3_file_path, new_file_name


def _build_original_file_name(prefix: str, period: str, source_id: str | None) -> str:
    """표시용(원본) 파일명. `new_file_name` 과 달리 사람이 읽는 이름이다.

    ⚠️ `PdfS3Meta` 의 필수 4종은 "메인이 파일을 다시 찾거나 **목록에 표시할 때** 필요한
       최소 집합"이다. 그래서 월 여러 건이 나오는 문서(CS 가이드라인은 알림마다 1건)는
       `{yyyyMM}` 만으로는 부족하다 — 5월 가이드라인이 전부 `cs-guideline_202605.pdf`
       가 되어 목록에서 구분이 안 된다. 저장은 `new_file_name` 의 uuid4 로 안전하지만
       표시용 이름은 별개 문제다.
    """
    stem = f"{prefix}_{period.replace('-', '')}"
    return f"{stem}_{source_id}.pdf" if source_id else f"{stem}.pdf"


async def upload_pdf_to_s3(
    pdf_bytes: bytes,
    report_type: str,
    period: str,
    company_id: str | None = None,
    source_id: str | None = None,
) -> PdfS3Meta:
    """🚧 스텁 — **실제 업로드는 하지 않고** 적재 메타데이터만 만든다.

    Args:
        period: 보고 대상 기간 `YYYY-MM`. 경로의 `{yyyy}/{mm}` 와 파일명의 `{yyyyMM}`
            을 **같은 값**으로 만든다.
        company_id: 고객사 PK/UUID. 생략하면 `S3_COMPANY_ID` 환경변수를 쓴다.
        source_id: 산출물을 가리키는 식별자(CS 는 `alert_id`). `original_file_name`
            뒤에 붙는다. **월 여러 건이 나오는 문서에는 반드시 넘겨야 한다** — 없으면
            같은 달 산출물이 전부 같은 표시용 이름이 되어 목록이 도배된다.
            월간 리포트는 월 1건이라 생략한다.

    Raises:
        PdfSizeExceededError: 용량이 MAX_PDF_SIZE_BYTES 를 초과할 때.
        S3NotConfiguredError: S3_ENABLED 가 꺼져 있거나 company_id 를 알 수 없을 때.
    """
    file_size = len(pdf_bytes)
    if not S3_ENABLED:
        raise S3NotConfiguredError(
            f"S3 업로드가 아직 구현되지 않았습니다(S3_ENABLED=false). "
            f"업로드하지 않은 파일을 성공으로 보고하지 않습니다 ({report_type}/{period}, "
            f"{file_size} bytes)"
        )

    resolved_company_id = company_id or S3_DEFAULT_COMPANY_ID
    if not resolved_company_id:
        # 경로가 회사 단위로 갈리므로, 모르는 채로 올리면 남의 폴더이거나 규칙 밖
        # 경로가 된다. 추측해서 올리느니 실패시킨다.
        raise S3NotConfiguredError(
            f"company_id 를 알 수 없습니다(S3_COMPANY_ID 미설정). "
            f"경로가 회사 단위로 갈리므로 임의 경로에 올리지 않습니다 ({report_type}/{period})"
        )

    if file_size > constants.MAX_PDF_SIZE_BYTES:
        raise PdfSizeExceededError(
            f"PDF 용량 초과: {file_size} bytes > {constants.MAX_PDF_SIZE_BYTES} bytes "
            f"({report_type}/{period})"
        )

    policy = resolve_storage_policy(report_type)
    s3_file_path, new_file_name = build_object_path(report_type, period, resolved_company_id)
    now = datetime.now(UTC)

    return PdfS3Meta(
        s3_bucket_name=policy.bucket_name,
        s3_file_path=s3_file_path,
        original_file_name=_build_original_file_name(policy.prefix, period, source_id),
        new_file_name=new_file_name,
        created_at=now,
        # 스키마가 s3_full_key == s3_file_path + new_file_name 을 강제한다
        s3_full_key=f"{s3_file_path}{new_file_name}",
        file_extension="pdf",
        file_size_bytes=file_size,
        # ⚠️ 운영에서 메일에 실리는 링크는 **발송 시점에 백엔드(Spring)가** 새로 발급한다
        #    (인프라 §5: 발송할 때마다 재발급, 재사용 금지). 여기 값은 업로드 직후의
        #    참조용이며, 만료되면 s3_full_key 로 다시 요청하면 된다.
        presigned_url=(
            f"https://{policy.bucket_name}.s3.amazonaws.com/{s3_file_path}{new_file_name}"
        ),
        presigned_expires_at=now + timedelta(hours=policy.presigned_ttl_hours),
        # S3 Lifecycle 이 실제로 지우는 시각. UI 가 "다운로드 기한"을 안내하고,
        # 백엔드가 만료 전 재발급/재생성을 판단하는 근거가 된다.
        object_expires_at=now + timedelta(hours=policy.retention_hours),
    )
