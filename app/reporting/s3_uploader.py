"""PDF S3 적재 — 문서 생성 스키마 §3-1.

⚠️ 보존 정책이 문서 종류별로 다르다 (2026-08-03 확정). 삭제는 **S3 Lifecycle** 이 하고,
   코드는 같은 값으로 "언제 사라지는지"(`object_expires_at`)를 계산해 알려주기만 한다.

  월간 리포트   PDF 가 **유일한 산출물**이다(데이터를 DB 에 적재하지 않는다).
               → 업로드 후 **6개월** 뒤 자동 삭제. 원본이 없으므로 **만료 = 영구 소실**이다.
  CS 가이드라인 출력 데이터가 DB 에 적재되어 언제든 재컴파일할 수 있다.
               → 업로드 후 **24시간** 뒤 자동 삭제.

presigned URL 은 객체 수명과 별개인 "링크의 만료"다. 만료 후에는 백엔드가 `s3_full_key`
로 재발급한다(SigV4 상 최대 7일이라 6개월짜리 링크는 애초에 만들 수 없다).
단, **링크가 객체보다 오래 살 수는 없다** — 그런 조합은 스키마가 거부한다.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.core import constants
from app.core.schemas import PdfS3Meta

MONTHLY_BUCKET_NAME = "sellon-reports"
GUIDELINE_BUCKET_NAME = "sellon-temp-reports"

# 링크 수명. 월간은 UI 가 링크로만 여는 구조라 상한(7일)까지 길게 잡고,
# CS 는 객체 수명과 같은 24h 로 맞춘다(객체가 사라진 뒤 살아있는 링크는 의미가 없다).
MONTHLY_PRESIGNED_TTL_HOURS = 24 * 7
GUIDELINE_PRESIGNED_TTL_HOURS = constants.GUIDELINE_RETENTION_HOURS

REPORT_TYPE_MONTHLY = "monthly"
REPORT_TYPE_GUIDELINE = "cs_guidelines"


@dataclass(frozen=True)
class StoragePolicy:
    """문서 종류별 적재 정책. S3 Lifecycle 설정과 짝이 맞아야 한다."""

    bucket_name: str
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
        bucket_name=MONTHLY_BUCKET_NAME,
        presigned_ttl_hours=MONTHLY_PRESIGNED_TTL_HOURS,
        retention_hours=constants.MONTHLY_RETENTION_DAYS * 24,
        recompilable=False,  # 원본을 보관하지 않는다 → 만료되면 끝이다
    ),
    REPORT_TYPE_GUIDELINE: StoragePolicy(
        bucket_name=GUIDELINE_BUCKET_NAME,
        presigned_ttl_hours=GUIDELINE_PRESIGNED_TTL_HOURS,
        retention_hours=constants.GUIDELINE_RETENTION_HOURS,
        recompilable=True,  # source_payload 로 재컴파일 가능
    ),
}


class PdfSizeExceededError(Exception):
    """PDF 가 10MB 상한을 넘었을 때. 호출부가 FAILED_SIZE_EXCEEDED 로 변환한다.

    S3 업로드·메일 발송 트랜잭션 **이전에** 차단하려고 예외로 올린다(§4-3).
    """


def resolve_storage_policy(report_type: str) -> StoragePolicy:
    """report_type → 적재 정책.

    등록되지 않은 종류는 짧은 보존(24h) 쪽으로 보낸다 — 6개월짜리 버킷에 정체 불명의
    객체가 쌓이는 것보다 하루 뒤 사라지는 쪽이 안전하다.
    """
    return _STORAGE_POLICY.get(report_type, _STORAGE_POLICY[REPORT_TYPE_GUIDELINE])


async def upload_pdf_to_s3(
    pdf_bytes: bytes,
    report_type: str,
    product_group_id: str,
    identifier: str,
) -> PdfS3Meta:
    """PDF 바이너리를 S3 에 적재하고 메타데이터를 반환한다.

    Raises:
        PdfSizeExceededError: 용량이 MAX_PDF_SIZE_BYTES 를 초과할 때.
    """
    file_size = len(pdf_bytes)
    if file_size > constants.MAX_PDF_SIZE_BYTES:
        raise PdfSizeExceededError(
            f"PDF 용량 초과: {file_size} bytes > {constants.MAX_PDF_SIZE_BYTES} bytes "
            f"({report_type}/{identifier})"
        )

    policy = resolve_storage_policy(report_type)

    now = datetime.now(UTC)
    unique_suffix = uuid.uuid4().hex[:8]
    # product_group_id 는 월간 합본에서 "ALL" 이 들어온다(월 1개 파일이라 상품 구분이 없다).
    new_file_name = (
        f"{report_type}_{product_group_id}_{identifier}_{now.strftime('%Y%m%d')}_{unique_suffix}.pdf"
    )
    s3_file_path = f"reports/{report_type}/{now.strftime('%Y/%m')}/"

    return PdfS3Meta(
        s3_bucket_name=policy.bucket_name,
        s3_file_path=s3_file_path,
        original_file_name=f"{report_type}_{identifier}.pdf",
        new_file_name=new_file_name,
        created_at=now,
        # 스키마가 s3_full_key == s3_file_path + new_file_name 을 강제한다
        s3_full_key=f"{s3_file_path}{new_file_name}",
        file_extension="pdf",
        file_size_bytes=file_size,
        presigned_url=(
            f"https://{policy.bucket_name}.s3.amazonaws.com/{s3_file_path}{new_file_name}"
        ),
        presigned_expires_at=now + timedelta(hours=policy.presigned_ttl_hours),
        # S3 Lifecycle 이 실제로 지우는 시각. UI 가 "다운로드 기한"을 안내하고,
        # 백엔드가 만료 전 재발급/재생성을 판단하는 근거가 된다.
        object_expires_at=now + timedelta(hours=policy.retention_hours),
    )
