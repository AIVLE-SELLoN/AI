"""PDF S3 적재. 규칙의 정본은 인프라 「S3 파일 구조 규칙 정의」다.

boto3 로 실제 업로드하고 Pre-signed URL 을 발급한다. `S3_ENABLED` 가 꺼져 있으면
성공을 반환하지 않고 예외를 던진다 — 업로드하지 않은 파일을 "적재 완료"로 보고하면
셀러 화면에 죽은 링크가 뜨고, 월간은 재생성 경로도 없다. 기본값은 꺼짐이다.

버킷은 하나이고 문서 종류는 프리픽스로 가른다. Lifecycle 이 프리픽스 단위로 걸리기
때문이다:

    reports/{report_type}/{company_id}/{yyyy}/{mm}/{report_type}_{yyyyMM}_{uuid4}.pdf

report_type 이 company_id 보다 위인 것이 핵심이다. S3 Lifecycle 규칙은 리터럴 prefix
완전 일치만 지원하고 와일드카드를 못 쓴다. 회사가 위면 공통 prefix 가 `reports/` 까지
밖에 안 잡혀 월간과 CS 의 보존 기간을 분리할 수 없다. 지금 순서면 회사 수와 무관하게
규칙이 2개로 고정된다.

`{yyyy}/{mm}` 은 업로드 시각이 아니라 보고 대상 기간이다. 업로드 시각을 쓰면 8/1
새벽에 올린 7월 리포트가 `2026/08` 폴더에 `…_202607_….pdf` 로 들어가 어긋난다.

보존은 S3 Lifecycle 이 지우고, 코드는 같은 값으로 `object_expires_at` 만 알린다:

  monthly-report  6개월. **재생성 경로가 없어 만료 = 영구 소실**이다 — 월간은 DB 에
                  데이터를 적재하지 않아 PDF 가 유일한 산출물이다.
  cs-guideline    7일. 화면 플로우에 운영 MD 승인이라는 사람 단계가 있고 메일은 승인
                  뒤에 나가므로, 더 짧으면 승인 대기 중에 발송할 객체가 사라진다.
                  원본(출력 JSON)을 DB 에 보관해 만료 후 재생성이 가능한 유일한
                  문서다. 다만 화면의 '재시도' 버튼은 재생성이 아니라 S3 문서를 다시
                  불러오는 동작이라 만료 뒤에는 그 버튼으로 복구되지 않는다.

Pre-signed URL 은 객체 수명과 별개인 "링크의 만료"이며 발급 시점 기준 7일 고정이다
(문서 종류 무관, SigV4 상한). AI 노드가 업로드 시점에 최초 1회 발급하고 메인 서버는
유효한 동안 재사용하다 만료됐을 때만 `s3_full_key` 로 재발급한다. 링크가 객체보다
오래 살 수는 없다 — 그런 조합은 스키마가 거부한다.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.core import constants
from app.core.schemas import PdfS3Meta

logger = logging.getLogger("S3Uploader")

# 켜면 실제로 S3 에 올린다. 꺼져 있으면 예외를 던진다 — 올린 척하지 않는다.
S3_ENABLED = os.getenv("S3_ENABLED", "false").lower() in ("1", "true", "yes")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "sellon-reports-dev-337658133748-ap-northeast-2-an")
S3_REGION = os.getenv("S3_REGION", "ap-northeast-2")

# Pre-signed URL 서명용 정적 액세스 키. IAM Role(임시 자격증명)로 서명하면 안 된다 —
# 임시 키가 만료되면 그 키로 만든 URL 도 같이 죽어 약속한 7일이 유지되지 않는다.
# 값은 `.env`(git 미포함)에만 둔다. boto3 가 이 표준 이름을 그대로 읽는다.
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# 버킷 정책상 `reports/` 로 시작하지 않는 키는 권한 오류가 난다.
S3_KEY_ROOT = "reports/"

# 고객사 식별자(PK/UUID). 경로가 회사 단위로 갈리므로 업로드에 반드시 필요하다.
# 어떤 입력 스키마에도 없어 단일 테넌트 가정으로 환경변수에서 받는다. 멀티테넌트가
# 되면 요청마다 달라지므로 입력 스키마에 넣어 호출부가 넘겨야 한다 — 팀 합의 대상.
S3_DEFAULT_COMPANY_ID = os.getenv("S3_COMPANY_ID", "")

# 표시용 고객사명. 경로에는 쓰지 않는다 — 회사명이 바뀌면 경로가 갈라져 이전 산출물을
# 못 찾는다. 메타데이터로만 실어 메인이 목록에 띄우게 한다.
S3_DEFAULT_COMPANY_NAME = os.getenv("S3_COMPANY_NAME", "") or None

# 로컬 미러 — 값이 있으면 업로드한 것과 똑같은 경로로 사본을 하나 더 떨어뜨린다.
# 버킷을 열지 않고도 경로 규칙이 트리 모양으로 드러난다. 운영에서는 쓰지 않는다.
S3_LOCAL_MIRROR_DIR = os.getenv("S3_LOCAL_MIRROR_DIR", "")

# 프리픽스 이름이자 파일명 접두어. 값이 바뀌면 Lifecycle 규칙이 걸린 프리픽스와
# 어긋나 객체가 영영 안 지워지거나 일찍 지워진다.
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
        recompilable=False,  # 재생성 경로 없음 — 만료되면 끝이다
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
    """S3 업로드가 구성되지 않았을 때. 호출부가 FAILED_ERROR 로 변환한다.

    "업로드한 척"보다 실패가 낫다 — 죽은 링크가 셀러에게 나가는 것을 막는다.
    """


class PdfSizeExceededError(Exception):
    """PDF 가 10MB 상한을 넘었을 때. 호출부가 FAILED_SIZE_EXCEEDED 로 변환한다.

    S3 업로드·메일 발송 트랜잭션 이전에 차단하려고 예외로 올린다.
    """


class S3UploadError(Exception):
    """업로드·서명이 실패했을 때. 호출부의 일반 예외 처리가 FAILED_ERROR 로 받는다.

    올리지 못한 파일을 성공으로 보고하지 않으려고 예외로 올린다 — 콜백에 죽은 링크가
    실리면 셀러가 눌렀을 때 404 를 본다.
    """


def _get_s3_client():
    """정적 키로 서명하는 S3 클라이언트. 세 설정 모두 못 박아야 링크가 산다.

    기본 자격증명 체인(IAM Role·인스턴스 프로파일)을 쓰지 않고 키를 명시적으로 넘긴다.
    임시 자격증명으로 서명하면 그 크리덴셜이 만료될 때 URL 도 같이 죽어 약속한 7일이
    유지되지 않는다.

    `signature_version="s3v4"`: 7일(604,800초)은 SigV4 서명의 상한이고 구버전 서명(s3)은
    그만큼 못 버틴다.

    `addressing_style="virtual"`: 없으면 presigned URL 이 403 이다(실측). 기본값(auto)은
    호스트를 `{버킷}.s3.amazonaws.com` 으로 만드는데 서명은 `region_name` 대로 하므로
    호스트에 리전이 빠져 둘이 어긋난다. 업로드는 SDK 가 리다이렉트를 따라가 성공하므로
    **적재는 되는데 링크만 죽는** 형태라 서버 로그만 봐서는 안 드러난다.

    boto3 는 여기서만 import 한다 — S3 경로를 안 타는 실행이 불필요하게 로딩하지 않게.
    weasyprint 를 pdf_compiler 함수 안에서 import 하는 것과 같은 이유다.
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "virtual"},
        ),
    )


def resolve_storage_policy(report_type: str) -> StoragePolicy:
    """report_type -> 적재 정책.

    등록되지 않은 종류는 짧은 쪽(cs-guideline, 7일)으로 보낸다 — 6개월 프리픽스에
    정체 불명의 객체가 쌓이는 것보다 낫다.
    """
    return _STORAGE_POLICY.get(report_type, _STORAGE_POLICY[REPORT_TYPE_GUIDELINE])


def build_object_path(report_type: str, period: str, company_id: str) -> tuple[str, str]:
    """인프라 규칙에 맞는 (디렉토리 경로, 파일명) 을 만든다.

        reports/{report_type}/{company_id}/{yyyy}/{mm}/
        {report_type}_{yyyyMM}_{uuid4}.pdf

    report_type 이 company_id 보다 위인 이유는 모듈 docstring 참고.

    period 는 보고 대상 기간(YYYY-MM)이지 업로드 시각이 아니다 — 폴더의 연월과 파일명의
    연월이 어긋나면 경로 규칙이 깨진다.
    """
    if not _PERIOD_PATTERN.match(period):
        raise ValueError(f"period 는 YYYY-MM 형식이어야 합니다: {period!r}")

    year, month = period.split("-")
    policy = resolve_storage_policy(report_type)
    s3_file_path = f"{S3_KEY_ROOT}{policy.prefix}/{company_id}/{year}/{month}/"
    new_file_name = f"{policy.prefix}_{year}{month}_{uuid.uuid4()}.pdf"
    return s3_file_path, new_file_name


def _build_original_file_name(prefix: str, period: str, source_id: str | None) -> str:
    """표시용(원본) 파일명. `new_file_name` 과 달리 사람이 읽는 이름이다.

    월 여러 건이 나오는 문서(CS 가이드라인은 알림마다 1건)는 `{yyyyMM}` 만으로는
    부족하다 — 5월 가이드라인이 전부 `cs-guideline_202605.pdf` 가 되어 목록에서 구분이
    안 된다. 저장은 `new_file_name` 의 uuid4 로 안전하지만 표시용 이름은 별개 문제다.
    """
    stem = f"{prefix}_{period.replace('-', '')}"
    return f"{stem}_{source_id}.pdf" if source_id else f"{stem}.pdf"


def _write_local_mirror(meta: PdfS3Meta, pdf_bytes: bytes) -> Path | None:
    """버킷 키와 동일한 경로로 로컬에 복제한다. 미설정이면 아무것도 하지 않는다.

    개발 편의 장치라 미러 실패가 생성 자체를 막으면 안 된다 — 오류는 로그만 남긴다.

    Windows 는 MAX_PATH(260자) 제한이 있다. 버킷명(48자) + S3 키(약 110자)를 얹으므로
    미러 뿌리를 깊은 경로로 두면 ENOENT 로 실패한다. `./data/s3_mirror` 처럼 짧게 둘 것.
    S3 자체는 키를 1024바이트까지 받으므로 운영에는 없는 제약이다.
    """
    if not S3_LOCAL_MIRROR_DIR:
        return None

    target = Path(S3_LOCAL_MIRROR_DIR) / meta.s3_bucket_name / meta.s3_full_key
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(pdf_bytes)
    except OSError as exc:
        logger.warning(f"[MIRROR] 로컬 복제 실패(생성은 계속): {target} — {exc}")
        return None

    logger.info(f"[MIRROR] {target}")
    return target


def _put_and_sign(*, bucket: str, key: str, pdf_bytes: bytes, ttl_hours: int) -> str:
    """객체를 올리고 Pre-signed URL 을 발급한다. 실패하면 S3UploadError.

    올린 뒤에 서명한다. 반대로 하면 업로드가 실패했는데 링크만 멀쩡히 나가서 셀러가
    눌렀을 때 404 를 본다.

    `ContentType` 을 명시한다. 없으면 S3 가 `binary/octet-stream` 으로 저장해 브라우저가
    PDF 뷰어 대신 다운로드 창을 띄운다 — 월간 화면이 PDF 뷰어로 띄우는 구조라 문제가 된다.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    client = _get_s3_client()
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=pdf_bytes,
            ContentType="application/pdf",
        )
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=ttl_hours * 3600,
        )
    except (BotoCoreError, ClientError) as exc:
        raise S3UploadError(f"S3 업로드·서명 실패: s3://{bucket}/{key} — {exc}") from exc


def ensure_s3_ready(company_id: str | None = None, *, context: str = "") -> str:
    """올릴 수 있는 구성인지 확인하고 회사 식별자를 확정한다. 못 올리면 예외.

    `upload_pdf_to_s3` 가 올리기 직전에 부르지만 생성 파이프라인이 시작하기 전에도
    부를 수 있게 따로 뺐다. 업로드는 `LLM 호출 -> PDF 컴파일 -> S3` 의 마지막 단계라,
    구성이 틀어져 있으면 알림 1건마다 LLM 값을 다 지불하고 FAILED 만 돌아온다. 여기서
    미리 걸러 비용을 0 으로 만든다.

    돈이 드는 일을 하기 전에 부를 수 있도록 PDF 바이트를 받지 않는다 — 용량 검사는
    바이트가 있어야 하므로 `upload_pdf_to_s3` 에 남겨 뒀다.

    Args:
        company_id: 생략하면 `S3_COMPANY_ID` 환경변수를 쓴다.
        context: 오류 메시지에 붙일 식별 문자열(`report_type/period` 또는 `alert_id=...`).

    Returns:
        확정된 `company_id`.

    Raises:
        S3NotConfiguredError: 셋 중 하나라도 준비되지 않았을 때.
    """
    where = f" ({context})" if context else ""

    if not S3_ENABLED:
        raise S3NotConfiguredError(
            f"S3 업로드가 꺼져 있습니다(S3_ENABLED=false). "
            f"업로드하지 않은 파일을 성공으로 보고하지 않습니다{where}"
        )

    resolved_company_id = company_id or S3_DEFAULT_COMPANY_ID
    if not resolved_company_id:
        # 모르는 채로 올리면 남의 폴더이거나 규칙 밖 경로가 된다. 추측하지 않는다.
        raise S3NotConfiguredError(
            f"company_id 를 알 수 없습니다(S3_COMPANY_ID 미설정). "
            f"경로가 회사 단위로 갈리므로 임의 경로에 올리지 않습니다{where}"
        )

    if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        # 정적 키가 없으면 7일짜리 서명을 만들 수 없다. 짧은 링크를 7일이라고
        # 안내하느니 실패시킨다 — 만료된 링크가 셀러 메일에 실리는 쪽이 더 나쁘다.
        raise S3NotConfiguredError(
            "Pre-signed URL 서명용 정적 액세스 키가 없습니다"
            "(AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY). IAM Role 임시 자격증명으로는 "
            f"7일 링크를 만들 수 없습니다{where}"
        )

    return resolved_company_id


async def upload_pdf_to_s3(
    pdf_bytes: bytes,
    report_type: str,
    period: str,
    company_id: str | None = None,
    company_name: str | None = None,
    source_id: str | None = None,
) -> PdfS3Meta:
    """PDF 를 S3 에 올리고 적재 메타데이터를 만든다.

    Args:
        period: 보고 대상 기간 `YYYY-MM`. 경로의 `{yyyy}/{mm}` 와 파일명의 `{yyyyMM}`
            을 **같은 값**으로 만든다.
        company_id: 고객사 PK/UUID. 생략하면 `S3_COMPANY_ID` 환경변수를 쓴다.
            경로(`reports/{report_type}/{company_id}/`)와 메타데이터에 같은 값이 들어간다.
        company_name: 표시용 고객사명. 생략하면 `S3_COMPANY_NAME` 환경변수를 쓴다.
            경로에는 쓰지 않는다 — 이름이 바뀌면 이전 산출물을 못 찾는다.
        source_id: 산출물을 가리키는 식별자(CS 는 `alert_id`). `original_file_name`
            뒤에 붙는다. 월 여러 건이 나오는 문서에는 반드시 넘겨야 한다 — 없으면 같은
            달 산출물이 전부 같은 표시용 이름이 된다. 월간은 월 1건이라 생략한다.

    Raises:
        PdfSizeExceededError: 용량이 MAX_PDF_SIZE_BYTES 를 초과할 때.
        S3NotConfiguredError: S3_ENABLED 가 꺼져 있거나, company_id 또는 정적 액세스 키를
            알 수 없을 때. 셋 다 올리기 전에 막는다.
        S3UploadError: 실제 업로드·서명이 실패했을 때.
    """
    file_size = len(pdf_bytes)
    resolved_company_id = ensure_s3_ready(company_id, context=f"{report_type}/{period}")

    if file_size > constants.MAX_PDF_SIZE_BYTES:
        raise PdfSizeExceededError(
            f"PDF 용량 초과: {file_size} bytes > {constants.MAX_PDF_SIZE_BYTES} bytes "
            f"({report_type}/{period})"
        )

    policy = resolve_storage_policy(report_type)
    s3_file_path, new_file_name = build_object_path(report_type, period, resolved_company_id)
    s3_full_key = f"{s3_file_path}{new_file_name}"
    now = datetime.now(UTC)

    # boto3 는 동기 라이브러리라 그냥 부르면 이벤트 루프를 잡고 있는다. REST 로 들어오면
    # 업로드가 끝날 때까지 같은 워커의 다른 요청이 전부 멈추고, 네트워크라 boto3 기본
    # 재시도까지 걸리면 대기가 길다. 스레드로 넘겨 루프를 돌려준다.
    presigned_url = await asyncio.to_thread(
        _put_and_sign,
        bucket=policy.bucket_name,
        key=s3_full_key,
        pdf_bytes=pdf_bytes,
        ttl_hours=policy.presigned_ttl_hours,
    )

    meta = PdfS3Meta(
        # 회사 구분을 메타데이터로 실어 메인이 S3 키를 파싱하지 않아도 되게 한다
        company_id=resolved_company_id,
        company_name=company_name or S3_DEFAULT_COMPANY_NAME,
        s3_bucket_name=policy.bucket_name,
        s3_file_path=s3_file_path,
        original_file_name=_build_original_file_name(policy.prefix, period, source_id),
        new_file_name=new_file_name,
        created_at=now,
        # 스키마가 s3_full_key == s3_file_path + new_file_name 을 강제한다
        s3_full_key=s3_full_key,
        file_size_bytes=file_size,
        # AI 노드가 업로드 시점에 최초 발급하고 메인 서버가 유효한 동안 재사용한다.
        # 만료됐을 때만 s3_full_key 로 재발급한다.
        presigned_url=presigned_url,
        presigned_expires_at=now + timedelta(hours=policy.presigned_ttl_hours),
        # S3 Lifecycle 이 실제로 지우는 시각. UI 가 "다운로드 기한"을 안내하고,
        # 백엔드가 만료 전 재발급/재생성을 판단하는 근거가 된다.
        object_expires_at=now + timedelta(hours=policy.retention_hours),
    )
    _write_local_mirror(meta, pdf_bytes)
    return meta
