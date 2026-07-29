from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.core.schemas import PdfS3Meta


async def upload_pdf_to_s3(
    pdf_bytes: bytes,
    report_type: str,
    product_group_id: str,
    identifier: str,
) -> PdfS3Meta:
    """PDF 바이너리를 S3 버킷에 적재하고 메타데이터를 반환한다."""
    now = datetime.now(UTC)
    file_size = len(pdf_bytes)
    unique_suffix = uuid.uuid4().hex[:8]
    new_file_name = f"{report_type}_{product_group_id}_{identifier}_{now.strftime('%Y%m%d')}_{unique_suffix}.pdf"
    s3_file_path = f"reports/{report_type}/{now.strftime('%Y/%m')}/"
    s3_full_key = f"{s3_file_path}{new_file_name}"
    s3_bucket_name = "sellon-report-bucket"

    presigned_url = f"https://{s3_bucket_name}.s3.amazonaws.com/{s3_full_key}"

    return PdfS3Meta(
        s3_bucket_name=s3_bucket_name,
        s3_file_path=s3_file_path,
        original_file_name=f"{report_type}_{identifier}.pdf",
        new_file_name=new_file_name,
        s3_full_key=s3_full_key,
        file_extension="pdf",
        file_size_bytes=file_size,
        presigned_url=presigned_url,
    )