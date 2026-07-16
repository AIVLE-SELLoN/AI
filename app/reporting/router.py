"""담당: 용준 — 리포팅 (차별화 기능).

완료 기준: 월간 리포트 + CS 답변 초안 + 메일 발송.

주의: reporting 은 파이프라인 밖 독립 기능입니다 (문서 2장).
      classification/detection/recommendation 을 import 하지 말고 core/ 만 쓰세요.
"""

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/api/v1", tags=["reporting"])


@router.get("/reports/ping")
async def ping() -> dict[str, str]:
    """0주차 확인용 — 앱 1개가 4명 코드로 뜨는지 보는 hello world."""
    return {"module": "reporting", "owner": "용준", "status": "ok"}


@router.post("/reports")
async def create_report():
    """월간 리포트 생성.

    TODO(용준): schemas.py 확정 후 구현 → report_service.py 호출.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="미구현 — schemas.py 확정 후 작업 예정",
    )


@router.post("/replies")
async def create_reply():
    """CS 답변 초안 생성.

    TODO(용준): schemas.py 확정 후 구현 → reply_service.py 호출.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="미구현 — schemas.py 확정 후 작업 예정",
    )
