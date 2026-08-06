"""담당: 지인 — `evidence.inquiry_ids` → CS 원문(`LinkedCSInquiry`) 매핑.

**개선안(Agent3)과 CS 가이드라인(리포팅)이 같은 입력을 쓴다.** 그래서 컴포넌트 폴더가
아니라 core 에 두고 배치가 한 번 만들어 둘 다에게 넘긴다 — 양쪽이 각자 만들면 같은
매핑이 두 벌이 되고, 아래 C4 가 풀릴 때 고칠 곳이 두 곳이 된다.

🔴 **C4 가 풀리면 바뀌는 곳은 이 파일 하나다.** 지금은 배치가 이미 손에 든 documents
   에서 뽑지만, 원문의 정본은 원본 DB(`cs`·`reviews`)다. `ClassifiedItem.item_id` 와
   두 테이블 PK 의 연결이 확인되면 여기에 DB 조회를 붙인다. 호출부 시그니처는 그대로다.
"""

from __future__ import annotations

import logging

from app.core.schemas import DetectionAlert, LinkedCSInquiry

logger = logging.getLogger(__name__)


def build_linked_inquiries(
    alert: DetectionAlert,
    documents: list[dict],
) -> list[LinkedCSInquiry]:
    """알림의 `evidence.inquiry_ids` 에 해당하는 CS 원문을 documents 에서 뽑는다.

    순서는 `evidence.inquiry_ids` 를 따르고 중복 ID 는 한 번만 담는다 —
    `CSGuidelineInput` 이 `linked_inquiries` 의 `item_id` 중복을 거부한다.

    **못 찾은 ID 와 원문이 빈 ID 는 버리고 경고만 남긴다.** 원문 없는 항목을 채워
    넣으면 "문의 원문"이라고 주장하는 빈 문자열이 생겨서, 인용·가이드라인이 근거
    없이 만들어진다. 전부 빠져 빈 리스트가 되면 호출부에서 걸린다
    (`CSGuidelineInput.linked_inquiries` 는 `min_length=1`).

    Args:
        alert: 탐지 알림. `evidence.inquiry_ids` 가 대상 목록이다.
        documents: 배치가 읽은 원본 문서. 키는 `id` · `text` · `created_at`
            (`app/detection/loader.py` 의 documents 계약과 같다).

    Returns:
        CS 원문 리스트. 대상이 없으면 빈 리스트.
    """
    by_id = {doc["id"]: doc for doc in documents}

    inquiries: list[LinkedCSInquiry] = []
    seen: set[str] = set()
    missing = 0
    blank = 0

    for inquiry_id in alert.evidence.inquiry_ids:
        if inquiry_id in seen:
            continue
        seen.add(inquiry_id)

        doc = by_id.get(inquiry_id)
        if doc is None:
            missing += 1
            continue

        raw_text = (doc.get("text") or "").strip()
        if not raw_text:
            blank += 1
            continue

        inquiries.append(
            LinkedCSInquiry(
                item_id=inquiry_id,
                raw_text=raw_text,
                created_at=doc["created_at"],
            )
        )

    if missing or blank:
        logger.warning(
            "alert=%s CS 원문 매핑 누락 — 문서 못 찾음 %d건 / 원문 비어 있음 %d건"
            " (인용·가이드라인 근거가 그만큼 줄어듭니다)",
            alert.alert_id,
            missing,
            blank,
        )
    return inquiries
