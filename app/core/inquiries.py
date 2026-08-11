"""담당: 지인 — `evidence.inquiry_ids` → CS 원문(`LinkedCSInquiry`) 매핑.

**개선안(Agent3)과 CS 가이드라인(리포팅)이 같은 입력을 쓴다.** 그래서 컴포넌트 폴더가
아니라 core 에 두고 배치가 한 번 만들어 둘 다에게 넘긴다 — 양쪽이 각자 만들면 같은
매핑이 두 벌이 되고, 아래 C4 가 풀릴 때 고칠 곳이 두 곳이 된다.

✅ **C4 해소(§5-1 A안) — DB 조회가 붙었다(2026-08-10).** `item_id` 가 `cs.id`/`reviews.id`
   그대로라 조인이 1컬럼이다. 다만 **입력원이 두 개고 둘 다 남는다:**

   | 함수 | 원문 출처 | 쓰는 곳 |
   |---|---|---|
   | `build_linked_inquiries` | 호출부가 이미 든 documents | 탐지 배치 · 크로스체크 |
   | `fetch_linked_inquiries` | raw DB 직접 조회 | REST(`/recommendations/generate`) |

🔴 **배치를 DB 조회로 바꾸지 말 것.** 배치는 35일치 원문을 이미 메모리에 들고 있고
   (`load_inputs_from_db`), `evidence.inquiry_ids` 는 항상 그 안(현재 윈도우)이다.
   조회로 바꾸면 **같은 행을 두 번 읽는다.** 정책(순서·중복·빈 원문 처리)은 한 곳이므로
   갈릴 걱정은 없다 — `fetch_linked_inquiries` 가 `build_linked_inquiries` 를 그대로 부른다.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence

from app.core import raw_schema
from app.core.raw_db import connect_readonly
from app.core.schemas import DetectionAlert, LinkedCSInquiry

logger = logging.getLogger(__name__)

_ID_CHUNK = 500
"""IN 절 한 번에 넣을 ID 수. sqlite 의 바인딩 파라미터 상한(구버전 999)에 안 닿게 자른다.

`evidence.inquiry_ids` 는 알림 1건의 `root_cause.total` 규모라 보통 수십 건이지만,
상한이 코드 밖 사정(문의량)으로 정해지므로 넘길 수 있다고 보고 자른다.
"""


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
                # `.get` 이다 — documents 계약엔 필수 키지만 이 함수를 부르는 스크립트·
                # 테스트가 안 넣어도 죽지 않게 한다. 없으면 출처 미상(None).
                source=doc.get("source"),
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


def fetch_linked_inquiries(
    alert: DetectionAlert,
    *,
    db_path: str | None = None,
) -> list[LinkedCSInquiry]:
    """같은 매핑을 **raw DB 에서 직접** 읽는다 — documents 를 손에 안 든 호출부용.

    원문의 정본은 원본 DB 이고 AI 노드는 읽기 권한이 있다(§5-2). `item_id` 가
    `cs.id`/`reviews.id` 그대로라(§5-1 A안) 조인이 1컬럼이다.

    ⚠️ **`cs.created_at` 이 아니라 `cs.inquired_at` 이다.** `cs` 에는 문의 발생 시각과
    레코드 적재 시각이 **둘 다** 있고 `LinkedCSInquiry.created_at` 의 정의는 "CS 접수
    일시" 라 전자가 맞다. 이름이 같아서 그냥 매핑하면 조용히 틀린 값이 들어간다 —
    `voc_document` 뷰가 두 소스의 시각을 `occurred_at` 하나로 맞춰 두므로 그 뷰를 쓴다.

    ⚠️ **리뷰(`RVW-`)도 딸려 온다 — 그게 확정 정책이다(2026-08-11, 용준님과 합의).**
    `evidence.inquiry_ids` 는 리뷰 소스 알림이면 리뷰 ID 다. 국내 커머스는 셀러가 리뷰에
    답글을 달고 그것도 CS 업무라, 리뷰 전용 알림의 가이드라인을 버릴 이유가 없다.
    구분이 필요한 쪽을 위해 `LinkedCSInquiry.source` 로 출처를 실어 보낸다.

    🔴 **거르는 쪽으로 되돌리려면 `is_guideline_target()`(`app/reporting/cs_reply_service.py`)
    도 같이 고쳐야 한다.** 그 게이트는 `bool(evidence.inquiry_ids)` 만 보므로 여기서만
    필터를 걸면, 리뷰 전용 알림이 게이트를 통과한 뒤 `build_guideline_input` 의
    `ValueError` 로 죽는다 — **정상 동작이 배치 요약의 "진짜 실패"로 집계된다**(그 함수
    docstring 이 경계하는 바로 그 상황).

    Args:
        alert: 탐지 알림. `evidence.inquiry_ids` 가 대상 목록이다.
        db_path: raw DB 경로. 기본은 `settings.raw_db_path`.

    Returns:
        CS 원문 리스트. 순서·중복·빈 원문 처리는 `build_linked_inquiries` 와 같다.

    Raises:
        FileNotFoundError: raw DB 가 없을 때(`connect_readonly`).
    """
    ids = list(dict.fromkeys(alert.evidence.inquiry_ids))
    if not ids:
        return []

    rows: list = []
    conn = connect_readonly(db_path)
    try:
        for chunk in _chunks(ids, _ID_CHUNK):
            placeholders = ",".join("?" * len(chunk))
            rows += conn.execute(
                "SELECT item_id, content, occurred_at, source"
                f" FROM {raw_schema.VOC_DOCUMENT} WHERE item_id IN ({placeholders})",
                chunk,
            ).fetchall()
    finally:
        conn.close()

    # documents 계약(`app/detection/loader.py`)의 키로 맞춰서 넘긴다 — 순서·중복·빈 원문
    # 정책을 여기서 다시 쓰지 않으려는 것이다. 두 벌이 되면 REST 와 배치의 근거가 갈린다.
    documents = [
        {
            "id": row["item_id"],
            "text": row["content"],
            "created_at": row["occurred_at"],
            "source": row["source"],
        }
        for row in rows
    ]
    return build_linked_inquiries(alert, documents)


def _chunks(values: Sequence[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])
