"""담당: 서영 (Agent2) — 탐지 입력 로더(원본 ⟕ 분류) 테스트.

핵심 불변식: **분모는 원본 문서 수다.** aspect 가 0개로 나온 문서도 분모에 든다.
`classified_item` 에서 분모를 세면 그 문서가 통째로 사라져 부정률이 부풀려진다.
"""

from datetime import datetime

from app.core.schemas import AspectSentiment, ClassifiedItem
from app.detection.aggregate import count_window
from app.detection.loader import build_rows, check_coverage


def _doc(doc_id, day=1, source="review", channel="COUPANG", product="P001"):
    return {
        "id": doc_id,
        "product": product,
        "channel": channel,
        "source": source,
        "created_at": datetime(2026, 7, day, 12, 0),
        "text": "…",
    }


def _classified(item_id, aspects, source="review", channel="COUPANG", product="P001"):
    return ClassifiedItem(
        item_id=item_id,
        source=source,
        channel=channel,
        product_group_id=product,
        raw_text="…",
        aspects=[AspectSentiment(aspect=a, sentiment=s) for a, s in aspects],
        created_at=datetime(2026, 7, 1, 12, 0),
    )


def test_empty_aspect_document_still_counts_in_denominator():
    """aspect 0개 리뷰도 분모 1건이다 — 이 모듈의 존재 이유.

    탐지분모산출방식 §1 의 리뷰 4건 예시 그대로.
      RVW-1 색상 부정 / RVW-2 긍정 2개 / RVW-3 중립 / RVW-4 aspect 없음
    진짜 색상 부정률 1/4 = 25%. classified_item 으로 세면 1/3 = 33% 로 부풀려진다.
    """
    docs = [_doc(f"RVW-{i}") for i in (1, 2, 3, 4)]
    classified = [
        _classified("RVW-1", [("색상", -1)]),
        _classified("RVW-2", [("색상", 1), ("사이즈", 1)]),
        _classified("RVW-3", [("소재", 0)]),
        # RVW-4 는 aspects 가 비어 classified_item 에 행이 없다 → 목록에도 없음
    ]

    rows = build_rows(docs, classified)
    day = rows[0]["day"]
    totals, negs = count_window(rows, day, day)

    assert len(rows) == 4  # 문서 1건 = 행 1개
    assert totals[("P001", "COUPANG", "review")] == 4  # 분모는 원본 기준
    assert negs[("P001", "색상", "COUPANG", "review")] == 1  # 분자는 분류 기준


def test_multi_aspect_document_is_one_row():
    """한 문서가 두 aspect 에 부정이어도 행은 1개 — 분모가 부풀면 안 된다."""
    docs = [_doc("RVW-1")]
    classified = [_classified("RVW-1", [("색상", -1), ("사이즈", -1)])]

    rows = build_rows(docs, classified)
    day = rows[0]["day"]
    totals, negs = count_window(rows, day, day)

    assert len(rows) == 1
    assert totals[("P001", "COUPANG", "review")] == 1
    assert negs[("P001", "색상", "COUPANG", "review")] == 1
    assert negs[("P001", "사이즈", "COUPANG", "review")] == 1


def test_classified_without_document_is_ignored():
    """원본에 없는 분류 결과는 버린다 — 분모의 기준은 원본 하나뿐이다."""
    rows = build_rows([_doc("RVW-1")], [_classified("RVW-99", [("색상", -1)])])

    assert len(rows) == 1
    assert rows[0]["id"] == "RVW-1"
    assert rows[0]["neg_aspects"] == []


def test_coverage_gap_is_reported_per_day():
    """커버리지 검증은 일자별 — 총합만 맞춰보면 날짜가 어긋난 걸 못 잡는다.

    1일 문서 2건 중 1건만 분류, 2일 문서 1건은 전부 분류.
    총합은 3건 중 2건이라 '어딘가 빠졌다'까지만 알 수 있지만,
    일자별로 보면 **1일이 미달**이라고 짚을 수 있다.
    """
    docs = [_doc("A", day=1), _doc("B", day=1), _doc("C", day=2)]
    classified = [
        _classified("A", [("색상", -1)]),
        _classified("C", [("색상", -1)]),
    ]

    gaps = check_coverage(docs, classified)

    assert len(gaps) == 1
    assert gaps[0]["documents"] == 2
    assert gaps[0]["classified"] == 1
    assert gaps[0]["day"] == datetime(2026, 7, 1).date().toordinal()


def test_full_coverage_reports_no_gap():
    docs = [_doc("A", day=1), _doc("B", day=1)]
    classified = [
        _classified("A", [("색상", -1)]),
        _classified("B", [("사이즈", 1)]),
    ]
    assert check_coverage(docs, classified) == []
