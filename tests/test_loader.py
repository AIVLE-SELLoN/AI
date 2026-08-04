"""담당: 서영 (Agent2) — 탐지 입력 로더(원본 ⟕ 분류) 테스트.

핵심 불변식: **분모는 원본 문서 수다.** aspect 가 0개로 나온 문서도 분모에 든다.
`classified_item` 에서 분모를 세면 그 문서가 통째로 사라져 부정률이 부풀려진다.
"""

from datetime import datetime

from app.core.schemas import AspectSentiment, ClassifiedItem
from app.detection.aggregate import count_window
from app.detection.loader import build_rows, check_coverage, unreliable_slots


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
    docs = [
        _doc("A", day=1, source="cs"),
        _doc("B", day=1, source="cs"),
        _doc("C", day=2, source="cs"),
    ]
    classified = [
        _classified("A", [("색상", -1)], source="cs"),
        _classified("C", [("색상", -1)], source="cs"),
    ]

    gaps = check_coverage(docs, classified)

    assert len(gaps) == 1
    assert gaps[0]["documents"] == 2
    assert gaps[0]["classified"] == 1
    assert gaps[0]["day"] == datetime(2026, 7, 1).date().toordinal()


def test_full_coverage_reports_no_gap():
    docs = [_doc("A", day=1, source="cs"), _doc("B", day=1, source="cs")]
    classified = [
        _classified("A", [("색상", -1)], source="cs"),
        _classified("B", [("사이즈", 1)], source="cs"),
    ]
    assert check_coverage(docs, classified) == []


def test_coverage_skips_review_because_empty_aspects_are_normal():
    """리뷰는 검사 대상이 아니다 — 빈 배열이 **정상 출력**이라서. (지인 리뷰 2026-08-04)

    허용 aspect 가 색상·사이즈·소재 3개뿐이라 무관 리뷰는 [] 를 낸다(모듈 docstring
    의 RVW-4 "배송 빨랐고 포장도 깔끔합니다"). 이걸 미분류로 세면 **분류가 100%
    성공해도** 그 슬롯이 gap 으로 잡히고, unreliable_slots() 이 BH family 에서
    통째로 빼버린다 — 긍정 리뷰가 하나만 섞여도 그 슬롯의 리뷰 탐지가 죽는다.

    더 근본적으로 explode_to_rows() 가 aspect 마다 1행을 만들므로 빈 배열은
    classified_item 에 0행이다. DB 에서 "무관 리뷰"와 "분류 안 됨"이 같은 모양이라
    리뷰 커버리지는 이 방법으로 원리적으로 검증할 수 없다.
    """
    docs = [_doc(f"RVW-{i}", day=1) for i in range(1, 5)]  # _doc 기본이 review
    classified = [
        _classified("RVW-1", [("색상", -1)]),
        _classified("RVW-2", [("사이즈", 1)]),
        _classified("RVW-3", [("소재", 0)]),
        _classified("RVW-4", []),  # 무관 리뷰 — 정상 출력
    ]
    assert check_coverage(docs, classified) == []
    assert unreliable_slots(check_coverage(docs, classified)) == set()


def test_coverage_checks_cs_because_fallback_guarantees_one_aspect():
    """CS 는 검사가 성립한다 — _cs_empty_fallback 이 aspect >= 1 을 보장하므로,
    aspect 0개는 '정상 빈 배열'이 아니라 **분류 자체가 빠진 것**이다."""
    docs = [_doc("A", day=1, source="cs"), _doc("B", day=1, source="cs")]
    classified = [_classified("A", [("기타", 0)], source="cs")]  # B 는 분류 누락

    gaps = check_coverage(docs, classified)
    assert len(gaps) == 1
    assert gaps[0]["source"] == "cs"
    assert (gaps[0]["documents"], gaps[0]["classified"]) == (2, 1)


def test_coverage_mixed_sources_only_flags_cs():
    """CS·리뷰가 섞여 들어와도 리뷰 쪽은 gap 을 만들지 않는다."""
    docs = [_doc("C1", day=1, source="cs"), _doc("R1", day=1)]
    classified = [_classified("R1", [])]  # CS 는 누락, 리뷰는 정상 빈 배열

    gaps = check_coverage(docs, classified)
    assert [g["source"] for g in gaps] == ["cs"]
