"""월간 집계(`app/reporting/monthly_aggregator.py`) 테스트.

「Raw DB 스키마 확정 (8/7)」로 원문이 cs·reviews 로 갈리면서 생긴 두 가지를 고정한다:

1. **기간 절단은 원문 테이블의 발생 시각으로 한다.** 분류 결과 테이블에는 발생 시각이
   없다(원문 사본을 만들지 않기로 했다 — 아키텍처 §6). 분류 시각으로 자르면 말일 문의를
   1일 새벽에 분류했을 때 그 건이 다음 달 리포트로 넘어간다.
2. **채널 표기는 조회 시점에 맞춘다.** 원문의 channel_id 표기는 메인 서버 소관이라
   우리가 보장할 수 없는데, 어긋나면 에러 없이 그 채널쌍의 판정만 조용히 틀어진다.
3. **상품그룹 대표 상품명은 결정적으로 고른다.** 최빈값 우선, 동점이면
   지그재그 > 네이버 > 쿠팡. 실행마다 흔들리면 같은 상품이 달마다 다른 이름으로 나간다.
"""

from __future__ import annotations

import sqlite3

import pytest

from app.core import raw_schema
from app.reporting.monthly_aggregator import (
    _fetch_aspect_sentiments,
    _fetch_negative_aspect_counts_by_channel,
    _fetch_product_names,
    _fetch_total_voc,
    list_product_groups,
    resolve_product_name,
)

MONTH = "2026-07"


# ── 상품명 규칙 ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("candidates", "expected", "why"),
    [
        ({"COUPANG": "미디 원피스", "NAVER": "미디 원피스", "ZIGZAG": "미디원피스"},
         "미디 원피스", "최빈값이 이긴다"),
        ({"COUPANG": "A", "NAVER": "A", "ZIGZAG": "B"},
         "A", "최빈값이 채널 우선순위보다 앞선다"),
        ({"COUPANG": "A", "NAVER": "B", "ZIGZAG": "C"},
         "C", "3파 동점 → 지그재그"),
        ({"COUPANG": "A", "NAVER": "B"},
         "B", "2파 동점 → 네이버"),
        ({"COUPANG": "A"}, "A", "후보 1개"),
        ({}, None, "후보 없음"),
        ({"COUPANG": "   ", "NAVER": ""}, None, "공백만 있는 이름은 후보가 아니다"),
    ],
)
def test_resolve_product_name(candidates: dict, expected: str | None, why: str) -> None:
    assert resolve_product_name(candidates) == expected, why


def test_resolve_product_name_is_order_independent() -> None:
    """입력 순서가 바뀌어도 같은 이름이 나온다 — 달마다 이름이 흔들리면 안 된다."""
    forward = {"COUPANG": "A", "NAVER": "B", "ZIGZAG": "C"}
    backward = {"ZIGZAG": "C", "NAVER": "B", "COUPANG": "A"}
    assert resolve_product_name(forward) == resolve_product_name(backward)


# ── DB 조회 ──────────────────────────────────────────────────────────────


@pytest.fixture
def conn() -> sqlite3.Connection:
    """원문 3건(7월) + 1건(8월) + 분류 결과. 8월 건은 기간 절단 확인용이다.

    확정 스키마 §2-4 `cs` · §2-5 `reviews` 를 그대로 쓰고, 집계가 읽는 것은 두 테이블을
    합친 `voc_document` 뷰다. 시각 컬럼명이 갈려 있어(inquired_at / created_at) 뷰가
    엉뚱한 쪽을 고르면 기간 절단이 통째로 어긋나므로, 픽스처도 실제 DDL 을 쓴다.
    """
    db = sqlite3.connect(":memory:")
    raw_schema.create_source_tables(db)
    raw_schema.create_classified_tables(db)

    rows = [
        ("INQ-1", "cs", "COUPANG", "2026-07-05T10:00:00+09:00", "색상"),
        ("INQ-2", "cs", "NAVER", "2026-07-20T10:00:00+09:00", "사이즈"),
        # 말일 마지막 순간 — 7월에 포함돼야 한다
        ("RVW-1", "review", "ZIGZAG", "2026-07-31T23:59:59+09:00", "소재"),
        # 8월 1일 — 7월 집계에서 빠져야 한다
        ("INQ-3", "cs", "COUPANG", "2026-08-01T00:00:01+09:00", "색상"),
    ]
    for item_id, source, channel, occurred_at, aspect in rows:
        if source == "cs":
            db.execute(
                "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id, "
                "content, inquired_at, created_at) VALUES (?,?,?,?,?,?,?)",
                (item_id, "C1", "P001", channel, "원문", occurred_at, occurred_at),
            )
        else:
            db.execute(
                "INSERT INTO reviews (id, channel_product_id, product_group_id, channel_id, "
                "content, rating, created_at) VALUES (?,?,?,?,?,?,?)",
                (item_id, "C1", "P001", channel, "원문", 2, occurred_at),
            )
        # ⚠️ 분류 시각은 일부러 **8월**로 둔다. 기간 절단이 발생 시각 기준이 아니면
        #    7월 건이 통째로 빠져 아래 단언이 깨진다.
        db.execute(
            "INSERT INTO classified_item (item_id, source, classified_at, prompt_version)"
            " VALUES (?,?,?,?)",
            (item_id, source, "2026-08-01T03:00:00+09:00", "v1"),
        )
        db.execute(
            "INSERT INTO classified_item_aspect (item_id, aspect, sentiment, mixed_signal) "
            "VALUES (?,?,?,?)",
            (item_id, aspect, -1, None),
        )
    db.commit()
    return db


def test_window_uses_occurred_at_not_classified_at(conn: sqlite3.Connection) -> None:
    """기간은 원문 발생 시각으로 자른다 — 분류 시각(8월)으로 자르면 7월이 비어버린다."""
    assert _fetch_total_voc(conn, "P001", MONTH) == 3

    sentiments = _fetch_aspect_sentiments(conn, "P001", MONTH)
    assert sentiments["색상"] == {-1: 1}  # 8월 건이 섞이면 2가 된다
    assert sentiments["사이즈"] == {-1: 1}
    assert sentiments["소재"] == {-1: 1}


def test_month_boundary_is_inclusive_to_last_moment(conn: sqlite3.Connection) -> None:
    """말일 23:59:59 는 그 달에 포함되고, 다음 달 00:00:01 은 빠진다."""
    assert list_product_groups(conn, MONTH) == ["P001"]
    assert _fetch_total_voc(conn, "P001", "2026-08") == 1


def test_channel_counts_come_from_source_table(conn: sqlite3.Connection) -> None:
    """채널은 원문 테이블에서 온다 — 분류 결과에는 채널 컬럼이 없다."""
    counts = _fetch_negative_aspect_counts_by_channel(conn, "P001", MONTH)

    # 순서는 JSD_ASPECT_ORDER(색상, 사이즈, 소재)
    assert counts["COUPANG"] == [1, 0, 0]
    assert counts["NAVER"] == [0, 1, 0]
    assert counts["ZIGZAG"] == [0, 0, 1]


# ── 채널 표기 정규화 ──────────────────────────────────────────────────────


def _insert(db: sqlite3.Connection, item_id: str, channel: str, aspect: str) -> None:
    occurred = "2026-07-10T10:00:00+09:00"
    db.execute(
        "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id, "
        "content, inquired_at, created_at) VALUES (?,?,?,?,?,?,?)",
        (item_id, "C1", "P002", channel, "원문", occurred, occurred),
    )
    db.execute(
        "INSERT INTO classified_item (item_id, source, classified_at, prompt_version)"
        " VALUES (?,?,?,?)",
        (item_id, "cs", occurred, "v1"),
    )
    db.execute(
        "INSERT INTO classified_item_aspect (item_id, aspect, sentiment, mixed_signal) "
        "VALUES (?,?,?,?)",
        (item_id, aspect, -1, None),
    )


def test_channel_case_is_normalized(conn: sqlite3.Connection) -> None:
    """원문의 채널 표기가 소문자로 와도 같은 채널로 묶인다.

    ⚠️ 예전에는 `classified_item.channel` 을 읽었고 그 값은 `ClassifiedItem` 을 거쳐
       `Channel` enum 이 표기를 보장했다. 지금은 원문 테이블 값이 그대로 나오는데 그
       표기는 메인 서버 소관이다. 맞추지 않으면 호출부의
       `channel_counts.get("COUPANG", [0,0,0])` 가 빗나가 그 채널이 **빈 분포**로
       취급되고, 에러 없이 그 쌍의 판정만 조용히 틀어진다.
    """
    _insert(conn, "INQ-L1", "coupang", "색상")
    _insert(conn, "INQ-U1", "COUPANG", "사이즈")
    conn.commit()

    counts = _fetch_negative_aspect_counts_by_channel(conn, "P002", MONTH)

    assert set(counts) == {"COUPANG"}, "대소문자만 다른 값이 따로 잡히면 안 된다"
    assert counts["COUPANG"] == [1, 1, 0]


def test_unknown_channel_is_excluded_and_logged(conn: sqlite3.Connection, caplog) -> None:
    """CHANNEL_PAIRS 에 없는 채널은 제외하되 **조용히 버리지는 않는다**.

    대소문자로 흡수되지 않는 값(연동 채널 추가 등)은 어느 쌍에도 못 들어간다. 경고가
    없으면 그 채널의 부정 의견이 통째로 빠진 리포트가 정상처럼 나간다.
    """
    _insert(conn, "INQ-K1", "COUPANG", "색상")
    _insert(conn, "INQ-X1", "11ST", "소재")
    conn.commit()

    with caplog.at_level("WARNING"):
        counts = _fetch_negative_aspect_counts_by_channel(conn, "P002", MONTH)

    assert set(counts) == {"COUPANG"}
    assert "11ST" in caplog.text


def test_product_name_falls_back_when_catalog_is_empty(conn: sqlite3.Connection) -> None:
    """카탈로그가 비어 있으면 None — 이름을 지어내지 않는다. 호출부가 코드를 그대로 쓴다.

    ⚠️ 예전에는 "테이블이 아예 없을 때"를 재는 테스트였다. 2026-08-11 에 확정 스키마
       §2-2·§2-3 이 `raw_schema` 로 들어와 두 테이블이 **항상 생기므로**, 이제 남은 경우는
       "테이블은 있는데 그 상품이 아직 매핑되지 않음" 이다. 백엔드가 매핑을 적재하기 전
       구간이 정확히 이 상태다.
    """
    assert _fetch_product_names(conn, "P001") is None


def test_product_name_uses_mode_across_channels(conn: sqlite3.Connection) -> None:
    """products ⋈ mapped_data 에서 채널별 표기명을 모아 최빈값을 고른다."""
    # 쿠팡 2개 variant·네이버 1개가 같은 이름, 지그재그만 다른 표기
    for variant, channel, name in [
        ("v1", "COUPANG", "미디 원피스"),
        ("v2", "COUPANG", "미디 원피스"),
        ("v3", "NAVER", "미디 원피스"),
        ("v4", "ZIGZAG", "미디원피스"),
    ]:
        # 컬럼을 명시한다 — 실제 products 는 8컬럼이라 위치 INSERT 는 스키마가 늘 때 깨진다.
        conn.execute(
            "INSERT INTO products (variant_row_id, channel_id, channel_product_id, "
            "channel_product_name) VALUES (?,?,?,?)",
            (variant, channel, f"CP-{variant}", name),
        )
        conn.execute(
            "INSERT INTO mapped_data (variant_row_id, product_group_id) VALUES (?,?)",
            (variant, "P001"),
        )
    conn.commit()

    assert _fetch_product_names(conn, "P001") == "미디 원피스"
