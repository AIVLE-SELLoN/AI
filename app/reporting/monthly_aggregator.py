"""월간 리포트 입력 집계 — raw DB → MonthlyReportInput.

배치 스케줄상 **가장 무거운 단계**다(순열검정 B=10,000). 그래서 생성과 분리해
전월 말일 자정부터 돌려 오전 8시까지 끝내고, 생성은 그 결과 파일만 읽어 10시까지 끝낸다.

⚠️ 분모는 **원본 테이블에서 센다**(탐지 분모 산출 방식, 2026-08-03 합의).
   `classified_item` 은 "aspect 언급 목록"이라 aspect 가 0개인 리뷰가 빠져 있어
   총 문서 수를 셀 수 없다. 여기서는 raw_event 를 분모로 쓰고 classified_item 을
   LEFT JOIN 해 분자만 가져온다.

⚠️ BH-FDR 은 **배치 전체(전 상품 × 전 채널쌍)** 를 한 family 로 묶어야 한다(§4-2 ②).
   상품별로 따로 보정하면 다중검정 방어가 깨진다. 그래서 이 모듈은 상품 하나가 아니라
   **상품 목록을 한 번에** 집계한다.

운영 DB(Postgres)로 옮길 때는 `_fetch_*` 계열 SQL 세 개만 바꾸면 된다.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timedelta

from app.core.schemas import MONTHLY_ASPECTS as SCHEMA_MONTHLY_ASPECTS
from app.core.schemas import (
    Aspect,
    ChannelDivergencePair,
    MonthlyAspectDistribution,
    MonthlyReportInput,
    MonthlySentimentDrift,
)
from app.reporting.metrics_calculator import (
    apply_bh_fdr,
    build_channel_divergence,
    build_channel_divergence_pair,
    calculate_sentiment_drift,
    calculate_sentiment_ratios,
    finalize_pair,
)

logger = logging.getLogger("MonthlyAggregator")

# JSD 카테고리 축 — **순서가 의미를 갖는다**(채널 간 분포를 같은 순서로 비교해야 한다).
# schemas.MONTHLY_ASPECTS 는 같은 3종의 frozenset(순서 없음)이라 이름을 달리 둔다.
# 아래 assert 로 두 정의가 갈라지지 않게 묶어 둔다.
JSD_ASPECT_ORDER: tuple[str, ...] = (Aspect.COLOR.value, Aspect.SIZE.value, Aspect.MATERIAL.value)
assert {Aspect(a) for a in JSD_ASPECT_ORDER} == set(SCHEMA_MONTHLY_ASPECTS), (
    "JSD_ASPECT_ORDER 와 schemas.MONTHLY_ASPECTS 가 어긋났습니다"
)

# 채널 분열은 채널쌍 전수를 본다. 조합 순서를 고정해 라벨이 실행마다 흔들리지 않게 한다.
CHANNEL_PAIRS: tuple[tuple[str, str], ...] = (
    ("COUPANG", "NAVER"),
    ("COUPANG", "ZIGZAG"),
    ("NAVER", "ZIGZAG"),
)


def month_bounds(report_month: str) -> tuple[date, date]:
    """'YYYY-MM' → (1일, 말일)."""
    import calendar

    year, month = int(report_month[:4]), int(report_month[5:7])
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def previous_month(report_month: str) -> str:
    start, _ = month_bounds(report_month)
    prev_end = start - timedelta(days=1)
    return f"{prev_end.year:04d}-{prev_end.month:02d}"


def _window(report_month: str) -> tuple[str, str]:
    """조회용 [시작, 끝) ISO 문자열. occurred_at 이 ISO 문자열이라 사전순 비교가 곧 시간순이다."""
    start, end = month_bounds(report_month)
    return start.isoformat(), (end + timedelta(days=1)).isoformat()


def list_product_groups(conn: sqlite3.Connection, report_month: str) -> list[str]:
    """해당 월에 원본이 하나라도 있는 상품 그룹. 리포트 생성 대상 목록이다."""
    start, end = _window(report_month)
    rows = conn.execute(
        "SELECT DISTINCT product_group_id FROM raw_event "
        "WHERE source IN ('cs','review') AND occurred_at >= ? AND occurred_at < ? "
        "  AND product_group_id IS NOT NULL "
        "ORDER BY product_group_id",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_total_voc(conn: sqlite3.Connection, product_group_id: str, report_month: str) -> int:
    """분모 — 원본 테이블에서 센다(classified_item 에서 세지 않는다)."""
    start, end = _window(report_month)
    return conn.execute(
        "SELECT COUNT(*) FROM raw_event "
        "WHERE source IN ('cs','review') AND product_group_id = ? "
        "  AND occurred_at >= ? AND occurred_at < ?",
        (product_group_id, start, end),
    ).fetchone()[0]


def _fetch_aspect_sentiments(
    conn: sqlite3.Connection, product_group_id: str, report_month: str
) -> dict[str, dict[int, int]]:
    """{aspect: {sentiment: 건수}}. 분자 집계라 classified_item 을 쓴다."""
    start, end = _window(report_month)
    rows = conn.execute(
        "SELECT aspect, sentiment, COUNT(*) FROM classified_item "
        "WHERE product_group_id = ? AND created_at >= ? AND created_at < ? "
        "GROUP BY aspect, sentiment",
        (product_group_id, start, end),
    ).fetchall()

    result: dict[str, dict[int, int]] = {a: {} for a in JSD_ASPECT_ORDER}
    for aspect, sentiment, count in rows:
        if aspect in result:
            result[aspect][int(sentiment)] = count
    return result


def _fetch_negative_aspect_counts_by_channel(
    conn: sqlite3.Connection, product_group_id: str, report_month: str
) -> dict[str, list[int]]:
    """{채널: [속성별 부정 문서 수]}. 채널 분열(JSD)의 입력 분포다.

    "두 채널의 여론이 얼마나 다른가"를 **부정 의견이 어느 속성에 쏠렸는지**로 본다.
    전체 문서로 재면 채널별 판매량 차이가 그대로 신호로 잡힌다.
    """
    start, end = _window(report_month)
    rows = conn.execute(
        "SELECT channel, aspect, COUNT(*) FROM classified_item "
        "WHERE product_group_id = ? AND sentiment = -1 "
        "  AND created_at >= ? AND created_at < ? "
        "GROUP BY channel, aspect",
        (product_group_id, start, end),
    ).fetchall()

    counts: dict[str, list[int]] = {}
    index = {a: i for i, a in enumerate(JSD_ASPECT_ORDER)}
    for channel, aspect, count in rows:
        if aspect not in index:
            continue
        counts.setdefault(channel, [0] * len(JSD_ASPECT_ORDER))[index[aspect]] = count
    return counts


def _build_distributions(
    sentiments: dict[str, dict[int, int]],
) -> list[MonthlyAspectDistribution]:
    distributions = []
    for aspect in JSD_ASPECT_ORDER:
        by_sentiment = sentiments.get(aspect, {})
        pos, neu, neg = by_sentiment.get(1, 0), by_sentiment.get(0, 0), by_sentiment.get(-1, 0)
        p_ratio, n_ratio, neg_ratio = calculate_sentiment_ratios(pos, neu, neg)
        total = pos + neu + neg
        # 관측 0건이면 비율도 0 으로 둔다(스키마가 이 경우를 허용한다).
        # 중립 100% 로 채우면 없는 관측을 있는 것처럼 보고하게 된다.
        distributions.append(
            MonthlyAspectDistribution(
                aspect=aspect,
                total_count=total,
                positive_ratio=p_ratio,
                neutral_ratio=n_ratio,
                negative_ratio=neg_ratio,
            )
        )
    return distributions


def _build_drifts(
    current: dict[str, dict[int, int]], previous: dict[str, dict[int, int]]
) -> list[MonthlySentimentDrift]:
    def _neg_ratio(source: dict[str, dict[int, int]], aspect: str) -> float:
        by_sentiment = source.get(aspect, {})
        total = sum(by_sentiment.values())
        return (by_sentiment.get(-1, 0) / total) if total else 0.0

    drifts = []
    for aspect in JSD_ASPECT_ORDER:
        drift_rate, status = calculate_sentiment_drift(
            _neg_ratio(current, aspect), _neg_ratio(previous, aspect)
        )
        drifts.append(
            MonthlySentimentDrift(
                aspect=aspect,
                drift_rate=drift_rate,
                status=status,
                # 전월 분모가 이번 달과 다르면(집계 규칙 변경 등) 여기서 True 로 올린다.
                # 지금은 같은 쿼리로 뽑으므로 항상 False.
                baseline_recalculated=False,
            )
        )
    return drifts


def aggregate_monthly_inputs(
    conn: sqlite3.Connection,
    report_month: str,
    *,
    product_group_ids: list[str] | None = None,
    n_permutations: int | None = None,
    seed: int = 42,
) -> list[MonthlyReportInput]:
    """상품 목록을 한 번에 집계해 MonthlyReportInput 리스트를 만든다.

    BH-FDR 을 배치 전체에 적용해야 하므로 상품별 호출로 쪼갤 수 없다(§4-2 ②).
    `n_permutations` 를 낮추면 빨라지지만 p값 해상도가 떨어진다 — 리허설 전용이다.
    """
    products = product_group_ids or list_product_groups(conn, report_month)
    prev_month = previous_month(report_month)
    start, end = month_bounds(report_month)

    logger.info(f"[AGGREGATE] {report_month} 대상 상품 {len(products)}개 집계 시작")

    # 1차: 상품별 분포·드리프트 + 채널쌍 p값 수집 (BH-FDR 은 아직 적용 안 함)
    staged: list[dict] = []
    pending_p_values: list[float] = []
    pending_index: list[tuple[int, int]] = []  # (staged 인덱스, pair 인덱스)

    permutation_kwargs = {"seed": seed}
    if n_permutations is not None:
        permutation_kwargs["n_permutations"] = n_permutations

    for product_id in products:
        current = _fetch_aspect_sentiments(conn, product_id, report_month)
        previous = _fetch_aspect_sentiments(conn, product_id, prev_month)
        channel_counts = _fetch_negative_aspect_counts_by_channel(conn, product_id, report_month)

        pairs: list[ChannelDivergencePair] = []
        for left, right in CHANNEL_PAIRS:
            pair, p_value = build_channel_divergence_pair(
                f"{left}_VS_{right}",
                channel_counts.get(left, [0] * len(JSD_ASPECT_ORDER)),
                channel_counts.get(right, [0] * len(JSD_ASPECT_ORDER)),
                **permutation_kwargs,
            )
            if p_value is not None:
                pending_index.append((len(staged), len(pairs)))
                pending_p_values.append(p_value)
            pairs.append(pair)

        staged.append(
            {
                "product_group_id": product_id,
                "total_voc_count": _fetch_total_voc(conn, product_id, report_month),
                "distributions": _build_distributions(current),
                "drifts": _build_drifts(current, previous),
                "pairs": pairs,
            }
        )

    # 2차: 배치 전체 p값에 BH-FDR → severity 확정 (반드시 이 순서)
    significant = apply_bh_fdr(pending_p_values)
    for (staged_index, pair_index), is_significant in zip(pending_index, significant):
        pairs = staged[staged_index]["pairs"]
        pairs[pair_index] = finalize_pair(pairs[pair_index], bh_significant=is_significant)

    logger.info(
        f"[AGGREGATE] 채널쌍 검정 {len(pending_p_values)}건 중 유의 {sum(significant)}건 "
        f"(BH-FDR, family=배치 전체)"
    )

    calculated_at = datetime.now().astimezone()
    inputs: list[MonthlyReportInput] = []
    for row in staged:
        inputs.append(
            MonthlyReportInput(
                report_month=report_month,
                start_date=start,
                end_date=end,
                product_group_id=row["product_group_id"],
                # 상품명은 커머스 DB 소관이라 여기서는 코드로 대체한다.
                # 연동되면 이 한 줄만 조인으로 바꾸면 된다.
                product_name=row["product_group_id"],
                total_voc_count=row["total_voc_count"],
                aspect_distributions=row["distributions"],
                sentiment_drifts=row["drifts"],
                channel_divergence=build_channel_divergence(row["pairs"], calculated_at),
                recommended_id=f"REC-{report_month.replace('-', '')}-{row['product_group_id']}",
            )
        )
    return inputs
