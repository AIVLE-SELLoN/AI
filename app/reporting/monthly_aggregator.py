"""월간 리포트 입력 집계 — raw DB → MonthlyReportInput.

배치 스케줄상 **가장 무거운 단계**다(순열검정 B=10,000). 그래서 생성과 분리해
전월 말일 자정부터 돌려 오전 8시까지 끝내고, 생성은 그 결과 파일만 읽어 10시까지 끝낸다.

⚠️ 분모는 **원본 테이블에서 센다**(탐지 분모 산출 방식, 2026-08-03 합의).
   `classified_item` 은 "aspect 언급 목록"이라 aspect 가 0개인 리뷰가 빠져 있어
   총 문서 수를 셀 수 없다. 여기서는 원문(cs ∪ reviews)을 분모로 쓰고 classified_item 을
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

from app.core import raw_schema
from app.core.constants import KST
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

# 원문 통합 뷰 — 분모와 "언제·어디서 발생했는가"의 정본.
#
# 분류 결과(§2-6 classified_item / classified_item_aspect)에는 발생 시각·채널·상품그룹이
# 없다. 원문 사본을 만들지 않기로 했기 때문이다(아키텍처 확정 §6). 그래서 집계는 항상
# 원문을 기준으로 잡고 분류 결과를 조인한다.
#
# 「Raw DB 스키마 확정 (8/7)」에서 원문은 `cs`(§2-4)·`reviews`(§2-5) 두 테이블로 갈렸고
# 시각 컬럼명도 다르다(cs.inquired_at / reviews.created_at). 두 테이블을 UNION 해
# `occurred_at` 하나로 맞춘 뷰가 `voc_document` 이고, 정의는 `app/core/raw_schema.py` 다.
# 이름을 여기 다시 적지 않고 상수를 가져다 쓴다 — 뷰 이름이 바뀌면 한 곳만 고치면 된다.
#
# ⚠️ 분모와 분자를 **한 쿼리로 묶지 않는다**(§4 예시 쿼리와 같은 이유). 분모는 이 뷰만
#    보고 세고(`_fetch_total_voc`), 분자는 분류 결과를 INNER JOIN 해서 따로 센다. 한
#    쿼리에서 GROUP BY 에 aspect 를 넣은 채로 분모까지 세면, 분류 안 된 문의가 빠져
#    분모가 조용히 줄어든다(§2-4 가 금지한 상황).
SOURCE_TABLE = raw_schema.VOC_DOCUMENT

# 상품그룹 대표 상품명을 고르는 규칙 (2026-08-05 확정).
#   1순위: 채널별 표기명 중 **최빈값**
#   2순위: 동점이면 아래 채널 우선순위로 깬다
# 채널마다 표기가 달라 어느 하나를 골라야 하는데, 실행마다 이름이 흔들리면 같은 상품의
# 리포트가 달의 표지마다 다른 이름으로 나간다. 그래서 결정적으로 고정한다.
PRODUCT_NAME_CHANNEL_PRIORITY: tuple[str, ...] = ("ZIGZAG", "NAVER", "COUPANG")


def resolve_product_name(candidates: dict[str, str]) -> str | None:
    """{채널: 표기 상품명} → 대표 상품명. 후보가 없으면 None.

    최빈값 우선, 동점이면 PRODUCT_NAME_CHANNEL_PRIORITY 순으로 깬다. 우선순위에 없는
    채널만 남으면 채널명 오름차순으로 깬다(입력 순서에 흔들리지 않게).
    """
    named = {ch: name.strip() for ch, name in candidates.items() if name and name.strip()}
    if not named:
        return None

    freq: dict[str, int] = {}
    for name in named.values():
        freq[name] = freq.get(name, 0) + 1
    top = max(freq.values())
    tied = {name for name, n in freq.items() if n == top}
    if len(tied) == 1:
        return next(iter(tied))

    def rank(channel: str) -> tuple[int, str]:
        try:
            return (PRODUCT_NAME_CHANNEL_PRIORITY.index(channel), "")
        except ValueError:
            return (len(PRODUCT_NAME_CHANNEL_PRIORITY), channel)

    for channel in sorted(named, key=rank):
        if named[channel] in tied:
            return named[channel]
    return None


def has_product_name_tables(conn: sqlite3.Connection) -> bool:
    """products·mapped_data 가 둘 다 있는가.

    ⚠️ 목 파이프라인도 이제 두 테이블을 만든다(2026-08-11) — `mock_producer` 가
       `seed_product_catalog()` 로 채운다. 다만 **비어 있을 수 있다**: 매핑 대본
       (`input_mapped_data.csv`)은 백엔드가 주는 것이라 받기 전에는 `mapped_data` 가
       0행이고, 그때 이 함수는 True 를 돌려주지만 아래 조회가 빈 결과를 낸다.
       그래서 "테이블 유무" 와 "매핑 유무" 는 다르다 — 호출부는 이름을 못 찾으면
       product_group_id 를 그대로 쓴다.

    상품 목록을 도는 루프 **바깥**에서 한 번만 부르라고 따로 뺐다. 상품이 126개면 안에서
    부를 때 같은 카탈로그 조회를 126번 한다 — 답이 실행 중에 바뀌지 않는 값이다.
    """
    return (
        conn.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='table' AND name IN ('products','mapped_data')"
        ).fetchone()[0]
        >= 2
    )


def _fetch_product_names(conn: sqlite3.Connection, product_group_id: str) -> str | None:
    """products ⋈ mapped_data 에서 채널별 표기명을 모아 대표명을 고른다.

    두 테이블이 없으면 None 을 돌려 호출부가 product_group_id 를 그대로 쓰게 한다 —
    이름을 지어내지 않는다.
    """
    if not has_product_name_tables(conn):
        return None

    rows = conn.execute(
        "SELECT p.channel_id, p.channel_product_name FROM mapped_data m "
        "JOIN products p ON p.variant_row_id = m.variant_row_id "
        "WHERE m.product_group_id = ? AND p.channel_product_name IS NOT NULL",
        (product_group_id,),
    ).fetchall()

    # 같은 채널에 여러 variant 가 있으면 채널 안에서도 최빈값을 먼저 고른다.
    by_channel: dict[str, dict[str, int]] = {}
    for channel, name in rows:
        if not name:
            continue
        counter = by_channel.setdefault(str(channel), {})
        counter[name] = counter.get(name, 0) + 1
    candidates = {
        channel: max(counter, key=lambda n: (counter[n], n)) for channel, counter in by_channel.items()
    }
    return resolve_product_name(candidates)


# 채널 분열은 채널쌍 전수를 본다. 조합 순서를 고정해 라벨이 실행마다 흔들리지 않게 한다.
CHANNEL_PAIRS: tuple[tuple[str, str], ...] = (
    ("COUPANG", "NAVER"),
    ("COUPANG", "ZIGZAG"),
    ("NAVER", "ZIGZAG"),
)

# 위 쌍이 짝지을 수 있는 채널 전부. 조회 결과가 여기 없는 값이면 그 채널의 분포는 어느
# 쌍에도 안 들어간다 — 아래 경고의 판정 기준이다.
KNOWN_CHANNELS: frozenset[str] = frozenset(c for pair in CHANNEL_PAIRS for c in pair)


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
        f"SELECT DISTINCT product_group_id FROM {SOURCE_TABLE} "
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
        f"SELECT COUNT(*) FROM {SOURCE_TABLE} "
        "WHERE source IN ('cs','review') AND product_group_id = ? "
        "  AND occurred_at >= ? AND occurred_at < ?",
        (product_group_id, start, end),
    ).fetchone()[0]


def _fetch_aspect_sentiments(
    conn: sqlite3.Connection, product_group_id: str, report_month: str
) -> dict[str, dict[int, int]]:
    """{aspect: {sentiment: 건수}}. 분자 집계다.

    ⚠️ 기준 시각·상품그룹은 **원문 테이블**에서 온다. 분류 결과 테이블(§2-6)에는
       발생 시각도 상품그룹도 없다 — 원문 사본을 만들지 않기로 했기 때문이다(§6).
       분류 시각(classified_at)으로 기간을 자르면 말일 문의를 1일 새벽에 분류했을 때
       그 건이 다음 달로 넘어간다.
    """
    start, end = _window(report_month)
    rows = conn.execute(
        f"SELECT a.aspect, a.sentiment, COUNT(*) FROM {SOURCE_TABLE} r "
        "JOIN classified_item_aspect a ON a.item_id = r.item_id "
        "WHERE r.product_group_id = ? AND r.occurred_at >= ? AND r.occurred_at < ? "
        "GROUP BY a.aspect, a.sentiment",
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

    ⚠️ 채널값을 **여기서 대문자로 맞춘다.** 예전에는 `classified_item.channel` 을 읽었고
       그 값은 `ClassifiedItem` 을 거치면서 `Channel` enum 이 표기를 보장해 줬다. 지금은
       원문 테이블(§2-4·§2-5)의 `channel_id` 가 그대로 나오는데, 그 표기는 메인 서버가
       채우는 값이라 우리가 보장할 수 없다.

       맞추지 않으면 **에러 없이 결과만 조용히 틀어진다**: 호출부가
       `channel_counts.get(left, [0,0,0])` 로 꺼내므로, 'coupang' 이 들어오면 'COUPANG'
       조회가 빗나가 그 채널이 **빈 분포로 취급**되고 그 쌍의 판정이 통째로 어긋난다.
       대소문자만 다른 경우는 UPPER 로 흡수하고, 아예 모르는 채널은 아래에서 경고한다.
    """
    start, end = _window(report_month)
    rows = conn.execute(
        f"SELECT UPPER(r.channel_id), a.aspect, COUNT(*) FROM {SOURCE_TABLE} r "
        "JOIN classified_item_aspect a ON a.item_id = r.item_id "
        "WHERE r.product_group_id = ? AND a.sentiment = -1 "
        "  AND r.occurred_at >= ? AND r.occurred_at < ? "
        "GROUP BY UPPER(r.channel_id), a.aspect",
        (product_group_id, start, end),
    ).fetchall()

    counts: dict[str, list[int]] = {}
    index = {a: i for i, a in enumerate(JSD_ASPECT_ORDER)}
    unknown: dict[str | None, int] = {}
    for channel, aspect, count in rows:
        if aspect not in index:
            continue
        if channel not in KNOWN_CHANNELS:
            # 대문자로 맞춰도 못 알아본 값 — 조용히 버리면 그 채널의 부정 의견이
            # 어느 쌍에도 안 들어간 채 리포트가 정상처럼 나간다.
            unknown[channel] = unknown.get(channel, 0) + count
            continue
        counts.setdefault(channel, [0] * len(JSD_ASPECT_ORDER))[index[aspect]] = count

    if unknown:
        detail = ", ".join(f"{c!r}({n}건)" for c, n in sorted(unknown.items(), key=lambda x: str(x[0])))
        logger.warning(
            f"[{product_group_id}/{report_month}] 채널 분열 집계에서 제외된 채널: {detail} "
            f"— CHANNEL_PAIRS({sorted(KNOWN_CHANNELS)}) 에 없는 값이라 어느 쌍에도 "
            "들어가지 않습니다. 원문의 channel_id 표기를 확인하세요."
        )
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

    # 🔴 **KST 로 찍는다 — 호스트 시간대를 보지 않는다.** 이 값은 합본 PDF 지면에 그대로
    #    인쇄되는데(`pdf_compiler` 의 "마지막 업데이트"), 렌더링이 `calculated_at[:16]` 으로
    #    **오프셋을 잘라내고 벽시계만** 보여준다. `datetime.now().astimezone()` 이면 UTC
    #    컨테이너에서 셀러가 보는 시각이 9시간 이르게 찍히고 tz 표기가 없어 구분할 방법이
    #    없다. 개발 머신이 KST 라 로컬 테스트로는 영원히 안 잡힌다. (2026-08-13)
    calculated_at = datetime.now(KST)
    # 카탈로그 존재 여부는 실행 중에 바뀌지 않는다 — 상품마다 다시 묻지 않는다.
    catalog_ready = has_product_name_tables(conn)
    inputs: list[MonthlyReportInput] = []
    for row in staged:
        inputs.append(
            MonthlyReportInput(
                report_month=report_month,
                start_date=start,
                end_date=end,
                product_group_id=row["product_group_id"],
                # 상품명은 커머스 DB 소관이라, 카탈로그가 없으면 코드로 대체한다.
                product_name=(
                    catalog_ready and _fetch_product_names(conn, row["product_group_id"])
                ) or row["product_group_id"],
                total_voc_count=row["total_voc_count"],
                aspect_distributions=row["distributions"],
                sentiment_drifts=row["drifts"],
                channel_divergence=build_channel_divergence(row["pairs"], calculated_at),
                recommended_id=f"REC-{report_month.replace('-', '')}-{row['product_group_id']}",
            )
        )
    return inputs
