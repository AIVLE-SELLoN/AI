"""담당: 서영 (Agent2) — [0] 집계 + [1] 과거 기준.

순수 함수만. 정규화된 행 리스트만 받는다 — raw CSV 정규화는 로더 몫이라 여기는 손계산
숫자로 오라클 테스트가 된다.

행: {"product", "channel", "source", "aspect", "is_negative", "day", "id", "text"}.
day 는 날짜의 ordinal 정수이며 loader.py 와 service.py 가 `date.toordinal()` 로 변환한다.

규약
    - 윈도우: 현재 = 최근 7일 / 과거 기준 = 직전 28일.
    - 분모: (상품, 채널, source) 총문의 — aspect 무관, 부정 아닌 것도 포함.
    - 분자: (상품, aspect, 채널, source) 부정 건수.
    - source 별 분리 집계 — 분모가 다르므로 합산하지 않는다.
"""

from collections import Counter, defaultdict

from app.core.constants import PAST_WINDOW_DAYS


def _negative_aspects(row: dict) -> tuple:
    """이 문의가 '부정'으로 잡힌 aspect 들. 단일 aspect 행과 `neg_aspects` 리스트를 다 받는다.

    한 문의가 색상·사이즈에 동시에 부정일 수 있는데, 행 2개로 쪼개면 분모(총문의)가 2로
    부풀어 부정률이 반토막 나고 탐지가 죽는다. 행은 문의 단위로 두고 분자만 aspect 수만큼 센다.
    """
    if "neg_aspects" in row:
        return tuple(row["neg_aspects"])
    if row["is_negative"] and row["aspect"]:
        return (row["aspect"],)
    return ()


def count_window(rows: list, day_start: int, day_end: int) -> tuple[Counter, Counter]:
    """[day_start, day_end] 구간(양끝 포함)의 총문의·부정을 센다. [0]·[1] 공용 코어.

    Returns:
        (totals, negs) — totals 는 (product, channel, source) 분모(aspect 무관),
        negs 는 (product, aspect, channel, source) 분자.
    """
    totals: Counter = Counter()
    negs: Counter = Counter()
    for r in rows:
        if not (day_start <= r["day"] <= day_end):
            continue
        # 분모는 문의 1건 = 1 (부정 아님·중립 포함).
        totals[(r["product"], r["channel"], r["source"])] += 1
        # 분자는 부정으로 잡힌 aspect 마다 1.
        for aspect in _negative_aspects(r):
            negs[(r["product"], aspect, r["channel"], r["source"])] += 1
    return totals, negs


def collect_texts(rows: list, day_start: int, day_end: int) -> dict:
    """구간 내 부정 문의 텍스트를 (상품,aspect,채널,source)별로 모은다. [6] 원인분류 입력."""
    texts: dict = defaultdict(list)
    for r in rows:
        if not (day_start <= r["day"] <= day_end):
            continue
        for aspect in _negative_aspects(r):
            key = (r["product"], aspect, r["channel"], r["source"])
            texts[key].append({"cs_id": r["id"], "raw_text": r["text"]})
    return dict(texts)


def build_baseline(
    rows: list,
    cur_start: int,
    *,
    aspects: list,
    past_days: int = PAST_WINDOW_DAYS,
    alert_days: set | None = None,
) -> tuple[Counter, Counter, Counter]:
    """[1] 과거 기준 — 현재 윈도우 직전 past_days 일의 총문의·부정 카운트.

    과거 윈도우 = [cur_start - past_days, cur_start - 1] (양끝 포함).

    기준선 오염 방지: 알림이 발행된 (상품, aspect, 채널)의 알림 구간 날짜는 과거 윈도우
    집계에서 뺀다. 지속되는 이상이 과거 윈도우에 섞이면 '새로운 평소'가 되어 알림이 스스로
    꺼지기 때문이다. 단일 배치 mock 에는 선행 알림이 없어 alert_days 기본값은 빈 집합이다.

    제외 단위가 aspect 를 포함하므로 과거 분모도 aspect 별로 따로 센다 — 색상 알림이 나갔던
    날을 뺄 때 그 날의 사이즈 문의까지 빠지면 사이즈의 과거 기준이 이유 없이 깎인다. 분자와
    분모를 같은 날짜 집합에서 세야 비율이 성립한다. 현재 윈도우는 제외가 없어 count_window
    의 aspect 무관 분모를 그대로 쓴다.

    Args:
        aspects: 판정 대상 aspect 택소노미. 슬롯마다 제외 날짜가 달라 필요하다.
        alert_days: 제외할 {(product, aspect, channel, day), …}. 기본 없음.

    Returns:
        (totals, negs, unfiltered_totals). 앞의 둘은 제외를 적용한 (product, aspect,
        channel, source) 분모·분자이고, unfiltered_totals 는 제외 전 (product, channel,
        source) 총문의로 "표본이 절반 이하로 줄면 설정값 폴백" 판정 재료다.
    """
    past_end = cur_start - 1
    past_start = past_end - past_days + 1
    excluded = alert_days or set()

    totals: Counter = Counter()
    negs: Counter = Counter()
    unfiltered_totals: Counter = Counter()

    for r in rows:
        if not (past_start <= r["day"] <= past_end):
            continue
        product, channel, source, day = r["product"], r["channel"], r["source"], r["day"]
        unfiltered_totals[(product, channel, source)] += 1

        negative = set(_negative_aspects(r))
        for aspect in aspects:
            if (product, aspect, channel, day) in excluded:
                continue  # 이 aspect 기준으로는 이 날짜가 통째로 빠진다 (분자·분모 모두)
            totals[(product, aspect, channel, source)] += 1
            if aspect in negative:
                negs[(product, aspect, channel, source)] += 1

    return totals, negs, unfiltered_totals


def build_combinations(
    rows: list,
    cur_start: int,
    cur_end: int,
    *,
    aspects: list,
    past_days: int = PAST_WINDOW_DAYS,
    alert_days: set | None = None,
    past_rate_fallback: dict | None = None,
) -> tuple[list, dict]:
    """[0]+[1] 진입점 — run_detection([2]) 입력 조합 + 부정 텍스트 맵.

    관측된 (상품,채널,source)마다 aspects 전체 슬롯을 방출한다. 상품별 BH family 는 '현재
    부정이 있는 조합'이 아니라 그 상품의 평가 가능한 전 슬롯(최대 36개)을 포함해야 한다.
    cur_neg=0 슬롯은 발화할 수 없지만 자기 상품 family 에 들어가 컷오프를 보수적으로 유지한다.
    canonical 배치의 1,464는 전체 상품의 검정 총량이지 family 크기가 아니다.
    scripts/validate_anomaly.py 의 검산 그리드도 이것과 같아야 한다 — 갈리면 검산기가 운영과
    다른 슬롯 구성으로 초록불을 낸다.

    Args:
        aspects: 판정 대상 aspect 택소노미. 하드코딩하지 않고 주입받는다 — 기본값을 붙이면
                 Aspect enum·service.ALL_ASPECTS 와 사본이 둘이 되어 조용히 드리프트한다.
        past_rate_fallback: {(channel, aspect): 과거 부정률} 설정값 테이블. 초기 구간 폴백용
                 이고, 값을 하드코딩하지 않는 이유는 _apply_baseline_fallback 참고.

    Returns:
        (combinations, texts). combinations 는 [(product, aspect, channel, source,
        (cur_neg, cur_total, past_neg, past_total)), …] 로 run_detection 입력이고, texts 는
        현재 윈도우 부정 문의 텍스트([6] 입력)다.
    """
    cur_totals, cur_negs = count_window(rows, cur_start, cur_end)
    past_totals, past_negs, unfiltered_totals = build_baseline(
        rows, cur_start, aspects=aspects, past_days=past_days, alert_days=alert_days
    )
    texts = collect_texts(rows, cur_start, cur_end)

    combinations = []
    for (product, channel, source), cur_total in cur_totals.items():
        for aspect in aspects:
            key = (product, aspect, channel, source)
            past_neg, past_total = _apply_baseline_fallback(
                past_neg=past_negs.get(key, 0),
                past_total=past_totals.get(key, 0),
                unfiltered_total=unfiltered_totals.get((product, channel, source), 0),
                fallback_rate=(past_rate_fallback or {}).get((channel, aspect)),
                assumed_total=round(cur_total * past_days / (cur_end - cur_start + 1)),
            )
            counts = (cur_negs.get(key, 0), cur_total, past_neg, past_total)
            combinations.append((product, aspect, channel, source, counts))
    return combinations, texts


def _apply_baseline_fallback(
    *,
    past_neg: int,
    past_total: int,
    unfiltered_total: int,
    fallback_rate: float | None,
    assumed_total: int,
) -> tuple[int, int]:
    """[1] 설정값 baseline 폴백.

    두 경로 모두 실측 부정 건수를 버리고 설정값 부정률로 2x2 표를 재구성한다.

      ① 초기 구간(past_total == 0) — 곱할 N 이 없으므로 현재 윈도우 볼륨을 윈도우 길이
         비로 늘린 assumed_total 을 N 으로 쓴다. 그 비율은 윈도우 정의에서 그대로 나온다.
      ② 표본 붕괴 — 제외로 과거 표본이 절반 이하로 줄면 살아남은 past_total 을 N 으로 쓴다.

    설정값이 주입되지 않으면 어느 경로도 못 타고 실측치를 그대로 돌려준다. past_total 이 0
    인 채로 나가면 statistics.build_batch 가 보류로 보낸다.

    설정값 테이블 값을 이 파일에 박지 않는다. 유일한 표가 시나리오 정의서인데 그 문서가
    스스로 "정답은 탐지 로직이 참조하지 않음. 채점 전용" 이라 적고 있어 읽으면 컨닝이 된다.

    Returns:
        (past_neg, past_total) — 폴백이 걸리면 둘 다 재구성된 값.
    """
    if fallback_rate is None:
        return past_neg, past_total

    # ① 초기 구간 — 과거 표본 자체가 없다.
    if past_total == 0:
        return round(fallback_rate * assumed_total), assumed_total

    # ② 알림 구간 제외로 표본이 절반 이하로 무너졌다.
    if past_total * 2 <= unfiltered_total:
        return round(fallback_rate * past_total), past_total

    return past_neg, past_total  # 표본이 충분히 살아있으면 실측치 그대로
