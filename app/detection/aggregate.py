"""담당: 서영 (Agent2) — [0] 집계 + [1] 과거 기준 (이상탐지 로직 V3 §[0]·§[1]).

순수 함수만. LLM·DB·CSV 로딩을 import 하지 않는다. **정규화된 행** 리스트를 받아
윈도우별 카운트를 낸다. raw CSV → 정규화(매핑 조인·라벨 부착·날짜→day)는 이 모듈
밖(로더)의 몫이다. 그래야 [2]~[6]처럼 손계산 숫자로 오라클 테스트가 된다.

정규화된 행 스키마:
    {"product", "channel", "source", "aspect", "is_negative": bool, "day": int,
     "id": str, "text": str}
      - aspect:      분류된 aspect (색상/사이즈/…). 부정 아님/중립이어도 분모엔 든다.
      - is_negative: 부정 감성(sentiment == -1) 여부.
      - day:         윈도우 day 번호(정수). 날짜→day 변환은 로더가 한다.

규약
    - 윈도우 (문서 §137): 현재 = 최근 7일 / 과거 기준 = 직전 28일.
    - 분모 (문서 §129): (상품, 채널, source) 총문의 — aspect 무관, 부정 아닌 것도 포함.
    - 분자:            (상품, aspect, 채널, source) 부정 건수.
    - source(cs/review)별 분리 집계 — 합산하지 않는다(분모가 다르므로, 문서 §136).
"""

from collections import Counter, defaultdict


def count_window(rows: list, day_start: int, day_end: int) -> tuple[Counter, Counter]:
    """[day_start, day_end] 구간(양끝 포함)의 총문의·부정을 센다. [0]·[1] 공용 코어.

    Returns:
        (totals, negs)
          - totals: Counter{(product, channel, source): 총문의}   ← 분모(aspect 무관)
          - negs:   Counter{(product, aspect, channel, source): 부정 건수}  ← 분자
    """
    totals: Counter = Counter()
    negs: Counter = Counter()
    for r in rows:
        if not (day_start <= r["day"] <= day_end):
            continue
        # 분모: 이 (상품,채널,source)의 모든 문의 (부정 아님·중립 포함).
        totals[(r["product"], r["channel"], r["source"])] += 1
        # 분자: 부정이면서 aspect 가 붙은 것만.
        if r["is_negative"] and r["aspect"]:
            negs[(r["product"], r["aspect"], r["channel"], r["source"])] += 1
    return totals, negs


def collect_texts(rows: list, day_start: int, day_end: int) -> dict:
    """구간 내 '부정 문의 텍스트'를 (상품,aspect,채널,source)별로 모은다. [6] 원인분류 입력.

    [0]은 숫자만 세는 게 아니라 텍스트도 모아둬야 [6]에 실제 문장이 넘어간다.
    """
    texts: dict = defaultdict(list)
    for r in rows:
        if day_start <= r["day"] <= day_end and r["is_negative"] and r["aspect"]:
            key = (r["product"], r["aspect"], r["channel"], r["source"])
            texts[key].append({"cs_id": r["id"], "raw_text": r["text"]})
    return dict(texts)


def build_baseline(
    rows: list,
    cur_start: int,
    *,
    past_days: int = 28,
    alert_days: set | None = None,
) -> tuple[Counter, Counter]:
    """[1] 과거 기준 — 현재 윈도우 직전 past_days 일의 총문의·부정 카운트. (문서 §[1])

    과거 윈도우 = [cur_start - past_days, cur_start - 1] (양끝 포함).

    기준선 오염 방지(문서 §150): alert_days 에 든 (상품, 채널, day)는 과거 집계에서 제외한다.
    지속되는 이상이 과거 윈도우에 섞이면 '새로운 평소'가 되어 알림이 스스로 꺼지므로,
    이미 알림이 나갔던 날짜는 뺀다. (단일 배치 mock 에선 선행 알림이 없어 이 경로는 안
    탄다 — 문서 §710. 그래서 alert_days 기본값은 빈 집합.)

    Args:
        alert_days: 제외할 {(product, channel, day), …}. 기본 없음.

    Returns:
        (totals, negs) — count_window 와 동일 구조.
    """
    past_end = cur_start - 1
    past_start = past_end - past_days + 1
    if alert_days:
        rows = [
            r for r in rows
            if (r["product"], r["channel"], r["day"]) not in alert_days
        ]
    return count_window(rows, past_start, past_end)


def build_combinations(
    rows: list,
    cur_start: int,
    cur_end: int,
    *,
    aspects: list,
    past_days: int = 28,
    alert_days: set | None = None,
) -> tuple[list, dict]:
    """[0]+[1] 진입점 — run_detection([2]) 입력 조합 + 부정 텍스트 맵.

    관측된 (상품,채널,source)마다 **aspects 전체 슬롯을 방출**한다. BH-FDR 배치(m)가
    '현재 부정이 있는 조합'만이 아니라 '평가 가능한 전 슬롯'이어야 컷오프가 문서 캘리브
    레이션(m≈1,464, 부록 A)과 맞기 때문이다. cur_neg=0 슬롯은 발화할 수 없지만 배치 크기
    에는 들어가 BH 를 보수적으로 유지한다(오탐 억제). validate_anomaly 의 그리드와 동일.

    Args:
        aspects: 판정 대상 aspect 택소노미(예: 색상/사이즈/소재/파손/오배송/기타).
                 순수·테스트가능 유지를 위해 하드코딩하지 않고 주입받는다.

    Returns:
        (combinations, texts)
          - combinations: [(product, aspect, channel, source,
                            (cur_neg, cur_total, past_neg, past_total)), …]  → run_detection 입력
          - texts:        {(product, aspect, channel, source): [{cs_id, raw_text}, …]}
                          현재 윈도우 부정 문의 텍스트 ([6] 원인분류용)
    """
    cur_totals, cur_negs = count_window(rows, cur_start, cur_end)
    past_totals, past_negs = build_baseline(
        rows, cur_start, past_days=past_days, alert_days=alert_days
    )
    texts = collect_texts(rows, cur_start, cur_end)

    combinations = []
    for (product, channel, source), cur_total in cur_totals.items():
        for aspect in aspects:
            key = (product, aspect, channel, source)
            counts = (
                cur_negs.get(key, 0),
                cur_total,
                past_negs.get(key, 0),
                past_totals.get((product, channel, source), 0),
            )
            combinations.append((product, aspect, channel, source, counts))
    return combinations, texts