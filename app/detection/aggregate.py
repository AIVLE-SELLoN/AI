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

from app.core.constants import PAST_WINDOW_DAYS


def _negative_aspects(row: dict) -> tuple:
    """이 문의가 '부정'으로 잡힌 aspect 들. 두 행 형태를 모두 받는다.

        {"aspect": "색상", "is_negative": True}     ← 단일 aspect 행
        {"neg_aspects": ["색상", "사이즈"]}          ← 한 문의가 여러 aspect 에 부정

    후자가 필요한 이유: ClassifiedItem.aspects 는 리스트라 한 문의가 색상·사이즈에
    동시에 부정일 수 있다. 이걸 행 2개로 쪼개면 **분모(총문의)가 2로 부풀어** 부정률이
    반토막 나고 탐지가 죽는다. 분모는 문의 1건 = 1 이어야 하므로 행은 문의 단위로
    유지하고, 분자만 aspect 수만큼 센다.
    """
    if "neg_aspects" in row:
        return tuple(row["neg_aspects"])
    if row["is_negative"] and row["aspect"]:
        return (row["aspect"],)
    return ()


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
        # 분모: 이 (상품,채널,source)의 모든 문의 (부정 아님·중립 포함). 문의 1건 = 1.
        totals[(r["product"], r["channel"], r["source"])] += 1
        # 분자: 부정으로 잡힌 aspect 마다 1. 한 문의가 여러 aspect 에 부정이면 각각 센다.
        for aspect in _negative_aspects(r):
            negs[(r["product"], aspect, r["channel"], r["source"])] += 1
    return totals, negs


def collect_texts(rows: list, day_start: int, day_end: int) -> dict:
    """구간 내 '부정 문의 텍스트'를 (상품,aspect,채널,source)별로 모은다. [6] 원인분류 입력.

    [0]은 숫자만 세는 게 아니라 텍스트도 모아둬야 [6]에 실제 문장이 넘어간다.
    """
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
    """[1] 과거 기준 — 현재 윈도우 직전 past_days 일의 총문의·부정 카운트. (문서 §[1])

    과거 윈도우 = [cur_start - past_days, cur_start - 1] (양끝 포함).

    기준선 오염 방지(문서 §150): **"알림이 발행된 (상품, aspect, 채널)의 알림 구간 날짜는
    과거 윈도우 집계에서 제외한다."** 지속되는 이상이 과거 윈도우에 섞이면 '새로운 평소'가
    되어 알림이 스스로 꺼지기 때문이다. (단일 배치 mock 에선 선행 알림이 없어 이 경로는
    안 탄다 — 문서 §710. 그래서 alert_days 기본값은 빈 집합.)

    ⚠️ 제외 단위가 aspect 를 포함하므로 **과거 분모도 aspect별로 따로 센다.** 색상 알림이
    나갔던 날을 뺄 때 그 날의 사이즈 문의까지 같이 빠지면, 사이즈의 과거 기준이 이유 없이
    깎인다. 같은 이유로 분자와 분모를 같은 날짜 집합에서 세야 비율이 성립한다.
    (현재 윈도우는 제외가 없으므로 count_window 의 aspect 무관 분모를 그대로 쓴다.)

    Args:
        aspects: 판정 대상 aspect 택소노미. 슬롯별로 제외 날짜가 다르므로 필요하다.
        alert_days: 제외할 {(product, aspect, channel, day), …}. 기본 없음.

    Returns:
        (totals, negs, unfiltered_totals)
          - totals:  Counter{(product, aspect, channel, source): 제외 후 총문의}  ← 분모
          - negs:    Counter{(product, aspect, channel, source): 부정 건수}       ← 분자
          - unfiltered_totals: Counter{(product, channel, source): 제외 전 총문의}
              문서 §152 "제외로 과거 표본이 **절반 이하로 줄면** 설정값 폴백" 판정 재료.
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

    관측된 (상품,채널,source)마다 **aspects 전체 슬롯을 방출**한다. BH-FDR 배치(m)가
    '현재 부정이 있는 조합'만이 아니라 '평가 가능한 전 슬롯'이어야 컷오프가 문서 캘리브
    레이션(m≈1,464, 부록 A)과 맞기 때문이다. cur_neg=0 슬롯은 발화할 수 없지만 배치 크기
    에는 들어가 BH 를 보수적으로 유지한다(오탐 억제). validate_anomaly 의 그리드와 동일.

    Args:
        aspects: 판정 대상 aspect 택소노미(예: 색상/사이즈/소재/파손/오배송/기타).
                 순수·테스트가능 유지를 위해 하드코딩하지 않고 주입받는다.
        past_rate_fallback: {(channel, aspect): 과거 부정률} 설정값 테이블. 문서 §142·§152
                 "설정값 테이블은 초기 구간 폴백용" 규칙의 입력. 값을 여기 하드코딩하지
                 않고 주입받는 이유는 아래 _apply_baseline_fallback 주석 참고.

    Returns:
        (combinations, texts)
          - combinations: [(product, aspect, channel, source,
                            (cur_neg, cur_total, past_neg, past_total)), …]  → run_detection 입력
          - texts:        {(product, aspect, channel, source): [{cs_id, raw_text}, …]}
                          현재 윈도우 부정 문의 텍스트 ([6] 원인분류용)
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
    """[1] 설정값 baseline 폴백. (문서 §142·§152·§153)

    두 경로 모두 실측 부정 건수를 버리고 설정값 부정률로 2x2 표를 재구성한다.

      ① 초기 구간 (past_total == 0) — §153 "과거 윈도우가 부족한 서비스 초기에만
         설정값 baseline 사용". 곱할 N 이 아예 없으므로 **현재 윈도우 볼륨을 윈도우
         길이 비로 늘린 값**(assumed_total)을 N 으로 쓴다. 이 비율은 §137 의 윈도우
         정의(현재 7일 / 과거 28일)에서 그대로 나온다 — 시나리오 정의서 §1 "N 원칙:
         과거 N = 현재 N × 4" 와도 일치하지만, 근거는 정답지가 아니라 윈도우 길이다.
      ② 표본 붕괴 (§152) — "제외로 과거 표본이 **절반 이하로 줄면** 초기 구간과 동일하게
         설정값 폴백". 이때는 살아남은 past_total 을 N 으로 쓴다.

    설정값 부정률이 주입되지 않으면 어느 경로도 못 타고 실측치를 그대로 돌려준다.
    past_total 이 0 인 채로 나가면 statistics.build_batch 가 보류로 보낸다.

    ⚠️ 설정값 테이블 값을 이 파일에 박지 않는다. 유일하게 존재하는 표가 시나리오 정의서
       §1 인데, 그 문서 1행이 **"이 정의서의 정답은 탐지 로직 코드가 절대 참조하지 않음.
       채점 전용."** 이라 app 코드가 읽으면 컨닝이 된다. 그래서 주입식으로만 받는다.

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