"""담당: 서영 (Agent2) — 탐지 입력 로더. 원본 문서 ⟕ 분류 결과 조인.

왜 이 모듈이 따로 있나
----------------------
`classified_item_aspect` 는 문서 목록이 아니라 **aspect 언급 목록**이다. aspect 가
0개인 문서는 자식 행이 생기지 않으므로, **자식 테이블에서 분모를 세면 안 된다.**
반면 `classified_item` 부모 테이블에는 정상 분류된 문서가 aspect 0개여도 1행 남는다.

    RVW-4 "배송 빨랐고 포장도 깔끔합니다"  →  부모 1행 + aspect 자식 0행

리뷰는 허용 aspect 가 색상·사이즈·소재 3개뿐이고(`REVIEW_ALLOWED_ASPECTS`),
프롬프트2가 "언급된 속성이 하나도 없으면 []" 를 지시한다. 그래서 빈 배열이 정상 출력이다.

⚠️ **CS 도 안전하지 않다.** 예전 주석은 "CS 는 `기타` 가 있어 문의 1건당 최소 1행이
   보장되므로 해당 없다"고 했는데 **실측에서 반증됐다** — 프롬프트1이 "반드시 하나
   이상"을 지시함에도 LLM 이 빈 배열을 냈다(284건 중 6건, 2.1%). 지금 1행이 보장되는
   것은 택소노미 덕이 아니라 `classification.service._cs_empty_fallback` 이 채우기
   때문이다. 그 폴백을 빼면 이 전제가 다시 깨진다.
   → 그래서 분모를 원본에서 세는 이 모듈은 CS 에도 그대로 필요하다.

분모를 `classified_item` 에서 세면 이런 일이 생긴다(리뷰 4건 중 1건이 빈 배열):

    진짜 색상 부정률 = 1/4 = 25%
    관측 색상 부정률 = 1/3 = 33%        ← 분자는 맞고 분모만 깎인다

Fisher 검정은 비율이 아니라 (부정건수, 총건수) 원시 카운트를 받으므로 p값 자체가
틀어진다. 부풀림 배율은 윈도우마다 달라서(이상이 터진 주엔 언급이 늘어 덜 깎임)
오탐·미탐 어느 방향인지 예측도 안 된다.

해결
----
**분모는 원본 문서에서 세고, 분자만 분류 결과에서 가져온다.** SQL 의 LEFT JOIN 과
같은 의미이며, 실서비스에서는 아래 형태가 된다:

    FROM reviews d LEFT JOIN classified_item c ON c.item_id = d.review_id

⚠️ `sentiment = -1` 조건을 WHERE 로 올리면 LEFT JOIN 이 INNER JOIN 으로 퇴화해서
   이 버그가 그대로 돌아온다. 반드시 SELECT 쪽(CASE WHEN)에 둘 것.

전제조건 — 분류 커버리지
------------------------
분자는 분류에 성공한 문서에서만 나온다. 커버리지가 100% 가 아니면 부정률이
과소추정된다(미탐 방향). `classified_item` 부모 행의 존재로 성공 여부를 판단하고,
`check_coverage()` 로 **CS와 리뷰 모두 일자별** 검증한다. 윈도우 총합만 비교하면 특정
날짜만 통째로 빠진 걸 못 잡는다(aggregate §99 "분자와 분모를 같은 날짜 집합에서 세야
비율이 성립한다").
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Container, Iterable
from datetime import datetime

from app.core.schemas import ClassifiedItem, Sentiment, Source

COVERAGE_CHECKED_SOURCES: frozenset[str] = frozenset(
    {Source.CS.value, Source.REVIEW.value}
)
"""부모 분류 레코드 존재 여부로 커버리지를 검증하는 source."""


def _neg_aspects(item: ClassifiedItem) -> list[str]:
    return [a.aspect.value for a in item.aspects if a.sentiment == Sentiment.NEGATIVE]


def build_rows(
    documents: Iterable[dict],
    classified: Iterable[ClassifiedItem],
) -> list[dict]:
    """원본 문서 ⟕ 분류 결과 → aggregate 가 먹는 정규화 행. **문서 1건 = 행 1개.**

    Args:
        documents: 분모의 출처. 원본 테이블/CSV 에서 그대로 온 문서들.
            필요한 키 — id · product · channel · source · created_at(datetime) · text(선택)
            **분류 여부와 무관하게 전부 넣을 것.** 이게 이 모듈의 존재 이유다.
        classified: 분자의 출처. `item_id` 로 documents 와 짝지어진다.
            documents 에 없는 item_id 는 무시한다(원본이 분모의 유일한 기준).

    Returns:
        [{"product", "channel", "source", "neg_aspects", "day", "id", "text"}, ...]
        service.normalize() 와 같은 모양이라 aggregate 는 둘을 구분하지 않는다.

    day 는 날짜의 ordinal 을 그대로 쓴다 — 기준일 오프셋을 두면 배치마다 값이 달라져
    alert_days 같은 (상품, 채널, day) 키가 배치 간에 안 맞는다.
    """
    neg_by_id: dict[str, list[str]] = {
        item.item_id: _neg_aspects(item) for item in classified
    }

    rows: list[dict] = []
    for doc in documents:
        created = doc["created_at"]
        if isinstance(created, str):
            created = datetime.fromisoformat(created)
        rows.append(
            {
                "product": doc["product"],
                "channel": doc["channel"],
                "source": doc["source"],
                # 분류 결과가 없으면 빈 리스트 — 분모 +1, 분자 +0 (LEFT JOIN 과 동일)
                "neg_aspects": neg_by_id.get(doc["id"], []),
                "day": created.date().toordinal(),
                "id": doc["id"],
                "text": doc.get("text", ""),
            }
        )
    return rows


def check_coverage(
    documents: Iterable[dict],
    classified: Iterable[ClassifiedItem],
    *,
    sources: Container[str] = COVERAGE_CHECKED_SOURCES,
) -> list[dict]:
    """분류가 빠진 (상품, 채널, source, 날짜) 를 찾는다. 빈 리스트면 커버리지 100%.

    **일자별로 센다.** 윈도우 총합만 맞춰보면 "3일치가 통째로 빠지고 다른 날이 더
    들어온" 상황을 못 잡는데, 그러면 그날 분자가 0 이 되면서 비율이 조용히 깎인다.

    여기서 '분류됨'은 aspect 개수가 아니라 **`classified_item` 부모 레코드가 존재하는
    문서**를 뜻한다. 워커는 정상 분류 결과가 빈 배열이어도 부모 행을 저장하고, 일일
    로더는 LEFT JOIN으로 그 행을 `ClassifiedItem(aspects=[])`로 복원한다. 따라서 무관
    리뷰의 정상 빈 배열과 아직 분류하지 않은 리뷰를 구분해 두 source 모두 검사할 수 있다.

    Args:
        sources: 검사할 source 집합. 기본은 CS와 리뷰 모두.

    Returns:
        [{"product", "channel", "source", "day", "documents", "classified"}, ...]
        classified < documents 인 슬롯만. 각 건수 포함.
    """
    doc_count: dict[tuple, int] = defaultdict(int)
    doc_key_of: dict[str, tuple] = {}
    for doc in documents:
        if doc["source"] not in sources:
            continue
        created = doc["created_at"]
        if isinstance(created, str):
            created = datetime.fromisoformat(created)
        key = (
            doc["product"],
            doc["channel"],
            doc["source"],
            created.date().toordinal(),
        )
        doc_count[key] += 1
        doc_key_of[doc["id"]] = key

    seen: dict[tuple, set] = defaultdict(set)
    for item in classified:
        key = doc_key_of.get(item.item_id)
        item_source = (
            item.source.value if isinstance(item.source, Source) else item.source
        )
        if key is not None and item_source == key[2]:
            seen[key].add(item.item_id)

    gaps: list[dict] = []
    for key, total in sorted(doc_count.items()):
        got = len(seen.get(key, ()))
        if got < total:
            product, channel, source, day = key
            gaps.append(
                {
                    "product": product,
                    "channel": channel,
                    "source": source,
                    "day": day,
                    "documents": total,
                    "classified": got,
                }
            )
    return gaps


def unreliable_slots(gaps: list[dict]) -> set[tuple[str, str, str]]:
    """check_coverage 결과 → build_batch 가 먹는 (product, channel, source) 집합.

    하루라도 분류가 빠지면 그 슬롯의 **윈도우 분모 전체**를 믿을 수 없다. 윈도우
    집계는 날짜를 합치기 때문이다.

    이 슬롯들은 **검정 전에** family 에서 빠져야 한다. 분모가 깎이면 부정률이
    부풀려져 p값이 실제보다 작게 나오는데, BH 는 step-up 이라 가짜로 작은 p값
    하나가 기각 개수를 늘려 **나머지 검정의 임계까지 완화**시킨다. 한 상품의 데이터
    결함이 다른 상품을 오탐시키는 셈이다.
    """
    return {(g["product"], g["channel"], g["source"]) for g in gaps}
