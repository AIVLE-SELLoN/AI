"""담당: 서영 (Agent2) — [4] 주 aspect 선택 · [5] 스코프 필터 (이상탐지 로직 V3 §[4]·§[5]).

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다 (statistics.py·verdict.py 와 동일 원칙).

[4] 한 채널에서 여러 aspect 가 동시 발화하면 상승폭(delta) 최대를 주 원인으로.
[5] 그 aspect 가 개선안 생성 가능한 것인지(색상·사이즈·소재) 판정.
    탐지는 전 aspect 수행하되, 원인분류([6])·개선안은 스코프로 제한한다.
"""

from app.core.schemas import Aspect

# 개선안 생성 가능 aspect (로직 §[5] 표). 파손·오배송·기타는 알림만 — 원인분류·개선안 없음.
# 도메인 정의(튜닝 대상 아님)라 상수 파일이 아닌 여기에 둔다.
# 값은 **반드시 Aspect enum 경유** — 문자열을 다시 적으면 schemas.py 가 정본인데도
# 값이 바뀔 때 여기만 옛 문자열로 남아 조용히 어긋난다.
SCOPE_ASPECTS = frozenset({Aspect.COLOR, Aspect.SIZE, Aspect.MATERIAL})


def pick_main_aspect(fired_aspects: dict) -> tuple:
    """[4] 여러 aspect 동시 발화 시 delta 최대를 주 aspect 로 선택. (로직 §[4])

    Args:
        fired_aspects: {aspect: delta}  (delta = 현재율 - 과거율). 발화 aspect 만 담는다.
            호출 측이 비어있지 않음을 보장한다(발화가 있어야 [4]에 온다).

    Returns:
        (main, subs)
          - main: delta 최대 aspect (주 원인)
          - subs: 나머지 aspect 리스트 (버리지 않고 부가 관찰로 병기 — §361)
    """
    main = max(fired_aspects, key=fired_aspects.get)
    subs = [a for a in fired_aspects if a != main]
    return main, subs


def is_in_scope(aspect: str) -> bool:
    """[5] 이 aspect 가 원인분류·개선안 생성 대상인지. (로직 §[5])

    색상·사이즈·소재만 True. 파손·오배송·기타는 False(알림만).
    """
    return aspect in SCOPE_ASPECTS