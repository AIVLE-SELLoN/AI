"""담당: 서영 (Agent2) — [4] 주 aspect 선택 · [5] 스코프 필터.

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.

탐지는 전 aspect 를 수행하고, 원인분류([6])·개선안만 스코프로 제한한다.
"""

from app.core.schemas import Aspect

# 개선안 생성 가능 aspect. 파손·오배송·기타는 알림만 — 원인분류·개선안이 없다.
# 튜닝 대상이 아닌 도메인 정의라 상수 파일이 아니라 여기 둔다. 값은 Aspect enum 경유 —
# 문자열로 다시 적으면 schemas.py 가 바뀔 때 여기만 옛 값으로 남아 조용히 어긋난다.
SCOPE_ASPECTS = frozenset({Aspect.COLOR, Aspect.SIZE, Aspect.MATERIAL})


def pick_main_aspect(fired_aspects: dict) -> tuple:
    """[4] 여러 aspect 동시 발화 시 delta 최대를 주 aspect 로 선택.

    Args:
        fired_aspects: {aspect: delta}. 발화 aspect 만 담기며, 비어있지 않음을 호출 측이
            보장한다(발화가 있어야 [4]에 온다).

    Returns:
        (main, subs) — subs 는 버리지 않고 부가 관찰로 병기한다.
    """
    main = max(fired_aspects, key=fired_aspects.get)
    subs = [a for a in fired_aspects if a != main]
    return main, subs


def is_in_scope(aspect: str) -> bool:
    """[5] 이 aspect 가 원인분류·개선안 생성 대상인지.

    색상·사이즈·소재만 True. 파손·오배송·기타는 False(알림만).
    그대로 alert 의 `scope_in` 필드 값이 된다.

    스펙 안에서 필드 정의와 판정표가 어긋난다 — 필드 정의는 "순수 aspect 속성"이라
    하고, 판정표는 전역형·구분불가 행을 scope_in=false 로 적어놨다. 필드 정의를 따른다.
    verdict 를 섞으면 "순수 aspect 속성"이라는 정의 자체가 깨지고, Agent3 는
    recommended_action 으로 작동 여부를 판단하므로 동작에도 영향이 없다.
    """
    return aspect in SCOPE_ASPECTS
