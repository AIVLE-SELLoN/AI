"""산출물 식별자 생성 — 서비스·검증기·발행기가 **같은 규칙**을 쓰도록 한 곳에 모은다.

ID 규칙이 갈라지면 백엔드 upsert 가 엉뚱한 문서를 덮어쓰므로 쓰는 곳이 반드시 같은 함수를
봐야 한다. core 에 있는 이유는 소비자가 `app/reporting/` 과 `app/core/mq.py` 로 갈려서다 —
어느 한쪽에 두면 core 가 컴포넌트를 import 하는 역방향 의존이 생긴다.
"""

from __future__ import annotations

ALERT_ID_PREFIX = "ALT-"
GUIDELINE_ID_PREFIX = "GD-"
RECOMMENDATION_ID_PREFIX = "REC-"


def _swap_alert_prefix(prefix: str, alert_id: str) -> str:
    """`ALT-` 접두어만 갈아끼운 파생 ID. 파생 산출물이 **전부 이 한 규칙**을 쓴다.

    alert_id 형식(`ALT-{window_end}-{상품}-{ASPECT}-{채널}`)을 파서가 알 필요가 없다 —
    접두어만 보므로 알림 ID 형식이 바뀌어도 파생 ID 가 자동으로 따라간다.
    그 형식이 아니면 통째로 뒤에 붙여 **어떤 입력에서도 alert_id 와 1:1** 을 보장한다
    (구형 `ALT-20260528-0001` 도 포함).
    """
    if alert_id.startswith(ALERT_ID_PREFIX):
        return f"{prefix}{alert_id[len(ALERT_ID_PREFIX) :]}"
    return f"{prefix}{alert_id}"


def build_guideline_id(alert_id: str) -> str:
    """alert_id → guideline_id. 가이드라인은 알림 1건과 1:1 이다.

    예: ALT-20260828-P001-COLOR-COUPANG → GD-20260828-P001-COLOR-COUPANG

    예전 규칙(`GD-{탐지일}-{상품}`)으로 되돌리지 말 것 — 탐지가 (상품, aspect, 채널)
    단위로 발화하므로 같은 날 같은 상품의 다른 알림이 전부 같은 ID 가 됐고, 백엔드 멱등
    upsert 때문에 나중에 도착한 가이드라인이 앞의 것을 조용히 덮어썼다(쿠팡 색상 가이드가
    네이버 사이즈 가이드로 바뀌는 식).
    """
    return _swap_alert_prefix(GUIDELINE_ID_PREFIX, alert_id)


def build_recommendation_id(alert_id: str) -> str:
    """alert_id → recommendation_id. 개선안도 알림 1건과 1:1 이다(선생성).

    예: ALT-20260828-P001-COLOR-COUPANG → REC-20260828-P001-COLOR-COUPANG

    예전 규칙(`REC-{uuid4[:12]}`)은 같은 알림을 재처리할 때마다 다른 ID 를 냈다. 백엔드
    중복 판정에는 영향이 없다 — `Proposal` 한 행에 `alert_id` 와 개선안 필드가 같이 들어가고
    그 컬럼에 유니크 제약이 있어 **중복 INSERT 가 구조적으로 불가능**하다. 바꾼 이유는
    재발행 시 payload 가 완전히 같아져 우리 쪽 재현·테스트가 쉬워지는 것이다.
    """
    return _swap_alert_prefix(RECOMMENDATION_ID_PREFIX, alert_id)
