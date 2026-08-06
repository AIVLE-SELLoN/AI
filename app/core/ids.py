"""산출물 식별자 생성 — 서비스·검증기·발행기가 **같은 규칙**을 쓰도록 한 곳에 모은다.

ID 규칙이 갈라지면 백엔드 upsert 가 엉뚱한 문서를 덮어쓰기 때문에, 쓰는 곳이 반드시
같은 함수를 봐야 한다. 원래 `app/reporting/` 안에 있었는데(검증기가 서비스를 import 하면
순환이라 거기로 뺐던 것), 2026-08-06 에 `app/core/mq.py` 가 발행 payload 를 만들면서
두 번째 컴포넌트가 생겨 core 로 올렸다 — core 가 컴포넌트를 import 하는 역방향 의존을
만들지 않기 위해서다.
"""

from __future__ import annotations

ALERT_ID_PREFIX = "ALT-"
GUIDELINE_ID_PREFIX = "GD-"


def build_guideline_id(alert_id: str) -> str:
    """alert_id → guideline_id. 예: ALT-20260528-P001-COUPANG → GD-20260528-P001-COUPANG

    가이드라인은 알림 1건과 1:1 이므로 ID 도 alert_id 와 **1:1** 이어야 한다.

    ⚠️ 예전 규칙(`GD-{탐지일}-{상품}`)은 틀렸다. 탐지가 (상품, aspect, 채널) 단위로
       발화하므로 같은 날 같은 상품의 다른 알림이 전부 같은 ID 가 됐고, 백엔드가 멱등
       upsert 를 하므로 나중에 도착한 가이드라인이 앞의 것을 조용히 덮어썼다
       (쿠팡 색상 가이드가 네이버 사이즈 가이드로 바뀌는 식).

    `ALT-` 접두어만 `GD-` 로 바꿔 기존 형태를 유지하고, 그 형식이 아니면 통째로 뒤에
    붙여 **어떤 입력에서도 alert_id 와 1:1** 을 보장한다.
    """
    if alert_id.startswith(ALERT_ID_PREFIX):
        return f"{GUIDELINE_ID_PREFIX}{alert_id[len(ALERT_ID_PREFIX) :]}"
    return f"{GUIDELINE_ID_PREFIX}{alert_id}"
