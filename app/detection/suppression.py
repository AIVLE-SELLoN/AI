"""담당: 서영 (Agent2) — 알림 억제 · 갱신 (이상탐지 로직 V3 §6 알림 운영 규칙).

순수 함수만. **상태 저장소를 모른다** — 직전 알림 목록을 인자로 받는다.
로직 §8 이 "실행 아키텍처 백엔드 확인 필요" 상태라 영속화 위치(Spring Boot DB vs
LangGraph checkpointer)가 미정이기 때문이다. 저장소가 정해지면 호출부만 연결하면 된다.

규칙 (§6):
    - 이미 알림 나간 (상품, aspect, 채널) 조합은 RENOTIFY_BLOCK_DAYS 일간 재알림 금지
      **또는 해당 건이 승인/반려 처리될 때까지** — 둘 중 먼저 오는 쪽까지 막는다.
      처리가 끝났으면 셀러가 이미 본 건이므로 억제할 이유가 사라진다.
    - 단, 부정률이 직전 알림 대비 RENOTIFY_DELTA_JUMP(+5%p) 이상 더 오르면 갱신 허용.
      갱신은 새 alert_id + updates_alert_id 에 원본 ID.

⚠️ 단일 배치 mock 에서는 선행 알림이 없어 이 경로를 타지 않는다(로직 §710).
   aggregate.build_baseline 의 alert_days 와 짝이다 — 알림 구간을 과거 윈도우에서
   빼야 지속되는 이상이 '새로운 평소'로 굳어 알림이 스스로 꺼지는 일이 없다.
"""

from app.core.constants import RENOTIFY_BLOCK_DAYS, RENOTIFY_DELTA_JUMP
from app.core.schemas import DetectionAlert

_RATE_EPSILON = 1e-9
"""부정률 경계 비교용 허용오차. 임계값 자체가 아니라 부동소수점 표현 오차만 흡수한다."""


def _key(alert: DetectionAlert) -> tuple:
    """억제 판정 단위 = (상품, aspect, 채널). §6."""
    return (alert.product_group_id, alert.main_aspect, alert.channel)


def filter_suppressed(
    alerts: list[DetectionAlert],
    prior_alerts: list[DetectionAlert] | None = None,
    *,
    resolved_alert_ids: set | None = None,
    block_days: int = RENOTIFY_BLOCK_DAYS,
    delta_jump: float = RENOTIFY_DELTA_JUMP,
) -> tuple[list[DetectionAlert], list[DetectionAlert]]:
    """직전 알림과 대조해 억제/갱신을 적용한다. (§6)

    Args:
        alerts: 이번 윈도우가 만든 알림들.
        prior_alerts: 과거 발행된 알림들. 없으면 전부 통과(첫 실행·단일 배치 mock).
        resolved_alert_ids: **승인/반려 처리가 끝난** alert_id 집합. §6 의 "또는 해당 건
            승인/반려 처리 전까지" 조건 — 여기 든 알림은 억제 근거가 되지 않는다.
            HITL 상태는 Agent3 쪽(Recommendation.hitl_status)이 들고 있으므로 주입받는다.
        block_days: 재알림 금지 기간.
        delta_jump: 갱신을 허용하는 추가 상승폭.

    Returns:
        (발행할 알림, 억제된 알림)
          - 발행할 알림 중 갱신분은 updates_alert_id 가 채워진 **새 객체**로 교체된다
            (원본을 제자리 수정하지 않는다 — 호출부가 입력 리스트를 재사용할 수 있으므로).
    """
    if not prior_alerts:
        return list(alerts), []

    # 처리가 끝난 알림은 억제 근거에서 빼둔다 (§6 "승인/반려 처리 전까지").
    resolved = resolved_alert_ids or set()

    # 같은 키의 직전 알림 중 가장 최근 것만 보면 된다.
    latest: dict[tuple, DetectionAlert] = {}
    for prior in prior_alerts:
        if prior.alert_id in resolved:
            continue
        key = _key(prior)
        if key not in latest or prior.detected_at > latest[key].detected_at:
            latest[key] = prior

    published: list[DetectionAlert] = []
    suppressed: list[DetectionAlert] = []

    for alert in alerts:
        prior = latest.get(_key(alert))
        if prior is None:
            published.append(alert)
            continue

        elapsed_days = (alert.detected_at - prior.detected_at).days
        if elapsed_days >= block_days:
            # 억제 기간이 지났다 → 신규 알림으로 그대로 발행.
            published.append(alert)
            continue

        # 억제 기간 안 — 추가 상승이 충분할 때만 '갱신'으로 내보낸다.
        # _RATE_EPSILON: 부정률은 neg/total 부동소수점이라 경계가 안 맞는다.
        # 예) 36/200 - 26/200 = 0.049999999999999996 < 0.05 → 갱신이 조용히 죽는다.
        if alert.stats.cur_rate - prior.stats.cur_rate >= delta_jump - _RATE_EPSILON:
            published.append(alert.model_copy(update={"updates_alert_id": prior.alert_id}))
        else:
            suppressed.append(alert)

    return published, suppressed
