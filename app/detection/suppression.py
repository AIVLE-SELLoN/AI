"""담당: 서영 (Agent2) — 알림 억제 · 갱신.

순수 함수만. 상태 저장소를 모른다 — 직전 알림 목록을 인자로 받는다. 영속화 위치가 정해지면
호출부만 연결하면 된다.

규칙:
    - 이미 알림 나간 (상품, aspect, 채널) 은 RENOTIFY_BLOCK_DAYS 일간 재알림 금지, 또는
      해당 건이 승인/반려될 때까지 — 둘 중 먼저 오는 쪽까지. 처리가 끝났으면 셀러가 이미
      본 건이라 억제할 이유가 사라진다.
    - 단 부정률이 직전 알림 대비 RENOTIFY_DELTA_JUMP 이상 더 오르면 갱신을 허용한다.

경과일은 window_end(데이터 시각)로 센다. detected_at(실행 시각)이 아니다 — 기준선 오염
방지(alert_days)가 날짜 기준이라 같은 시계를 써야 하고, 실행 시각으로 세면 데모가 깨진다.
압축 재생은 60일치를 몇 분에 흘려서 detected_at 차이가 항상 0일이라 억제가 영영 안 풀리고
조합당 알림이 1건만 뜨고 끝난다.

aggregate.build_baseline 의 alert_days 와 짝이다 — 알림 구간을 과거 윈도우에서 빼야 지속
되는 이상이 '새로운 평소'로 굳어 알림이 스스로 꺼지는 일이 없다.
"""

from app.core.constants import RENOTIFY_BLOCK_DAYS, RENOTIFY_DELTA_JUMP
from app.core.schemas import DetectionAlert

_RATE_EPSILON = 1e-9
"""부정률 경계 비교용 허용오차. 임계값 자체가 아니라 부동소수점 표현 오차만 흡수한다."""


def _key(alert: DetectionAlert) -> tuple:
    """억제 판정 단위 = (상품, aspect, 채널)."""
    return (alert.product_group_id, alert.main_aspect, alert.channel)


def _updates_target(alert: DetectionAlert, prior: DetectionAlert) -> str | None:
    """갱신 알림이 가리킬 원본 ID. 자기 자신은 가리키지 않는다.

    `alert_id` 가 결정론적이라(논리 키 + `window_end`) 같은 구간을 다시 돌리면 새 ID 가
    `prior.alert_id` 와 글자까지 같아진다. 그대로 넣으면 백엔드가 upsert 한 행이 자기 자신의
    갱신 대상이 된다. 같은 논리 알림의 수치 갱신이지 새 알림이 아니므로 `None` 이 맞다.
    형식만 바꿔서는 안 없어지는 경로다 — 억제 기간 안에서 상승 조건만 채우면 여기 온다.

    구형 ID 가 캐시에 남아 있으면 새 ID 와 절대 같아지지 않아 그 값이 그대로 들어간다.
    억제 매칭은 `_key`(상품·aspect·채널)로 하니 과도기에도 갱신 체인이 끊기지 않는다.
    """
    if alert.alert_id == prior.alert_id:
        return None
    return prior.alert_id


def filter_suppressed(
    alerts: list[DetectionAlert],
    prior_alerts: list[DetectionAlert] | None = None,
    *,
    resolved_alert_ids: set | None = None,
    block_days: int = RENOTIFY_BLOCK_DAYS,
    delta_jump: float = RENOTIFY_DELTA_JUMP,
) -> tuple[list[DetectionAlert], list[DetectionAlert]]:
    """직전 알림과 대조해 억제/갱신을 적용한다.

    Args:
        alerts: 이번 윈도우가 만든 알림들.
        prior_alerts: 과거 발행된 알림들. 없으면 전부 통과(첫 실행·단일 배치 mock).
        resolved_alert_ids: 승인/반려가 끝난 alert_id 집합. 여기 든 알림은 억제 근거가
            되지 않는다. HITL 상태는 Agent3(Recommendation.hitl_status)이 들고 있어 주입받는다.
        block_days: 재알림 금지 기간.
        delta_jump: 갱신을 허용하는 추가 상승폭.

    Returns:
        (발행할 알림, 억제된 알림). 갱신분은 updates_alert_id 가 채워진 새 객체로 교체된다 —
        호출부가 입력 리스트를 재사용할 수 있으므로 원본을 제자리 수정하지 않는다.
    """
    if not prior_alerts:
        return list(alerts), []

    # 처리가 끝난 알림은 억제 근거에서 빼둔다 ("승인/반려 처리 전까지").
    resolved = resolved_alert_ids or set()

    # 같은 키의 직전 알림 중 최근 1건만 보면 된다. '최근'의 기준도 window_end 다.
    latest: dict[tuple, DetectionAlert] = {}
    for prior in prior_alerts:
        if prior.alert_id in resolved:
            continue
        key = _key(prior)
        if key not in latest or prior.window_end > latest[key].window_end:
            latest[key] = prior

    published: list[DetectionAlert] = []
    suppressed: list[DetectionAlert] = []

    for alert in alerts:
        prior = latest.get(_key(alert))
        if prior is None:
            published.append(alert)
            continue

        # 데이터 시각 기준 — 모듈 docstring 참고. 실행 시각으로 세면 데모가 깨진다.
        elapsed_days = (alert.window_end - prior.window_end).days
        if elapsed_days >= block_days:
            published.append(alert)
            continue

        # 억제 기간 안 — 추가 상승이 충분할 때만 '갱신'으로 내보낸다. _RATE_EPSILON 은
        # 36/200 - 26/200 = 0.049999999999999996 같은 경계에서 갱신이 조용히 죽는 것을 막는다.
        if alert.stats.cur_rate - prior.stats.cur_rate >= delta_jump - _RATE_EPSILON:
            published.append(
                alert.model_copy(
                    update={"updates_alert_id": _updates_target(alert, prior)}
                )
            )
        else:
            suppressed.append(alert)

    return published, suppressed
