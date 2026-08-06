"""담당: 지인 — RabbitMQ 발행기. 계약 정본은 `docs/mq_events.md`.

리포팅(용준)·배치(서영)가 같이 쓰므로 컴포넌트 폴더가 아니라 core 에 둔다.

⚠️ **지금은 시그니처 스텁이다.** 배치·리포팅이 이 함수들에 맞춰 호출부를 짜고 있어
   구현보다 먼저 커밋한다. `new_trace_id()` 만 실제로 동작하고 발행 2종은
   `NotImplementedError` 다.

발행 계약 3가지 (구현할 때 지킬 것):

1. **안 보낸 것을 보냈다고 하지 않는다.** 호출부(`app/batch/daily.py`)는 예외가 없으면
   발행 성공으로 보고 그 알림을 `prior_alerts` 캐시에 넣는다 — 캐시에 들어가면
   `RENOTIFY_BLOCK_DAYS` 동안 재알림이 억제되므로, 실제로 안 나간 메시지를 조용히
   성공 처리하면 셀러가 그 알림을 7일간 못 본다. 브로커가 없거나 꺼져 있으면 던진다.
   (용준 `S3_ENABLED=false` → `S3NotConfiguredError` 와 같은 원칙)
2. **`trace_id` 는 인자로만 받는다.** 여기서 만들면 배치 1회 = `traceId` 1개 규약이
   깨진다(§3). 배치가 `new_trace_id()` 로 한 번 만들어 전달한다.
3. **Envelope 은 camelCase, payload 안은 snake_case** (§3). 발행 단위는 알림 1건당
   메시지 1개 — 배열로 묶지 않는다(멱등 키가 건별이라).
"""

from __future__ import annotations

import uuid

from app.core.schemas import DetectionAlert, GenerationCallback, Recommendation


def new_trace_id() -> str:
    """배치 1회분의 `traceId`. 그 배치가 발행하는 모든 메시지가 이 값을 공유한다(§3)."""
    return f"trace-{uuid.uuid4().hex[:16]}"


async def publish_anomaly_analyzed(
    alert: DetectionAlert,
    rec: Recommendation | None,
    trace_id: str,
) -> None:
    """`ai.anomaly.analyzed` 발행 — 알림 1건 + 개선안 1건(없으면 null). 멱등 키 `alert_id`.

    Args:
        alert: 탐지 알림. payload 최상위가 이 필드들 그대로다(§4-1).
        rec: 개선안. `recommended_action != "개선안 생성"` 이면 None 이고 payload 에도
            `"recommendation": null` 로 나간다(§4-2).
        trace_id: 배치가 만든 값. 이 함수가 생성하지 않는다.

    Raises:
        NotImplementedError: 미구현.
    """
    raise NotImplementedError("app/core/mq.py 미구현 — 발행기 작업 중")


async def publish_guideline_generated(
    callback: GenerationCallback,
    trace_id: str,
) -> None:
    """`ai.guideline.generated` 발행 — CS 가이드라인 1건. 멱등 키 `guideline_id`.

    알림 1건마다 트리거된다(월간 리포트와 생명주기가 다르다, §6).

    Args:
        callback: 용준 `generate_guideline()` 산출물. `guideline_id` 가 채워져 있어야
            한다 — `report_id` 만 있으면 월간 리포트라 이 라우팅 키가 아니다.
        trace_id: 배치가 만든 값.

    Raises:
        NotImplementedError: 미구현.
    """
    raise NotImplementedError("app/core/mq.py 미구현 — 발행기 작업 중")
