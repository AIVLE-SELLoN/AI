"""담당: 지인 — 메인 → AI 이벤트 수신(`ai.inbound`, `feedback.#`). 계약은 `docs/mq_events.md` §8.

메인이 AI 로 되돌리는 건 **사용자 피드백 2종뿐**이다. "연산해달라"는 요청 이벤트는 없다.

    feedback.recommendation.reviewed  → 개선안 승인/반려 (지인) — 컬렉션2 축적
    feedback.report.created           → 보고서 피드백 (용준) — 핸들러 미등록

**한 큐(`ai.inbound`)에 둘 다 들어온다.** 그래서 처리기가 없는 이벤트를 ACK 하면 그
메시지는 조용히 사라진다 — 처리한 적 없는 걸 처리했다고 하는 것이므로, 모르는
`eventType` 은 nack 해서 DLX 로 보낸다(§10 공통 정책).

멱등성: 이벤트별로 키가 다르다(§10). `feedback.recommendation.reviewed` 는
`recommendation_id` 이고, `record_hitl_outcome()` 이 그 ID 로 upsert 하므로 같은
이벤트가 두 번 와도 컬렉션2 문서가 하나다 — 컨슈머가 따로 중복 제거를 하지 않는다.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel, ValidationError

from app.config import get_settings
from app.core.exceptions import HitlContextUnavailableError, MqDisabledError
from app.core.schemas import DetectionAlert, HitlFeedback, HitlStatus, Recommendation

logger = logging.getLogger(__name__)

INBOUND_QUEUE = "ai.inbound"
FEEDBACK_BINDING = "feedback.#"

RECOMMENDATION_REVIEWED = "feedback.recommendation.reviewed"
REPORT_CREATED = "feedback.report.created"

PREFETCH = 10
"""동시에 처리 중인 미확인 메시지 상한(§10 은 10~50). 핸들러가 Chroma 쓰기라 낮게 잡는다."""


class RecommendationReviewed(BaseModel):
    """`feedback.recommendation.reviewed` payload (§8).

    ⚠️ **`alert`·`recommendation` 은 계약에 없는 필드다.** 백엔드가 전문을 실어주면
    쓰고, 없으면 `_load_hitl_context()` 가 명확히 막는다 — 아래 함수 주석 참고.
    필드 추가는 옵셔널로만 하는 게 스키마 변경 규칙(§11)이라 이렇게 둬도 계약 위반이 아니다.
    """

    recommendation_id: str
    alert_id: str
    hitl_status: HitlStatus
    hitl_feedback: HitlFeedback | None = None

    alert: DetectionAlert | None = None
    recommendation: Recommendation | None = None


def _load_hitl_context(
    event: RecommendationReviewed,
) -> tuple[DetectionAlert, Recommendation]:
    """이벤트 → `record_hitl_outcome()` 이 요구하는 (alert, recommendation).

    🔴 **여기가 유일한 미정 지점이다. 결정되면 이 함수만 바꾸면 된다.**

    `record_hitl_outcome()` 은 "원인 라벨 + CS 요약 + 개선안 본문"으로 컬렉션2 문서를
    만드는데(§4-2), §8 payload 에는 ID 4개뿐이라 그 재료가 없다. 두 안 중 하나가 필요하다:

    (A) 백엔드가 `ai.anomaly.analyzed` 로 받았던 alert·recommendation 을 그대로 되실어
        준다 → 이 함수는 지금 코드 그대로 동작하고 컨슈머는 무상태다. **권장.**
    (B) AI 가 발행분을 로컬에 저장해두고 `recommendation_id` 로 되찾는다 → 이 함수가
        그 저장소를 읽도록 바뀐다. 보관 기간이 지나면 유실되는 게 약점.

    hitl 값은 **이벤트 쪽이 정본이다.** 실어 보낸 recommendation 은 발행 시점 사본이라
    `hitl_status` 가 `대기`로 굳어 있고, 그대로 쓰면 `record_hitl_outcome()` 이
    "아직 결정 전"이라며 거부한다.
    """
    if event.alert is None or event.recommendation is None:
        raise HitlContextUnavailableError(
            f"recommendation_id={event.recommendation_id}: payload 에 alert·recommendation "
            "전문이 없어 컬렉션2에 적재할 수 없습니다 (docs/mq_events.md §8 확장 필요)"
        )

    recommendation = event.recommendation.model_copy(
        update={
            "hitl_status": event.hitl_status,
            "hitl_feedback": event.hitl_feedback,
        }
    )
    return event.alert, recommendation


def handle_recommendation_reviewed(payload: dict) -> None:
    """승인/반려 1건을 컬렉션2에 적재한다.

    Raises:
        ValidationError: payload 가 계약과 다름.
        HitlContextUnavailableError: 적재 재료 부족(`_load_hitl_context` 참고).
        ValueError: `record_hitl_outcome()` 의 정합성 검사 실패(서로 다른 건 / 대기 상태).
    """
    # 순환 import 방지 — recommendation 이 core 를 import 하므로 반대 방향은 함수 안에서.
    from app.recommendation.pipeline import record_hitl_outcome

    event = RecommendationReviewed.model_validate(payload)
    alert, recommendation = _load_hitl_context(event)
    record_hitl_outcome(alert, recommendation)
    logger.info(
        "HITL 적재 완료 recommendation_id=%s status=%s",
        event.recommendation_id,
        event.hitl_status.value,
    )


HANDLERS: dict[str, Any] = {
    RECOMMENDATION_REVIEWED: handle_recommendation_reviewed,
    # REPORT_CREATED 는 리포팅(용준) 담당이라 아직 없다. 여기 등록되기 전까지 그 이벤트는
    # DLX 로 간다 — 우리가 ACK 해버리면 용준 쪽에서 영영 못 받는다.
}


async def dispatch(event_type: str, body: bytes) -> None:
    """메시지 1건 처리. 예외를 던지면 호출부가 nack 한다.

    Raises:
        KeyError: 등록된 핸들러가 없는 `eventType`.
    """
    envelope = json.loads(body.decode("utf-8"))
    handler = HANDLERS[event_type]
    handler(envelope.get("payload", {}))


async def consume(*, queue_name: str = INBOUND_QUEUE) -> None:
    """`ai.inbound` 를 구독한다. **Manual ACK** (§10) — 처리에 성공한 것만 확인한다.

    무한 대기하므로 배치가 아니라 상시 프로세스로 띄운다. 종료는 취소(Ctrl+C)로.

    실패 처리:
      - 계약 위반·재료 부족 → `requeue=False` 로 nack → DLX. 다시 넣어도 같은 이유로
        실패하므로 재시도 루프만 돌게 된다.
      - 그 외(일시적 오류) → `requeue=True`. 인프라 공통 정책의 재시도 5회를 탄다.

    Raises:
        MqDisabledError: `MQ_ENABLED=false`.
    """
    from aio_pika import ExchangeType, connect_robust

    settings = get_settings()
    if not settings.mq_enabled:
        raise MqDisabledError("MQ_ENABLED=false 라 컨슈머를 띄우지 않습니다")

    connection = await connect_robust(
        host=settings.mq_host,
        port=settings.mq_port,
        login=settings.mq_user,
        password=settings.mq_password,
        virtualhost=settings.mq_vhost,
    )
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH)
        exchange = await channel.declare_exchange(
            settings.mq_exchange, ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=FEEDBACK_BINDING)
        logger.info("컨슈머 시작 queue=%s binding=%s", queue_name, FEEDBACK_BINDING)

        async with queue.iterator() as messages:
            async for message in messages:
                event_type = message.type or message.routing_key or ""
                try:
                    await dispatch(event_type, message.body)
                except (KeyError, ValidationError, HitlContextUnavailableError) as exc:
                    # 다시 넣어도 결과가 같은 실패 — 바로 DLX 로 보낸다.
                    logger.error("처리 불가 eventType=%s: %r", event_type, exc)
                    await message.nack(requeue=False)
                except Exception as exc:  # noqa: BLE001 - 일시적 오류는 재시도 대상
                    logger.warning(
                        "처리 실패(재시도) eventType=%s: %r", event_type, exc
                    )
                    await message.nack(requeue=True)
                else:
                    await message.ack()
