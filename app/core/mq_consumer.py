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

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

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


def load_hitl_context(
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


HANDLERS: dict[str, Callable[[dict], None]] = {}
"""eventType → 처리 함수. **비어 있는 채로 시작한다.**

core 가 컴포넌트(`app/recommendation/` 등)를 import 하면 의존 방향이 거꾸로 뒤집힌다
(팀 규칙: 각 컴포넌트가 core 에서 가져다 쓴다). 그래서 core 는 "무엇을 처리할지"를
모르고, 실행 진입점(`app/consumer.py`)이 시작할 때 등록해 준다.

`feedback.report.created`(리포팅) 도 같은 방식으로 붙이면 된다. 등록되기 전까지 그
이벤트는 DLX 로 간다 — 우리가 ACK 해버리면 담당자가 영영 못 받는다."""


def register_handler(event_type: str, handler: Callable[[dict], None]) -> None:
    """이벤트 처리 함수를 등록한다. 같은 `event_type` 이면 덮어쓴다."""
    HANDLERS[event_type] = handler


async def dispatch(event_type: str, body: bytes) -> None:
    """메시지 1건 처리. 예외를 던지면 호출부가 nack 한다.

    핸들러는 **워커 스레드에서 돌린다.** 등록된 처리 함수가 동기 함수인데 안에서
    블로킹 I/O(컬렉션2 Chroma 쓰기)를 하기 때문이다 — 이벤트 루프에서 직접 부르면
    그동안 하트비트를 못 보내 브로커가 커넥션을 끊을 수 있다. 순서는 그대로 유지된다
    (여기서 await 하므로 한 번에 한 건). (서영님 PR 리뷰 §3, 2026-08-07)

    Raises:
        KeyError: 등록된 핸들러가 없는 `eventType`.
    """
    envelope = json.loads(body.decode("utf-8"))
    handler = HANDLERS[event_type]
    await asyncio.to_thread(handler, envelope.get("payload", {}))


async def resolve_queue(channel: Any, queue_name: str, settings: Any) -> Any:
    """`ai.inbound` 큐를 얻는다. **우리 큐가 아니다 — 다시 선언하지 않는다.**

    계약(§2-1)상 `ai.inbound` 는 **이미 있는 큐이고 우리는 바인딩만 추가**한다. 백엔드가
    quorum 타입에 DLX·delivery-limit·TTL 을 걸어 만들어 뒀는데 우리가 맨 인자로
    `declare` 하면 브로커가 `PRECONDITION_FAILED` 로 거부해 컨슈머가 아예 못 뜬다.
    로컬에선 우리가 만든 큐라 이 사고가 안 나서 조용히 통과한다 — 실제로 붙일 때 터진다.
    """
    if settings.mq_declare_topology:
        return await channel.declare_queue(queue_name, durable=True)
    return await channel.get_queue(queue_name, ensure=True)


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
    from aio_pika import connect_robust

    from app.core.mq import resolve_exchange

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
        exchange = await resolve_exchange(channel, settings)
        queue = await resolve_queue(channel, queue_name, settings)
        # 바인딩도 인프라(Messaging Topology Operator CRD)가 건다 —
        # `ai-inbound-feedback-binding` 이 그것이다(MQ 컨벤션 §2.1). 우리 AI 계정에
        # 바인딩 권한이 없을 수 있으므로 운영에서는 시도하지 않는다.
        if settings.mq_declare_topology:
            await queue.bind(exchange, routing_key=FEEDBACK_BINDING)
        logger.info("컨슈머 시작 queue=%s binding=%s", queue_name, FEEDBACK_BINDING)

        async with queue.iterator() as messages:
            async for message in messages:
                event_type = message.type or message.routing_key or ""
                try:
                    await dispatch(event_type, message.body)
                except (KeyError, ValueError, HitlContextUnavailableError) as exc:
                    # 다시 넣어도 결과가 같은 실패 — 바로 DLX 로 보낸다.
                    #
                    # ValueError 가 덮는 범위: 깨진 JSON(JSONDecodeError) · 계약 위반
                    # (pydantic ValidationError) · record_hitl_outcome 의 alert_id 불일치와
                    # hitl_status 대기. 셋 다 ValueError 서브클래스이고 전부 재전달해도
                    # 같은 결과다. 재시도 대상으로 두면 delivery-limit 5 를 다 태운 뒤에야
                    # DLX 로 가고, 로컬 classic 큐는 그 상한이 없어 무한 재전달이 된다.
                    logger.error("처리 불가 eventType=%s: %r", event_type, exc)
                    await message.nack(requeue=False)
                except Exception as exc:  # noqa: BLE001 - 일시적 오류는 재시도 대상
                    logger.warning(
                        "처리 실패(재시도) eventType=%s: %r", event_type, exc
                    )
                    await message.nack(requeue=True)
                else:
                    await message.ack()
