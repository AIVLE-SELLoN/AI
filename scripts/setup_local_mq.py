"""로컬 RabbitMQ 에 **백엔드 소유 토폴로지의 대역**을 만든다. 로컬 전용.

    docker compose up -d rabbitmq
    python scripts/setup_local_mq.py

왜 필요한가
-----------
`app/core/mq.py` 는 exchange 만 다루고 **큐를 만들지 않는다** — 운영에서 `main.inbound` ·
`ai.inbound` 는 백엔드 인프라(Messaging Topology Operator CRD)가 만들고, 우리가 다른
인자로 선언하면 `PRECONDITION_FAILED` 로 거부당하기 때문이다(docs/mq_events.md §2-1).

그래서 **빈 로컬 브로커에는 `ai.#` 를 받는 큐가 하나도 없다.** 토픽 exchange 는 바인딩된
큐가 없으면 메시지를 버리므로, 이 스크립트를 안 돌리고 배치를 돌리면 발행이 전부
`MqPublishError` (unroutable) 로 실패한다. **동작은 정상이다** — `_require_ack()` 가 그걸
잡으라고 있는 것이고, 조용히 성공하던 예전이 오히려 버그였다.

`scripts/smoke_mq.py` 는 자기 큐를 `auto_delete=True` 로 만들어 끝나면 지운다(검증 도구가
환경을 남기면 안 되므로 그게 맞다). 그래서 재현 가능한 로컬 환경은 이 스크립트가 맡는다.

만드는 것
---------
    main.inbound  ← ai.#        (백엔드 큐의 대역 — 우리 발행이 여기 꽂힌다)
    ai.inbound    ← feedback.#  (우리 컨슈머가 읽는 큐, 계약과 같은 이름·바인딩)

운영에서는 **둘 다 우리가 만들면 안 된다.** 그래서 `MQ_DECLARE_TOPOLOGY=true` 가 아니면
아예 거부한다 — 실수로 운영 브로커를 향해 돌리는 걸 막는 유일한 방어선이다.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MAIN_INBOUND = "main.inbound"
AI_BINDING = "ai.#"


async def setup() -> None:
    from aio_pika import ExchangeType, connect_robust

    from app.config import get_settings
    from app.core.mq_consumer import FEEDBACK_BINDING, INBOUND_QUEUE

    settings = get_settings()
    if not settings.mq_declare_topology:
        raise SystemExit(
            "MQ_DECLARE_TOPOLOGY=false 입니다 — 이 스크립트는 로컬 전용입니다.\n"
            "운영 토폴로지는 백엔드 인프라가 소유하고, 우리가 큐를 만들면 "
            "PRECONDITION_FAILED 로 거부당합니다 (docs/mq_events.md §2-1)."
        )

    connection = await connect_robust(
        host=settings.mq_host,
        port=settings.mq_port,
        login=settings.mq_user,
        password=settings.mq_password,
        virtualhost=settings.mq_vhost,
    )
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            settings.mq_exchange, ExchangeType.TOPIC, durable=True
        )
        print(f"exchange {settings.mq_exchange} (topic, durable)")

        for queue_name, binding in (
            (MAIN_INBOUND, AI_BINDING),
            (INBOUND_QUEUE, FEEDBACK_BINDING),
        ):
            queue = await channel.declare_queue(queue_name, durable=True)
            await queue.bind(exchange, routing_key=binding)
            print(f"  {queue_name} ← {binding}")

    print(
        f"\n완료. vhost={settings.mq_vhost} — 이제 배치가 발행하면 {MAIN_INBOUND} 로 들어갑니다."
    )


if __name__ == "__main__":
    asyncio.run(setup())
