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

운영에서는 **둘 다 우리가 만들면 안 된다.** 그래서 실행 전에 두 가지를 본다
(`assert_local_broker`) — `MQ_DECLARE_TOPOLOGY=true` **그리고** 브로커 호스트가 로컬인지.
플래그 하나만 보면 운영 접속정보를 넣은 채 플래그를 안 내린 순간 그대로 통과한다.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MAIN_INBOUND = "main.inbound"
AI_BINDING = "ai.#"

LOCAL_BROKER_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "::1", "rabbitmq", "host.docker.internal"}
)
"""로컬로 인정하는 브로커 호스트.

`rabbitmq` 는 docker-compose 서비스명, `host.docker.internal` 은 컨테이너 안에서 호스트를
가리키는 이름이다. 그 밖은 전부 "우리 것이 아닐 수 있다" 로 본다 — **허용 목록이지
차단 목록이 아니다.** 운영 호스트명을 미리 알 수 없으니 반대로는 못 막는다.
"""


def assert_local_broker(settings) -> None:
    """로컬 브로커가 맞는지 확인한다. 아니면 아무것도 만들지 않고 멈춘다.

    **두 가지를 다 본다.** 예전엔 `MQ_DECLARE_TOPOLOGY` 하나만 봤는데, 그 플래그는
    "우리가 토폴로지를 만든다" 는 뜻이지 "여기가 로컬이다" 라는 뜻이 아니다. 운영
    접속정보(C1)를 `.env` 에 넣으면서 플래그를 같이 안 내리면 그대로 통과해서 **운영
    브로커에 우리 큐와 바인딩이 생긴다.** 그때 나는 사고는 조용하다 — 큐가 만들어지고
    스크립트는 성공으로 끝나며, 백엔드가 나중에 정상 토폴로지를 올릴 때
    `PRECONDITION_FAILED` 로 처음 드러난다.

    Raises:
        SystemExit: 로컬이 아니거나 플래그가 꺼져 있을 때.
    """
    if not settings.mq_declare_topology:
        raise SystemExit(
            "MQ_DECLARE_TOPOLOGY=false 입니다. 이 스크립트는 로컬 전용입니다.\n"
            "운영 토폴로지는 백엔드 인프라가 소유하고, 우리가 큐를 만들면 "
            "PRECONDITION_FAILED 로 거부당합니다 (docs/mq_events.md §2-1)."
        )

    host = (settings.mq_host or "").strip().lower()
    if host not in LOCAL_BROKER_HOSTS:
        raise SystemExit(
            f"MQ_HOST={settings.mq_host!r} 는 로컬 브로커가 아닙니다. 중단합니다.\n"
            "이 스크립트는 백엔드 소유 큐(main.inbound·ai.inbound)를 만들기 때문에, "
            "운영 브로커에 돌리면 남의 토폴로지를 우리 인자로 선점하게 됩니다.\n"
            f"로컬로 인정하는 호스트: {', '.join(sorted(LOCAL_BROKER_HOSTS))}\n"
            "⚠️ 운영으로 전환할 때는 접속정보 4개만 바꾸면 안 됩니다 — "
            "MQ_DECLARE_TOPOLOGY=false 와 MQ_VHOST 도 같이 맞추세요."
        )


async def setup() -> None:
    from aio_pika import connect_robust

    from app.config import get_settings
    from app.core import mq
    from app.core.mq_consumer import FEEDBACK_BINDING, INBOUND_QUEUE

    settings = get_settings()
    # ⚠️ **연결보다 먼저**. 확인이 연결 뒤에 있으면 운영 브로커에 접속은 이미 한 뒤다.
    assert_local_broker(settings)

    connection = await connect_robust(
        host=settings.mq_host,
        port=settings.mq_port,
        login=settings.mq_user,
        password=settings.mq_password,
        virtualhost=settings.mq_vhost,
    )
    async with connection:
        channel = await connection.channel()
        # 운영과 같은 함수를 탄다 — 여기서만 직접 declare 하면 exchange 인자(TOPIC·durable)를
        # 아는 곳이 둘로 갈려서, 한쪽만 바뀌면 로컬이 PRECONDITION_FAILED 로 막힌다.
        # 가드를 통과했다는 건 mq_declare_topology=True 라 어차피 declare 분기를 탄다는 뜻이다.
        # (smoke_mq.py 가 같은 자리에서 같은 이유로 이 함수를 쓴다.)
        exchange = await mq.resolve_exchange(channel, settings)
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
    # ⚠️ 출력보다 먼저. 완료 메시지의 `—`(U+2014)가 cp949 에 없어서, 큐를 다 만들어 놓고
    #    마지막 print 에서 죽어 **성공이 exit 1 로 보고**됐다. 그러면 종료코드가 성공과
    #    "가드에 막혀 아무것도 안 함"을 구분하지 못한다. (app/core/console.py 참고)
    from app.core.console import force_utf8_output

    force_utf8_output()
    asyncio.run(setup())
