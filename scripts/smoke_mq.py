"""RabbitMQ 발행 스모크 테스트 — **실제 브로커에 붙여서** 한 바퀴 돌린다.

    docker compose up -d rabbitmq
    python scripts/smoke_mq.py

왜 필요한가
-----------
`tests/test_mq.py` 는 exchange 를 가짜로 바꿔 조립·직렬화·라우팅 키까지만 본다. 그래서
**브로커가 실제로 받아주는지는 검증되지 않는다** — `declare_exchange` 인자가 기존 선언과
어긋나거나(PRECONDITION_FAILED), `connect_robust` 파라미터가 틀렸거나, publisher confirm 이
안 걸려 있어도 유닛 테스트는 전부 통과한다.

특히 **토픽 exchange 는 바인딩된 큐가 없으면 메시지를 조용히 버린다.** 발행은 성공으로
찍히는데 아무도 못 받는 상태가 되므로, 이 스크립트는 큐를 먼저 만들어 `ai.#` 로 바인딩한
뒤 발행하고 **되받아서** 확인한다. 백엔드의 `main.inbound` 가 하는 일과 같다.

LLM 을 부르지 않는다(비용 0). 픽스처로 만든 알림·개선안·가이드라인을 쓴다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SMOKE_QUEUE = "smoke.ai.inbound"
"""검증 전용 큐. 운영 큐(`main.inbound`)는 백엔드 소유라 건드리지 않는다."""


def _build_fixtures():
    """알림 1건 + 개선안 1건 + 가이드라인 콜백 1건. import 는 env 설정 뒤에 한다."""
    from app.core.schemas import (
        Aspect,
        CallbackStatus,
        Channel,
        DetectionAlert,
        DetectionConfidence,
        DetectionStats,
        Evaluator,
        EvaluatorChecks,
        Evidence,
        GenerationCallback,
        Proposal,
        ProposalType,
        Recommendation,
        RecommendedAction,
        Source,
        SourceSignals,
        Verdict,
    )

    alert = DetectionAlert(
        alert_id="ALT-SMOKE-P001-COUPANG",
        detected_at=datetime(2026, 8, 28, 9, 0, tzinfo=timezone.utc),
        product_group_id="P001",
        channel=Channel.COUPANG,
        window_start=date(2026, 8, 22),
        window_end=date(2026, 8, 28),
        verdict=Verdict.BIASED,
        significant_channels=[Channel.COUPANG],
        main_aspect=Aspect.COLOR,
        stats=DetectionStats(
            source=Source.CS,
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=1e-4,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(cs=True, review=None, interpretation="CS 선행"),
        detection_confidence=DetectionConfidence.HIGH,
        scope_in=True,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        evidence=Evidence(inquiry_ids=["INQ-000412"]),
    )
    rec = Recommendation(
        recommendation_id="REC-smoke000001",
        alert_id=alert.alert_id,
        created_at=datetime(2026, 8, 28, 9, 0, 12, tzinfo=timezone.utc),
        proposal=Proposal(
            type=ProposalType.COPY_DRAFT,
            target_field="상세설명",
            current_text="현재 문구",
            proposed_text="모니터 환경에 따라 색상 차이가 있을 수 있습니다",
            rationale="사진 색감이 실물과 다르다는 문의 다수",
            detailpage_grounded=True,
        ),
        evaluator=Evaluator(
            passed=True,
            attempts=1,
            checks=EvaluatorChecks(
                grounding=True, consistency=True, actionability=True
            ),
        ),
    )
    callback = GenerationCallback(
        guideline_id="GD-SMOKE-P001-COUPANG",
        status=CallbackStatus.FAILED_ERROR,
        notice_message="스모크 테스트 — S3 미구성",
    )
    return alert, rec, callback


async def run_feedback(args: argparse.Namespace) -> int:
    """메인 → AI 방향(`feedback.#` → `ai.inbound`)을 실제 브로커로 한 바퀴 돌린다.

    백엔드 컨슈머가 없으므로 **우리가 메인 서버 흉내를 내서** 이벤트를 쏘고, 우리
    컨슈머가 그걸 실제로 꺼내 처리하는지 본다. 컬렉션2(Chroma)에 진짜로 쓰지는
    않는다 — 적재 함수를 가로채서 "여기까지 연결됐다"만 확인한다.
    """
    from aio_pika import DeliveryMode, Message, connect_robust

    import app.recommendation.pipeline as pipeline_module
    from app.config import get_settings
    from app.consumer import wire_handlers
    from app.core import mq, mq_consumer

    settings = get_settings()
    alert, rec, _callback = _build_fixtures()

    # 운영과 같은 배선을 탄다. 이걸 빼면 HANDLERS 가 비어 dispatch 가 KeyError 로 떨어진다
    # — 실제로 그렇게 한 번 깨졌다(2026-08-07).
    wire_handlers()

    recorded: list = []
    pipeline_module.record_hitl_outcome = lambda a, r: recorded.append(
        (a.alert_id, r.hitl_status.value)
    )

    payload = {
        "recommendation_id": rec.recommendation_id,
        "alert_id": alert.alert_id,
        "hitl_status": "반려",
        "hitl_feedback": {
            "processed_at": "2026-08-29T09:11:50Z",
            "processed_by": "seller_smoke",
            "rejection_reason": {
                "reason_code": "이미조치함",
                "reason_text": "지난주에 상세페이지 이미 수정했습니다",
            },
            "edited_text": None,
        },
        # ⚠️ 계약(§8)에 없는 확장 필드다. 없으면 컬렉션2 적재 자체가 불가능해서
        #    백엔드에 요청 중인 상태 — `_load_hitl_context()` 주석 참고.
        "alert": alert.model_dump(mode="json"),
        "recommendation": rec.model_dump(mode="json"),
    }
    envelope = {
        "eventId": "smoke-feedback-0001",
        "eventType": mq_consumer.RECOMMENDATION_REVIEWED,
        "occurredAt": "2026-08-29T09:12:00.000Z",
        "source": "main-server",
        "traceId": mq.new_trace_id(),
        "payload": payload,
    }

    connection = await connect_robust(
        host=settings.mq_host,
        port=settings.mq_port,
        login=settings.mq_user,
        password=settings.mq_password,
        virtualhost=settings.mq_vhost,
    )
    async with connection:
        channel = await connection.channel()
        # 운영과 같은 함수를 탄다 — 이 스크립트만 다른 방식으로 exchange 를 잡으면
        # 검증 대상이 실제 코드가 아니게 된다.
        exchange = await mq.resolve_exchange(channel, settings)
        # 운영 큐 이름을 그대로 쓰되 검증용으로 auto_delete. 바인딩은 실제와 같은 feedback.#
        queue = await channel.declare_queue(
            "smoke.feedback.inbound", durable=False, auto_delete=True
        )
        await queue.bind(exchange, routing_key=mq_consumer.FEEDBACK_BINDING)
        print(
            f"큐 smoke.feedback.inbound 준비 완료 (바인딩 {mq_consumer.FEEDBACK_BINDING})"
        )

        await exchange.publish(
            Message(
                json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                type=envelope["eventType"],
            ),
            routing_key=mq_consumer.RECOMMENDATION_REVIEWED,
        )
        print(f"  발행(메인 서버 흉내) {mq_consumer.RECOMMENDATION_REVIEWED}")

        try:
            async with asyncio.timeout(args.timeout):
                async with queue.iterator() as messages:
                    async for message in messages:
                        await mq_consumer.dispatch(
                            message.type or message.routing_key or "", message.body
                        )
                        await message.ack()
                        break
        except TimeoutError:
            print(f"\n❌ {args.timeout}초 안에 못 받았습니다")
            return 1

    if recorded != [(alert.alert_id, "반려")]:
        print(f"\n❌ 적재까지 도달하지 못했습니다: {recorded}")
        return 1

    print(f"\n  컬렉션2 적재 호출됨: {recorded[0]}")
    print("\n✅ 메인 → AI 수신 → HITL 적재까지 통과했습니다.")
    return 0


async def run(args: argparse.Namespace) -> int:
    from aio_pika import connect_robust

    from app.config import get_settings
    from app.core import mq

    settings = get_settings()
    alert, rec, callback = _build_fixtures()

    # 수신 쪽은 발행기와 **별도 연결**로 붙는다. 백엔드 컨슈머가 다른 프로세스인 것과 같게.
    connection = await connect_robust(
        host=settings.mq_host,
        port=settings.mq_port,
        login=settings.mq_user,
        password=settings.mq_password,
        virtualhost=settings.mq_vhost,
    )
    async with connection:
        channel = await connection.channel()
        # 운영과 같은 함수를 탄다 — 이 스크립트만 다른 방식으로 exchange 를 잡으면
        # 검증 대상이 실제 코드가 아니게 된다.
        exchange = await mq.resolve_exchange(channel, settings)
        # ⚠️ **발행 전에 바인딩한다.** 토픽 exchange 는 받는 큐가 없으면 메시지를 버리는데
        #    발행 쪽은 성공으로 찍힌다 — 순서를 바꾸면 이 스크립트가 통과해도 아무 의미가 없다.
        queue = await channel.declare_queue(
            SMOKE_QUEUE, durable=False, auto_delete=True
        )
        await queue.bind(exchange, routing_key="ai.#")
        print(f"큐 {SMOKE_QUEUE} 준비 완료 (바인딩 ai.#)")

        trace_id = mq.new_trace_id()
        print(f"trace_id={trace_id}")

        await mq.publish_anomaly_analyzed(alert, rec, trace_id)
        print("  발행 ai.anomaly.analyzed")
        await mq.publish_guideline_generated(callback, trace_id)
        print("  발행 ai.guideline.generated")

        received = []
        try:
            async with asyncio.timeout(args.timeout):
                async with queue.iterator() as messages:
                    async for message in messages:
                        async with message.process():
                            received.append(
                                (
                                    message.routing_key,
                                    json.loads(message.body.decode("utf-8")),
                                )
                            )
                        if len(received) == 2:
                            break
        except TimeoutError:
            print(
                f"\n❌ {args.timeout}초 안에 2건을 못 받았습니다 (받은 건 {len(received)}건)"
            )

        await mq.close_mq()

    return _report(received, trace_id)


def _report(received: list, trace_id: str) -> int:
    """되받은 메시지를 계약(docs/mq_events.md §3)과 대조한다. 반환값은 종료 코드."""
    print(f"\n수신 {len(received)}건")
    problems: list[str] = []

    keys_by_routing = {routing: envelope for routing, envelope in received}
    for expected in ("ai.anomaly.analyzed", "ai.guideline.generated"):
        if expected not in keys_by_routing:
            problems.append(f"{expected} 를 못 받았습니다")

    for routing, envelope in received:
        print(f"\n  [{routing}] eventId={envelope.get('eventId')}")
        print(
            f"    occurredAt={envelope.get('occurredAt')} source={envelope.get('source')}"
            f" companyId={envelope.get('companyId')}"
        )
        missing = {
            "eventId",
            "eventType",
            "occurredAt",
            "source",
            "traceId",
            "companyId",
            "payload",
        } - set(envelope)
        if missing:
            problems.append(f"{routing}: Envelope 필드 누락 {sorted(missing)}")
        if envelope.get("traceId") != trace_id:
            problems.append(f"{routing}: traceId 불일치")
        if envelope.get("eventType") != routing:
            problems.append(f"{routing}: eventType 과 라우팅 키가 다릅니다")
        if not envelope.get("companyId"):
            problems.append(f"{routing}: companyId 가 비어 있습니다")

        payload = envelope.get("payload", {})
        # 한글 enum 이 깨지지 않고 왕복하는지 — ensure_ascii=False 로 보내고 utf-8 로 읽는다.
        preview = {k: payload[k] for k in list(payload)[:4]}
        print(f"    payload(앞 4개)={json.dumps(preview, ensure_ascii=False)}")
        if routing == "ai.anomaly.analyzed":
            if payload.get("main_aspect") != "색상":
                problems.append("한글 enum 이 깨졌습니다 (main_aspect)")
            if (payload.get("recommendation") or {}).get("hitl_status") != "대기":
                problems.append(
                    "recommendation 이 안 실렸거나 hitl_status 가 대기가 아닙니다"
                )
        if (
            routing == "ai.guideline.generated"
            and payload.get("alert_id") != "ALT-SMOKE-P001-COUPANG"
        ):
            problems.append("guideline payload 의 alert_id 파생이 틀렸습니다")

    if problems:
        print("\n❌ 계약 위반")
        for p in problems:
            print(f"   - {p}")
        return 1

    print("\n✅ 발행 → 수신 → 계약 대조까지 통과했습니다.")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5672)
    ap.add_argument("--user", default="sellon")
    ap.add_argument("--password", default="sellon")
    ap.add_argument("--timeout", type=float, default=10.0, help="수신 대기 상한(초)")
    ap.add_argument(
        "--feedback",
        action="store_true",
        help="메인 → AI 방향(feedback.# → ai.inbound)을 검증한다. 기본은 AI → 메인.",
    )
    args = ap.parse_args()

    # 윈도우 콘솔 기본 코드페이지(cp949)는 한글은 되지만 em대시·`❌` 같은 문자에서 터진다.
    # payload 를 그대로 찍는 스크립트라 내용에 따라 검증이 아니라 출력에서 죽는다.
    # 원래 여기서 stdout 만 직접 돌렸는데, 같은 처리가 배치·셋업 스크립트에도 필요해져
    # 공용 헬퍼로 뺐다(stderr 까지 함께 돌린다). app/core/console.py 참고.
    # import 가 함수 안인 이유는 이 파일의 다른 app import 와 같다 — sys.path 조정 뒤여야 한다.
    from app.core.console import force_utf8_output

    force_utf8_output()

    # get_settings() 는 lru_cache 라 **import 전에** 넣어야 반영된다. .env 를 고치지 않고
    # 이 스크립트만으로 검증할 수 있게 하려는 것 — 운영 기본값은 MQ_ENABLED=false 다.
    os.environ["MQ_ENABLED"] = "true"
    # 로컬 브로커엔 exchange 가 없으니 기본은 우리가 만든다. 운영과 같은 조건(있는 걸
    # 확인만)으로 돌려보려면 MQ_DECLARE_TOPOLOGY=false 를 주면 된다 — setdefault 라
    # 밖에서 준 값이 이긴다.
    os.environ.setdefault("MQ_DECLARE_TOPOLOGY", "true")
    # 로컬 브로커는 기본 vhost 가 "/" 다. 운영은 "/app" (MQ 컨벤션 §2.1 CRD).
    os.environ.setdefault("MQ_VHOST", "/")
    # 발행기가 companyId 없이는 안 보낸다 — 검증용 더미. 실제 값은 백엔드 대기 중.
    os.environ.setdefault("MQ_COMPANY_ID", "SLN-smoketest")
    os.environ["MQ_HOST"] = args.host
    os.environ["MQ_PORT"] = str(args.port)
    os.environ["MQ_USER"] = args.user
    os.environ["MQ_PASSWORD"] = args.password

    sys.exit(asyncio.run(run_feedback(args) if args.feedback else run(args)))


if __name__ == "__main__":
    main()
