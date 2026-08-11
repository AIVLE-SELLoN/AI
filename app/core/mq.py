"""담당: 지인 — RabbitMQ 발행기. 계약 정본은 `docs/mq_events.md`.

리포팅(용준)·배치(서영)가 같이 쓰므로 컴포넌트 폴더가 아니라 core 에 둔다.

발행 계약 3가지:

1. **안 보낸 것을 보냈다고 하지 않는다.** 호출부(`app/batch/daily.py`)는 예외가 없으면
   발행 성공으로 보고 그 알림을 `prior_alerts` 캐시에 넣는다 — 캐시에 들어가면
   `RENOTIFY_BLOCK_DAYS` 동안 재알림이 억제되므로, 실제로 안 나간 메시지를 조용히
   성공 처리하면 셀러가 그 알림을 7일간 못 본다. 그래서 `MQ_ENABLED=false` 는
   no-op 이 아니라 `MqDisabledError` 다 (용준 `S3_ENABLED=false` 와 같은 원칙).
2. **`trace_id` 는 인자로만 받는다.** 여기서 만들면 배치 1회 = `traceId` 1개 규약이
   깨진다(§3). 배치가 `new_trace_id()` 로 한 번 만들어 전달한다.
3. **Envelope 은 camelCase, payload 안은 snake_case** (§3). 발행 단위는 알림 1건당
   메시지 1개 — 배열로 묶지 않는다(멱등 키가 건별이라 묶으면 1건 실패로 전체가 재처리).

조립(`build_*`)과 전송(`publish_*`)을 나눠 뒀다. **브로커 없이도 payload 모양을 검증할
수 있어야** 하기 때문이다 — 백엔드 접속 정보(C1)가 오기 전에도 계약 준수는 확인된다.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.core.exceptions import MqConfigError, MqDisabledError, MqPublishError
from app.core.ids import ALERT_ID_PREFIX, GUIDELINE_ID_PREFIX
from app.core.schemas import (
    CallbackStatus,
    DetectionAlert,
    GenerationCallback,
    Recommendation,
)

logger = logging.getLogger(__name__)

SOURCE = "ai-server"
"""Envelope 의 `source`. AI 노드가 발행한 것임을 표시한다(§3)."""

ANOMALY_ANALYZED = "ai.anomaly.analyzed"
GUIDELINE_GENERATED = "ai.guideline.generated"
REPORT_GENERATED = "ai.report.generated"
"""라우팅 키. `ai.#` 바인딩으로 `main.inbound` 에 꽂힌다(§2-1).

⚠️ `ai.anomaly.detected` 는 구 이름이다(2026-08-03 개명). 탐지만이 아니라 개선안까지
   끝난 상태라 `analyzed` 가 맞다 — 백엔드 데이터플로우 문서에 구 이름이 남아 있다."""

_connection: Any = None
_channel: Any = None
"""배치 1회가 메시지 수십 건을 보내므로 연결을 재사용한다. 건당 연결은 핸드셰이크가
메시지보다 비싸다. `close_mq()` 로 닫는다."""


def new_trace_id() -> str:
    """배치 1회분의 `traceId`. 그 배치가 발행하는 모든 메시지가 이 값을 공유한다(§3)."""
    return f"trace-{uuid.uuid4().hex[:16]}"


def _now_iso() -> str:
    """`occurredAt` 형식 — 밀리초까지, UTC, `Z` 접미(§3 예시와 동일)."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_envelope(
    event_type: str, payload: dict, trace_id: str, *, company_id: str = ""
) -> dict:
    """공통 Envelope 조립. **키는 camelCase, payload 안은 손대지 않는다**(§3).

    `occurredAt` 은 **발행 시각**이지 탐지 시각이 아니다 — 개선안 생성에 5~20초가
    걸려서 `payload.detected_at` 과 몇 분씩 벌어진다. 화면의 "탐지 시각"은 payload 쪽이다.

    `companyId` 는 백엔드가 회사 구분용으로 추가한 필드다(§3). 값은 배포마다 고정이라
    설정에서 온다 — `_publish()` 가 채워 넣고, 여기서는 받은 값을 그대로 싣는다.
    """
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "occurredAt": _now_iso(),
        "source": SOURCE,
        "traceId": trace_id,
        "companyId": company_id,
        "payload": payload,
    }


def build_anomaly_payload(alert: DetectionAlert, rec: Recommendation | None) -> dict:
    """`ai.anomaly.analyzed` payload — 알림 전 필드 + 개선안 1건(§4).

    `rec` 는 `recommended_action == "개선안 생성"` 일 때만 채워지고, 그 외 조치 6종에서는
    `"recommendation": null` 로 나간다. 키를 빼지 않고 명시적 null 을 보낸다 — 소비 측이
    "필드가 없음"과 "개선안이 없음"을 구분할 필요가 없게.
    """
    payload = alert.model_dump(mode="json")
    payload["recommendation"] = rec.model_dump(mode="json") if rec else None
    return payload


def build_guideline_payload(callback: GenerationCallback) -> dict:
    """`ai.guideline.generated` payload(§6). 멱등 키는 `guideline_id`.

    `alert_id` 는 `GenerationCallback` 에 없어서 따로 채운다 — `_alert_id_of()` 참고.

    Raises:
        ValueError: `guideline_id` 가 없을 때. `report_id` 만 있으면 월간 리포트라
            라우팅 키가 `ai.report.generated` 여야 한다 — 잘못 실으면 백엔드가 CS팀에
            메일을 쏘고 JSONB 에 적재한다(§6 소비자 동작).
    """
    if callback.guideline_id is None:
        raise ValueError(
            "guideline_id 가 없습니다 — 월간 리포트는 ai.report.generated 로 발행할 것"
        )

    payload = callback.model_dump(mode="json")
    payload.pop("report_id", None)  # 가이드라인 payload 에 없는 필드(§6)
    payload["alert_id"] = _alert_id_of(callback)
    return payload


MONTHLY_EVENT_STATUSES = frozenset(
    {
        CallbackStatus.SUCCESS.value,
        CallbackStatus.FAILED_SIZE_EXCEEDED.value,
        CallbackStatus.FAILED_ERROR.value,
    }
)
"""`ai.report.generated` 에 실릴 수 있는 status **3종**(§5).

`HOLD_INSUFFICIENT_DATA`·`FAILED_VALIDATION` 은 여기 못 온다 — 둘 다 **상품 단위** 판정인데
이벤트는 월 단위이기 때문이다. 보류·검증실패 상품은 합본에서 빠지고 그 사실이
`notice_message` 로 나간다. (두 상태는 `POST /api/v1/reports` 상품 1건 REST 에서는 살아 있다.)
"""


def build_report_payload(callback: GenerationCallback, report_month: str) -> dict:
    """`ai.report.generated` payload(§5). 멱등 키는 `report_id` — **월 1건**이다.

    ⚠️ `report_month` 를 인자로 받는 이유: `GenerationCallback` 에 없는 필드인데 §5 가
       payload 에 요구한다. `report_id`(`RPT-202607`)에서 잘라 쓸 수도 있지만 그러지
       않는다 — `_alert_id_of()` 주석과 같은 이유로, **정본이 손에 있으면 재구성하지
       않는다.** 배치는 `args.month` 를 이미 들고 있다.

    ⚠️ `source_payload` 는 싣지 않는다. 월간은 PDF 가 유일 산출물이라 콜백 자체가
       그 필드를 안 채우지만(§3-2), 나중에 채워지더라도 이 이벤트로는 안 나가게 막는다.

    Raises:
        ValueError: `report_id` 가 없을 때(= CS 가이드라인 산출물), 또는 status 가 월 단위
            이벤트에 올 수 없는 값일 때.
    """
    # `GenerationCallback` 스키마가 report_id / guideline_id 를 **배타**로 강제한다
    # (정확히 하나). 그래서 이 검사 하나가 양방향을 다 막는다 — guideline_id 가 채워진
    # 산출물은 report_id 가 None 이라 여기서 걸린다. 뒤바뀌면 백엔드가 엉뚱한 소비
    # 동작(CS팀 메일 발송)을 타므로 반드시 막아야 한다(§6).
    if callback.report_id is None:
        raise ValueError(
            "report_id 가 없습니다 — 월간 합본 콜백이 아닙니다. "
            "guideline_id 가 있는 산출물은 ai.guideline.generated 로 발행할 것"
        )
    if callback.status.value not in MONTHLY_EVENT_STATUSES:
        raise ValueError(
            f"'{callback.status.value}' 는 월 단위 이벤트에 실을 수 없는 상태입니다"
            f"(허용: {sorted(MONTHLY_EVENT_STATUSES)}) — 상품 단위 판정은 notice_message 로 나간다"
        )

    payload = callback.model_dump(mode="json")
    payload.pop("guideline_id", None)  # 월간 payload 에 없는 필드(§5)
    payload.pop("source_payload", None)  # 월간은 원본을 보관하지 않는다
    payload["report_month"] = report_month
    return payload


def _alert_id_of(callback: GenerationCallback) -> str:
    """콜백에서 `alert_id` 를 얻는다. **정본이 있으면 재구성하지 않는다.**

    `source_payload["input"]` 이 `CSGuidelineInput` 원본이고 거기 `alert_id` 가 필수
    필드로 들어 있다(`app/reporting/callback.py`). 그게 정본이므로 먼저 본다.

    ⚠️ 접두어 치환은 **`source_payload` 가 없는 실패 경로 전용 폴백**이다
    (`source_payload` 는 SUCCESS 일 때만 실린다, §3-2). `build_guideline_id()` 는
    `ALT-` 로 시작하지 않는 입력을 통째로 뒤에 붙이므로 그 경우 역변환이 원본과
    달라진다 — 실제 `alert_id` 는 항상 `ALT-` 로 시작해서 지금은 문제가 없지만,
    되돌릴 수 있다고 가정하지 말 것.
    """
    source = callback.source_payload or {}
    input_data = source.get("input")
    if isinstance(input_data, dict) and input_data.get("alert_id"):
        return str(input_data["alert_id"])

    guideline_id = str(callback.guideline_id)
    if guideline_id.startswith(GUIDELINE_ID_PREFIX):
        return f"{ALERT_ID_PREFIX}{guideline_id[len(GUIDELINE_ID_PREFIX) :]}"
    return guideline_id


async def publish_anomaly_analyzed(
    alert: DetectionAlert,
    rec: Recommendation | None,
    trace_id: str,
) -> None:
    """`ai.anomaly.analyzed` 발행 — 알림 1건 + 개선안 1건(없으면 null). 멱등 키 `alert_id`.

    Args:
        alert: 탐지 알림. payload 최상위가 이 필드들 그대로다(§4-1).
        rec: 개선안. 게이트 밖이면 None.
        trace_id: 배치가 만든 값. 이 함수가 생성하지 않는다.

    Raises:
        MqDisabledError: `MQ_ENABLED=false`. **삼키지 말 것** — 안 나간 알림이다.
        MqPublishError: 접속·발행 실패.
    """
    await _publish(
        ANOMALY_ANALYZED,
        build_anomaly_payload(alert, rec),
        trace_id,
        key=alert.alert_id,
    )


async def publish_guideline_generated(
    callback: GenerationCallback,
    trace_id: str,
) -> None:
    """`ai.guideline.generated` 발행 — CS 가이드라인 1건. 멱등 키 `guideline_id`.

    알림 1건마다 트리거된다(월간 리포트와 생명주기가 다르다, §6).

    Args:
        callback: 용준 `generate_guideline()` 산출물. `guideline_id` 가 있어야 한다.
        trace_id: 배치가 만든 값.

    Raises:
        ValueError: `guideline_id` 가 없을 때(= 월간 리포트).
        MqDisabledError: `MQ_ENABLED=false`.
        MqPublishError: 접속·발행 실패.
    """
    payload = build_guideline_payload(callback)
    await _publish(
        GUIDELINE_GENERATED, payload, trace_id, key=str(callback.guideline_id)
    )


async def publish_report_generated(
    callback: GenerationCallback,
    report_month: str,
    trace_id: str,
) -> None:
    """`ai.report.generated` 발행 — 월간 합본 1건. 멱등 키 `report_id`(`RPT-202607`).

    **월 1건이다.** 상품별로 나가지 않는다 — PDF 가 전 상품을 합친 1개라 콜백도 이벤트도
    월 단위다. 같은 달을 다시 돌려도 `report_id` 가 같아 메인이 upsert 하면 된다.

    Args:
        callback: `compile_and_upload_monthly_book()` 산출물. `report_id` 가 있어야 한다.
        report_month: `YYYY-MM`. 콜백에 없는 값이라 배치가 넘긴다(`build_report_payload` 참고).
        trace_id: 배치가 만든 값.

    Raises:
        ValueError: `report_id` 가 없거나 `guideline_id` 가 채워져 있을 때, 또는 상품 단위
            상태(HOLD·FAILED_VALIDATION)를 실으려 할 때.
        MqDisabledError: `MQ_ENABLED=false`.
        MqPublishError: 접속·발행 실패.
    """
    payload = build_report_payload(callback, report_month)
    await _publish(REPORT_GENERATED, payload, trace_id, key=str(callback.report_id))


# ── 전송 ─────────────────────────────────────────────────────────


async def _publish(event_type: str, payload: dict, trace_id: str, *, key: str) -> None:
    """Envelope 을 실어 보낸다. `key` 는 멱등 키(로그용).

    Publisher Confirm 을 켜고 보낸다 — 확인 없이 성공을 반환하면 호출부가 그 알림을
    발행 성공으로 캐시에 넣어 재알림이 7일간 억제된다.
    """
    settings = get_settings()
    if not settings.mq_enabled:
        raise MqDisabledError(
            f"MQ_ENABLED=false 라 {event_type}({key}) 를 발행하지 않았습니다"
        )
    if not settings.mq_company_id:
        # 빈 값으로 내보내면 백엔드 DB 에 회사 미상 행이 쌓인다. 나중에 어느 회사 것인지
        # 복구할 방법이 없으므로(발행 시각 말고 단서가 없다) 아예 안 보낸다.
        raise MqConfigError(
            f"MQ_COMPANY_ID 가 비어 있어 {event_type}({key}) 를 발행하지 않았습니다 "
            "(MQ 컨벤션 §3 — Envelope 의 companyId 는 배포마다 고정값)"
        )

    envelope = build_envelope(
        event_type, payload, trace_id, company_id=settings.mq_company_id
    )
    body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    try:
        exchange = await _get_exchange(settings)
        message = _build_message(envelope, body)
        confirmation = await exchange.publish(
            message, routing_key=event_type, timeout=settings.mq_publish_timeout_seconds
        )
    except MqConfigError:
        # 설정 오류는 재시도 대상이 아니다. MqPublishError 로 싸면 "다음 배치가 다시
        # 시도한다" 는 뜻이 되는데, 플래그를 안 고치는 한 영원히 같은 자리에서 실패한다.
        # 위 MQ_COMPANY_ID 검사가 이미 MqConfigError 로 나가고 있어 그쪽과 짝을 맞춘다.
        raise
    except Exception as exc:
        # 연결은 닫지 않는다. connect_robust 가 스스로 복구하고, 채널이 브로커 오류로
        # 닫혔으면 _get_exchange 가 다음 호출에서 새로 연다. 여기서 끊으면 그 복구를
        # 우리가 없애는 꼴이라, 일시적 실패가 배치 전체 실패로 번진다.
        raise MqPublishError(f"{event_type}({key}) 발행 실패: {exc!r}") from exc

    _require_ack(confirmation, event_type, key)

    logger.info(
        "발행 %s key=%s eventId=%s trace=%s",
        event_type,
        key,
        envelope["eventId"],
        trace_id,
    )


def _require_ack(confirmation: Any, event_type: str, key: str) -> None:
    """브로커가 **큐에 넣었다**고 확인했는지 본다. `Basic.Ack` 이 아니면 발행 실패다.

    ⚠️ **토픽 exchange 는 바인딩된 큐가 없으면 메시지를 조용히 버린다.** 그때 aio_pika 는
    예외를 던지지 않고 `Basic.Return` 을 담은 `DeliveredMessage` 를 돌려준다 —
    2026-08-07 로컬 브로커로 실측했다(라우팅되면 `Basic.Ack`, 안 되면 `Return`).
    반환값을 안 보면 "발행 성공"으로 넘어가고, 호출부(`app/batch/daily.py`)가 그 알림을
    `prior_alerts` 캐시에 넣어 `RENOTIFY_BLOCK_DAYS` 동안 재알림이 막힌다 —
    **셀러가 그 알림을 영영 못 본다.**

    이건 가정이 아니라 실제로 일어날 시나리오다: 백엔드가 `main.inbound` 바인딩을 아직
    안 걸었거나, 라우팅 키를 바꿨거나(`ai.anomaly.detected` 구 이름이 문서에 남아 있다),
    `ai.#` 대신 더 좁은 패턴을 걸면 전부 여기로 떨어진다. **접속이 되는 것과 배달이 되는
    것은 다르다** — publisher confirm 은 "브로커가 받았다"까지만 보증한다.
    """
    from pamqp.commands import Basic

    if isinstance(confirmation, Basic.Ack):
        return

    delivery = getattr(confirmation, "delivery", confirmation)
    if isinstance(delivery, Basic.Return):
        raise MqPublishError(
            f"{event_type}({key}) 가 어느 큐에도 도착하지 않았습니다 "
            f"(unroutable: {delivery.reply_text}). exchange={_exchange_name()} 에 "
            f"'{event_type}' 을 받는 바인딩이 있는지 확인할 것 — 계약상 main.inbound 가 "
            "'ai.#' 로 바인딩돼 있어야 합니다 (docs/mq_events.md §2-1)"
        )
    raise MqPublishError(
        f"{event_type}({key}) 를 브로커가 확인하지 않았습니다: {confirmation!r}"
    )


def _exchange_name() -> str:
    """오류 메시지용. 설정을 못 읽어도 메시지는 나와야 한다."""
    try:
        return get_settings().mq_exchange
    except Exception:  # noqa: BLE001 - 진단 문구를 만들다 진짜 오류를 가리면 안 된다
        return "?"


def _build_message(envelope: dict, body: bytes) -> Any:
    """`aio_pika.Message` 조립. 전송 계층 import 를 여기로 가둔다."""
    from aio_pika import DeliveryMode, Message

    return Message(
        body,
        content_type="application/json",
        content_encoding="utf-8",
        # 큐가 durable 이어도 메시지가 persistent 가 아니면 브로커 재시작에 사라진다.
        delivery_mode=DeliveryMode.PERSISTENT,
        message_id=envelope["eventId"],
        type=envelope["eventType"],
        correlation_id=envelope["traceId"],
    )


async def resolve_exchange(channel: Any, settings: Any) -> Any:
    """`app.events` 토픽 exchange 를 얻는다. **남의 토폴로지를 다시 선언하지 않는다.**

    운영 exchange 는 백엔드 인프라가 만들고 quorum·DLX·TTL 설정이 붙어 있다. 우리가
    `declare` 로 다른 인자를 들이밀면 브로커가 `PRECONDITION_FAILED` 로 거부해서 발행도
    수신도 아예 못 뜬다. 그래서 기본은 **있는 것을 확인만** 한다(passive).

    `MQ_DECLARE_TOPOLOGY=true` 일 때만 우리가 만든다 — 로컬 docker-compose 처럼 아직
    아무것도 없는 환경 전용이다. 컨슈머도 같은 함수를 쓴다.

    두 방향 다 **설정이 어긋났다는 뜻이라 `MqConfigError` 로 바꿔서 올린다.** 브로커가
    주는 원문(`PRECONDITION_FAILED - inequivalent arg 'type'`)은 무엇을 고쳐야 하는지를
    안 알려줘서, 이 자리에 걸린 사람이 로그만 보고는 플래그를 찾아가지 못한다.

    Raises:
        MqConfigError: exchange 소유권이 어긋남 — 재시도해도 안 고쳐진다.
    """
    from aio_pika import ExchangeType
    from aio_pika.exceptions import ChannelNotFoundEntity, ChannelPreconditionFailed

    if settings.mq_declare_topology:
        try:
            return await channel.declare_exchange(
                settings.mq_exchange, ExchangeType.TOPIC, durable=True
            )
        except ChannelPreconditionFailed as exc:
            raise MqConfigError(
                f"exchange '{settings.mq_exchange}' 가 이미 다른 설정으로 존재해 "
                "선언이 거부됐습니다 (MQ_DECLARE_TOPOLOGY=true). 이 브로커는 exchange 를 "
                "이미 소유하고 있습니다 — 운영이라면 MQ_DECLARE_TOPOLOGY=false 로 "
                f"내리세요. vhost 도 같이 확인할 것 (지금 {settings.mq_vhost!r}). "
                "운영 토폴로지는 백엔드 인프라 소유입니다 (docs/mq_events.md §2-1)"
            ) from exc

    # ensure=True 면 passive declare 로 존재만 확인한다 — 없으면 그 자리에서 터진다.
    try:
        return await channel.get_exchange(settings.mq_exchange, ensure=True)
    except ChannelNotFoundEntity as exc:
        raise MqConfigError(
            f"exchange '{settings.mq_exchange}' 가 vhost {settings.mq_vhost!r} 에 "
            "없습니다. 로컬이면 `python scripts/setup_local_mq.py` 를 먼저 돌리세요 "
            "(MQ_DECLARE_TOPOLOGY=true 필요). 운영이면 백엔드가 아직 안 만들었거나 "
            "MQ_VHOST 가 틀린 것이고, 어느 쪽이든 **우리가 만들면 안 됩니다** "
            "(docs/mq_events.md §2-1)"
        ) from exc


async def _get_exchange(settings: Any) -> Any:
    """발행용 exchange. 연결·채널은 프로세스당 하나를 재사용한다."""
    global _connection, _channel

    from aio_pika import connect_robust

    if _channel is None or _channel.is_closed:
        if _connection is None or _connection.is_closed:
            _connection = await connect_robust(
                host=settings.mq_host,
                port=settings.mq_port,
                login=settings.mq_user,
                password=settings.mq_password,
                virtualhost=settings.mq_vhost,
            )
        _channel = await _connection.channel(publisher_confirms=True)

    return await resolve_exchange(_channel, settings)


async def close_mq() -> None:
    """연결을 닫는다. 배치 종료 시·발행 실패 후 호출. 이미 닫혀 있어도 안전하다."""
    global _connection, _channel

    if _connection is not None and not _connection.is_closed:
        try:
            await _connection.close()
        except Exception as exc:  # noqa: BLE001 - 닫는 중 실패가 배치를 죽이면 안 된다
            logger.warning("MQ 연결 종료 실패 (무시): %r", exc)
    _connection = _channel = None
