"""담당: 지인 — RabbitMQ 발행기. 계약 정본은 `docs/mq_events.md`.

리포팅·배치가 같이 쓰므로 컴포넌트 폴더가 아니라 core 에 둔다.

발행 계약 3가지:

1. **안 보낸 것을 보냈다고 하지 않는다.** 호출부(`app/batch/daily.py`)는 예외가 없으면
   발행 성공으로 보고 그 알림을 `prior_alerts` 캐시에 넣는데, 캐시에 들어가면
   `RENOTIFY_BLOCK_DAYS` 동안 재알림이 억제된다 — 실제로 안 나간 메시지를 조용히 성공
   처리하면 셀러가 그 알림을 7일간 못 본다. 그래서 `MQ_ENABLED=false` 는 no-op 이 아니라
   `MqDisabledError` 다.
2. **`trace_id` 는 인자로만 받는다.** 여기서 만들면 배치 1회 = `traceId` 1개 규약이 깨진다.
3. **Envelope 은 camelCase, payload 안은 snake_case.** 발행 단위는 알림 1건당 메시지 1개 —
   배열로 묶으면 멱등 키가 건별이라 1건 실패로 전체가 재처리된다.

조립(`build_*`)과 전송(`publish_*`)을 나눠 뒀다. **브로커 없이도 payload 모양을 검증할 수
있어야** 하기 때문이다.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.core.constants import KST
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
"""Envelope 의 `source`. AI 노드가 발행한 것임을 표시한다."""

ANOMALY_ANALYZED = "ai.anomaly.analyzed"
GUIDELINE_GENERATED = "ai.guideline.generated"
REPORT_GENERATED = "ai.report.generated"
"""라우팅 키. `ai.#` 바인딩으로 `main.inbound` 에 꽂힌다.

`ai.anomaly.detected` 는 구 이름이다 — 탐지만이 아니라 개선안까지 끝난 상태라 `analyzed`
가 맞다. 백엔드 데이터플로우 문서에 구 이름이 남아 있다.
"""

_connection: Any = None
_channel: Any = None
"""배치 1회가 메시지 수십 건을 보내므로 연결을 재사용한다. `close_mq()` 로 닫는다."""


def new_trace_id() -> str:
    """배치 1회분의 `traceId`. 그 배치가 발행하는 모든 메시지가 이 값을 공유한다."""
    return f"trace-{uuid.uuid4().hex[:16]}"


def _now_iso() -> str:
    """`occurredAt` 형식 — 밀리초까지, UTC, `Z` 접미."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _to_backend_local_iso(value: str) -> str:
    """시간대가 붙은 ISO 시각을 **KST 벽시계 naive** 문자열로. 아니면 원본 그대로.

    백엔드 DTO 가 `java.time.LocalDateTime` 이라 **존 성분을 아예 못 읽는다.** 실패 로그의
    포맷이 `ISO_LOCAL_DATE_TIME`(연-월-일 T 시:분[:초[.나노]])이고 오프셋 자리가 없다 —
    `+09:00` 도 `Z` 도 같은 위치에서 `unparsed text` 로 거부된다. 그래서 UTC 로 바꿔
    `Z` 를 붙이는 것으로는 안 고쳐진다. 오프셋을 **떼야** 한다.

    KST 로 맞춘 뒤 떼는 이유: 백엔드가 이 값을 로컬 시각으로 읽어 화면에 그대로 쓴다.
    UTC 로 떼면 표시 시각이 9시간 당겨진다.

    날짜만 있는 값(`2026-06-30`)과 이미 naive 인 값은 건드리지 않는다 — 전자는 스키마상
    DATE 고(§2-9), 후자는 이미 원하는 모양이다.

    2026-08-21 실측: `payload.recommendation.created_at` 이 `...506524+09:00` 으로 나가
    `ai.anomaly.analyzed` 3건이 `MALFORMED_PAYLOAD` 로 데드레터에 쌓였다.
    """
    if "T" not in value:
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        return value
    return parsed.astimezone(KST).replace(tzinfo=None).isoformat()


def normalize_payload_datetimes(value: Any) -> Any:
    """payload 안의 모든 시각을 백엔드가 읽을 수 있는 모양으로 맞춘다.

    빌더마다 손대지 않고 **발행 길목 한 곳**에서 돈다. 빌더는 `model_dump(mode="json")`
    결과를 그대로 싣기 때문에, 새 스키마 필드가 늘 때마다 사람이 기억해야 하는 구조면
    조용히 빠진다 — 실제로 `occurredAt` 에만 정규화가 걸려 있어서 이 사고가 났다.
    """
    if isinstance(value, dict):
        return {k: normalize_payload_datetimes(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_payload_datetimes(v) for v in value]
    if isinstance(value, str):
        return _to_backend_local_iso(value)
    return value


def build_envelope(
    event_type: str, payload: dict, trace_id: str, *, company_id: str = ""
) -> dict:
    """공통 Envelope 조립. **키는 camelCase, payload 안은 손대지 않는다.**

    `occurredAt` 은 **발행 시각**이지 탐지 시각이 아니다 — 개선안 생성에 5~20초가 걸려서
    `payload.detected_at` 과 몇 분씩 벌어진다. 화면의 "탐지 시각"은 payload 쪽이다.
    `companyId` 는 배포마다 고정이라 `_publish()` 가 설정에서 읽어 채워 넣는다.
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


def build_anomaly_payload(
    alert: DetectionAlert,
    rec: Recommendation | None,
    classifier_versions: dict | None = None,
) -> dict:
    """`ai.anomaly.analyzed` payload — 알림 전 필드 + 개선안 1건 + 분류기 신원.

    `rec` 는 `recommended_action == "개선안 생성"` 일 때만 채워지고 그 외에는 명시적 null
    이다 — 키를 빼지 않아 소비 측이 "필드가 없음"과 "개선안이 없음"을 구분할 필요가 없다.

    **`classifier_versions` 는 `DetectionAlert` 에 없는 필드다.** 스키마가 아니라 여기서
    붙이는 이유가 둘이다. 첫째, `ProcessHitlRequest.alert` 가 `DetectionAlert` 라 백엔드가
    저장해 둔 알림이 그대로 되돌아오는데, 모델 필드로 만들면 이 필드 이전 알림이 전부
    422 가 되거나(필수) 뜻이 둘인 `None` 을 떠안는다(선택). payload 시점에만 붙이면 되돌아온
    여분 키는 pydantic 이 버린다. 둘째, **값의 근거가 알림이 아니라 입력 경로에 있다.**

    **호출부가 넘긴다 — 여기서 만들지 않는다.** "이 알림은 프롬프트 X 로 분류된 행에서
    나왔다"를 보장하는 것은 `daily.py` 의 활성 버전 필터뿐이고, 그 필터를 안 타는 입력원
    (`--input-source golden`)에서는 같은 주장이 성립하지 않는다. `None` 이면 `null` 로
    나간다 — **"버전 미상"이라는 정직한 값**이지 누락이 아니다.

    **`model`·`pipeline` 을 실어도 되는 근거는 필터에 있다.** 설정값을 그냥 읽어 채우면 행이
    말하는 것이 아니라 발행 시점 설정을 보고하는 것이라, 분류와 탐지 사이에 `LLM_MODEL` 이
    바뀌면 payload 가 거짓말을 한다. 지금 실을 수 있는 것은 탐지 조회가 세 축을 등호로
    강제하고 안 맞는 행이 있으면 `_check_version_cutover()` 가 배치를 세우기 때문이다 —
    알림이 나갔다는 것 자체가 관측이다. **그 강제를 느슨하게 하면 이 필드도 같이 거짓이 된다.**
    (컬럼 명세: `docs/classified_item_version_columns.md`)
    """
    payload = alert.model_dump(mode="json")
    payload["recommendation"] = rec.model_dump(mode="json") if rec else None
    payload["classifier_versions"] = classifier_versions
    return payload


def build_guideline_payload(callback: GenerationCallback) -> dict:
    """`ai.guideline.generated` payload. 멱등 키는 `guideline_id`.

    Raises:
        ValueError: `guideline_id` 가 없을 때. `report_id` 만 있으면 월간 리포트라 라우팅
            키가 `ai.report.generated` 여야 한다 — 잘못 실으면 백엔드가 CS팀에 메일을 쏘고
            JSONB 에 적재한다.
    """
    if callback.guideline_id is None:
        raise ValueError(
            "guideline_id 가 없습니다 — 월간 리포트는 ai.report.generated 로 발행할 것"
        )

    payload = callback.model_dump(mode="json")
    payload.pop("report_id", None)  # 가이드라인 payload 에 없는 필드
    payload["alert_id"] = _alert_id_of(callback)
    return payload


MONTHLY_EVENT_STATUSES = frozenset(
    {
        CallbackStatus.SUCCESS.value,
        CallbackStatus.FAILED_SIZE_EXCEEDED.value,
        CallbackStatus.FAILED_ERROR.value,
    }
)
"""`ai.report.generated` 에 실릴 수 있는 status.

`HOLD_INSUFFICIENT_DATA`·`FAILED_VALIDATION` 은 여기 못 온다 — 둘 다 **상품 단위** 판정인데
이벤트는 월 단위이기 때문이다. 그 상품들은 합본에서 빠지고 사실이 `notice_message` 로 나간다
(두 상태는 상품 1건 REST 에서는 살아 있다).
"""


def build_report_payload(callback: GenerationCallback, report_month: str) -> dict:
    """`ai.report.generated` payload. 멱등 키는 `report_id` — **월 1건**이다.

    `report_month` 를 인자로 받는 이유: 계약이 payload 에 요구하는데 `GenerationCallback` 에
    없는 필드다. `report_id`(`RPT-202607`)에서 잘라 쓸 수도 있지만 **정본이 손에 있으면
    재구성하지 않는다**(배치는 `args.month` 를 이미 들고 있다).

    `source_payload` 는 싣지 않는다 — 월간은 PDF 가 유일 산출물이라 콜백 자체가 그 필드를
    안 채우지만, 나중에 채워지더라도 이 이벤트로는 안 나가게 막는다.

    Raises:
        ValueError: `report_id` 가 없을 때(= CS 가이드라인 산출물), 또는 status 가 월 단위
            이벤트에 올 수 없는 값일 때.
    """
    # `GenerationCallback` 스키마가 report_id / guideline_id 를 **배타**로 강제하므로
    # 이 검사 하나가 양방향을 다 막는다. 뒤바뀌면 백엔드가 엉뚱한 소비 동작(CS팀 메일
    # 발송)을 탄다.
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
    payload.pop("guideline_id", None)  # 월간 payload 에 없는 필드
    payload.pop("source_payload", None)  # 월간은 원본을 보관하지 않는다
    payload["report_month"] = report_month
    return payload


def _alert_id_of(callback: GenerationCallback) -> str:
    """콜백에서 `alert_id` 를 얻는다. **정본이 있으면 재구성하지 않는다.**

    `source_payload["input"]` 이 `CSGuidelineInput` 원본이고 거기 `alert_id` 가 필수 필드라
    그게 정본이다. 접두어 치환은 **`source_payload` 가 없는 실패 경로 전용 폴백**이고
    (그 필드는 SUCCESS 일 때만 실린다), `build_guideline_id()` 가 `ALT-` 로 시작하지 않는
    입력을 통째로 뒤에 붙이므로 그 경우 역변환이 원본과 달라진다 — 실제 `alert_id` 는 항상
    `ALT-` 로 시작해서 지금은 문제가 없지만 되돌릴 수 있다고 가정하지 말 것.
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
    classifier_versions: dict | None = None,
) -> None:
    """`ai.anomaly.analyzed` 발행 — 알림 1건 + 개선안 1건(없으면 null). 멱등 키 `alert_id`.

    Args:
        alert: 탐지 알림. payload 최상위가 이 필드들 그대로다.
        rec: 개선안. 게이트 밖이면 None.
        trace_id: 배치가 만든 값. 이 함수가 생성하지 않는다.
        classifier_versions: 이 알림의 숫자를 만든 분류기 신원. **보장하는 쪽이 만들어
            넘긴다** — `build_anomaly_payload` 참고. 모르면 None(=`null` 로 발행).

    Raises:
        MqDisabledError · MqConfigError · MqPublishError: `_publish()` 참고.
            **`MqDisabledError` 를 삼키지 말 것** — 안 나간 알림이다.
    """
    await _publish(
        ANOMALY_ANALYZED,
        build_anomaly_payload(alert, rec, classifier_versions),
        trace_id,
        key=alert.alert_id,
    )


async def publish_guideline_generated(
    callback: GenerationCallback,
    trace_id: str,
) -> None:
    """`ai.guideline.generated` 발행 — CS 가이드라인 1건. 멱등 키 `guideline_id`.

    알림 1건마다 트리거된다(월간 리포트와 생명주기가 다르다).

    Raises:
        ValueError: `guideline_id` 가 없을 때(= 월간 리포트).
        MqDisabledError · MqConfigError · MqPublishError: `_publish()` 참고.
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
        callback: `compile_and_upload_monthly_book()` 산출물.
        report_month: `YYYY-MM`. 콜백에 없는 값이라 배치가 넘긴다.
        trace_id: 배치가 만든 값.

    Raises:
        ValueError: `build_report_payload()` 참고.
        MqDisabledError · MqConfigError · MqPublishError: `_publish()` 참고.
    """
    payload = build_report_payload(callback, report_month)
    await _publish(REPORT_GENERATED, payload, trace_id, key=str(callback.report_id))


# ── 전송 ─────────────────────────────────────────────────────────


async def _publish(event_type: str, payload: dict, trace_id: str, *, key: str) -> None:
    """Envelope 을 실어 보낸다. `key` 는 멱등 키(로그용).

    Publisher Confirm 을 켜고 보낸다 — 확인 없이 성공을 반환하면 호출부가 그 알림을 캐시에
    넣어 재알림이 7일간 억제된다.

    Raises:
        MqDisabledError: `MQ_ENABLED=false`.
        MqConfigError: 설정 오류(companyId 미설정·토폴로지 소유권). **재시도 대상이 아니다.**
        MqPublishError: 접속·발행 실패, 또는 unroutable(`_require_ack`). 재시도 대상.
    """
    settings = get_settings()
    if not settings.mq_enabled:
        raise MqDisabledError(
            f"MQ_ENABLED=false 라 {event_type}({key}) 를 발행하지 않았습니다"
        )
    if not settings.mq_company_id:
        # 빈 값으로 내보내면 백엔드 DB 에 회사 미상 행이 쌓인다. 발행 시각 말고 단서가
        # 없어 나중에 복구할 방법이 없으므로 아예 안 보낸다.
        raise MqConfigError(
            f"MQ_COMPANY_ID 가 비어 있어 {event_type}({key}) 를 발행하지 않았습니다 "
            "(MQ 컨벤션 §3 — Envelope 의 companyId 는 배포마다 고정값)"
        )

    envelope = build_envelope(
        event_type,
        normalize_payload_datetimes(payload),
        trace_id,
        company_id=settings.mq_company_id,
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
        raise
    except Exception as exc:
        # 연결은 닫지 않는다. connect_robust 가 스스로 복구하고, 채널이 브로커 오류로
        # 닫혔으면 _get_exchange 가 다음 호출에서 새로 연다 — 여기서 끊으면 그 복구를
        # 없애는 꼴이라 일시적 실패가 배치 전체 실패로 번진다.
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

    **토픽 exchange 는 바인딩된 큐가 없으면 메시지를 조용히 버린다.** 그때 aio_pika 는 예외를
    던지지 않고 `Basic.Return` 을 돌려준다(로컬 브로커 실측). 반환값을 안 보면 "발행 성공"
    으로 넘어가고, 호출부가 그 알림을 캐시에 넣어 `RENOTIFY_BLOCK_DAYS` 동안 재알림이
    막힌다 — **셀러가 그 알림을 영영 못 본다.**

    가정이 아니라 실제로 일어날 시나리오다: 백엔드가 `main.inbound` 바인딩을 아직 안
    걸었거나, 라우팅 키를 바꿨거나, `ai.#` 대신 더 좁은 패턴을 걸면 전부 여기로 떨어진다.
    **접속이 되는 것과 배달이 되는 것은 다르다** — publisher confirm 은 "브로커가 받았다"
    까지만 보증한다.
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


def topology_config_errors() -> tuple[type[Exception], ...]:
    """토폴로지 소유권이 어긋났다는 뜻의 브로커 오류들. **재시도로 안 고쳐진다.**

    여기서 토폴로지는 exchange(`app.events`) · 큐(`main.inbound`·`ai.inbound`) · 둘을 잇는
    바인딩을 묶어 부르는 말이고, 운영에서는 백엔드 인프라가 만들고 우리는 쓰기만 한다
    (`MQ_DECLARE_TOPOLOGY` 가 그 스위치다). `aiormq` 가 채널 오류 코드를 `ChannelClosed`
    하위로 매핑한다 — 403 ACCESS_REFUSED · 404 NOT_FOUND · 406 PRECONDITION_FAILED.

    **403 을 빼지 말 것.** 운영 토폴로지가 백엔드 소유라 우리 계정에 `configure`/`write`
    권한이 없을 수 있고, 그때 브로커는 406 이 아니라 403 을 준다 — 빠지면 권한 오류가
    `MqPublishError`(= 재시도 대상)로 나가서, 권한을 안 고치는 한 매일 같은 자리에서
    실패하는 것이 일시적 장애처럼 보인다. 반대로 `ChannelClosed` 를 통째로 잡지도 않는다 —
    `reply_code=None` 인 평범한 채널 종료까지 삼켜 **일시적 장애를 비재시도로 오분류**한다.

    `mq_consumer.resolve_queue()` 가 같은 튜플을 쓴다. 두 벌로 두면 한쪽만 넓혔을 때
    exchange 는 잡히고 큐는 안 잡히는 상태가 조용히 생긴다.

    **`aio_pika.exceptions` 가 아니라 `aiormq.exceptions` 에서 가져온다** — 정의도 raise 도
    aiormq 쪽이고, aio_pika 는 일부만 re-export 하는데 거기에 `ChannelAccessRefused` 가
    빠져 있다(9.5.7 확인). 클래스 객체는 양쪽이 동일하다. 모듈 상단에서 import 하지 않는
    이유는 다른 전송 계층 import 와 같다 — `MQ_ENABLED=false` 로 도는 배포까지 브로커
    라이브러리 설치를 강제하게 된다.
    """
    from aiormq.exceptions import (
        ChannelAccessRefused,
        ChannelNotFoundEntity,
        ChannelPreconditionFailed,
    )

    return (ChannelAccessRefused, ChannelNotFoundEntity, ChannelPreconditionFailed)


LOCAL_BROKER_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "::1", "rabbitmq", "host.docker.internal"}
)
"""로컬로 인정하는 브로커 호스트. `rabbitmq` 는 compose 서비스명이다.

**허용 목록이지 차단 목록이 아니다** — 운영 호스트명을 미리 알 수 없으니 반대로는 못 막는다.
목록은 여기 한 곳이고 `scripts/setup_local_mq.py` 가 import 한다. 두 벌로 두면 한쪽에만
호스트를 추가했을 때 스크립트는 통과하는데 런타임은 막히는 상태가 조용히 생긴다.
"""


def is_local_broker_host(host: str | None) -> bool:
    """브로커 호스트가 로컬인가. `.env` 값의 대소문자·공백은 무시한다.

    빈 값은 **로컬이 아니다** — `mq_host` 기본값이 `""` 이라 여기서 참을 주면 아무것도 설정
    안 한 환경이 로컬로 통과한다(fail-closed).
    """
    return (host or "").strip().lower() in LOCAL_BROKER_HOSTS


def require_local_topology_target(settings: Any) -> None:
    """토폴로지를 만들려 한다면 브로커가 로컬인지 확인한다.

    `MQ_DECLARE_TOPOLOGY` 는 **"우리가 토폴로지를 만든다" 는 뜻이지 "여기가 로컬이다" 가
    아니다** — 운영 접속정보를 `.env` 에 넣으면서 플래그를 같이 안 내리면 declare 분기가
    그대로 운영 브로커를 향한다.

    **`topology_config_errors()` 로는 이걸 못 막는다.** 그쪽은 브로커가 **거부했을 때** 문구를
    고쳐 주는 것이라, 운영 exchange·큐가 아직 없고 우리 계정에 `configure` 권한이 있으면
    declare 가 예외 없이 성공한다 — quorum·DLX·TTL 없이 우리 인자로 선점되고, 백엔드가
    나중에 정상 토폴로지를 올릴 때 그쪽이 터진다. **사고가 조용한 쪽은 이 경로다.**

    플래그가 꺼져 있으면 아무것도 보지 않는다 — 운영이 그 상태이고 운영 호스트를 거부하면
    안 된다.

    Raises:
        MqConfigError: 플래그가 켜져 있는데 브로커가 로컬이 아님 — 재시도로 안 고쳐진다.
    """
    if not settings.mq_declare_topology or is_local_broker_host(settings.mq_host):
        return

    raise MqConfigError(
        f"MQ_DECLARE_TOPOLOGY=true 인데 MQ_HOST={settings.mq_host!r} 는 로컬 브로커가 "
        "아닙니다. 운영 토폴로지(exchange·큐·바인딩)는 백엔드 인프라 소유라 우리가 "
        "만들면 안 됩니다 — 운영이라면 MQ_DECLARE_TOPOLOGY=false 로 내리고 MQ_VHOST 도 "
        f"같이 확인하세요 (지금 {settings.mq_vhost!r}). "
        f"로컬로 인정하는 호스트: {', '.join(sorted(LOCAL_BROKER_HOSTS))} "
        "(docs/mq_events.md §2-1)"
    )


async def resolve_exchange(channel: Any, settings: Any) -> Any:
    """`app.events` 토픽 exchange 를 얻는다. **남의 토폴로지를 다시 선언하지 않는다.**

    운영 exchange 는 백엔드 인프라가 만들고 quorum·DLX·TTL 이 붙어 있어서, 우리가 다른
    인자로 `declare` 하면 브로커가 `PRECONDITION_FAILED` 로 거부해 발행도 수신도 아예 못
    뜬다. 그래서 기본은 **있는 것을 확인만** 한다(passive). `MQ_DECLARE_TOPOLOGY=true` 일
    때만 우리가 만든다 — 로컬 compose 처럼 아직 아무것도 없는 환경 전용이고 컨슈머도 같은
    함수를 쓴다.

    두 방향 다 설정이 어긋났다는 뜻이라 `MqConfigError` 로 바꿔서 올린다 — 브로커 원문
    (`inequivalent arg 'type'`)은 무엇을 고쳐야 하는지를 안 알려줘서, 걸린 사람이 로그만
    보고는 플래그를 찾아가지 못한다.

    Raises:
        MqConfigError: exchange 소유권이 어긋남. 브로커가 거부한 경우와, 애초에 만들면 안
            되는 브로커를 향한 경우(`require_local_topology_target`) 둘 다 여기로 나온다.
    """
    from aio_pika import ExchangeType

    config_errors = topology_config_errors()
    require_local_topology_target(settings)

    if settings.mq_declare_topology:
        try:
            return await channel.declare_exchange(
                settings.mq_exchange, ExchangeType.TOPIC, durable=True
            )
        except config_errors as exc:
            raise MqConfigError(
                f"exchange '{settings.mq_exchange}' 를 선언하지 못했습니다 "
                f"(MQ_DECLARE_TOPOLOGY=true, vhost={settings.mq_vhost!r}, 브로커: {exc}). "
                "이미 다른 설정으로 존재하거나 선언 권한이 없다는 뜻이고, 둘 다 이 브로커가 "
                "exchange 를 이미 소유하고 있다는 신호입니다 — 운영이라면 "
                "MQ_DECLARE_TOPOLOGY=false 로 내리고 MQ_VHOST 도 같이 확인하세요. "
                "운영 토폴로지는 백엔드 인프라 소유입니다 (docs/mq_events.md §2-1)"
            ) from exc

    # ensure=True 면 passive declare 로 존재만 확인한다 — 없으면 그 자리에서 터진다.
    try:
        return await channel.get_exchange(settings.mq_exchange, ensure=True)
    except config_errors as exc:
        raise MqConfigError(
            f"exchange '{settings.mq_exchange}' 를 vhost {settings.mq_vhost!r} 에서 "
            f"확인하지 못했습니다 (브로커: {exc}). 로컬이면 "
            "`python scripts/setup_local_mq.py` 를 먼저 돌리세요 "
            "(MQ_DECLARE_TOPOLOGY=true 필요). 운영이면 백엔드가 아직 안 만들었거나 "
            "MQ_VHOST·계정 권한이 틀린 것이고, 어느 쪽이든 **우리가 만들면 안 됩니다** "
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
