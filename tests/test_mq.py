"""담당: 지인 — RabbitMQ 발행기(`app/core/mq.py`).

**브로커 없이 돈다.** 조립(build_*)과 전송(publish_*)이 갈라져 있어서 payload 모양은
접속 정보(C1) 없이도 검증된다. 계약 정본은 `docs/mq_events.md`.
"""

import inspect
import json
from datetime import date, datetime, timezone

import pytest
from pamqp.commands import Basic

from app.core import mq
from app.core.exceptions import MqConfigError, MqDisabledError, MqPublishError
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


@pytest.fixture
def alert() -> DetectionAlert:
    return DetectionAlert(
        alert_id="ALT-20260828-P001-COUPANG",
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


@pytest.fixture
def recommendation() -> Recommendation:
    return Recommendation(
        recommendation_id="REC-a1b2c3d4e5f6",
        alert_id="ALT-20260828-P001-COUPANG",
        created_at=datetime(2026, 8, 28, 9, 0, 12, tzinfo=timezone.utc),
        proposal=Proposal(
            type=ProposalType.COPY_DRAFT,
            target_field="상세설명",
            current_text="현재 문구",
            proposed_text="모니터 환경에 따라 색상 차이가 있을 수 있습니다",
            rationale="사진 색감이 실물과 다르다는 문의가 20건 중 14건",
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


@pytest.fixture
def guideline_callback() -> GenerationCallback:
    return GenerationCallback(
        guideline_id="GD-20260828-P001-COUPANG",
        status=CallbackStatus.FAILED_ERROR,
        notice_message="생성 실패",
    )


# ── 시그니처 (호출부 계약) ────────────────────────────────────────


def test_trace_id_is_unique_per_call():
    """배치 1회 = traceId 1개. 배치가 한 번 만들어 그 배치의 모든 메시지에 붙인다(§3)."""
    assert mq.new_trace_id() != mq.new_trace_id()


def test_publishers_take_trace_id_as_argument():
    """`trace_id` 를 발행 함수가 자체 생성하면 배치 1회 = traceId 1개 규약이 깨진다.

    인자 이름·순서는 `app/batch/daily.py` 호출부와 맞춰져 있다.
    """
    assert list(inspect.signature(mq.publish_anomaly_analyzed).parameters) == [
        "alert",
        "rec",
        "trace_id",
    ]
    assert list(inspect.signature(mq.publish_guideline_generated).parameters) == [
        "callback",
        "trace_id",
    ]


# ── Envelope (§3) ────────────────────────────────────────────────


def test_envelope_keys_are_camel_case_and_payload_is_untouched():
    """Envelope 은 camelCase, payload 안은 snake_case 그대로(§3).

    평가 스크립트와 Pydantic 정본이 payload 필드명으로 join 하므로 payload 는 변환하지 않는다.
    """
    envelope = mq.build_envelope(
        "ai.anomaly.analyzed", {"alert_id": "ALT-1"}, "trace-1"
    )

    assert set(envelope) == {
        "eventId",
        "eventType",
        "occurredAt",
        "source",
        "traceId",
        "companyId",
        "payload",
    }
    assert envelope["source"] == "ai-server"
    assert envelope["traceId"] == "trace-1"
    assert envelope["payload"] == {"alert_id": "ALT-1"}


def test_event_id_differs_while_trace_id_is_shared():
    """같은 배치의 메시지는 traceId 를 공유하고 eventId 는 건별로 다르다(§3)."""
    first = mq.build_envelope("ai.anomaly.analyzed", {}, "trace-1")
    second = mq.build_envelope("ai.anomaly.analyzed", {}, "trace-1")

    assert first["traceId"] == second["traceId"]
    assert first["eventId"] != second["eventId"]


def test_occurred_at_is_utc_with_milliseconds():
    """§3 예시 형식(`2026-08-01T05:10:00.000Z`). 로컬 시각이면 소비 측 정렬이 틀어진다."""
    occurred_at = mq.build_envelope("x", {}, "t")["occurredAt"]

    assert occurred_at.endswith("Z")
    assert datetime.fromisoformat(occurred_at.replace("Z", "+00:00")).tzinfo is not None


def test_routing_key_is_analyzed_not_detected():
    """`ai.anomaly.detected` 는 2026-08-03 에 개명된 구 이름이다(§2)."""
    assert mq.ANOMALY_ANALYZED == "ai.anomaly.analyzed"
    assert mq.GUIDELINE_GENERATED == "ai.guideline.generated"


# ── payload (§4 · §6) ────────────────────────────────────────────


def test_anomaly_payload_carries_alert_fields_and_recommendation(alert, recommendation):
    """알림 전 필드 + 개선안 1건이 한 메시지에 실린다(§4-1). 멱등 키는 alert_id."""
    payload = mq.build_anomaly_payload(alert, recommendation)

    assert payload["alert_id"] == "ALT-20260828-P001-COUPANG"
    assert payload["channel"] == "COUPANG"
    assert payload["main_aspect"] == "색상"  # enum 은 한글 값으로 실린다(§9)
    assert payload["recommendation"]["recommendation_id"] == "REC-a1b2c3d4e5f6"
    assert payload["recommendation"]["hitl_status"] == "대기"  # 발행 시점엔 항상 대기


def test_anomaly_payload_is_json_serializable(alert, recommendation):
    """datetime·date·Enum 이 그대로 남으면 발행 순간에 터진다."""
    body = json.dumps(
        mq.build_anomaly_payload(alert, recommendation), ensure_ascii=False
    )

    assert "2026-08-28" in body


def test_recommendation_is_explicit_null_when_absent(alert):
    """개선안이 없으면 키를 빼지 않고 null 을 보낸다.

    소비 측이 "필드 없음"과 "개선안 없음"을 구분할 필요가 없게 한다 — 조치 7종 중
    6종이 이 경로다.
    """
    payload = mq.build_anomaly_payload(alert, None)

    assert "recommendation" in payload
    assert payload["recommendation"] is None


def test_guideline_payload_prefers_alert_id_from_source_payload():
    """정본이 손에 있으면 문자열 수술로 재구성하지 않는다.

    source_payload["input"] 이 CSGuidelineInput 원본이고 alert_id 가 필수 필드다.
    접두어 치환은 되돌릴 수 있다는 보장이 없다(`_alert_id_of` 참고).
    """
    callback = GenerationCallback(
        guideline_id="GD-무관한값",
        status=CallbackStatus.FAILED_ERROR,
        notice_message="생성 실패",
        source_payload={"input": {"alert_id": "ALT-20260828-P001-COUPANG"}},
    )

    assert (
        mq.build_guideline_payload(callback)["alert_id"] == "ALT-20260828-P001-COUPANG"
    )


def test_guideline_payload_falls_back_to_prefix_swap(guideline_callback):
    """source_payload 는 SUCCESS 일 때만 실린다(§3-2) — 실패 경로는 접두어로 되돌린다."""
    assert guideline_callback.source_payload is None
    payload = mq.build_guideline_payload(guideline_callback)

    assert payload["guideline_id"] == "GD-20260828-P001-COUPANG"
    assert payload["alert_id"] == "ALT-20260828-P001-COUPANG"
    assert "report_id" not in payload  # 가이드라인 payload 에 없는 필드


def test_monthly_report_cannot_go_out_as_guideline():
    """report_id 만 있는 산출물을 이 라우팅 키로 실으면 안 된다.

    백엔드는 `ai.guideline.generated` 를 받으면 CS팀에 메일을 쏘고 JSONB 에 적재한다(§6).
    """
    monthly = GenerationCallback(
        report_id="RPT-202608",
        status=CallbackStatus.FAILED_ERROR,
        notice_message="생성 실패",
    )

    with pytest.raises(ValueError, match="guideline_id"):
        mq.build_guideline_payload(monthly)


# ── 비활성 상태 ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_disabled_mq_raises_instead_of_silently_skipping(
    alert, recommendation, guideline_callback, monkeypatch
):
    """⚠️ MQ_ENABLED=false 는 no-op 이 아니다.

    호출부는 예외가 없으면 발행 성공으로 보고 그 알림을 prior_alerts 캐시에 넣는다 —
    캐시에 들어가면 재알림이 7일간 억제되므로, 조용히 넘기면 셀러가 그 알림을 영영
    못 본다. (용준 S3_ENABLED=false → S3NotConfiguredError 와 같은 원칙)
    """
    monkeypatch.setattr(mq.get_settings(), "mq_enabled", False)

    with pytest.raises(MqDisabledError):
        await mq.publish_anomaly_analyzed(alert, recommendation, "trace-1")
    with pytest.raises(MqDisabledError):
        await mq.publish_guideline_generated(guideline_callback, "trace-1")


@pytest.mark.asyncio
async def test_publish_sends_envelope_on_the_right_routing_key(
    alert, recommendation, monkeypatch
):
    """전송 직전까지 태운다 — 브로커만 가짜고 Envelope·직렬화·라우팅 키는 실물이다."""
    sent: dict = {}

    class FakeExchange:
        async def publish(self, message, routing_key, timeout=None):
            sent["routing_key"] = routing_key
            sent["body"] = message.body
            sent["message_id"] = message.message_id
            # 브로커가 "큐에 넣었다"고 확인한 상태 — 실물이 돌려주는 값과 같은 타입이다.
            return Basic.Ack(delivery_tag=1)

    async def fake_get_exchange(_settings):
        return FakeExchange()

    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "SLN-test")
    monkeypatch.setattr(mq, "_get_exchange", fake_get_exchange)

    await mq.publish_anomaly_analyzed(alert, recommendation, "trace-1")

    envelope = json.loads(sent["body"].decode("utf-8"))
    assert sent["routing_key"] == "ai.anomaly.analyzed"
    assert envelope["traceId"] == "trace-1"
    assert envelope["payload"]["alert_id"] == "ALT-20260828-P001-COUPANG"
    # 멱등 재전송을 소비 측이 알아볼 수 있게 message_id 를 eventId 로 맞춘다.
    assert sent["message_id"] == envelope["eventId"]


@pytest.mark.asyncio
async def test_unroutable_message_is_a_failure_not_a_success(alert, monkeypatch):
    """⚠️ 어느 큐에도 안 닿은 메시지를 발행 성공으로 보고하지 않는다.

    토픽 exchange 는 바인딩된 큐가 없으면 메시지를 **조용히 버린다.** aio_pika 는 그때
    예외를 던지지 않고 `Basic.Return` 을 돌려준다(2026-08-07 로컬 브로커 실측). 반환값을
    안 보면 배치가 그 알림을 prior_alerts 캐시에 넣어 RENOTIFY_BLOCK_DAYS 동안 재알림이
    막히고 **셀러가 그 알림을 영영 못 본다.**

    백엔드가 main.inbound 바인딩을 안 걸었거나 라우팅 키를 바꾼 상황이 정확히 이것이라,
    가정이 아니라 실제로 일어날 경로다.
    """

    class UnroutableExchange:
        async def publish(self, message, routing_key, timeout=None):
            # 실물이 돌려주는 모양: delivery 에 Basic.Return 이 담긴 래퍼.
            class Delivered:
                delivery = Basic.Return(
                    reply_code=312, reply_text="NO_ROUTE", exchange="app.events"
                )

            return Delivered()

    async def fake_get_exchange(_settings):
        return UnroutableExchange()

    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "SLN-test")
    monkeypatch.setattr(mq, "_get_exchange", fake_get_exchange)

    with pytest.raises(MqPublishError, match="어느 큐에도 도착하지 않았습니다"):
        await mq.publish_anomaly_analyzed(alert, None, "trace-1")


@pytest.mark.asyncio
async def test_unconfirmed_publish_is_a_failure(alert, monkeypatch):
    """Ack 이 아닌 응답(Nack·None 등)도 성공으로 넘기지 않는다."""

    class SilentExchange:
        async def publish(self, message, routing_key, timeout=None):
            return None

    async def fake_get_exchange(_settings):
        return SilentExchange()

    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "SLN-test")
    monkeypatch.setattr(mq, "_get_exchange", fake_get_exchange)

    with pytest.raises(MqPublishError, match="확인하지 않았습니다"):
        await mq.publish_anomaly_analyzed(alert, None, "trace-1")


@pytest.mark.asyncio
async def test_publish_failure_is_not_swallowed(alert, monkeypatch):
    """브로커가 죽으면 예외로 나가야 한다 — 삼키면 안 나간 알림이 7일간 억제된다."""

    async def boom(_settings):
        raise ConnectionError("broker down")

    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "SLN-test")
    monkeypatch.setattr(mq, "_get_exchange", boom)

    with pytest.raises(MqPublishError):
        await mq.publish_anomaly_analyzed(alert, None, "trace-1")


@pytest.mark.asyncio
async def test_disabled_check_runs_before_broker_connection(alert, monkeypatch):
    """꺼져 있으면 접속 시도조차 하지 않는다 — 접속 정보가 빈 값이라 타임아웃만 먹는다."""
    monkeypatch.setattr(mq.get_settings(), "mq_enabled", False)

    async def fail(*_args, **_kwargs):
        raise AssertionError("MQ 가 꺼져 있는데 브로커에 접속했습니다")

    monkeypatch.setattr(mq, "_get_exchange", fail)

    with pytest.raises(MqDisabledError):
        await mq.publish_anomaly_analyzed(alert, None, "trace-1")


# ── companyId (§3) ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_refuses_without_company_id(alert, monkeypatch):
    """⚠️ companyId 가 비면 발행하지 않는다.

    빈 값으로 나가면 백엔드 DB 에 회사 미상 행이 쌓이는데, 나중에 어느 회사 것인지
    복구할 단서가 없다(발행 시각뿐). 접속이 되는데 내용이 틀린 메시지를 보내는 건
    안 보내는 것보다 나쁘다.
    """
    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "")

    async def fail(*_args, **_kwargs):
        raise AssertionError("companyId 가 없는데 브로커에 접속했습니다")

    monkeypatch.setattr(mq, "_get_exchange", fail)

    with pytest.raises(MqConfigError, match="MQ_COMPANY_ID"):
        await mq.publish_anomaly_analyzed(alert, None, "trace-1")


def test_company_id_rides_on_the_envelope_not_the_payload():
    """companyId 는 Envelope 소속이다(§3 예시) — payload 안이 아니다."""
    envelope = mq.build_envelope("x", {"alert_id": "ALT-1"}, "t", company_id="SLN-abc")

    assert envelope["companyId"] == "SLN-abc"
    assert "companyId" not in envelope["payload"]


# ── 토폴로지 소유권 ──────────────────────────────────────────────


class _FakeChannel:
    """어떤 AMQP 호출이 나갔는지만 기록한다."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def declare_exchange(self, *_args, **_kwargs):
        self.calls.append("declare_exchange")
        return "exchange"

    async def get_exchange(self, *_args, **_kwargs):
        self.calls.append("get_exchange")
        return "exchange"


@pytest.mark.asyncio
async def test_does_not_redeclare_someone_elses_exchange(monkeypatch):
    """⚠️ 기본값은 **선언하지 않고 확인만** 한다.

    운영 exchange 는 백엔드 인프라 소유이고 quorum·DLX·TTL 설정이 붙어 있다. 우리가
    다른 인자로 declare 하면 PRECONDITION_FAILED 로 거부당해 발행이 통째로 죽는다.
    로컬에선 우리가 만든 exchange 라 이 사고가 안 나서 조용히 통과한다.
    """
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    channel = _FakeChannel()

    await mq.resolve_exchange(channel, settings)

    assert channel.calls == ["get_exchange"]


@pytest.mark.asyncio
async def test_declares_topology_only_when_told_to(monkeypatch):
    """로컬 docker-compose 처럼 아무것도 없는 환경에서만 우리가 만든다."""
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    channel = _FakeChannel()

    await mq.resolve_exchange(channel, settings)

    assert channel.calls == ["declare_exchange"]
