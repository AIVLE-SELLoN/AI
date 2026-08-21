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
    PdfS3Meta,
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
        channel_rates=[
            {"channel": "COUPANG", "rate": 0.13, "excluded": False, "total": 200},
            {"channel": "NAVER", "rate": 0.05, "excluded": False, "total": 160},
            {"channel": "ZIGZAG", "rate": None, "excluded": True, "total": 0},
        ],
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
        # 분류기 신원. 발행 함수가 만들지 않고 **보장하는 쪽(daily.py)이 넘긴다** —
        # trace_id 를 자체 생성하지 않는 것과 같은 이유다.
        "classifier_versions",
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
    """`ai.anomaly.detected` 는 개명 전 구 이름이다(§2)."""
    assert mq.ANOMALY_ANALYZED == "ai.anomaly.analyzed"
    assert mq.GUIDELINE_GENERATED == "ai.guideline.generated"


# ── payload (§4 · §6) ────────────────────────────────────────────


def test_anomaly_payload_carries_alert_fields_and_recommendation(alert, recommendation):
    """알림 전 필드 + 개선안 1건이 한 메시지에 실린다(§4-1). 멱등 키는 alert_id."""
    payload = mq.build_anomaly_payload(alert, recommendation)

    assert payload["alert_id"] == "ALT-20260828-P001-COUPANG"
    assert payload["channel"] == "COUPANG"
    assert payload["main_aspect"] == "색상"  # enum 은 한글 값으로 실린다(§9)
    assert payload["channel_rates"] == [
        {"channel": "COUPANG", "rate": 0.13, "excluded": False, "total": 200},
        {"channel": "NAVER", "rate": 0.05, "excluded": False, "total": 160},
        {"channel": "ZIGZAG", "rate": None, "excluded": True, "total": 0},
    ]
    assert payload["recommendation"]["recommendation_id"] == "REC-a1b2c3d4e5f6"
    assert payload["recommendation"]["hitl_status"] == "대기"  # 발행 시점엔 항상 대기


def test_anomaly_payload_is_json_serializable(alert, recommendation):
    """datetime·date·Enum 이 그대로 남으면 발행 순간에 터진다."""
    body = json.dumps(
        mq.build_anomaly_payload(alert, recommendation), ensure_ascii=False
    )

    assert "2026-08-28" in body


def test_anomaly_payload_carries_classifier_versions(alert, recommendation):
    """분류기 신원이 payload 에 실린다(§4-1).

    35일 창에 두 프롬프트 결과가 섞이면 라벨러 교체가 고객 이상으로 둔갑한다. 탐지는
    활성 버전만 읽어 섞임을 막지만, **소비 측은 그 사실을 알 길이 없다** — 교체 전후 알림을
    나눠 보려면 알림마다 기준이 적혀 있어야 한다.
    """
    versions = {
        "prompt_cs": "classify_aspect_v5",
        "prompt_review": "classify_sentiment_v4",
        "model": "gpt-4o-mini",
        "pipeline": "classify_pipeline_v1",
    }

    payload = mq.build_anomaly_payload(alert, recommendation, versions)

    assert payload["classifier_versions"] == versions


def test_classifier_versions_is_explicit_null_when_unknown(alert):
    """근거가 없으면 **null 을 싣는다** — 키를 빼지도, 지어내지도 않는다.

    값의 근거는 `daily.py` 의 활성 버전 필터뿐이다. 그 필터를 안 타는 입력원
       (`--input-source golden`)에서 발행 시점 설정으로 채우면, 검증한 적 없는 것을
       검증된 것처럼 보고하게 된다. `recommendation` 과 같은 이유로 키는 남긴다.
    """
    payload = mq.build_anomaly_payload(alert, None)

    assert "classifier_versions" in payload
    assert payload["classifier_versions"] is None


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
    """MQ_ENABLED=false 는 no-op 이 아니다.

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
    """어느 큐에도 안 닿은 메시지를 발행 성공으로 보고하지 않는다.

    토픽 exchange 는 바인딩된 큐가 없으면 메시지를 **조용히 버린다.** aio_pika 는 그때
    예외를 던지지 않고 `Basic.Return` 을 돌려준다. 반환값을
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
    """Ack 도 Return 도 아닌 응답을 성공으로 넘기지 않는다.

    **Nack 은 여기 안 온다** — aiormq 가 `DeliveryError` 예외로 던져서
    (`aiormq/channel.py` `_confirm_delivery`) 위쪽 try/except 가 먼저 `MqPublishError` 로
    감싼다. 즉 세 갈래(Ack / Return / 그 외)가 전부 막히되 경로가 다르다. 이 분기는
    라이브러리가 계약을 바꿔도 조용히 새지 않게 두는 방어다.
    """

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
    """companyId 가 비면 발행하지 않는다.

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
    """기본값은 **선언하지 않고 확인만** 한다.

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
    """로컬 docker-compose 처럼 아무것도 없는 환경에서만 우리가 만든다.

    `mq_host` 를 여기서 명시한다. 가드가 호스트도 보는데 기본값이 `""`(fail-closed)
    이라, `.env` 에 기대면 **`.env` 를 만든 사람만 통과하는 테스트**가 된다.
    """
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    monkeypatch.setattr(settings, "mq_host", "localhost")
    channel = _FakeChannel()

    await mq.resolve_exchange(channel, settings)

    assert channel.calls == ["declare_exchange"]


@pytest.mark.asyncio
async def test_refuses_to_declare_against_a_remote_broker(monkeypatch):
    """플래그가 켜져 있어도 브로커가 로컬이 아니면 **선언 자체를 시도하지 않는다.**

    `topology_config_errors()` 로는 이 사고를 못 막는다 — 그쪽은 브로커가 거부했을 때
    문구를 고쳐 주는 것이고, 운영 exchange 가 **아직 없으면 declare 는 그냥 성공한다.**
    그러면 quorum·DLX·TTL 없는 우리 인자로 운영 토폴로지가 선점되고, 백엔드가 나중에
    정상 토폴로지를 올릴 때 그쪽이 터진다.

    그래서 `calls` 가 비어 있는지까지 본다. 예외만 확인하면 "브로커에 다녀온 뒤 터진 것"
    과 구분되지 않는다.
    """
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    monkeypatch.setattr(settings, "mq_host", "mq.sellon.example.com")
    channel = _FakeChannel()

    with pytest.raises(MqConfigError) as exc:
        await mq.resolve_exchange(channel, settings)

    assert channel.calls == []
    # 이 가드에 걸린 사람은 지금 막 운영 전환을 하는 중이다 — 뭘 내려야 하는지 알려준다.
    assert "MQ_DECLARE_TOPOLOGY=false" in str(exc.value)


@pytest.mark.asyncio
async def test_remote_broker_is_fine_when_we_do_not_declare(monkeypatch):
    """반대 방향 — **운영 설정(플래그 off + 원격 호스트)은 막으면 안 된다.**

    가드를 `mq_declare_topology` 와 무관하게 걸면 운영에서 발행이 통째로 죽는다.
    운영이야말로 원격 호스트가 정상인 환경이다.
    """
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    monkeypatch.setattr(settings, "mq_host", "mq.sellon.example.com")
    channel = _FakeChannel()

    await mq.resolve_exchange(channel, settings)

    assert channel.calls == ["get_exchange"]


@pytest.mark.parametrize("host", sorted(mq.LOCAL_BROKER_HOSTS))
def test_local_hosts_are_recognized(host):
    assert mq.is_local_broker_host(host)


@pytest.mark.parametrize(
    "host",
    ["mq.sellon.example.com", "10.0.1.20", "sellon-rabbitmq.prod.internal"],
)
def test_remote_hosts_are_not_local(host):
    assert not mq.is_local_broker_host(host)


@pytest.mark.parametrize("host", ["", None, "   "])
def test_missing_host_is_not_local(host):
    """`mq_host` 기본값이 `""` 이다. 여기서 참을 주면 미설정 환경이 로컬로 통과한다."""
    assert not mq.is_local_broker_host(host)


def test_host_comparison_ignores_case_and_padding():
    assert mq.is_local_broker_host("  LocalHost  ")


class _RefusingChannel:
    """토폴로지 호출을 브로커가 거부하는 채널. 두 방향을 각각 재현한다."""

    def __init__(self, declare_exc=None, get_exc=None) -> None:
        self._declare_exc = declare_exc
        self._get_exc = get_exc

    async def declare_exchange(self, *_args, **_kwargs):
        if self._declare_exc is not None:
            raise self._declare_exc
        return "exchange"

    async def get_exchange(self, *_args, **_kwargs):
        if self._get_exc is not None:
            raise self._get_exc
        return "exchange"


@pytest.mark.asyncio
async def test_declare_on_someone_elses_exchange_says_which_flag_to_drop(monkeypatch):
    """운영 브로커에 `MQ_DECLARE_TOPOLOGY=true` 로 붙었을 때의 안내.

    브로커 원문은 `PRECONDITION_FAILED - inequivalent arg 'type'` 뿐이라 **무엇을
    고쳐야 하는지를 안 알려준다.** 연동 주에 이 로그만 보고 플래그를 찾아가야 하는데,
    그 자리에서 헤매라고 둘 이유가 없다.

    타입도 같이 본다 — `MqPublishError`(재시도 대상)로 나가면 안 된다. 플래그를
    안 고치는 한 다음 배치도 같은 자리에서 실패한다.
    """
    from aio_pika.exceptions import ChannelPreconditionFailed

    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    # 로컬 호스트여야 이 경로에 온다 — 원격이면 `require_local_topology_target()` 이
    # 브로커에 가기 전에 세운다. 즉 여기는 **선언을 시도할 자격은 있었는데 거부당한**
    # 경우이고, 로컬 브로커에 옛 실험이 남긴 exchange 가 있으면 실제로 난다.
    monkeypatch.setattr(settings, "mq_host", "localhost")
    channel = _RefusingChannel(
        declare_exc=ChannelPreconditionFailed("PRECONDITION_FAILED - inequivalent arg")
    )

    with pytest.raises(MqConfigError) as exc:
        await mq.resolve_exchange(channel, settings)

    message = str(exc.value)
    assert "MQ_DECLARE_TOPOLOGY=false" in message
    assert settings.mq_exchange in message
    # vhost 도 같이 봐야 한다 — 운영 전환 시 둘 다 안 내리면 같은 자리에서 또 걸린다.
    assert "vhost" in message


@pytest.mark.asyncio
async def test_missing_exchange_points_at_the_local_setup_script(monkeypatch):
    """반대 방향 — `MQ_DECLARE_TOPOLOGY=false` 인데 exchange 가 아직 없을 때.

    로컬이면 `setup_local_mq.py` 를 안 돌린 것이고, 운영이면 백엔드가 아직 안 만들었거나
    vhost 가 틀린 것이다. **어느 쪽이든 우리가 만들면 안 된다**는 것까지 문구에 남긴다 —
    여기서 막힌 사람의 첫 유혹이 플래그를 켜는 것이라서다.
    """
    from aio_pika.exceptions import ChannelNotFoundEntity

    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    channel = _RefusingChannel(
        get_exc=ChannelNotFoundEntity("NOT_FOUND - no exchange 'app.events'")
    )

    with pytest.raises(MqConfigError) as exc:
        await mq.resolve_exchange(channel, settings)

    message = str(exc.value)
    assert "setup_local_mq.py" in message
    assert "우리가 만들면 안 됩니다" in message


@pytest.mark.asyncio
@pytest.mark.parametrize("declare_topology", [True, False])
async def test_permission_denied_is_a_config_error_too(monkeypatch, declare_topology):
    """403 ACCESS_REFUSED 도 설정 오류다 — 406/404 만 잡으면 새어 나간다.

    운영 토폴로지는 백엔드 인프라 소유라 우리 AI 계정에 `configure`/`write` 권한이
    없을 수 있다 (`consume()` 이 같은 이유로 운영에서 바인딩을 시도하지 않는다).
    그때 브로커가 주는 건 406 이 아니라 **403** 이라, 빠뜨리면 권한 오류가
    `MqPublishError`(= 다음 배치가 다시 시도한다)로 나간다 — 권한을 안 고치는 한
    매일 같은 자리에서 실패하는 것이 일시적 장애처럼 보인다.
    """
    # `aio_pika.exceptions` 에는 이 이름이 없다 — 정의처인 aiormq 에서 가져온다
    #    (`topology_config_errors()` 주석 참고).
    from aiormq.exceptions import ChannelAccessRefused

    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", declare_topology)
    # declare 쪽 파라미터가 로컬 가드에 먼저 걸리지 않게 한다 — 여기서 재현하려는 건
    # **권한 거부**이지 "만들면 안 되는 브로커" 가 아니다.
    monkeypatch.setattr(settings, "mq_host", "localhost")
    refused = ChannelAccessRefused("ACCESS_REFUSED - configure access to exchange")
    channel = _RefusingChannel(declare_exc=refused, get_exc=refused)

    with pytest.raises(MqConfigError) as exc:
        await mq.resolve_exchange(channel, settings)

    # 브로커 원문을 문구에 남긴다 — 403·404·406 을 한 자리에서 잡으므로, 무엇이 왔는지는
    # 메시지로만 구분된다.
    assert "ACCESS_REFUSED" in str(exc.value)


def test_topology_error_tuple_does_not_swallow_plain_channel_close():
    """`ChannelClosed` 를 통째로 잡으면 안 된다 — 평범한 채널 종료까지 비재시도가 된다.

    `reply_code=None` 인 종료(`_on_close_ok_frame`)는 소유권 문제가 아니라 일시적
    장애다. 그걸 `MqConfigError` 로 분류하면 다음 배치가 재시도해야 할 것을 안 한다.
    """
    from aio_pika.exceptions import ChannelClosed

    errors = mq.topology_config_errors()

    assert ChannelClosed not in errors
    assert not isinstance(ChannelClosed(None, None), errors)


@pytest.mark.asyncio
async def test_publish_does_not_relabel_config_error_as_retryable(monkeypatch):
    """`_publish` 의 광범위 except 가 설정 오류를 재시도 대상으로 바꾸면 안 된다.

    `MqPublishError` 의 정의가 "재시도 대상(다음 배치가 다시 시도한다)" 이라, 설정
    오류를 그걸로 싸면 **영원히 같은 자리에서 실패하는 것**이 일시적 장애처럼 보인다.
    바로 위 `MQ_COMPANY_ID` 검사가 이미 `MqConfigError` 로 나가고 있어 그쪽과 짝을 맞춘다.
    """
    settings = mq.get_settings()
    monkeypatch.setattr(settings, "mq_enabled", True)
    monkeypatch.setattr(settings, "mq_company_id", "SLN-test")

    async def _boom(_settings):
        raise MqConfigError("exchange 소유권이 어긋났습니다")

    monkeypatch.setattr(mq, "_get_exchange", _boom)

    with pytest.raises(MqConfigError):
        await mq._publish("ai.anomaly.analyzed", {"a": 1}, "trace-1", key="ALT-1")


# ── ai.report.generated (§5) ─────────────────────────────────────


@pytest.fixture
def report_callback() -> GenerationCallback:
    """월간 합본 성공 콜백. PDF 메타는 스키마가 SUCCESS 에 요구한다."""
    return GenerationCallback(
        report_id="RPT-202607",
        status=CallbackStatus.SUCCESS,
        pdf_s3_meta=PdfS3Meta(
            company_id="c0ffee00-0000-4000-8000-000000000000",
            s3_bucket_name="sellon-reports",
            s3_file_path="reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/07/",
            original_file_name="monthly-report_202607.pdf",
            new_file_name="monthly-report_202607_a1b2.pdf",
            s3_full_key=(
                "reports/monthly-report/c0ffee00-0000-4000-8000-000000000000/2026/07/"
                "monthly-report_202607_a1b2.pdf"
            ),
            created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            file_size_bytes=497000,
            presigned_url="https://example.test/signed",
        ),
        notice_message="표본 부족으로 보류된 상품 2개: …",
    )


def test_report_payload_shape(report_callback):
    """§5 표대로 — report_id·report_month 는 있고, guideline_id·source_payload 는 없다."""
    payload = mq.build_report_payload(report_callback, "2026-07")

    assert payload["report_id"] == "RPT-202607"
    assert payload["report_month"] == "2026-07"
    assert payload["status"] == "SUCCESS"
    assert payload["pdf_s3_meta"] is not None
    assert "guideline_id" not in payload
    assert "source_payload" not in payload
    # SUCCESS 인데도 notice_message 가 실린다 — 소비 측이 무시하면 안 되는 값이다
    assert payload["notice_message"]


def test_report_month_is_passed_not_derived(report_callback):
    """report_month 는 인자로 받는다 — report_id 에서 잘라 쓰지 않는다.

    `RPT-202607` → `2026-07` 로 되돌릴 수 있어 보이지만, 정본이 손에 있는데 재구성하는
       패턴은 alert_id 에서 이미 문제가 됐다(`_alert_id_of` 주석). 인자로 준 값이
       그대로 실리는지 고정한다.
    """
    payload = mq.build_report_payload(report_callback, "2026-05")

    assert payload["report_month"] == "2026-05"  # report_id 는 202607 이지만 인자가 이긴다


def test_guideline_callback_rejected_on_report_key(guideline_callback):
    """guideline_id 가 있는 산출물을 월간 키로 실으면 안 된다.

    `build_guideline_payload` 의 역방향 가드와 짝이다. 둘이 뒤바뀌면 백엔드가 엉뚱한
    소비 동작(CS팀 메일 발송)을 탄다.

    스키마가 report_id / guideline_id 를 **배타**로 강제하므로(정확히 하나),
       가이드라인 콜백은 report_id 가 None 이라 그 검사에서 걸린다. 검사가 하나여도
       양방향이 다 막힌다는 것을 이 테스트가 고정한다.
    """
    assert guideline_callback.report_id is None  # 배타 제약의 결과

    with pytest.raises(ValueError, match="ai.guideline.generated 로 발행"):
        mq.build_report_payload(guideline_callback, "2026-07")


@pytest.mark.parametrize(
    "status", [CallbackStatus.HOLD_INSUFFICIENT_DATA, CallbackStatus.FAILED_VALIDATION]
)
def test_product_level_status_never_leaves_as_monthly_event(status):
    """상품 단위 판정은 월 단위 이벤트에 실을 수 없다(§5).

    보류·검증실패는 상품 하나의 결과인데 이벤트는 월 1건이다. 실어 보내면 백엔드가
    "이번 달 리포트 전체가 보류됐다"로 읽는다 — 실제로는 나머지 상품이 정상 발행됐는데도.
    """
    callback = GenerationCallback(
        report_id="RPT-202607", status=status, notice_message="상품 단위 판정"
    )

    with pytest.raises(ValueError, match="월 단위 이벤트"):
        mq.build_report_payload(callback, "2026-07")


def test_report_publisher_signature_matches_batch_call():
    """배치 호출부와 시그니처가 어긋나지 않는지만 본다.

    이 테스트는 **발행 동작을 검증하지 않는다.** 이름이 그렇게 읽히지 않도록 바꿨다 —
       예전 이름(`..._uses_report_id_as_idempotency_key`)은 멱등 키를 검증하는 것처럼
       보였는데 실제로는 파라미터 이름만 봤다. 라우팅 키를 오타내도 통과했다.
       실제 발행은 아래 `test_report_publish_sends_...` 가 본다.
    """
    assert list(inspect.signature(mq.publish_report_generated).parameters) == [
        "callback",
        "report_month",
        "trace_id",
    ]


@pytest.mark.asyncio
async def test_report_publish_sends_envelope_on_the_right_routing_key(
    report_callback, monkeypatch
):
    """전송 직전까지 태운다 — 라우팅 키·멱등 키·Envelope 이 전부 실물이다.

    시그니처만 보는 테스트로는 `REPORT_GENERATED` 오타를 못 잡는다. 오타가 나면
       바인딩(`ai.#`)에는 걸려도 백엔드가 그 이벤트를 안 읽어 **리포트 행이 안 생기는데**,
       배치는 발행 성공으로 보고 끝난다. 여기서 문자열을 직접 확인한다.
    """
    sent: dict = {}

    class FakeExchange:
        async def publish(self, message, routing_key, timeout=None):
            sent["routing_key"] = routing_key
            sent["body"] = message.body
            return Basic.Ack(delivery_tag=1)

    async def fake_get_exchange(_settings):
        return FakeExchange()

    monkeypatch.setattr(mq.get_settings(), "mq_enabled", True)
    monkeypatch.setattr(mq.get_settings(), "mq_company_id", "SLN-test")
    monkeypatch.setattr(mq, "_get_exchange", fake_get_exchange)

    await mq.publish_report_generated(report_callback, "2026-07", "trace-rpt")

    envelope = json.loads(sent["body"].decode("utf-8"))
    assert sent["routing_key"] == "ai.report.generated"
    assert envelope["eventType"] == "ai.report.generated"
    assert envelope["traceId"] == "trace-rpt"
    # 멱등 키는 report_id — 같은 달을 다시 돌려도 메인이 upsert 한다
    assert envelope["payload"]["report_id"] == "RPT-202607"
    assert envelope["payload"]["report_month"] == "2026-07"
    assert "guideline_id" not in envelope["payload"]


@pytest.mark.asyncio
async def test_report_publish_blocked_when_mq_disabled(report_callback, monkeypatch):
    """MQ_ENABLED=false 는 no-op 이 아니다 — 다른 두 발행 함수와 같은 규칙."""
    monkeypatch.setattr(mq.get_settings(), "mq_enabled", False)

    with pytest.raises(MqDisabledError):
        await mq.publish_report_generated(report_callback, "2026-07", "trace-1")
