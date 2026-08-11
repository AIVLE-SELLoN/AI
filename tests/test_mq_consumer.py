"""담당: 지인 — `ai.inbound` 컨슈머(`app/core/mq_consumer.py`).

브로커 없이 돈다 — 파싱·디스패치·재수화만 본다. 실제 수신은 `scripts/smoke_mq.py --feedback`.
"""

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.core import mq_consumer
from app.core.exceptions import HitlContextUnavailableError, MqConfigError
from app.core.schemas import (
    Aspect,
    Channel,
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evaluator,
    EvaluatorChecks,
    Evidence,
    HitlStatus,
    Proposal,
    ProposalType,
    Recommendation,
    RecommendedAction,
    RejectionReasonCode,
    Source,
    SourceSignals,
    Verdict,
)

ALERT_ID = "ALT-20260828-P001-COUPANG"
REC_ID = "REC-a1b2c3d4e5f6"


def _alert() -> DetectionAlert:
    return DetectionAlert(
        alert_id=ALERT_ID,
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


def _recommendation() -> Recommendation:
    """발행 시점 사본 — `hitl_status` 가 대기로 굳어 있다(§4-2)."""
    return Recommendation(
        recommendation_id=REC_ID,
        alert_id=ALERT_ID,
        created_at=datetime(2026, 8, 28, 9, 0, 12, tzinfo=timezone.utc),
        proposal=Proposal(
            type=ProposalType.COPY_DRAFT,
            target_field="상세설명",
            current_text="현재 문구",
            proposed_text="모니터 환경에 따라 색상 차이가 있을 수 있습니다",
            rationale="사진 색감 관련 문의 다수",
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


def _payload(**overrides) -> dict:
    payload = {
        "recommendation_id": REC_ID,
        "alert_id": ALERT_ID,
        "hitl_status": "반려",
        "hitl_feedback": {
            "processed_at": "2026-08-29T09:11:50Z",
            "processed_by": "seller_001",
            "rejection_reason": {
                "reason_code": "이미조치함",
                "reason_text": "지난주에 상세페이지 이미 수정했습니다",
            },
            "edited_text": None,
        },
    }
    payload.update(overrides)
    return payload


# ── payload 파싱 (§8) ────────────────────────────────────────────


def test_parses_spec_payload_with_korean_enums():
    """§8 예시 그대로 파싱된다. enum 은 한글 값으로 들어온다(§9)."""
    event = mq_consumer.RecommendationReviewed.model_validate(_payload())

    assert event.hitl_status == HitlStatus.REJECTED
    assert event.hitl_feedback.rejection_reason.reason_code == (
        RejectionReasonCode.ALREADY_HANDLED
    )


def test_rejects_unknown_hitl_status():
    """계약 밖 값은 파싱 단계에서 막는다 — 컬렉션2에 이상한 라벨이 쌓이면 되돌리기 어렵다."""
    with pytest.raises(ValidationError):
        mq_consumer.RecommendationReviewed.model_validate(_payload(hitl_status="보류"))


# ── 재수화 (미정 지점) ───────────────────────────────────────────


def test_missing_context_is_loud_not_silent():
    """⚠️ 전문이 없으면 조용히 넘기지 않는다.

    이 이벤트가 **컬렉션2 축적의 유일한 경로**라, 조용히 스킵하면 학습 자료가 영영
    안 쌓이는데 아무도 모른다. 컨슈머는 이 예외를 받아 DLX 로 보낸다.
    """
    event = mq_consumer.RecommendationReviewed.model_validate(_payload())

    with pytest.raises(HitlContextUnavailableError, match=REC_ID):
        mq_consumer.load_hitl_context(event)


def test_event_hitl_values_win_over_embedded_copy():
    """실어 보낸 recommendation 은 발행 시점 사본이라 hitl_status 가 '대기'다.

    그대로 쓰면 record_hitl_outcome() 이 "아직 결정 전"이라며 거부한다 —
    이벤트 값이 정본이므로 덮어써야 한다.
    """
    payload = _payload(
        alert=_alert().model_dump(mode="json"),
        recommendation=_recommendation().model_dump(mode="json"),
    )
    event = mq_consumer.RecommendationReviewed.model_validate(payload)

    assert event.recommendation.hitl_status == HitlStatus.PENDING  # 사본은 대기

    _alert_out, recommendation = mq_consumer.load_hitl_context(event)

    assert recommendation.hitl_status == HitlStatus.REJECTED
    assert recommendation.hitl_feedback.processed_by == "seller_001"
    # 사본을 제자리에서 고치지 않는다 — 같은 이벤트를 두 번 태워도 결과가 같아야 한다.
    assert event.recommendation.hitl_status == HitlStatus.PENDING


# ── 디스패치 ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatches_to_registered_handler(monkeypatch):
    seen: dict = {}

    def fake_handler(payload):
        seen["payload"] = payload

    monkeypatch.setitem(
        mq_consumer.HANDLERS, mq_consumer.RECOMMENDATION_REVIEWED, fake_handler
    )
    body = (
        b'{"eventType": "feedback.recommendation.reviewed", '
        b'"payload": {"recommendation_id": "REC-1"}}'
    )

    await mq_consumer.dispatch(mq_consumer.RECOMMENDATION_REVIEWED, body)

    assert seen["payload"] == {"recommendation_id": "REC-1"}


@pytest.mark.asyncio
async def test_unhandled_event_type_raises_instead_of_acking():
    """⚠️ 핸들러 없는 이벤트를 ACK 하면 그 메시지는 사라진다.

    `ai.inbound` 한 큐에 `feedback.#` 이 전부 들어오는데, 보고서 피드백(용준)은 아직
    핸들러가 없다. 우리가 삼키면 용준 쪽에서 영영 못 받는다 — 던져서 DLX 로 보낸다.
    """
    body = b'{"eventType": "feedback.report.created", "payload": {}}'

    with pytest.raises(KeyError):
        await mq_consumer.dispatch(mq_consumer.REPORT_CREATED, body)


def test_core_does_not_import_components():
    """⚠️ core 는 컴포넌트를 import 하지 않는다 (팀 규칙: 컴포넌트가 core 에서 가져다 쓴다).

    처리 함수는 실행 진입점이 `register_handler()` 로 꽂아 준다. 여기서 core 가
    `app.recommendation` 을 직접 부르기 시작하면 의존 방향이 거꾸로 뒤집힌다.
    """
    source = Path(mq_consumer.__file__).read_text(encoding="utf-8")

    assert "app.recommendation" not in source
    assert "app.reporting" not in source


@pytest.mark.asyncio
async def test_register_handler_makes_dispatch_work(monkeypatch):
    """등록한 이벤트만 처리된다 — 배선이 실제로 먹는지 확인한다."""
    seen: dict = {}

    def handler(payload: dict) -> None:
        seen["payload"] = payload

    # 전역 등록표를 복사본으로 바꿔 다른 테스트에 새지 않게 한다.
    monkeypatch.setattr(mq_consumer, "HANDLERS", dict(mq_consumer.HANDLERS))
    mq_consumer.register_handler("feedback.test", handler)

    await mq_consumer.dispatch("feedback.test", b'{"payload": {"x": 1}}')

    assert seen["payload"] == {"x": 1}


# ── 토폴로지 소유권 ──────────────────────────────────────────────


class _FakeChannel:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def declare_queue(self, *_args, **_kwargs):
        self.calls.append("declare_queue")
        return "queue"

    async def get_queue(self, *_args, **_kwargs):
        self.calls.append("get_queue")
        return "queue"


@pytest.mark.asyncio
async def test_does_not_redeclare_inbound_queue(monkeypatch):
    """⚠️ `ai.inbound` 는 우리 큐가 아니다 — 바인딩만 추가하는 게 계약(§2-1)이다.

    백엔드가 quorum·DLX·TTL 을 걸어 만든 큐를 우리가 맨 인자로 declare 하면
    PRECONDITION_FAILED 로 컨슈머가 아예 못 뜬다.
    """
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    channel = _FakeChannel()

    await mq_consumer.resolve_queue(channel, mq_consumer.INBOUND_QUEUE, settings)

    assert channel.calls == ["get_queue"]


@pytest.mark.asyncio
async def test_declares_queue_only_for_local_topology(monkeypatch):
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    channel = _FakeChannel()

    await mq_consumer.resolve_queue(channel, mq_consumer.INBOUND_QUEUE, settings)

    assert channel.calls == ["declare_queue"]


class _RefusingQueueChannel:
    """큐 호출을 브로커가 거부하는 채널. 두 방향을 각각 재현한다."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def declare_queue(self, *_args, **_kwargs):
        raise self._exc

    async def get_queue(self, *_args, **_kwargs):
        raise self._exc


@pytest.mark.asyncio
async def test_declaring_someone_elses_queue_says_which_flag_to_drop(monkeypatch):
    """🔴 운영 브로커에 `MQ_DECLARE_TOPOLOGY=true` 로 붙었을 때의 안내.

    **exchange 보다 이쪽이 먼저 걸린다.** 우리가 주는 인자는 `durable=True` 하나뿐인데
    운영 큐는 quorum·DLX·delivery-limit·TTL 이라(§2-1) 맞을 수가 없다 — exchange 는
    topic·durable 이 우연히 같아 통과할 수 있다. 그러면 `resolve_exchange` 의 친절한
    문구를 못 보고 여기서 브로커 원문만 보게 된다.

    컨슈머가 못 뜨면 `feedback.recommendation.reviewed` 가 안 들어오고, 그건 컬렉션2
    축적의 유일한 경로다.
    """
    from aio_pika.exceptions import ChannelPreconditionFailed

    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", True)
    channel = _RefusingQueueChannel(
        ChannelPreconditionFailed("PRECONDITION_FAILED - inequivalent arg 'x-queue-type'")
    )

    with pytest.raises(MqConfigError) as exc:
        await mq_consumer.resolve_queue(channel, mq_consumer.INBOUND_QUEUE, settings)

    message = str(exc.value)
    assert "MQ_DECLARE_TOPOLOGY=false" in message
    assert mq_consumer.INBOUND_QUEUE in message
    assert "vhost" in message


@pytest.mark.asyncio
async def test_missing_queue_points_at_the_local_setup_script(monkeypatch):
    """반대 방향 — `MQ_DECLARE_TOPOLOGY=false` 인데 큐가 아직 없을 때.

    로컬이면 `setup_local_mq.py` 를 안 돌린 것이고, 운영이면 백엔드가 아직 안
    만들었거나 vhost·권한이 틀린 것이다. **어느 쪽이든 우리가 만들면 안 된다**는 것까지
    문구에 남긴다 — 여기서 막힌 사람의 첫 유혹이 플래그를 켜는 것이라서다.
    """
    from aio_pika.exceptions import ChannelNotFoundEntity

    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    channel = _RefusingQueueChannel(
        ChannelNotFoundEntity("NOT_FOUND - no queue 'ai.inbound'")
    )

    with pytest.raises(MqConfigError) as exc:
        await mq_consumer.resolve_queue(channel, mq_consumer.INBOUND_QUEUE, settings)

    message = str(exc.value)
    assert "setup_local_mq.py" in message
    assert "우리가 만들면 안 됩니다" in message


@pytest.mark.asyncio
async def test_queue_and_exchange_share_one_error_list(monkeypatch):
    """🔴 두 함수가 **같은** 예외 목록을 봐야 한다 — 한쪽만 넓히면 조용히 갈린다.

    403 을 exchange 쪽에만 넣으면 exchange 는 잡히고 큐는 안 잡히는 상태가 되는데,
    둘 다 같은 브로커·같은 계정이라 실제로는 항상 같이 온다.
    """
    # ⚠️ `aio_pika.exceptions` 에는 이 이름이 없다 — 정의처인 aiormq 에서 가져온다
    #    (`mq.topology_config_errors()` 주석 참고).
    from aiormq.exceptions import ChannelAccessRefused

    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_declare_topology", False)
    channel = _RefusingQueueChannel(
        ChannelAccessRefused("ACCESS_REFUSED - read access to queue")
    )

    with pytest.raises(MqConfigError) as exc:
        await mq_consumer.resolve_queue(channel, mq_consumer.INBOUND_QUEUE, settings)

    assert "ACCESS_REFUSED" in str(exc.value)


# ── 실패 분류(nack requeue) ──────────────────────────────────────
#
# consume() 의 루프를 브로커만 가짜로 두고 실제로 태운다. 여기까지 안 오면
# "영구 실패를 재시도로 분류"하는 버그가 유닛 테스트를 그대로 통과한다.


def _raise(exc: Exception):
    """주어진 예외를 던지는 핸들러."""

    def handler(_payload: dict) -> None:
        raise exc

    return handler


class _FakeMessage:
    def __init__(self, body: bytes, event_type: str) -> None:
        self.body = body
        self.type = event_type
        self.routing_key = event_type
        self.actions: list[tuple[str, bool | None]] = []

    async def ack(self) -> None:
        self.actions.append(("ack", None))

    async def nack(self, requeue: bool = True) -> None:
        self.actions.append(("nack", requeue))


class _FakeIterator:
    def __init__(self, messages: list) -> None:
        self._messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class _FakeQueue:
    def __init__(self, messages: list) -> None:
        self._messages = messages

    async def bind(self, *_args, **_kwargs) -> None:
        return None

    def iterator(self):
        return _FakeIterator(self._messages)


class _FakeConsumerChannel:
    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue

    async def set_qos(self, **_kwargs) -> None:
        return None

    async def get_queue(self, *_args, **_kwargs):
        return self._queue

    async def declare_queue(self, *_args, **_kwargs):
        return self._queue


class _FakeConnection:
    def __init__(self, channel: _FakeConsumerChannel) -> None:
        self._channel = channel

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def channel(self, **_kwargs):
        return self._channel


async def _consume_one(monkeypatch, body: bytes, handler) -> _FakeMessage:
    """메시지 1건을 consume() 루프에 태우고 ack/nack 결과를 돌려준다."""
    import aio_pika

    from app.config import get_settings
    from app.core import mq

    settings = get_settings()
    monkeypatch.setattr(settings, "mq_enabled", True)
    monkeypatch.setattr(settings, "mq_declare_topology", False)

    message = _FakeMessage(body, mq_consumer.RECOMMENDATION_REVIEWED)
    connection = _FakeConnection(_FakeConsumerChannel(_FakeQueue([message])))

    async def fake_connect(**_kwargs):
        return connection

    async def fake_exchange(*_args, **_kwargs):
        return object()

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect)
    monkeypatch.setattr(mq, "resolve_exchange", fake_exchange)
    handlers = {mq_consumer.RECOMMENDATION_REVIEWED: handler} if handler else {}
    monkeypatch.setattr(mq_consumer, "HANDLERS", handlers)

    await mq_consumer.consume()
    return message


_GOOD_BODY = b'{"eventType": "feedback.recommendation.reviewed", "payload": {"x": 1}}'


@pytest.mark.asyncio
async def test_successful_handling_acks(monkeypatch):
    message = await _consume_one(monkeypatch, _GOOD_BODY, lambda _payload: None)

    assert message.actions == [("ack", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body, handler",
    [
        # record_hitl_outcome 의 alert_id 불일치·hitl_status 대기 — 둘 다 ValueError 다.
        (_GOOD_BODY, _raise(ValueError("alert_id 가 다릅니다"))),
        # 계약 위반. pydantic ValidationError 도 ValueError 서브클래스라 같이 걸린다.
        (_GOOD_BODY, _raise(HitlContextUnavailableError("재료 부족"))),
        # 깨진 JSON — 핸들러에 닿지도 못한다(JSONDecodeError 는 ValueError).
        (b"{not json", lambda _payload: None),
        # 핸들러 미등록(용준 feedback.report.created) — ACK 하면 그 메시지가 사라진다.
        (_GOOD_BODY, None),
    ],
)
async def test_permanent_failures_go_straight_to_dlx(monkeypatch, body, handler):
    """⚠️ 다시 넣어도 결과가 같은 실패는 requeue 하지 않는다.

    재시도로 분류하면 운영에서는 delivery-limit 5 를 다 태운 뒤에야 DLX 로 가고
    (Chroma 쓰기 5회 + 에러 로그 5줄), 로컬 classic 큐는 그 상한이 없어 무한 재전달이
    된다. 2026-08-07 재검토에서 ValueError 계열이 이 분류에서 빠져 있던 것을 고쳤다.
    """
    message = await _consume_one(monkeypatch, body, handler)

    assert message.actions == [("nack", False)]


@pytest.mark.asyncio
async def test_transient_failures_are_retried(monkeypatch):
    """벡터DB 다운 같은 일시적 오류만 requeue 한다 — 다음 전달에서 성공할 수 있다."""
    message = await _consume_one(
        monkeypatch, _GOOD_BODY, _raise(ConnectionError("chroma down"))
    )

    assert message.actions == [("nack", True)]
