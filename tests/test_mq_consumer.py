"""담당: 지인 — `ai.inbound` 컨슈머(`app/core/mq_consumer.py`).

브로커 없이 돈다 — 파싱·디스패치·재수화만 본다. 실제 수신은 `scripts/smoke_mq.py --feedback`.
"""

from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from app.core import mq_consumer
from app.core.exceptions import HitlContextUnavailableError
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
        mq_consumer._load_hitl_context(event)


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

    _alert_out, recommendation = mq_consumer._load_hitl_context(event)

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


@pytest.mark.asyncio
async def test_handler_records_hitl_outcome(monkeypatch):
    """전문이 실려 오면 컬렉션2 적재 함수까지 연결된다."""
    recorded: dict = {}

    def fake_record(alert, recommendation):
        recorded["alert_id"] = alert.alert_id
        recorded["status"] = recommendation.hitl_status

    monkeypatch.setattr("app.recommendation.pipeline.record_hitl_outcome", fake_record)
    payload = _payload(
        alert=_alert().model_dump(mode="json"),
        recommendation=_recommendation().model_dump(mode="json"),
    )

    mq_consumer.handle_recommendation_reviewed(payload)

    assert recorded == {"alert_id": ALERT_ID, "status": HitlStatus.REJECTED}
