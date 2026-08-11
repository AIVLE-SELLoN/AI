"""담당: 지인 — 배치 진입점 `generate_outcome_for_alert()` / `generate_for_alert()`
(`app/recommendation/pipeline.py`).

**계약의 핵심은 "예외를 안 던진다"** 다. 배치가 알림 20건을 도는 중이라, 개선안 1건의
실패가 밖으로 나가면 그 알림의 **발행까지 막혀** 셀러가 이상 알림 자체를 못 받는다.
그래서 "안 던진다"를 말로 적어두는 대신 실제로 던져보고 확인한다.

두 번째 계약은 **"개선안이 없는 사유를 값으로 돌려준다"** 다(2026-08-10). 근거 0건은
데이터 갭이라 배치 실패가 아니고, 그 구분이 `None` 하나로는 안 된다.

LLM 은 부르지 않는다 — `run_with_outcome()` 을 몽키패치로 막는다.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

from app.core.exceptions import LlmCallError, LlmParseError, VectorDbError
from app.core.schemas import (
    Aspect,
    Channel,
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evaluator,
    EvaluatorChecks,
    Evidence,
    LinkedCSInquiry,
    Recommendation,
    RecommendedAction,
    Source,
    SourceSignals,
    Verdict,
)
from app.recommendation import pipeline


def _alert(action=RecommendedAction.GENERATE_RECOMMENDATION) -> DetectionAlert:
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
        recommended_action=action,
        evidence=Evidence(inquiry_ids=["INQ-000412"]),
    )


def _inquiries() -> list[LinkedCSInquiry]:
    return [
        LinkedCSInquiry(
            item_id="INQ-000412",
            raw_text="사진이랑 색이 너무 달라요",
            created_at=datetime(2026, 8, 27, 10, 0, tzinfo=timezone.utc),
        )
    ]


def _recommendation() -> Recommendation:
    return Recommendation(
        recommendation_id="REC-a1b2c3d4e5f6",
        alert_id="ALT-20260828-P001-COUPANG",
        created_at=datetime(2026, 8, 28, 9, 0, 12, tzinfo=timezone.utc),
        evaluator=Evaluator(
            passed=True,
            attempts=1,
            checks=EvaluatorChecks(
                grounding=True, consistency=True, actionability=True
            ),
        ),
    )


@pytest.mark.asyncio
async def test_returns_recommendation_on_success(monkeypatch):
    """정상 경로는 run 결과를 그대로 돌려주고, **inquiries 를 그대로 넘긴다**.

    전달을 같이 고정하는 이유: 배선이 끊겨도 결과 객체는 똑같이 나오는데, 실제로는
    image_guide 가 근거를 잃어 전건 fallback 이 된다(조용한 열화). 아래
    `test_never_raises_whatever_run_throws` 는 무엇이든 흡수하므로 이 끊김을 못 잡는다.
    """
    expected = _recommendation()
    inquiries = _inquiries()
    seen: dict = {}

    async def fake_run(alert, passed_inquiries=()):
        seen["inquiries"] = passed_inquiries
        return pipeline.RecommendationOutcome(expected)

    monkeypatch.setattr(pipeline, "run_with_outcome", fake_run)

    outcome = await pipeline.generate_outcome_for_alert(_alert(), inquiries)
    assert outcome.recommendation is expected
    assert outcome.reason is None, "성공엔 사유가 붙지 않는다"
    assert seen["inquiries"] == inquiries

    # 얇은 래퍼(팀 확정 시그니처)도 같은 결과를 돌려준다.
    assert await pipeline.generate_for_alert(_alert(), inquiries) is expected


@pytest.mark.asyncio
async def test_gate_closed_skips_llm_entirely(monkeypatch):
    """조치 7종 중 '개선안 생성' 이 아니면 LLM 을 한 번도 안 부른다.

    게이트가 새면 알림 대부분(6종)에 쓸데없는 LLM 비용이 붙는다.
    """

    async def boom(alert, inquiries=()):
        raise AssertionError("게이트가 닫혔는데 run 이 불렸습니다")

    monkeypatch.setattr(pipeline, "run_with_outcome", boom)
    alert = _alert(action=RecommendedAction.LOGISTICS_CHECK)

    outcome = await pipeline.generate_outcome_for_alert(alert, _inquiries())
    assert outcome.recommendation is None
    assert outcome.reason is pipeline.SkipReason.GATE_CLOSED
    assert not outcome.is_evidence_gap, "게이트 미충족은 근거 갭이 아니다"
    assert await pipeline.generate_for_alert(alert, _inquiries()) is None


@pytest.mark.parametrize(
    "error",
    [
        LlmCallError("OpenAI 타임아웃"),
        LlmParseError("JSON 파싱 실패"),
        VectorDbError("Chroma 접속 실패"),
        RuntimeError("예상 못 한 오류"),
        ValueError("스키마 조립 실패"),
    ],
)
@pytest.mark.asyncio
async def test_never_raises_whatever_run_throws(monkeypatch, error, caplog):
    """⚠️ 무엇이 터지든 None 으로 흡수한다.

    여기서 예외가 새면 배치 루프의 `except` 가 잡아 **그 알림이 발행되지 않는다** —
    개선안이 없는 것과 알림 자체가 안 가는 것은 셀러 입장에서 전혀 다르다.
    사유는 로그로 남겨야 추적이 된다(배치 요약엔 "개선안 없음"으로만 보인다).
    """

    async def fake_run(alert, inquiries=()):
        raise error

    monkeypatch.setattr(pipeline, "run_with_outcome", fake_run)

    outcome = await pipeline.generate_outcome_for_alert(_alert(), _inquiries())
    assert outcome.recommendation is None
    assert outcome.reason is pipeline.SkipReason.ERROR
    assert not outcome.is_evidence_gap, (
        "예외를 데이터 갭으로 세면 고장 난 배치가 종료코드 0 으로 끝난다"
    )
    assert repr(error) == outcome.detail, "요약에 진짜 사유가 남아야 한다"
    assert any("개선안 생성 실패" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_is_not_swallowed(monkeypatch):
    """취소는 통과시킨다 — 배치를 멈췄는데 '개선안 실패'로 둔갑해 루프가 계속 돌면 안 된다.

    `asyncio.CancelledError` 는 BaseException 이라 `except Exception` 에 안 걸린다.
    이건 사고가 아니라 의도이므로 고정해 둔다.
    """

    async def fake_run(alert, inquiries=()):
        raise asyncio.CancelledError()

    monkeypatch.setattr(pipeline, "run_with_outcome", fake_run)

    with pytest.raises(asyncio.CancelledError):
        await pipeline.generate_outcome_for_alert(_alert(), _inquiries())
