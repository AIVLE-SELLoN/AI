"""담당: 지인 — router.py 엔드포인트 테스트.

TestClient로 실제 HTTP 요청/응답 형태까지 확인한다. 파이프라인 자체(LLM·ChromaDB)는
이미 다른 테스트가 검증했으니, 여기선 service.py 호출 지점만 모킹해서 라우터의
요청 파싱·응답 스키마·에러 변환(400)만 본다 — 비용 0 유지.
"""

from fastapi.testclient import TestClient

import app.recommendation.router as recommendation_router
from app.core.schemas import (
    Citation,
    Evaluator,
    EvaluatorChecks,
    HitlFeedback,
    HitlStatus,
    Recommendation,
)
from app.main import app

client = TestClient(app)


def _recommendation(alert_id: str, hitl_status: str = "대기", hitl_feedback: HitlFeedback | None = None) -> Recommendation:
    return Recommendation(
        recommendation_id="REC-ROUTER-TEST",
        alert_id=alert_id,
        created_at="2026-05-28T10:31:40",
        citations=[Citation(inquiry_id="INQ-000412", quote="발췌")],
        evaluator=Evaluator(
            passed=True, attempts=1, checks=EvaluatorChecks(grounding=True, consistency=True, actionability=True)
        ),
        hitl_status=hitl_status,
        hitl_feedback=hitl_feedback,
    )


def test_generate_endpoint_returns_recommendation(monkeypatch, biased_alert):
    expected = _recommendation(biased_alert.alert_id)

    async def _fake_generate(alert):
        assert alert.alert_id == biased_alert.alert_id
        return expected

    monkeypatch.setattr(recommendation_router.service, "generate_recommendation", _fake_generate)

    response = client.post(
        "/api/v1/recommendations/generate",
        json={"alert": biased_alert.model_dump(mode="json")},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["recommendation"]["recommendation_id"] == "REC-ROUTER-TEST"


def test_generate_endpoint_returns_null_when_trigger_not_met(monkeypatch, global_alert):
    async def _fake_generate(alert):
        return None

    monkeypatch.setattr(recommendation_router.service, "generate_recommendation", _fake_generate)

    response = client.post(
        "/api/v1/recommendations/generate",
        json={"alert": global_alert.model_dump(mode="json")},
    )

    assert response.status_code == 200
    assert response.json() == {"recommendation": None}


def test_hitl_endpoint_records_outcome(monkeypatch, biased_alert):
    recorded_calls = []
    monkeypatch.setattr(
        recommendation_router.service,
        "record_hitl_outcome",
        lambda alert, recommendation: recorded_calls.append((alert.alert_id, recommendation.recommendation_id)),
    )

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.APPROVED,
        hitl_feedback=HitlFeedback(processed_at="2026-05-29T09:00:00", processed_by="seller-001"),
    )

    response = client.post(
        "/api/v1/recommendations/hitl",
        json={
            "alert": biased_alert.model_dump(mode="json"),
            "recommendation": recommendation.model_dump(mode="json"),
        },
    )

    assert response.status_code == 200
    assert response.json() == {"recorded": True}
    assert recorded_calls == [(biased_alert.alert_id, "REC-ROUTER-TEST")]


def test_hitl_endpoint_returns_400_when_service_raises_value_error(monkeypatch, biased_alert):
    def _raise(alert, recommendation):
        raise ValueError("hitl_status가 아직 결정되지 않았습니다(PENDING) — 적재 대상 아님")

    monkeypatch.setattr(recommendation_router.service, "record_hitl_outcome", _raise)

    recommendation = _recommendation(biased_alert.alert_id)  # hitl_status 기본값 = 대기(PENDING)

    response = client.post(
        "/api/v1/recommendations/hitl",
        json={
            "alert": biased_alert.model_dump(mode="json"),
            "recommendation": recommendation.model_dump(mode="json"),
        },
    )

    assert response.status_code == 400
    assert "PENDING" in response.json()["detail"]
