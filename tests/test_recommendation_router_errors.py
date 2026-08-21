"""담당: 지인 — 개선안 라우터의 예외 → HTTP 상태코드 매핑.

임베딩이 OpenAI 를 타면서 레이트리밋·인증 실패가 REST 경로로 올라올 수 있게 됐다.
안 잡으면 500 + 스택트레이스라 호출자가 "우리 요청이 잘못됐나 / 일시 장애인가"를
가릴 수 없다.

매핑 대상은 `VectorDbError` 하나다. 상위 `AiServiceError` 를 통째로 503 에 묶으면
설정 오류(`MqConfigError`)까지 "잠시 후 재시도"가 되어 거짓 안내가 된다.
"""

import pytest
from fastapi.testclient import TestClient

from app.core.exceptions import MqConfigError, VectorDbError
from app.core.schemas import (
    Evaluator,
    EvaluatorChecks,
    HitlStatus,
    Proposal,
    ProposalType,
    Recommendation,
)
from app.main import app
from app.recommendation import router as rec_router


@pytest.fixture
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def approved_recommendation(biased_alert) -> Recommendation:
    """승인된 개선안 1건. 라우터가 service 를 부르기 전 스키마 검증을 통과할 최소 형태."""
    return Recommendation(
        recommendation_id="REC-ROUTER-TEST",
        alert_id=biased_alert.alert_id,
        created_at="2026-05-28T10:31:40",
        proposal=Proposal(
            type=ProposalType.IMAGE_GUIDE,
            target_field="색상",
            current_text="사진이랑 색이 너무 달라요",
            proposed_text="자연광에서 재촬영을 진행하세요.",
            rationale="원인 분류: 사진_색감_오차",
            detailpage_grounded=False,
        ),
        citations=[],
        evaluator=Evaluator(
            passed=True,
            attempts=1,
            checks=EvaluatorChecks(grounding=True, consistency=True, actionability=True),
        ),
        hitl_status=HitlStatus.APPROVED,
    )


def test_generate_maps_vectordb_error_to_503(client, monkeypatch, biased_alert):
    async def boom(_alert):
        raise VectorDbError("query 실패 — 임베딩 API 오류: 429")

    monkeypatch.setattr(rec_router.service, "generate_recommendation", boom)

    response = client.post(
        "/api/v1/recommendations/generate",
        json={"alert": biased_alert.model_dump(mode="json")},
    )

    assert response.status_code == 503
    assert response.json()["detail"] == rec_router.VECTORDB_UNAVAILABLE_DETAIL


def test_generate_does_not_swallow_other_service_errors(client, monkeypatch, biased_alert):
    """설정 오류까지 503 으로 바꾸면 '잠시 후 재시도' 라는 거짓 안내가 된다."""

    async def boom(_alert):
        raise MqConfigError("companyId 미설정")

    monkeypatch.setattr(rec_router.service, "generate_recommendation", boom)

    response = client.post(
        "/api/v1/recommendations/generate",
        json={"alert": biased_alert.model_dump(mode="json")},
    )

    assert response.status_code == 500


def test_hitl_maps_vectordb_error_to_503(client, monkeypatch, biased_alert, approved_recommendation):
    """적재도 문서를 임베딩하므로 이 경로로도 공급자 오류가 올라온다."""

    def boom(_alert, _recommendation):
        raise VectorDbError("upsert 실패 — 임베딩 API 오류: 401")

    monkeypatch.setattr(rec_router.service, "record_hitl_outcome", boom)

    response = client.post(
        "/api/v1/recommendations/hitl",
        json={
            "alert": biased_alert.model_dump(mode="json"),
            "recommendation": approved_recommendation.model_dump(mode="json"),
        },
    )

    assert response.status_code == 503