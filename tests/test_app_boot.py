"""0주차 체크리스트: "각자 hello world 라우터 1개 올려서 앱 1개가 4명 코드로 뜨는지 확인"

이 테스트가 통과하면 4명의 라우터가 하나의 FastAPI 앱에 문제없이 등록된 것.
누가 라우터를 깨뜨리면 여기서 먼저 걸린다.
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    ("path", "module"),
    [
        ("/api/v1/classify/ping", "classification"),
        ("/api/v1/detect/ping", "detection"),
        ("/api/v1/recommendations/ping", "recommendation"),
        ("/api/v1/reports/ping", "reporting"),
    ],
)
def test_module_ping(path: str, module: str) -> None:
    """4개 모듈이 전부 등록됐는지."""
    response = client.get(path)
    assert response.status_code == 200
    assert response.json()["module"] == module


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/reports",
        "/api/v1/replies",
    ],
)
def test_report_endpoints_validate_input(path: str) -> None:
    """리포팅 엔드포인트는 구현 완료 — 빈 body 는 501 이 아니라 422(스키마 검증 실패)다.

    원래 이 자리는 "아직 501" 을 확인하던 테스트였다. 4개 모듈이 순서대로 구현되며
    recommendations→tests/test_recommendation_router.py, detect→tests/test_pipeline.py,
    reports·replies→tests/test_report.py 로 각각 옮겨갔고, 여기에는 "라우터가 앱에
    등록돼 있고 입력 검증이 걸린다"는 부팅 확인만 남긴다.
    """
    response = client.post(path, json={})
    assert response.status_code == 422
