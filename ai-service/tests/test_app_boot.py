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
        "/api/v1/classify",
        "/api/v1/detect",
        "/api/v1/recommendations/generate",
        "/api/v1/reports",
        "/api/v1/replies",
    ],
)
def test_endpoints_are_registered_but_unimplemented(path: str) -> None:
    """실제 엔드포인트는 경로만 잡혀있고 아직 501.

    구현이 끝나면 이 테스트는 지우고 각 모듈 테스트로 옮기세요.
    """
    response = client.post(path, json={})
    assert response.status_code == 501
