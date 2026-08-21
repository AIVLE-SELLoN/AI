"""담당: 지인 — 임베딩 API 예외가 `VectorDbError` 로 감싸지는가.

실제 ChromaDB·OpenAI 는 안 쓴다. 컬렉션 대역이 공급자 예외를 던지게 해서 경계에서만
검증한다(tests=비용 0 원칙).

**왜 이 테스트가 필요한가.** 임베딩 모델을 OpenAI 로 바꾸면서 컬렉션이 네트워크를 타게
됐는데, `openai.OpenAIError` 는 `ChromaError` 의 하위가 아니라 기존 `except ChromaError`
를 그대로 통과했다 — 레이트리밋 한 번에 REST 500 / 배치의 해당 알림 개선안 유실로
이어진다. 회귀하면 조용히 같은 모양이 되므로 계약으로 고정한다.
"""

import httpx
import pytest
from chromadb.errors import ChromaError
from openai import APIConnectionError, AuthenticationError, RateLimitError

from app.core.exceptions import VectorDbError
from app.core.vectordb import get_documents, query_documents, upsert_documents

_REQUEST = httpx.Request("POST", "https://api.openai.com/v1/embeddings")


def _response(status: int) -> httpx.Response:
    return httpx.Response(status_code=status, request=_REQUEST)


def _rate_limit() -> RateLimitError:
    return RateLimitError("rate limited", response=_response(429), body=None)


def _auth() -> AuthenticationError:
    return AuthenticationError("bad key", response=_response(401), body=None)


def _connection() -> APIConnectionError:
    return APIConnectionError(request=_REQUEST)


class RaisingCollection:
    """query/upsert 가 주어진 예외를 던지는 컬렉션 대역."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def query(self, **_):
        raise self._exc

    def upsert(self, **_):
        raise self._exc


@pytest.mark.parametrize(
    "exc",
    [
        _rate_limit(),
        _auth(),
        _connection(),
        ChromaError("chroma 쪽 실패"),
    ],
    ids=["rate_limit", "auth", "connection", "chroma_error"],
)
def test_query_wraps_provider_errors(exc):
    with pytest.raises(VectorDbError):
        query_documents(RaisingCollection(exc), query_text="색상", n_results=1)


@pytest.mark.parametrize(
    "exc",
    [
        _rate_limit(),
        _auth(),
        _connection(),
        ChromaError("chroma 쪽 실패"),
    ],
    ids=["rate_limit", "auth", "connection", "chroma_error"],
)
def test_upsert_wraps_provider_errors(exc):
    with pytest.raises(VectorDbError):
        upsert_documents(
            RaisingCollection(exc), ids=["a"], documents=["문구"], metadatas=[{"aspect": "색상"}]
        )


def test_unrelated_errors_are_not_swallowed():
    """임베딩과 무관한 버그(예: 오타로 인한 AttributeError)까지 VectorDbError 로 바꾸면
    진짜 원인이 가려진다. 감싸는 대상은 공급자 예외뿐이다."""
    with pytest.raises(AttributeError):
        query_documents(RaisingCollection(AttributeError("오타")), query_text="색상")


# ── get_documents 의 include 지원 ───────────────────────────────────
class _RecordingCollection:
    """`get()` 인자를 기록하고, Chroma 처럼 **빠진 필드를 빈 리스트로** 돌려준다."""

    def __init__(self):
        self.kwargs: dict = {}

    def get(self, **kwargs):
        self.kwargs = kwargs
        include = kwargs.get("include")
        return {
            "ids": ["a", "b"],
            "documents": [] if include == ["metadatas"] else ["본문A", "본문B"],
            "metadatas": [{"company_id": "SLN-aaa"}, {}],
        }


def test_include_metadatas_only_still_returns_every_row():
    """`include` 로 본문을 빼도 **행이 사라지면 안 된다.**

    Chroma 는 빠진 필드를 **빈 리스트**로 준다. 예전 구현은 세 리스트를 `zip` 으로 묶어서,
    `documents` 가 비면 **전체가 0건으로 잘렸다** — 조회는 성공했는데 결과만 조용히
    사라지는 모양이라 호출부에서 "구형 문서 0건" 으로 오독된다.
    """
    collection = _RecordingCollection()

    rows = get_documents(collection, where={"company_id": "x"}, include=["metadatas"])

    assert len(rows) == 2, "include 로 본문을 빼면 zip 이 전체를 잘라내던 자리입니다"
    assert [r["metadata"] for r in rows] == [{"company_id": "SLN-aaa"}, {}]
    assert [r["document"] for r in rows] == [None, None]
    assert collection.kwargs["include"] == ["metadatas"]


def test_include_is_omitted_when_not_requested():
    """`include` 를 안 주면 Chroma 기본값(본문 포함)을 그대로 쓴다 — 인자를 넣지 않는다."""
    collection = _RecordingCollection()

    rows = get_documents(collection, where={"company_id": "x"})

    assert "include" not in collection.kwargs
    assert [r["document"] for r in rows] == ["본문A", "본문B"]
