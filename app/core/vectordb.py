"""ChromaDB 클라이언트 — 컬렉션1(상세페이지) / 컬렉션2(반려사유).

문서 5장의 "메타데이터 필터 + get/query 이원화" 원칙:
  - query(): 의미 검색. "이 이상징후와 비슷한 내용" 처럼 뜻으로 찾을 때.
  - get():   메타데이터 완전일치 조회. "sku=ABC 의 상세페이지" 처럼 키를 알 때.
    get 은 임베딩을 거치지 않아 정확하고 싸다. 키를 아는데 query 를 쓰지 말 것.

임베딩 모델은 두 컬렉션 다 `EMBEDDING_MODEL` 로 명시 지정한다. Chroma 기본값
(all-MiniLM-L6-v2)은 영어 모델이고 우리 코퍼스는 전부 한국어다. 실제로 임베딩을
타는 건 컬렉션2(query) 한 곳이다.
"""

import logging
from functools import lru_cache
from typing import Any

import chromadb
from chromadb.api import ClientAPI
from chromadb.errors import ChromaError
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

from app.config import get_settings
from app.core.constants import (
    COLLECTION_DETAIL_PAGES,
    COLLECTION_REJECTION_REASONS,
    EMBEDDING_MODEL,
)
from app.core.exceptions import VectorDbError

logger = logging.getLogger(__name__)


@lru_cache
def get_client() -> ClientAPI:
    """Chroma 클라이언트.

    CHROMA_PERSIST_DIR 이 설정돼 있으면 로컬 파일 모드(개발용),
    아니면 HTTP 모드(k8s 배포용)로 붙는다.
    """
    settings = get_settings()

    if settings.chroma_persist_dir:
        logger.info("ChromaDB 로컬 모드: %s", settings.chroma_persist_dir)
        return chromadb.PersistentClient(path=settings.chroma_persist_dir)

    logger.info("ChromaDB HTTP 모드: %s:%s", settings.chroma_host, settings.chroma_port)
    return chromadb.HttpClient(host=settings.chroma_host, port=settings.chroma_port)


@lru_cache
def get_embedding_function() -> Any:
    """컬렉션이 쓸 임베딩 함수.

    ⚠️ Chroma 의 `OpenAIEmbeddingFunction` 은 `OPENAI_API_KEY` 가 아니라
    `CHROMA_OPENAI_API_KEY` 를 본다. 키는 `settings.llm_api_key` 한 곳에서 관리하므로
    명시적으로 넘긴다.

    ⚠️ 키가 없으면 **컬렉션을 여는 것 자체가** 막힌다 — 컬렉션1의 `get()` 처럼 임베딩이
    필요 없는 조회도 같이 막힌다(임베딩 함수가 컬렉션 설정의 일부라서).
    """
    api_key = get_settings().llm_api_key
    if not api_key:
        raise VectorDbError(
            "LLM_API_KEY 가 비어 있어 임베딩 함수를 만들 수 없습니다 — .env 를 확인하세요 "
            f"(임베딩 모델: {EMBEDDING_MODEL})."
        )
    return OpenAIEmbeddingFunction(api_key=api_key, model_name=EMBEDDING_MODEL)


def _get_collection(name: str) -> Any:
    try:
        return get_client().get_or_create_collection(
            name=name,
            embedding_function=get_embedding_function(),
        )
    except ChromaError as exc:
        raise VectorDbError(f"컬렉션 접근 실패: {name}") from exc
    except ValueError as exc:
        # Chroma 는 임베딩 함수를 컬렉션 설정에 저장해두고, 다른 함수로 열면 ValueError 로
        # 거부한다(ChromaError 가 아니라 위 except 에 안 걸린다). 옛 모델 벡터와 새 모델
        # 쿼리는 섞일 수 없고, 대신 모델을 바꾸면 컬렉션 재생성이 필수다.
        if "embedding function" not in str(exc).lower():
            raise
        raise VectorDbError(
            f"컬렉션 '{name}' 이 다른 임베딩 모델로 만들어져 있습니다 "
            f"(현재 설정: {EMBEDDING_MODEL}). "
            f"`python scripts/seed_vectordb.py --reset` 으로 재시딩하세요. ({exc})"
        ) from exc


def get_detail_pages() -> Any:
    """컬렉션1 — 상세페이지. 개선안 생성의 인용 근거 원문."""
    return _get_collection(COLLECTION_DETAIL_PAGES)


def get_rejection_reasons() -> Any:
    """컬렉션2 — 반려 사유. B5에서 반려된 개선안의 사유가 쌓인다."""
    return _get_collection(COLLECTION_REJECTION_REASONS)


def query_documents(
    collection: Any,
    *,
    query_text: str,
    n_results: int = 5,
    where: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """의미 검색. 뜻으로 찾을 때만 사용.

    Args:
        where: 메타데이터 사전 필터 (`{"sku": "ABC"}`). 후보를 좁힐수록 정확해진다.

    Returns:
        `{"id", "document", "metadata", "distance"}` 리스트. distance 는 작을수록 유사.
    """
    try:
        result = collection.query(
            query_texts=[query_text],
            n_results=n_results,
            where=where,
        )
    except ChromaError as exc:
        raise VectorDbError(f"query 실패: {exc}") from exc

    return _flatten_query_result(result)


def get_documents(
    collection: Any,
    *,
    where: dict[str, Any],
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """메타데이터 완전일치 조회. 키를 알 때 사용 (임베딩 안 거침).

    Returns:
        `{"id", "document", "metadata"}` 리스트.
    """
    try:
        result = collection.get(where=where, limit=limit)
    except ChromaError as exc:
        raise VectorDbError(f"get 실패: {exc}") from exc

    return [
        {"id": doc_id, "document": document, "metadata": metadata or {}}
        for doc_id, document, metadata in zip(
            result.get("ids") or [],
            result.get("documents") or [],
            result.get("metadatas") or [],
        )
    ]


def _flatten_query_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Chroma query() 는 쿼리별로 한 겹 더 중첩된 리스트를 준다. 단일 쿼리 기준으로 펴준다."""
    ids = (result.get("ids") or [[]])[0]
    documents = (result.get("documents") or [[]])[0]
    metadatas = (result.get("metadatas") or [[]])[0]
    distances = (result.get("distances") or [[]])[0]

    return [
        {
            "id": doc_id,
            "document": document,
            "metadata": metadata or {},
            "distance": distance,
        }
        for doc_id, document, metadata, distance in zip(
            ids, documents, metadatas, distances
        )
    ]
