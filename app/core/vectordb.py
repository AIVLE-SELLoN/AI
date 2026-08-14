"""ChromaDB 클라이언트 — 컬렉션1(상세페이지) / 컬렉션2(반려사유).

문서 5장의 "메타데이터 필터 + get/query 이원화" 원칙:
  - query(): 의미 검색. "이 이상징후와 비슷한 내용" 처럼 뜻으로 찾을 때.
  - get():   메타데이터 완전일치 조회. "sku=ABC 의 상세페이지" 처럼 키를 알 때.
    get 은 임베딩을 거치지 않아 정확하고 싸다. 키를 아는데 query 를 쓰지 말 것.

임베딩 모델은 두 컬렉션 다 `EMBEDDING_MODEL` 로 명시 지정한다. Chroma 기본값
(all-MiniLM-L6-v2)은 영어 모델이고 우리 코퍼스는 전부 한국어다.

**임베딩 API 를 타는 지점은 둘이다** — 조회는 `query()` 뿐이지만(`get()` 은 안 탄다),
적재(`upsert()`)는 **두 컬렉션 모두** 문서를 임베딩한다. 즉 시딩과 반려사유 적재도
네트워크와 유효한 키를 요구한다. 그 호출들은 `VectorDbError` 로 감싸서 내보낸다
(`_embedding_backed`) — 감싸지 않으면 `openai.RateLimitError` 같은 공급자 예외가
호출부를 그대로 뚫고 나가 REST 500·배치 중단으로 이어진다.
"""

import logging
from contextlib import contextmanager
from functools import lru_cache
from typing import Any

import chromadb
from chromadb.api import ClientAPI
from chromadb.errors import ChromaError
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from openai import OpenAIError

from app.config import get_settings
from app.core.constants import (
    COLLECTION_DETAIL_PAGES,
    COLLECTION_REJECTION_REASONS,
    EMBEDDING_MODEL,
)
from app.core.exceptions import VectorDbError

logger = logging.getLogger(__name__)

TENANT_METADATA_KEY = "company_id"
"""회사 범위를 가르는 metadata 키. 조회 `where` 에도 같은 키를 쓴다."""

LOCAL_TENANT = "_local"
"""`MQ_COMPANY_ID` 가 비어 있는 개발·테스트 환경의 대체값.

운영은 배포마다 실제 값이 박혀 있어(`SLN-xxxxxxxxxx`) 이 값과 섞이지 않는다.
⚠️ 빈 값으로 쌓은 뒤 `MQ_COMPANY_ID` 를 채우면 그 전 문서는 조회에서 빠진다 —
지금은 컬렉션2가 0건이라 실害가 없고, 운영 전환 시엔 처음부터 값이 있다.
"""


def current_tenant() -> str:
    """이 배포가 담당하는 회사 식별자. **벡터DB 문서를 회사 범위로 가르는 축이다.**

    🔴 **왜 필요한가** — `product_group_id` 가 회사별 시퀀스라 A사에도 `P001`, B사에도
       `P001` 이 있다(2026-08-12 백엔드 확인). 그래서 `alert_id` 와 그 파생
       (`recommendation_id`)은 **회사 안에서만 유일**하다. 백엔드는 `(companyId, alert_id)`
       복합 유니크로 그걸 흡수하지만, **벡터DB엔 그 축이 없었다** — 회사 두 곳이 같은
       (window_end, 상품, aspect, 채널) 조합을 만들면 나중 HITL 결과가 먼저 저장된 다른
       회사 문서를 조용히 덮고, 조회도 `aspect` 하나로만 좁혀 다른 회사 반려 사례가
       `similar_case` 로 새어 나갔다. (서영님 PR #77 리뷰)

    ⚠️ **지금 데모는 1회사라 위 경로가 도달 불가다.** 그래도 넣는 이유는 **컬렉션2가
       0건인 지금이 비용 0이고, HITL 이 돌기 시작하면 기존 문서 이관 작업이 되기**
       때문이다.

    ✅ **두 컬렉션 다 이 축을 쓴다.** 컬렉션2는 PR #77, 컬렉션1(`detail_pages`)은 그
       후속에서 붙였다(시딩 ID·metadata·조회 필터 셋 다). 컬렉션1을 나눠서 한 이유는
       504건 **재시딩이 걸려 팀 전원이 다시 시딩**해야 했기 때문이지 설계가 달라서가
       아니다.

    ✅ **분리 기제 = metadata + 문서 ID 접두어(데이터 레벨)로 확정.** 근거·실측·다른
       두 기제를 반려한 사유·다시 볼 조건은 `docs/vectordb_tenancy.md` 에 있다.
       요약하면 Chroma `tenant`/`database` 는 **사전 생성이 필요**하고(없는 값으로 열면
       `NotFoundError`) HTTP 모드에선 우리가 소유하지 않은 서버의 admin 권한을 요구해
       지금 고를 수 없다. `get_client()` 가 그 인자를 안 쓰고 기본값으로 여는 건
       그래서다 — 미구현이 아니라 선택이다.

    🔴 **셋은 짝이다 — 쓰기 ID · 쓰기 metadata · 조회 `where`.** 하나만 지워도 격리가
       깨지는데 **모양이 다르다**: ID 축을 지우면 다른 회사 문서를 *덮고*, 조회 필터를
       지우면 덮지 않은 채 *새어 나온다*. 그래서 뮤테이션도 따로 잡아야 한다.
    """
    return get_settings().mq_company_id or LOCAL_TENANT


def scoped_document_id(tenant: str, raw_id: str) -> str:
    """회사 축을 붙인 벡터DB 문서 ID. 예: `SLN-aaa:REC-20260828-P001-COLOR-COUPANG`

    ID 를 그대로 쓰면 회사가 다른 같은 논리 알림이 **서로를 덮는다**(`current_tenant`
    docstring 참고). 접두어라 원래 ID 를 알면 역으로 만들 수 있다.

    ⚠️ **`tenant` 를 인자로 받는다 — 안에서 `current_tenant()` 를 부르지 않는다.**
       그러면 호출부가 metadata·조회 필터용으로 한 번 더 읽어서 **tenant 를 읽는 곳이
       둘**이 된다. 쓰기 ID·쓰기 metadata·조회 필터 셋이 같은 값이어야 하므로
       **호출부가 한 번 읽어 셋에 같이 넘기는** 형태가 맞다(그래야 어긋날 수 없다).
    """
    return f"{tenant}:{raw_id}"


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
    with _embedding_backed("query"):
        result = collection.query(
            query_texts=[query_text],
            n_results=n_results,
            where=where,
        )

    return _flatten_query_result(result)


@contextmanager
def _embedding_backed(operation: str):
    """임베딩 API 를 타는 Chroma 호출의 예외를 `VectorDbError` 로 통일한다.

    `OpenAIError` 를 같이 잡는 이유: 임베딩 함수가 네트워크를 타면서 공급자 예외가
    **Chroma 를 그대로 통과한다**(`isinstance(exc, ChromaError)` 가 False). 레이트리밋은
    상시 발생 가능하고, 안 감싸면 `retrieve_context()` 를 뚫고 나가 REST 는 500,
    배치는 그 알림의 개선안이 통째로 유실된다.
    """
    try:
        yield
    except ChromaError as exc:
        raise VectorDbError(f"{operation} 실패: {exc}") from exc
    except OpenAIError as exc:
        raise VectorDbError(
            f"{operation} 실패 — 임베딩 API 오류({EMBEDDING_MODEL}): {exc}"
        ) from exc


def upsert_documents(
    collection: Any,
    *,
    ids: list[str],
    documents: list[str],
    metadatas: list[dict[str, Any]],
) -> None:
    """적재. **문서를 임베딩하므로 네트워크와 키가 필요하다.**

    `collection.upsert()` 를 직접 부르지 말 것 — 그 경로는 공급자 예외가 안 감싸져
    호출부에 raw 401/429 가 올라간다.
    """
    with _embedding_backed("upsert"):
        collection.upsert(ids=ids, documents=documents, metadatas=metadatas)


def get_documents(
    collection: Any,
    *,
    where: dict[str, Any],
    limit: int | None = None,
    include: list[str] | None = None,
) -> list[dict[str, Any]]:
    """메타데이터 완전일치 조회. 키를 알 때 사용 (임베딩 안 거침).

    Args:
        include: Chroma 가 실어 보낼 필드. 기본(None)은 문서 본문 + metadata 둘 다다.
            **metadata 만 필요하면 `["metadatas"]` 를 줘서 본문 전송을 없앨 것** —
            상세페이지 본문이 건당 700자대라 수십 건만 훑어도 수만 자가 오간다.

    Returns:
        `{"id", "document", "metadata"}` 리스트. `include` 로 뺀 필드는 `document=None`
        · `metadata={}` 로 채워진다(키 자체는 항상 있다).
    """
    try:
        result = (
            collection.get(where=where, limit=limit)
            if include is None
            else collection.get(where=where, limit=limit, include=include)
        )
    except ChromaError as exc:
        raise VectorDbError(f"get 실패: {exc}") from exc

    # ⚠️ `zip` 으로 묶지 말 것 — `include` 로 뺀 필드는 **빈 리스트**로 와서 zip 이
    #    전체를 0건으로 잘라낸다(조회는 성공했는데 결과가 사라진다).
    ids = result.get("ids") or []
    documents = result.get("documents") or []
    metadatas = result.get("metadatas") or []
    return [
        {
            "id": doc_id,
            "document": documents[i] if i < len(documents) else None,
            "metadata": (metadatas[i] if i < len(metadatas) else None) or {},
        }
        for i, doc_id in enumerate(ids)
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
