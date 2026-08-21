"""실데이터 CSV → ChromaDB 초기 적재.

실행:
    python scripts/seed_vectordb.py

컬렉션1(상세페이지)에 data/input/input_detail_fields.csv(504행)를 적재합니다.
컬렉션2(반려사유)는 운영 중 B5 반려로 쌓이는 것이라 seed 대상이 아닙니다.

메타데이터 키는 core/schemas.py의 런타임 계약을 따른다 — product_group_id
(golden_group_id 아님. golden_group_id는 data/golden/ 채점용이고 app/ 코드는
읽지 않는다. detail_pages는 pipeline.retrieve_context()가 DetectionAlert의
product_group_id로 조회하므로 여기도 product_group_id로 통일한다).

입력은 `data/input/input_detail_fields.csv` 실데이터다(컬럼:
product_group_id/channel/aspect/detail_text). channel·aspect 값은 core/schemas.py 의
Channel·Aspect enum 에 있어야 한다.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chromadb.errors import NotFoundError

from app.core.console import force_utf8_output
from app.core.constants import (
    COLLECTION_DETAIL_PAGES,
    COLLECTION_REJECTION_REASONS,
    EMBEDDING_MODEL,
)
from app.core.exceptions import VectorDbError
from app.core.vectordb import (
    TENANT_METADATA_KEY,
    current_tenant,
    get_client,
    get_detail_pages,
    get_documents,
    scoped_document_id,
    upsert_documents,
)

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "input" / "input_detail_fields.csv"


def _make_id(entry: dict[str, Any]) -> str:
    """product_group_id+channel+aspect로 결정적 id 생성 — 재실행해도 중복 안 쌓인다.

    **회사 축은 여기 없다** — 호출부가 `scoped_document_id(tenant, ...)` 로 감싼다.
    `product_group_id` 는 회사별 시퀀스라 이 값만으로는 회사 간에 유일하지 않다
    (`vectordb.current_tenant` docstring).
    """
    return f"{entry['product_group_id']}:{entry['channel']}:{entry['aspect']}"


def reset_collections() -> None:
    """두 컬렉션을 지운다. `EMBEDDING_MODEL` 을 바꿨을 때 필요하다.

    컬렉션2 도 같이 지우는 이유는 임베딩 모델이 컬렉션별 설정이라, 안 지우면 옛 모델로
    남아 새 모델과 벡터 공간이 갈리기 때문이다. 컬렉션2 는 시드 대상이 아니므로 다시
    안 채운다 — 다음 `record_hitl_outcome()` 때 새 설정으로 생성된다.

    컬렉션2 에 쌓인 반려 사례는 **복구 불가**다(`.chroma/` 는 gitignore).
    """
    client = get_client()
    for name in (COLLECTION_DETAIL_PAGES, COLLECTION_REJECTION_REASONS):
        try:
            count = client.get_collection(name=name).count()
        except (ValueError, NotFoundError):
            print(f"  - {name}: 없음, 건너뜀")
            continue
        client.delete_collection(name=name)
        print(f"  - {name}: {count}건 삭제")


def report_legacy_documents(collection: Any, tenant: str) -> int:
    """회사 축이 **없는** 구형 문서가 몇 건 남았는지 시딩 직후 알려준다.

    **판별을 런타임이 아니라 여기서 한다.** "이 컬렉션이 구형인가" 는 알림별이 아니라
    **컬렉션 전체의 성질**이다. 런타임(`_log_detail_page_miss`)에서 미스마다 다시 계산하면
    ① 같은 답을 수십 번 구하고 ② 핫 패스라 전수를 못 봐 표본으로 어림잡게 된다. 여기는 **한
    번만 돌고 전수를 보며**, 무엇보다 **사람이 이 콘솔 앞에 서 있는 시점**이다.

    **Chroma 1.5.9 엔 `$exists` 가 없다** — `where` 연산자 목록에서 거부한다
    (`ValueError: Expected where operator to be one of $gt … $not_contains`). 대신 **`$nin` 이
    키가 아예 없는 문서도 매칭**하므로(실측) 그걸로 후보를 뽑고, metadata 에 키가 실제로 있는지는
    파이썬에서 본다.

    `include=["metadatas"]` 로 **본문 전송을 없앤다** — 상세페이지가 건당 700자대라 빼지 않으면
    수십 건만 훑어도 수만 자가 오간다.

    Returns:
        구형 문서 수(정리 여부 판단용). 조회 실패 시 -1.
    """
    try:
        rows = get_documents(
            collection,
            where={TENANT_METADATA_KEY: {"$nin": [tenant]}},
            include=["metadatas"],
        )
    except VectorDbError as exc:
        print(f"  ⚠️ 구형 문서 확인 실패(적재 자체는 성공): {exc}")
        return -1

    legacy = sum(1 for row in rows if TENANT_METADATA_KEY not in row["metadata"])
    others = len(rows) - legacy

    if others:
        print(f"  - 다른 회사 문서 {others}건이 같은 컬렉션에 있습니다(정상, 조회에서 격리됨).")
    if not legacy:
        return 0

    print(
        f"  ⚠️ 회사 축이 없는 구형 문서 {legacy}건이 남아 있습니다.\n"
        "     조회 필터에 안 걸리므로 **그대로 둬도 동작에는 문제가 없습니다**"
        "(저장 공간만 차지합니다).\n"
        "     확인: python scripts/inspect_detail_pages.py --all-companies\n"
        "     🔴 정리하려고 `--reset` 을 쓰지 마세요 — 컬렉션2(HITL 반려 이력)와"
        " 다른 회사 문서까지 지웁니다."
    )
    return legacy


def main(reset: bool = False) -> None:
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as f:
        entries = list(csv.DictReader(f))
    if not entries:
        print(f"CSV가 비어 있습니다: {CSV_PATH}")
        return

    if reset:
        print(f"임베딩 모델 = {EMBEDDING_MODEL} / 컬렉션 초기화")
        reset_collections()

    # **한 번만 읽어 ID·metadata 에 같이 쓴다** — 두 번 읽으면 어긋날 수 있고, 조회 필터
    # (`pipeline._get_detail_page_text`)까지 셋이 같은 값이어야 격리가 성립한다.
    tenant = current_tenant()

    # 적재는 문서를 임베딩하므로 네트워크와 유효한 키가 필요하다. 실패는
    # `upsert_documents` 가 VectorDbError 로 감싸 사유를 알아볼 수 있게 준다.
    collection = get_detail_pages()
    try:
        upsert_documents(
            collection,
            # 회사 축을 붙인다. `product_group_id` 가 회사별 시퀀스라 A사 P001 과 B사
            # P001 이 **같은 ID** 를 받아 나중 시딩이 앞엣것을 덮는다.
            ids=[scoped_document_id(tenant, _make_id(entry)) for entry in entries],
            documents=[entry["detail_text"] for entry in entries],
            metadatas=[
                {
                    # 조회 필터가 이 키로 회사를 좁힌다. 쓰기와 읽기가 짝이므로
                    # 한쪽만 지우면 조용히 다른 회사 상세페이지를 근거로 쓴다.
                    TENANT_METADATA_KEY: tenant,
                    "product_group_id": entry["product_group_id"],
                    "channel": entry["channel"],
                    "aspect": entry["aspect"],
                }
                for entry in entries
            ],
        )
    except VectorDbError as exc:
        print(f"적재 실패: {exc}")
        print("  .env 의 LLM_API_KEY 와 네트워크 연결을 확인하세요.")
        raise SystemExit(1) from exc

    print(f"{len(entries)}건 적재 완료 → collection={collection.name} / company_id={tenant}")
    report_legacy_documents(collection, tenant)


if __name__ == "__main__":
    force_utf8_output()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="적재 전에 두 컬렉션을 삭제한다 (EMBEDDING_MODEL 변경 시 필수).",
    )
    main(reset=parser.parse_args().reset)
