"""fixture → ChromaDB 초기 적재.

실행:
    python scripts/seed_vectordb.py

컬렉션1(상세페이지)에 detail_pages.json 을 적재합니다.
컬렉션2(반려사유)는 운영 중 B5 반려로 쌓이는 것이라 seed 대상이 아닙니다.

TODO: schemas.py 확정 + fixture 작성 후 구현.
  - detail_pages.json 로딩
  - core.vectordb.get_detail_pages() 로 컬렉션 확보
  - collection.add(ids=..., documents=..., metadatas=...)
  - 메타데이터에 sku 등 필터 키를 꼭 넣을 것 (get() 조회가 여기 의존)
  - 재실행해도 중복 안 쌓이도록 id 를 결정적으로 만들 것
"""


def main() -> None:
    raise NotImplementedError("schemas.py 확정 후 구현")


if __name__ == "__main__":
    main()
