"""컬렉션1(상세페이지) 전체를 CSV로 덤프 — 육안 확인용.

Chroma에 실제로 뭐가 적재됐는지 엑셀에서 눈으로 보고 싶을 때 쓴다.
Windows 콘솔은 한글 출력이 코드페이지 때문에 깨지므로, 터미널 출력 대신
UTF-8(BOM) CSV로 내보낸다 — 엑셀이 BOM을 보고 UTF-8로 바로 인식한다.

실행:
    python scripts/inspect_detail_pages.py [출력경로]
    (기본 출력경로: detail_pages_dump.csv)
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.vectordb import TENANT_METADATA_KEY, get_detail_pages


def main() -> None:
    out_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("detail_pages_dump.csv")

    collection = get_detail_pages()
    rows = collection.get()

    # company_id 를 같이 내보낸다 — 이 컬럼이 비어 있으면 **회사 축 도입 전에 시딩된
    # 컬렉션**이고, 그 상태에서는 조회가 전건 0건이 된다(재시딩 필요).
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["id", "company_id", "product_group_id", "channel", "aspect", "document"]
        )
        for doc_id, metadata, document in zip(
            rows["ids"], rows["metadatas"], rows["documents"]
        ):
            writer.writerow(
                [
                    doc_id,
                    metadata.get(TENANT_METADATA_KEY, ""),
                    metadata.get("product_group_id", ""),
                    metadata.get("channel", ""),
                    metadata.get("aspect", ""),
                    document,
                ]
            )

    print(f"{len(rows['ids'])}건 → {out_path.resolve()}")


if __name__ == "__main__":
    main()
