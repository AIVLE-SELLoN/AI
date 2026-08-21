"""컬렉션1(상세페이지)을 CSV로 덤프 — 육안 확인용.

Chroma에 실제로 뭐가 적재됐는지 엑셀에서 눈으로 보고 싶을 때 쓴다.
Windows 콘솔은 한글 출력이 코드페이지 때문에 깨지므로, 터미널 출력 대신
UTF-8(BOM) CSV로 내보낸다 — 엑셀이 BOM을 보고 UTF-8로 바로 인식한다.

**기본은 현재 회사(`current_tenant()`) 문서만 내보낸다.** 컬렉션은 회사 여럿을 담을 수 있어서,
`where` 없이 전건을 뜨면 공유 Chroma 에서 **다른 회사 상세페이지 원문이 한 CSV 로 새어 나간다.**
metadata 격리는 권한 경계가 아니므로(`docs/vectordb_tenancy.md` §5), 저장소가 주는 운영 도구라도
기본 동작은 회사 범위를 지켜야 한다.

`--all-companies` 는 전사 관리자용 탈출구다. 회사 축이 **없는 구형 문서**를 보려면 이 옵션이
필요하다 — 구형은 `company_id` 가 아예 없어서 회사 필터에 안 걸린다.

실행:
    python scripts/inspect_detail_pages.py [출력경로]
    python scripts/inspect_detail_pages.py --all-companies [출력경로]
    (기본 출력경로: detail_pages_dump.csv)
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.console import force_utf8_output
from app.core.vectordb import TENANT_METADATA_KEY, current_tenant, get_detail_pages

COLUMNS = ["id", "company_id", "product_group_id", "channel", "aspect", "document"]


def dump(out_path: Path, *, all_companies: bool = False) -> int:
    collection = get_detail_pages()
    tenant = current_tenant()

    # `where=None` 을 넘기면 Chroma 가 전건을 준다 — 전사 덤프는 그 경로다.
    rows = collection.get() if all_companies else collection.get(
        where={TENANT_METADATA_KEY: tenant}
    )

    # company_id 컬럼이 비어 있으면 **회사 축 도입 전에 시딩된 구형 문서**다
    # (`--all-companies` 로만 보인다). 일반 시딩으로 갱신하면 된다.
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for doc_id, metadata, document in zip(
            rows["ids"], rows["metadatas"], rows["documents"]
        ):
            writer.writerow(
                [
                    doc_id,
                    (metadata or {}).get(TENANT_METADATA_KEY, ""),
                    (metadata or {}).get("product_group_id", ""),
                    (metadata or {}).get("channel", ""),
                    (metadata or {}).get("aspect", ""),
                    document,
                ]
            )

    scope = "전 회사" if all_companies else f"company_id={tenant}"
    print(f"{len(rows['ids'])}건 ({scope}) → {out_path.resolve()}")
    return len(rows["ids"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="컬렉션1(상세페이지) CSV 덤프")
    parser.add_argument(
        "out_path",
        nargs="?",
        default="detail_pages_dump.csv",
        help="출력 CSV 경로 (기본: detail_pages_dump.csv)",
    )
    parser.add_argument(
        "--all-companies",
        action="store_true",
        help="회사 범위를 풀고 전건을 덤프한다 (전사 관리자용 · 구형 문서 확인용).",
    )
    args = parser.parse_args(argv)

    if args.all_companies:
        print("⚠️ 회사 범위를 풀고 덤프합니다 — 다른 회사 상세페이지 원문이 포함됩니다.")

    dump(Path(args.out_path), all_companies=args.all_companies)


if __name__ == "__main__":
    force_utf8_output()
    main()
