"""실데이터 CSV → ChromaDB 초기 적재.

실행:
    python scripts/seed_vectordb.py

컬렉션1(상세페이지)에 data/input/input_detail_fields.csv(504행)를 적재합니다.
컬렉션2(반려사유)는 운영 중 B5 반려로 쌓이는 것이라 seed 대상이 아닙니다.

메타데이터 키는 core/schemas.py의 런타임 계약을 따른다 — product_group_id
(golden_group_id 아님. golden_group_id는 data/golden/ 채점용이고 app/ 코드는
읽지 않는다. detail_pages는 pipeline.retrieve_context()가 DetectionAlert의
product_group_id로 조회하므로 여기도 product_group_id로 통일한다).

2026-07-28: tests/fixtures/detail_pages.json(5행 샘플) 대신 현진님이 채워넣은
실데이터를 읽도록 변경. 컬럼 구성(product_group_id/channel/aspect/detail_text)이
스펙과 정확히 일치함을 확인함(중복 키 0건, 빈 값 0건, channel/aspect 전부
core/schemas.py의 Channel·Aspect enum 값에 포함).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any

# 저장소 루트를 sys.path에 넣어야 함(실행 방식에 따라 자동으로 안 잡힐 수 있어서 명시)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.vectordb import get_detail_pages

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "input" / "input_detail_fields.csv"


def _make_id(entry: dict[str, Any]) -> str:
    """product_group_id+channel+aspect로 결정적 id 생성 — 재실행해도 중복 안 쌓인다."""
    return f"{entry['product_group_id']}:{entry['channel']}:{entry['aspect']}"


def main() -> None:
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as f:
        entries = list(csv.DictReader(f))
    if not entries:
        print(f"CSV가 비어 있습니다: {CSV_PATH}")
        return

    collection = get_detail_pages()
    collection.upsert(
        ids=[_make_id(entry) for entry in entries],
        documents=[entry["detail_text"] for entry in entries],
        metadatas=[
            {
                "product_group_id": entry["product_group_id"],
                "channel": entry["channel"],
                "aspect": entry["aspect"],
            }
            for entry in entries
        ],
    )
    print(f"{len(entries)}건 적재 완료 → collection={collection.name}")


if __name__ == "__main__":
    main()
