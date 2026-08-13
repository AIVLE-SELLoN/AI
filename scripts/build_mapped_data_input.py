"""목 파이프라인용 `input_mapped_data.csv` 생성 — golden 매핑에서 변환.

왜 이 스크립트가 있나
--------------------
`mock_producer.py` 는 목 파이프라인에서 **main server 자리를 대신한다**(§1 소유권).
그래서 `products`(§2-2)·`mapped_data`(§2-3) 대본이 `data/input/` 에 있어야 하는데,
`input_mapped_data.csv` 는 저장소에 만드는 코드가 없었다.

🔴 **2026-08-13 확인: 백엔드는 CSV 를 만들지 않는다. raw DB 에 직접 적재한다.**
   그전 가정("이 파일은 백엔드가 준다")이 깨졌으므로, 목에서 쓸 대본은 **우리가**
   준비한다. 운영에서 백엔드가 하는 일과 최종 상태는 같다 — `mapped_data` 테이블이
   채워지는 것.

⚠️ **이건 매핑 알고리즘이 아니다. oracle 매핑이다.**
   `golden_mapping.csv` 의 정답 매핑을 그대로 옮긴다. 채널 상품을 무엇으로 묶을지
   정하는 규칙(제목 정규화·유사도 등)은 **백엔드 소관이고 우리 저장소에 없다.**
   따라서 이 파일로 돌린 결과를 "상품 매핑 성능"으로 말하면 안 된다. 매핑은 분석의
   **전제**이지 측정 대상이 아니다.

왜 producer 가 직접 golden 을 안 읽고 이 스크립트를 거치나
    `mock_producer.validate_data_directory` 가드와 팀 규칙(CLAUDE.md 9)이 producer 의
    golden 접근을 막는다. 변환을 사람이 한 번 해서 input 쪽에 두는 구조다
    (`mock_producer.MAPPED_DATA_FILE` 주석).

매핑이 비면 무엇이 깨지나 (`build_channel_product_map` docstring)
    상품 하나가 채널마다 다른 `product_group_id` 로 갈린다:
      - 탐지의 채널 간 비교(편중형/전역형)가 성립하지 않는다
      - 월간 리포트의 채널 격차(JSD)는 비교할 짝이 없어 산출물이 비어 버린다
      - ChromaDB 컬렉션1(상세페이지)은 `P001` 로 적재돼 있어 조회가 전부 빗나간다
    실측(2026-08-13, `data/input/` 1,134행): 매핑 없이 적재하면
    `cs.product_group_id` 가 `C1101` 처럼 channel_product_id 로 폴백되고
    (`_resolve_group`), 채널 2개 이상 걸친 상품 그룹이 42종 → **0종**이 된다.
    앞의 둘은 "부정확해지는" 게 아니라 **아예 안 나온다** — 짝이 없으면 계산이
    시작되지 않는다. 조용히 비는 실패라 리포트를 열어봐야 안다.

실행::

    python scripts/build_mapped_data_input.py
    python scripts/build_mapped_data_input.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DEFAULT_SRC = ROOT / "data" / "golden" / "golden_mapping.csv"
DEFAULT_DST = ROOT / "data" / "input" / "input_mapped_data.csv"

# `mock_producer.load_product_catalog` 이 읽는 컬럼. 그쪽이 정본이라 이름을 맞춘다.
FIELDS = ["variant_row_id", "product_group_id"]


def build_rows(src: Path) -> list[dict[str, str]]:
    """golden 매핑 → mapped_data 대본 행.

    `golden_group_id` → `product_group_id` 로 컬럼명만 바꾼다. `canonical_option`·
    `mock_scenario_tag` 는 **가져오지 않는다** — 시나리오 메타데이터라 운영 테이블에
    있을 수 없는 값이고, 들고 오면 목이 운영보다 많이 아는 상태가 된다.
    """
    with src.open(encoding="utf-8-sig", newline="") as f:
        rows = [
            {"variant_row_id": vrid, "product_group_id": group}
            for row in csv.DictReader(f)
            if (vrid := (row.get("variant_row_id") or "").strip())
            and (group := (row.get("golden_group_id") or "").strip())
        ]

    if not rows:
        raise SystemExit(f"매핑 행이 0건입니다: {src} — 컬럼명을 확인하세요.")

    seen: dict[str, str] = {}
    for row in rows:
        vrid, group = row["variant_row_id"], row["product_group_id"]
        # variant_row_id 는 mapped_data 의 PRIMARY KEY 다. 중복이면 적재 시
        # INSERT OR REPLACE 로 조용히 덮어써져서 어느 쪽이 남는지 알 수 없다.
        if vrid in seen and seen[vrid] != group:
            raise SystemExit(
                f"variant_row_id 가 두 그룹에 매핑돼 있습니다: "
                f"{vrid} → {seen[vrid]} / {group}"
            )
        seen[vrid] = group
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--src", default=str(DEFAULT_SRC), help=f"기본 {DEFAULT_SRC}")
    ap.add_argument("--out", default=str(DEFAULT_DST), help=f"기본 {DEFAULT_DST}")
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 요약만")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    src, dst = Path(args.src), Path(args.out)
    if not src.exists():
        raise SystemExit(f"golden 매핑이 없습니다: {src}")

    rows = build_rows(src)
    groups = sorted({r["product_group_id"] for r in rows})

    print(f"변환 {src.name} → {dst.name}")
    print(f"  variant {len(rows):,}행 · 상품 그룹 {len(groups)}개")
    print(f"  예: {', '.join(groups[:5])}{' …' if len(groups) > 5 else ''}")

    if args.dry_run:
        print("  [dry-run] 파일을 쓰지 않았습니다.")
        return

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  기록 완료: {dst}")
    print("  ⚠️ oracle 매핑입니다 — '상품 매핑 성능'으로 인용하지 마세요.")


if __name__ == "__main__":
    main()
