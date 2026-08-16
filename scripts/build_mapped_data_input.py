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

🔴 **전제: `data/golden/golden_mapping.csv` 를 먼저 받아야 한다.**
   `data/**` 가 `.gitignore` 대상이라 **입력도 출력도 저장소에 없다.** 새로 클론한
   상태에서 이 스크립트만으로는 아무것도 못 만든다 — 팀 데이터 번들을 받거나
   `scripts/generate_cs_review_data.py` 로 생성한 뒤에 돌린다.
   (2026-08-13 지적 반영: "스크립트만 있으면 같은 상태를 만들 수 있다"는 틀린 말이었다.)

실행::

    python scripts/build_mapped_data_input.py
    python scripts/build_mapped_data_input.py --dry-run

입력이 불완전하면 **부분 파일을 만들지 않고 죽는다.** 빈 값·중복 `variant_row_id`·
필수 컬럼 누락은 전부 사유와 행 번호를 찍고 중단한다. `input_channel_products.csv` 가
있으면 variant 집합까지 대조한다 — 자세한 건 `build_rows` · `check_against_catalog`.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.console import force_utf8_output

DEFAULT_SRC = ROOT / "data" / "golden" / "golden_mapping.csv"
DEFAULT_DST = ROOT / "data" / "input" / "input_mapped_data.csv"
# variant 집합 대조용. `mock_producer` 가 이 파일과 매핑을 조인하므로, 여기 있는 variant 가
# 매핑에 없으면 그 채널상품이 폴백된다.
DEFAULT_CATALOG = ROOT / "data" / "input" / "input_channel_products.csv"

# `mock_producer.load_product_catalog` 이 읽는 컬럼. 그쪽이 정본이라 이름을 맞춘다.
FIELDS = ["variant_row_id", "product_group_id"]

# golden 쪽 컬럼명. 이 둘이 없으면 변환할 수 없다.
SRC_VARIANT, SRC_GROUP = "variant_row_id", "golden_group_id"

# 데이터 번들을 못 받았을 때 안내. 저장소에 없는 파일이라 경로만 찍으면 원인을 모른다.
_BUNDLE_HINT = (
    "`data/**` 는 .gitignore 대상이라 저장소에 들어 있지 않습니다 — "
    "팀 데이터 번들을 먼저 받아 두세요."
)


def _require_columns(fieldnames: list[str] | None, src: Path, required: tuple[str, ...]) -> None:
    """필수 헤더 확인. 없으면 어떤 컬럼이 필요한지 말하고 죽는다.

    헤더가 통째로 다르면 읽는 쪽이 전 행을 "빈 값"으로 보게 되는데, 그때 나오는 사유가
    원인과 다르다. golden 쪽은 "1행이 비었다"로, catalog 쪽은 "매핑에만 있는 variant"로
    엉뚱한 곳을 지목한다. 원인은 컬럼명이므로 **읽기 전에** 잡는다.

    ⚠️ **두 파일이 같이 쓴다.** golden 은 두 컬럼, catalog 는 `variant_row_id` 하나만
       필요해서 `required` 를 받는다.
    """
    present = set(fieldnames or ())
    missing = [c for c in required if c not in present]
    if missing:
        raise SystemExit(
            f"{src}: 필수 컬럼이 없습니다 — {', '.join(missing)}. "
            f"헤더: {', '.join(fieldnames or ['(없음)'])}"
        )


def build_rows(src: Path) -> list[dict[str, str]]:
    """golden 매핑 → mapped_data 대본 행.

    `golden_group_id` → `product_group_id` 로 컬럼명만 바꾼다. `canonical_option`·
    `mock_scenario_tag` 는 **가져오지 않는다** — 시나리오 메타데이터라 운영 테이블에
    있을 수 없는 값이고, 들고 오면 목이 운영보다 많이 아는 상태가 된다.

    🔴 **불완전한 행은 건너뛰지 않고 죽는다.** 처음엔 값이 빈 행을 조건절에서 걸러
       냈는데, 그러면 **부분 매핑 파일이 종료코드 0 으로 만들어진다**(2026-08-13
       서영님 지적, 재현됨: 2행짜리 입력에서 1행만 나오고 성공으로 끝났다).
       빠진 variant 는 producer 에서 채널상품 ID 로 폴백되므로, 이 PR 이 막으려던
       "부분 매핑 때문에 채널 비교가 조용히 사라지는" 실패가 변환 단계에서 그대로
       재현된다. 골라서 버리는 것과 다 가져오는 것 중 **하나만 맞는데, 여기서는
       판단할 근거가 없다** — 그래서 사람에게 돌려준다.

    Raises:
        SystemExit: 필수 컬럼 없음 · 빈 값 · `variant_row_id` 중복 · 행 0건.
    """
    rows: list[dict[str, str]] = []
    seen: dict[str, tuple[str, int]] = {}

    with src.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, src, (SRC_VARIANT, SRC_GROUP))

        for row in reader:
            line = reader.line_num  # 헤더가 1행이라 데이터는 2행부터
            vrid = (row.get(SRC_VARIANT) or "").strip()
            group = (row.get(SRC_GROUP) or "").strip()

            if not vrid or not group:
                blank = SRC_VARIANT if not vrid else SRC_GROUP
                raise SystemExit(
                    f"{src}:{line} — {blank} 가 비었습니다. 매핑이 한 행이라도 빠지면 "
                    "그 채널상품은 폴백돼 채널 간 비교에서 빠집니다. 원본을 고치거나, "
                    "빠져도 되는 행이라면 원본에서 지우고 다시 돌리세요."
                )

            # variant_row_id 는 mapped_data 의 PRIMARY KEY 다. 중복이면 적재 시
            # INSERT OR REPLACE 로 조용히 덮어써져서 어느 쪽이 남는지 알 수 없다.
            #
            # ⚠️ **그룹이 같아도 죽인다.** 지금 결과가 같다고 넘기면 원본이 1:1 이라는
            #    전제가 깨진 것을 아무도 모르고, 나중에 한쪽만 고쳐져 갈라질 때
            #    비로소 드러난다. 그때는 어느 행이 맞는지 알 수 없다.
            if vrid in seen:
                prev_group, prev_line = seen[vrid]
                same = " (그룹은 같지만 원본이 1:1 이어야 합니다)" if prev_group == group else ""
                raise SystemExit(
                    f"{src}: variant_row_id 가 중복입니다 — {vrid} "
                    f"({prev_line}행 → {prev_group} / {line}행 → {group}){same}"
                )

            seen[vrid] = (group, line)
            rows.append({"variant_row_id": vrid, "product_group_id": group})

    if not rows:
        raise SystemExit(f"매핑 행이 0건입니다: {src} — 헤더만 있고 데이터가 없습니다.")

    return rows


def check_against_catalog(rows: list[dict[str, str]], catalog: Path) -> None:
    """products 대본의 variant 집합과 대조한다. **빠진 쪽만 치명적이다.**

    `mock_producer.build_channel_product_map` 이 두 파일을 조인하므로, products 에 있는
    variant 가 매핑에 없으면 그 채널상품은 `_resolve_group` 에서 채널상품 ID 로 폴백된다
    — 파일은 멀쩡해 보이는데 채널 비교만 조용히 줄어드는, 이 PR 이 막으려는 그 상태다.

    반대(매핑에만 있고 products 에 없음)는 **경고로 끝낸다.** producer 가 products 를
    돌면서 조인하므로 남는 매핑 행은 읽히지 않아 폴백을 만들지 않는다. 다만 두 파일이
    어긋났다는 신호이므로 조용히 넘기지는 않는다.

    catalog 파일이 **없으면** 건너뛴다 — `data/**` 가 gitignore 라 없는 게 정상인 환경이
    있고, 대조는 이 변환기의 본업이 아니다.

    🔴 **다만 "없음"과 "헤더가 틀림"은 다르게 다룬다.** 파일이 있는데 `variant_row_id`
       컬럼이 없으면 전 행이 빈 값으로 읽혀 `catalog_variants` 가 공집합이 되고, 그러면
       대조가 통과해 버린다(2026-08-14 서영님 지적, 재현됨: 종료코드 0 · 출력 파일까지
       기록). 게다가 그때 나가는 경고가 "매핑에만 있는 variant"라 **원인과 반대쪽을
       지목한다** — 문제는 매핑이 아니라 catalog 헤더다.

       같은 파일을 producer 가 읽으면 `build_channel_product_map` 의 조인이 통째로
       비어 채널 비교가 사라진다. 이 변환기가 막으려는 바로 그 실패라, 건너뛰지 않고
       중단한다.

    Raises:
        SystemExit: catalog 헤더에 `variant_row_id` 가 없음 · products 에 있는 variant 가
            매핑에서 빠짐.
    """
    if not catalog.exists():
        print(f"  [건너뜀] products 대본이 없어 variant 집합을 대조하지 않았습니다: {catalog}")
        return

    with catalog.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, catalog, (SRC_VARIANT,))
        catalog_variants = {v for row in reader if (v := (row.get(SRC_VARIANT) or "").strip())}

    mapped = {r["variant_row_id"] for r in rows}
    missing = sorted(catalog_variants - mapped)
    extra = sorted(mapped - catalog_variants)

    if missing:
        raise SystemExit(
            f"products 대본({catalog.name})에 있는 variant {len(missing)}개가 매핑에 "
            f"없습니다 — 예: {', '.join(missing[:5])}. 이대로 적재하면 그 채널상품은 "
            "상품 그룹 대신 채널상품 ID 로 폴백돼 채널 간 비교에서 빠집니다."
        )

    if extra:
        print(
            f"  [경고] 매핑에만 있는 variant {len(extra)}개 — 예: {', '.join(extra[:5])}. "
            "producer 는 products 를 기준으로 조인하므로 무시되지만, 두 대본이 "
            "어긋나 있습니다."
        )
    else:
        print(f"  variant 집합 일치 확인: {catalog.name} {len(catalog_variants):,}개")


def main() -> None:
    # 🔴 **첫 문장이어야 한다.** 예전엔 `parse_args()` 뒤에서 stdout·stderr 를
    #    직접 `reconfigure` 했는데, 그러면 `--help` 가 그 전에 찍힌다 — 이 스크립트의
    #    `description` 첫 줄에 `—`(U+2014) 가 있어서 cp949 콘솔에서는 도움말만 요청해도
    #    `UnicodeEncodeError` 로 죽었다(2026-08-14 실측). 사유 전문은 `app/core/console.py`.
    # ⚠️ stdout 만이 아니라 **stderr 도** 바꿔야 한다 — 이 스크립트의 실패는 전부
    #    `SystemExit` 이고, 어느 행이 왜 틀렸는지가 그 메시지에 실린다(`_BUNDLE_HINT` 포함).
    force_utf8_output()

    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--src", default=str(DEFAULT_SRC), help=f"기본 {DEFAULT_SRC}")
    ap.add_argument("--out", default=str(DEFAULT_DST), help=f"기본 {DEFAULT_DST}")
    ap.add_argument(
        "--products", default=str(DEFAULT_CATALOG), help=f"variant 대조용. 기본 {DEFAULT_CATALOG}"
    )
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 요약만")
    args = ap.parse_args()

    src, dst = Path(args.src), Path(args.out)
    if not src.exists():
        raise SystemExit(f"golden 매핑이 없습니다: {src}\n  {_BUNDLE_HINT}")

    rows = build_rows(src)
    groups = sorted({r["product_group_id"] for r in rows})

    print(f"변환 {src.name} → {dst.name}")
    print(f"  variant {len(rows):,}행 · 상품 그룹 {len(groups)}개")
    print(f"  예: {', '.join(groups[:5])}{' …' if len(groups) > 5 else ''}")

    check_against_catalog(rows, Path(args.products))

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
