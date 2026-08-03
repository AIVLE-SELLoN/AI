"""
input_orders.csv 생성기
========================
Mock 데이터 정의서 §2(input_orders.csv — 대시보드·채널 비교 전용, 이상탐지 미사용) 기준.

컬럼: channel, channel_product_id, order_date, quantity, order_amount
규칙: 42상품 × 3채널 × 60일, 요일 효과 + 자연 노이즈.
      ⚠️ config_anomaly.csv는 참조하지 않음 — 이 파일은 "V2 부정률 방식" 이상탐지가
      안 쓰는 데이터라, 스파이크(케이스) 연동이 문서에 명시적으로 불필요하다고 돼있음.
      대신 channel_product_id는 상품매핑 결과(golden_mapping.csv)에서 그대로 가져와
      다른 산출물(input_cs_inquiries.csv 등)과 ID 체계를 일관되게 유지한다.

사용법
------
    python generate_orders.py \
        --products-config config_products.csv \
        --mapping-dir mapping_42 \
        --golden-mapping-dir mapping_42 \
        --outdir ./output \
        --anchor-date 2026-08-28 --seed 11
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

CHANNELS = ["COUPANG", "NAVER", "ZIGZAG"]
DAYS = 60
WEEKEND_MULTIPLIER = 1.35  # 토·일 판매량 증가(요일 효과)


def load_products(path: str) -> dict[str, dict]:
    with open(path, encoding="utf-8-sig") as f:
        return {r["golden_group_id"]: r for r in csv.DictReader(f)}


def load_channel_product_id_map(mapping_dir: str, golden_mapping_dir: str | None = None) -> dict[tuple[str, str], str]:
    """(golden_group_id, channel) -> channel_product_id.
    golden_mapping_dir 생략 시 mapping_dir과 동일(하위호환)."""
    mapping_dir = Path(mapping_dir)
    golden_dir = Path(golden_mapping_dir) if golden_mapping_dir else mapping_dir
    golden_path = golden_dir / "golden_mapping.csv"
    raw_path = mapping_dir / "input_channel_products.csv"
    if not (golden_path.exists() and raw_path.exists()):
        raise FileNotFoundError(f"매핑 파일을 찾을 수 없음(golden: {golden_path}, input: {raw_path})")

    with open(raw_path, encoding="utf-8-sig") as f:
        raw_by_id = {r["variant_row_id"]: r for r in csv.DictReader(f)}

    pid_map: dict[tuple[str, str], str] = {}
    with open(golden_path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            raw = raw_by_id.get(r["variant_row_id"])
            if raw:
                key = (r["golden_group_id"], raw["channel"])
                if key not in pid_map:  # 상품당 채널 1개 대표 ID만(옵션별로 여러 행 있어도 대표 1개)
                    pid_map[key] = raw["channel_product_id"]
    return pid_map


def generate_orders(products: dict[str, dict], pid_map: dict[tuple[str, str], str],
                     anchor_date: datetime, rng: random.Random) -> list[dict]:
    rows = []
    skipped = []

    for gid, product in products.items():
        base_price = int(product.get("base_price", 30000) or 30000)
        # 상품별 기본 일 판매량 — 5~30개 사이에서 상품마다 다르게(무작위지만 seed 고정으로 재현 가능)
        base_qty = rng.randint(5, 30)

        for channel in CHANNELS:
            cpid = pid_map.get((gid, channel))
            if not cpid:
                skipped.append((gid, channel))
                continue  # 매핑 자체가 없는 조합(완전누락 시나리오 등)은 주문도 없음 — 자연스러운 결과

            for day_offset in range(DAYS):
                date = anchor_date - timedelta(days=DAYS - 1 - day_offset)
                is_weekend = date.weekday() >= 5  # 5=토, 6=일
                multiplier = WEEKEND_MULTIPLIER if is_weekend else 1.0
                noise = rng.gauss(1.0, 0.2)  # 자연 노이즈(정규분포, 표준편차 20%)
                quantity = max(0, round(base_qty * multiplier * noise))
                order_amount = quantity * base_price

                rows.append({
                    "channel": channel,
                    "channel_product_id": cpid,
                    "order_date": date.strftime("%Y-%m-%d"),
                    "quantity": quantity,
                    "order_amount": order_amount,
                })

    if skipped:
        print(f"  ⚠️ 매핑 없어서 스킵된 (상품,채널) {len(skipped)}건(정상 — 완전누락 시나리오 등)")
    return rows


def write_csv(rows: list[dict], path: Path):
    if not rows:
        raise ValueError(
            f"생성된 행이 0건이라 CSV를 쓸 수 없습니다 ({path}). "
            "--mapping-dir / --golden-mapping-dir 경로를 확인하세요."
        )
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--products-config", default="config_products.csv")
    ap.add_argument("--mapping-dir", required=True, help="input_channel_products.csv 위치")
    ap.add_argument("--golden-mapping-dir", default=None, help="golden_mapping.csv 위치(생략 시 --mapping-dir와 동일)")
    ap.add_argument("--outdir", default="./output")
    ap.add_argument("--anchor-date", required=True, help="YYYY-MM-DD, 데이터의 마지막 날짜")
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    anchor_date = datetime.strptime(args.anchor_date, "%Y-%m-%d")

    products = load_products(args.products_config)
    print(f"상품 {len(products)}개 로딩 완료")

    pid_map = load_channel_product_id_map(args.mapping_dir, args.golden_mapping_dir)
    print(f"매핑 {len(pid_map)}개 (product,channel) 조합 로딩 완료")

    rows = generate_orders(products, pid_map, anchor_date, rng)
    print(f"생성됨 — input_orders {len(rows)}건 (기대: {len(pid_map)}조합 × {DAYS}일)")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    write_csv(rows, outdir / "input_orders.csv")
    print(f"저장 완료 → {outdir}/input_orders.csv")


if __name__ == "__main__":
    main()