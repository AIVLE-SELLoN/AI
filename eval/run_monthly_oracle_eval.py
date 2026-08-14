"""월간 JSD 산술 재현 - 골든 매핑·분류 라벨을 쓰는 평가 전용 러너.

원본 ``data/raw.db``는 읽기 전용으로 열고 메모리 DB에 복사한다. 메모리 복사본에만
골든 상품 매핑과 aspect/sentiment 라벨을 주입하므로 운영 DB를 수정하지 않는다.

이 결과는 분류 오차가 0%인 oracle이다. 월간 집계·JSD·BH 배선을 검증할 수는 있지만,
운영 분류기 성능이나 운영 E2E 성공으로 보고하면 안 된다.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.core import raw_schema
from app.core.raw_db import connect_readonly
from app.reporting.monthly_aggregator import (
    aggregate_monthly_inputs,
    list_product_groups,
)
from scripts.golden_inputs import load_golden_inputs


def _enum_value(value):
    return getattr(value, "value", value)


def build_oracle_connection(db_path: Path) -> tuple[sqlite3.Connection, int, int]:
    """읽기 전용 원본을 메모리에 복사하고 골든 매핑·분류 결과를 주입한다."""
    source = connect_readonly(str(db_path))
    conn = sqlite3.connect(":memory:")
    try:
        source.backup(conn)
    finally:
        source.close()

    before = conn.execute(
        "SELECT COUNT(DISTINCT product_group_id) FROM voc_document"
    ).fetchone()[0]
    raw_schema.create_classified_tables(conn)

    items, _documents = load_golden_inputs()
    cs_mapping = []
    review_mapping = []
    parents = []
    aspects = []
    for item in items:
        source_name = _enum_value(item.source)
        mapping_row = (item.product_group_id, item.item_id)
        if source_name == "cs":
            cs_mapping.append(mapping_row)
        else:
            review_mapping.append(mapping_row)
        parents.append((item.item_id, source_name, "oracle", "oracle", "oracle"))
        aspects.extend(
            (
                item.item_id,
                _enum_value(label.aspect),
                int(label.sentiment),
                None,
            )
            for label in item.aspects
        )

    conn.executemany("UPDATE cs SET product_group_id = ? WHERE id = ?", cs_mapping)
    conn.executemany("UPDATE reviews SET product_group_id = ? WHERE id = ?", review_mapping)
    conn.executemany(
        "INSERT INTO classified_item "
        "(item_id, source, prompt_version, model_version, pipeline_version) "
        "VALUES (?, ?, ?, ?, ?)",
        parents,
    )
    conn.executemany(
        "INSERT INTO classified_item_aspect "
        "(item_id, aspect, sentiment, mixed_signal) VALUES (?, ?, ?, ?)",
        aspects,
    )
    conn.commit()

    after = conn.execute(
        "SELECT COUNT(DISTINCT product_group_id) FROM voc_document"
    ).fetchone()[0]
    return conn, before, after


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2026-07", help="보고서 연월 (YYYY-MM)")
    parser.add_argument("--product", default="P001", help="상세 출력할 상품 그룹")
    parser.add_argument("--db", default="data/raw.db", help="읽기 전용 원본 raw DB")
    parser.add_argument(
        "--permutations",
        type=int,
        default=None,
        help="순열검정 반복 수 override. 생략하면 운영 기본값 10,000",
    )
    args = parser.parse_args()

    conn, before, after = build_oracle_connection(ROOT / args.db)
    try:
        inputs = aggregate_monthly_inputs(
            conn,
            args.month,
            n_permutations=args.permutations,
        )
        products = list_product_groups(conn, args.month)
    finally:
        conn.close()

    target = next((item for item in inputs if item.product_group_id == args.product), None)
    if target is None:
        raise SystemExit(f"상품을 찾지 못했습니다: {args.product}")

    print("=" * 72)
    print("월간 JSD oracle 평가 - LLM 호출 0회 / 운영 성능 아님")
    print("=" * 72)
    print(f"원본 상품 그룹 수 {before} -> 골든 매핑 후 {after}")
    print(f"{args.month} 집계 상품 {len(products)}개 / 대상 {args.product}")
    judged_pairs = [
        (item.product_group_id, pair)
        for item in inputs
        for pair in item.channel_divergence.pairs
        if pair.severity is not None
    ]
    crisis_pairs = [row for row in judged_pairs if row[1].severity.value == "CRISIS"]
    caution_pairs = [row for row in judged_pairs if row[1].severity.value == "CAUTION"]
    print(
        f"전체 판정 채널쌍 {len(judged_pairs)}개 / "
        f"CRISIS {len(crisis_pairs)}개 / CAUTION {len(caution_pairs)}개"
    )
    for product, pair in (crisis_pairs + caution_pairs)[:5]:
        excess = pair.jsd_score - pair.jsd_baseline
        print(
            f"  위험 후보 {product} {pair.comparison_pair}: "
            f"excess={excess:.4f}, severity={pair.severity.value}"
        )
    print(f"총 VOC {target.total_voc_count}건")
    print("속성별 전월 대비 부정 비율 변화")
    for drift in target.sentiment_drifts:
        print(
            f"  {_enum_value(drift.aspect)}: delta={drift.drift_rate:+.4f}, "
            f"status={drift.status.value}"
        )
    print(f"worst_pair {target.channel_divergence.worst_pair}")
    for pair in target.channel_divergence.pairs:
        if pair.hold_reason is not None:
            print(
                f"  {pair.comparison_pair}: 보류({pair.hold_reason.value}), "
                f"표본 {pair.sample_size}"
            )
            continue
        excess = pair.jsd_score - pair.jsd_baseline
        print(
            f"  {pair.comparison_pair}: JSD={pair.jsd_score:.4f}, "
            f"baseline={pair.jsd_baseline:.4f}, excess={excess:.4f}, "
            f"BH={pair.bh_significant}, severity={pair.severity.value}, "
            f"표본={pair.sample_size}"
        )


if __name__ == "__main__":
    main()
