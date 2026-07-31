"""Agent1 → Agent2 소규모 실연동 확인 (실험②의 축소판).

무엇을 보나
-----------
1. 계약: Agent1 이 낸 ClassifiedItem 을 Agent2 가 그대로 먹는가
2. 전파: 분류 오차가 카운트를 얼마나 흔드는가 (oracle 대비)
3. 판정: 그래도 그 케이스가 발화하는가

⚠️ 비용: 두 군데서 LLM 을 부른다. --limit 로 규모를 조절할 것.
   ① Agent1 분류 — **문의 1건당 1회**. 기본값 200(현재 윈도우만).
      과거 윈도우는 golden 라벨을 그대로 쓴다(oracle) — 이번 확인의 관심사는
      '현재 윈도우 분류가 카운트를 흔드는가'이기 때문.
   ② Agent2 [6] 원인분류 — detect_anomaly() 가 **편중형·스코프 내 후보마다** 부른다
      (service.py:327). --limit 로 줄어들지 않으니 발화 후보 수만큼 배치 호출이 붙는다.

실행:
    python scripts/check_agent1_to_agent2.py --case SC-001            # 200건, 기본
    python scripts/check_agent1_to_agent2.py --case SC-001 --limit 50 # 더 싸게
    python scripts/check_agent1_to_agent2.py --case SC-001 --dry-run  # 비용 0, 대상만 확인
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.classification.service import ClassifyRequestItem, classify_aspect
from app.core.schemas import AspectSentiment, ClassifiedItem
from app.detection.service import detect_anomaly

DAY1 = date(2026, 6, 30)


def read(path: str) -> list[dict]:
    with (ROOT / path).open(encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def build_context(case_id: str):
    """케이스가 속한 (상품, 윈도우)와, 그 상품의 전체 문의를 모은다."""
    cfg = [r for r in read("data/config/config_anomaly.csv") if r["case_id"] == case_id]
    if not cfg:
        raise SystemExit(f"config_anomaly 에 {case_id} 없음")
    product = cfg[0]["golden_group_id"]
    cur_end_day = int(cfg[0]["window_end_day"])
    cur_end = date.fromordinal(DAY1.toordinal() + cur_end_day - 1)

    variant_group = {
        r["variant_row_id"]: r["golden_group_id"]
        for r in read("data/golden/golden_mapping.csv")
    }
    product_of = {}
    for r in read("data/input/input_channel_products.csv"):
        g = variant_group.get(r["variant_row_id"])
        if g:
            product_of.setdefault((r["channel"], r["channel_product_id"]), g)

    labels = {r["inquiry_id"]: r for r in read("data/golden/golden_cs_labels.csv")}
    rows = []
    for r in read("data/input/input_cs_inquiries.csv"):
        if product_of.get((r["channel"], r["channel_product_id"])) != product:
            continue
        rows.append({**r, "label": labels.get(r["inquiry_id"], {}), "product": product})
    return cfg, product, cur_end, rows


def to_classified(row: dict, aspects: list[AspectSentiment]) -> ClassifiedItem:
    return ClassifiedItem(
        item_id=row["inquiry_id"],
        source="cs",
        channel=row["channel"],
        product_group_id=row["product"],
        raw_text=row["content"],
        aspects=aspects,
        created_at=datetime.fromisoformat(row["inquired_at"]),
    )


def oracle_aspects(row: dict) -> list[AspectSentiment]:
    label = row["label"]
    if not label.get("true_aspect"):
        return []
    return [
        AspectSentiment(
            aspect=label["true_aspect"], sentiment=int(label["true_sentiment"])
        )
    ]


async def main(args):
    cfg, product, cur_end, rows = build_context(args.case)
    cur_start = date.fromordinal(cur_end.toordinal() - 6)

    in_window = [
        r
        for r in rows
        if cur_start <= datetime.fromisoformat(r["inquired_at"]).date() <= cur_end
    ]
    past = [r for r in rows if r not in in_window]
    target = in_window[: args.limit] if args.limit > 0 else in_window

    print(f"케이스 {args.case} | 상품 {product} | 현재 윈도우 {cur_start} ~ {cur_end}")
    print(f"  현재 윈도우 문의 {len(in_window)}건 중 {len(target)}건을 Agent1 에 태움")
    print(f"  과거 윈도우 {len(past)}건은 golden 라벨 사용(oracle)")
    print(f"  → LLM 호출 예상 {len(target)}회")
    for r in cfg:
        print(
            f"  의도: {r['channel']} {r['aspect']} cur={r['cur_neg']}/{r['cur_total']} "
            f"past={r['past_neg']}/{r['past_total']}"
        )
    if args.dry_run:
        print("\n[dry-run] LLM 호출 안 함.")
        return

    # ── Agent1 ───────────────────────────────────────────────
    requests = [
        ClassifyRequestItem(
            item_id=r["inquiry_id"],
            source="cs",
            channel=r["channel"],
            product_group_id=product,
            raw_text=r["content"],
            created_at=datetime.fromisoformat(r["inquired_at"]),
        )
        for r in target
    ]
    print(f"\nAgent1 분류 중... ({len(requests)}건)")
    classified = await classify_aspect(requests)
    print(f"  ✅ {len(classified)}건 수신 — ClassifiedItem 계약 통과")

    # ── 전파 측정: oracle 대비 카운트 차이 ────────────────────
    by_id = {c.item_id: c for c in classified}
    agree = disagree = 0
    oracle_neg: Counter = Counter()
    agent1_neg: Counter = Counter()
    for r in target:
        gold = oracle_aspects(r)
        got = by_id[r["inquiry_id"]].aspects
        g = {(a.aspect.value, int(a.sentiment)) for a in gold}
        p = {(a.aspect.value, int(a.sentiment)) for a in got}
        agree += g == p
        disagree += g != p
        for a in gold:
            if int(a.sentiment) == -1:
                oracle_neg[a.aspect.value] += 1
        for a in got:
            if int(a.sentiment) == -1:
                agent1_neg[a.aspect.value] += 1

    print(f"\n── 분류 일치도 (표본 {len(target)}건) ──")
    print(f"  완전 일치 {agree} / 불일치 {disagree}  ({agree / len(target):.1%})")
    print("\n── 부정 카운트 (oracle vs Agent1) ──")
    for aspect in sorted(set(oracle_neg) | set(agent1_neg)):
        o, p = oracle_neg[aspect], agent1_neg[aspect]
        mark = "✅" if o == p else f"⚠️  {p - o:+d}"
        print(f"  {aspect:6s} oracle {o:3d}  →  Agent1 {p:3d}   {mark}")

    # ── Agent2 ───────────────────────────────────────────────
    items = [by_id[r["inquiry_id"]] for r in target]
    items += [to_classified(r, oracle_aspects(r)) for r in in_window[len(target) :]]
    items += [to_classified(r, oracle_aspects(r)) for r in past]

    print(f"\nAgent2 탐지 중... (입력 {len(items)}건)")
    alerts, _ = await detect_anomaly(items, window_end=cur_end)
    print(f"  발행 {len(alerts)}건")
    for a in alerts:
        print(
            f"    [{a.alert_id}] {a.channel.value} {a.verdict} {a.main_aspect} "
            f"{a.stats.past_rate:.1%}→{a.stats.cur_rate:.1%} 확신도={a.detection_confidence}"
        )
        if a.root_cause:
            print(
                f"        원인={a.root_cause.label} ({a.root_cause.count}/{a.root_cause.total})"
            )
    if not alerts:
        print(
            "    ⚠️  미발행 — 단일 상품만 넣어 BH family 가 작아진 탓일 수 있음(구조 확인용)"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", default="SC-001", help="config_anomaly 의 case_id")
    ap.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Agent1 에 태울 문의 수 (0=현재 윈도우 전량)",
    )
    ap.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 대상만 확인")
    asyncio.run(main(ap.parse_args()))
