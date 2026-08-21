"""Inspect the remaining false rows not explained by config case-past overlap."""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.console import force_utf8_output

IN_CSV = ROOT / "eval/results/no_true_false_breakdown_20260807.csv"
OUT_CSV = ROOT / "eval/results/unexplained_false_rows_20260807.csv"
OUT_MD = ROOT / "eval/results/unexplained_false_rows_20260807.md"
DAY1 = datetime(2026, 6, 30).date()


def read(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.is_absolute():
        p = ROOT / p
    with p.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def day_number(raw: str) -> int:
    return 1 + (datetime.fromisoformat(raw).date() - DAY1).days


def product_mapping() -> dict[tuple[str, str], str]:
    variant_group = {
        r["variant_row_id"]: r["golden_group_id"]
        for r in read("data/golden/golden_mapping.csv")
    }
    out = {}
    for r in read("data/input/input_channel_products.csv"):
        group = variant_group.get(r["variant_row_id"])
        if group:
            out[(r["channel"], r["channel_product_id"])] = group
    return out


def label_maps() -> dict[str, dict[str, dict]]:
    return {
        "cs": {r["inquiry_id"]: r for r in read("data/golden/golden_cs_labels.csv")},
        "review": {r["review_id"]: r for r in read("data/golden/golden_review_labels.csv")},
    }


def daily_counts() -> dict:
    product_of = product_mapping()
    labels = label_maps()
    inputs = [
        ("cs", "data/input/input_cs_inquiries.csv", "inquiry_id", "inquired_at"),
        ("review", "data/input/input_reviews.csv", "review_id", "created_at"),
    ]
    out = defaultdict(lambda: {"neg": 0, "total": 0})
    for source, path, id_key, time_key in inputs:
        for row in read(path):
            product = product_of.get((row["channel"], row["channel_product_id"]))
            if not product:
                continue
            label = labels[source].get(row[id_key], {})
            day = day_number(row[time_key])
            aspects = ["색상", "사이즈", "소재"] if source == "review" else ["색상", "사이즈", "소재", "파손", "오배송", "기타"]
            for aspect in aspects:
                key = (product, row["channel"], source, day, aspect)
                out[key]["total"] += 1
                if label.get("true_aspect") == aspect and label.get("true_sentiment") == "-1":
                    out[key]["neg"] += 1
    return out


def sum_window(counts: dict, product: str, channel: str, source: str, aspect: str, start: int, end: int) -> dict:
    neg = total = 0
    by_day = []
    for day in range(max(1, start), min(60, end) + 1):
        c = counts.get((product, channel, source, day, aspect), {"neg": 0, "total": 0})
        neg += c["neg"]
        total += c["total"]
        by_day.append(f"d{day}:{c['neg']}/{c['total']}")
    return {
        "neg": neg,
        "total": total,
        "rate": neg / total if total else 0.0,
        "days": " ".join(by_day),
    }


def config_summary(product: str) -> str:
    rows = [r for r in read("data/config/config_anomaly.csv") if r["golden_group_id"] == product]
    return " | ".join(
        f"{r['case_id']} {r['channel']} {r['source']} {r['aspect']} "
        f"{r['window_start_day']}-{r['window_end_day']} scoring={r['scoring_included']} "
        f"intended={r['intended_answer'] or 'blank'} note={r.get('note', '')}"
        for r in rows
    )


def analyze() -> list[dict]:
    counts = daily_counts()
    rows = [
        r for r in read(IN_CSV)
        if r["category"] == "unexplained_by_config_overlap"
    ]
    out = []
    for r in rows:
        day = int(r["day"])
        cur = sum_window(counts, r["product"], r["channel"], r["source"], r["aspect"], day - 6, day)
        past = sum_window(counts, r["product"], r["channel"], r["source"], r["aspect"], day - 34, day - 7)
        prev7 = sum_window(counts, r["product"], r["channel"], r["source"], r["aspect"], day - 13, day - 7)
        out.append({
            "day": day,
            "product": r["product"],
            "source": r["source"],
            "aspect": r["aspect"],
            "channel": r["channel"],
            "current_neg_total": f"{cur['neg']}/{cur['total']}",
            "current_rate": round(cur["rate"], 5),
            "past28_neg_total": f"{past['neg']}/{past['total']}",
            "past28_rate": round(past["rate"], 5),
            "prev7_neg_total": f"{prev7['neg']}/{prev7['total']}",
            "prev7_rate": round(prev7["rate"], 5),
            "current_daily": cur["days"],
            "past28_daily": past["days"],
            "product_config": config_summary(r["product"]),
        })
    return out


def write_outputs(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# 미설명 false row 상세 확인, 2026-08-07",
        "",
        "범위: `no_true_false_breakdown`에서 `unexplained_by_config_overlap`으로 남은 2건.",
        "",
        "## 요약",
        "",
        f"- 대상 row: {len(rows)}",
        "- 두 row 모두 oracle/golden에서도 발생했으므로 real classification만의 문제는 아니다.",
        "- 단순한 미래 case-past overlap으로는 설명되지 않는다.",
        "- 현재 7일 window의 부정률이 직전/과거 window보다 높게 샘플링된 배경 변동 또는 hot/gap 생성 영향으로 보인다.",
        "",
        "## 상세",
        "",
        "| Day | Product | Source | Aspect | Channel | Current | Past28 | Prev7 |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['day']} | {r['product']} | {r['source']} | {r['aspect']} | {r['channel']} | "
            f"{r['current_neg_total']} ({r['current_rate']:.2%}) | "
            f"{r['past28_neg_total']} ({r['past28_rate']:.2%}) | "
            f"{r['prev7_neg_total']} ({r['prev7_rate']:.2%}) |"
        )
    lines.extend([
        "",
        "## 해석",
        "",
        "이 2건은 현재 감사 기준으로 마지막 미해결 row다. 둘 다 oracle에서도 발생하므로 프롬프트 수정으로 해결할 대상은 아니다.",
        "",
        "가능성이 높은 원인은 config window 밖에서 발생한 mock background/hot-gap 샘플링 변동이다. 다만 case-past와 달리 명확한 시나리오 overlap이 없으므로, 공식 개선 전에 팀이 row-level로 라벨/시나리오 의도를 확인해야 한다.",
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    # 첫 문장이어야 한다 — 사설 `sys.stdout.reconfigure()` 를 대체한다(stderr 미변경 ·
    # `contextlib.suppress` 부재). 사유 전문은 `app/core/console.py`.
    force_utf8_output()

    rows = analyze()
    write_outputs(rows)
    print(f"unexplained rows: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
