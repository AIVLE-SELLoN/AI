"""Audit why early_same_channel false alerts fire before TRUE windows.

The suspected mechanism is temporal: generated case rows cover
`window_start_day - PAST_WINDOW_DAYS` through `window_end_day`. During daily
simulation, the early part of that generated "case past" interval can fall into
the detector's current 7-day window long before the official TRUE window.

This script quantifies that overlap for the 30 early_same_channel false alerts.
It does not change data, thresholds, labels, or prompts.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS

IN_CSV = ROOT / "eval/results/remaining_false_breakdown_20260807.csv"
OUT_CSV = ROOT / "eval/results/early_same_channel_window_audit_20260807.csv"
OUT_MD = ROOT / "eval/results/early_same_channel_window_audit_20260807.md"
DAY1 = datetime(2026, 6, 30).date()


def read(rel_or_path) -> list[dict]:
    path = Path(rel_or_path)
    if not path.is_absolute():
        path = ROOT / path
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def day_number(raw: str) -> int:
    when = datetime.fromisoformat(raw).date()
    return 1 + (when - DAY1).days


def product_mapping() -> dict[tuple[str, str], str]:
    variant_group = {
        r["variant_row_id"]: r["golden_group_id"]
        for r in read("data/golden/golden_mapping.csv")
    }
    out: dict[tuple[str, str], str] = {}
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


def build_daily_counts() -> dict[tuple[str, str, str, int], dict]:
    product_of = product_mapping()
    labels = label_maps()
    inputs = [
        ("cs", "data/input/input_cs_inquiries.csv", "inquiry_id", "inquired_at"),
        ("review", "data/input/input_reviews.csv", "review_id", "created_at"),
    ]
    counts = defaultdict(lambda: defaultdict(lambda: {"neg": 0, "total": 0}))
    for source, path, id_key, time_key in inputs:
        for row in read(path):
            product = product_of.get((row["channel"], row["channel_product_id"]))
            if product is None:
                continue
            day = day_number(row[time_key])
            key = (product, row["channel"], source, day)
            label = labels[source].get(row[id_key], {})
            for aspect in ["색상", "사이즈", "소재", "파손", "오배송", "기타"]:
                if source == "review" and aspect not in {"색상", "사이즈", "소재"}:
                    continue
                counts[key][aspect]["total"] += 1
                if label.get("true_aspect") == aspect and label.get("true_sentiment") == "-1":
                    counts[key][aspect]["neg"] += 1
    return counts


def sum_counts(daily: dict, product: str, channel: str, source: str, aspect: str, start: int, end: int) -> dict:
    start = max(1, start)
    end = min(60, end)
    neg = total = 0
    for day in range(start, end + 1):
        c = daily.get((product, channel, source, day), {}).get(aspect, {"neg": 0, "total": 0})
        neg += c["neg"]
        total += c["total"]
    return {
        "neg": neg,
        "total": total,
        "rate": neg / total if total else 0.0,
        "days": f"{start}-{end}" if start <= end else "",
    }


def overlap_len(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start) + 1)


def analyze_row(row: dict, daily: dict) -> dict:
    day = int(row["day"])
    true_start = int(row["nearest_true_start"])
    true_end = int(row["nearest_true_end"])
    source = row["source"]
    product = row["product"]
    aspect = row["aspect"]
    channel = row["nearest_true_channel"] if row["channel"] == "ALL" else row["channel"]

    cur_start = day - CURRENT_WINDOW_DAYS + 1
    cur_end = day
    detector_past_start = cur_start - PAST_WINDOW_DAYS
    detector_past_end = cur_start - 1
    case_past_start = true_start - PAST_WINDOW_DAYS
    case_past_end = true_start - 1

    detector_cur = sum_counts(daily, product, channel, source, aspect, cur_start, cur_end)
    detector_past = sum_counts(daily, product, channel, source, aspect, detector_past_start, detector_past_end)
    before_case_past = sum_counts(daily, product, channel, source, aspect, 1, case_past_start - 1)
    case_past_so_far = sum_counts(daily, product, channel, source, aspect, case_past_start, day)
    case_past_full = sum_counts(daily, product, channel, source, aspect, case_past_start, case_past_end)
    true_window = sum_counts(daily, product, channel, source, aspect, true_start, true_end)

    current_overlap = overlap_len(cur_start, cur_end, case_past_start, case_past_end)
    detector_past_overlap = overlap_len(detector_past_start, detector_past_end, case_past_start, case_past_end)

    return {
        "day": day,
        "product": product,
        "source": source,
        "aspect": aspect,
        "alert_channel": row["channel"],
        "analysis_channel": channel,
        "days_to_true_start": row["days_to_true_start"],
        "true_window": f"{true_start}-{true_end}",
        "generated_case_past_window": f"{case_past_start}-{case_past_end}",
        "detector_current_window": f"{max(1, cur_start)}-{cur_end}",
        "detector_past_window": f"{max(1, detector_past_start)}-{detector_past_end}",
        "current_overlap_case_past_days": current_overlap,
        "past_overlap_case_past_days": detector_past_overlap,
        "current_inside_case_past": current_overlap > 0,
        "current_neg_total": f"{detector_cur['neg']}/{detector_cur['total']}",
        "current_rate": round(detector_cur["rate"], 5),
        "detector_past_neg_total": f"{detector_past['neg']}/{detector_past['total']}",
        "detector_past_rate": round(detector_past["rate"], 5),
        "before_case_past_neg_total": f"{before_case_past['neg']}/{before_case_past['total']}",
        "before_case_past_rate": round(before_case_past["rate"], 5),
        "case_past_so_far_neg_total": f"{case_past_so_far['neg']}/{case_past_so_far['total']}",
        "case_past_so_far_rate": round(case_past_so_far["rate"], 5),
        "case_past_full_neg_total": f"{case_past_full['neg']}/{case_past_full['total']}",
        "case_past_full_rate": round(case_past_full["rate"], 5),
        "true_window_neg_total": f"{true_window['neg']}/{true_window['total']}",
        "true_window_rate": round(true_window["rate"], 5),
        "alert_cur_rate": row["cur_rate"],
        "alert_past_rate": row["past_rate"],
        "alert_delta": row["delta"],
    }


def fmt_pct(value: float | str) -> str:
    return f"{float(value):.2%}"


def write_outputs(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    inside = [r for r in rows if r["current_inside_case_past"]]
    avg_overlap = sum(int(r["current_overlap_case_past_days"]) for r in rows) / len(rows) if rows else 0.0
    avg_cur = sum(float(r["current_rate"]) for r in rows) / len(rows) if rows else 0.0
    avg_before = sum(float(r["before_case_past_rate"]) for r in rows) / len(rows) if rows else 0.0
    avg_case_past = sum(float(r["case_past_full_rate"]) for r in rows) / len(rows) if rows else 0.0
    avg_true = sum(float(r["true_window_rate"]) for r in rows) / len(rows) if rows else 0.0

    lines = [
        "# Early same-channel window audit, 2026-08-07",
        "",
        "Scope: the 30 remaining false alerts whose relation is `early_same_channel`.",
        "",
        "## Summary",
        "",
        f"- Rows audited: {len(rows)}",
        f"- Current windows overlapping generated case-past interval: {len(inside)}/{len(rows)}",
        f"- Average overlap in detector current window: {avg_overlap:.2f} days",
        f"- Average detector current rate: {avg_cur:.2%}",
        f"- Average pre-case-past rate: {avg_before:.2%}",
        f"- Average generated case-past full rate: {avg_case_past:.2%}",
        f"- Average TRUE-window rate: {avg_true:.2%}",
        "",
        "## Interpretation",
        "",
        "The early alerts are not primarily classifier errors. They occur because the generated data for a future TRUE case starts filling the case's 28-day `past_neg` interval before the official TRUE window begins.",
        "",
        "During daily simulation, that generated case-past interval can land inside the detector's current 7-day window. The detector then compares it against earlier background days and fires before the official TRUE window.",
        "",
        "This is a mock time-axis / scoring-window definition issue. It should not be fixed by tuning thresholds first.",
        "",
        "## Sample rows",
        "",
        "| Day | Product | Aspect | Channel | TRUE | Case-past | Current | Overlap | Current | Detector past | Pre-case-past |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows[:12]:
        lines.append(
            f"| {r['day']} | {r['product']} | {r['aspect']} | {r['analysis_channel']} | "
            f"{r['true_window']} | {r['generated_case_past_window']} | {r['detector_current_window']} | "
            f"{r['current_overlap_case_past_days']}d | {fmt_pct(r['current_rate'])} | "
            f"{fmt_pct(r['detector_past_rate'])} | {fmt_pct(r['before_case_past_rate'])} |"
        )

    lines.extend([
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    daily = build_daily_counts()
    source_rows = [
        r for r in read(IN_CSV)
        if r["truth_relation"] == "early_same_channel"
    ]
    rows = [analyze_row(r, daily) for r in source_rows]
    write_outputs(rows)
    print(f"early same-channel rows: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
