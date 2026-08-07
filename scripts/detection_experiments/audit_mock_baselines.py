"""Slice mock baseline rates without using detection outcomes.

This audit answers three separate questions:

1. Does the pure generated background match BASELINE_RATE?
2. How different are case products outside their configured windows?
3. How much does the cached real classifier move negative rates away from the
   golden labels?

It never tunes thresholds and never uses alert success/failure to select rows.
"""

from __future__ import annotations

import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from validate_anomaly import BASELINE_RATE

OUT_CSV = ROOT / "eval/results/mock_baseline_audit_20260806.csv"
OUT_MD = ROOT / "eval/results/mock_baseline_audit_20260806.md"
CACHE = ROOT / "data/eval_cache/pipeline_full_batch_classify_aspect_v5-15290041_run1.json"

ALL_ASPECTS = ["색상", "사이즈", "소재", "파손", "오배송", "기타"]
REVIEW_ASPECTS = {"색상", "사이즈", "소재"}
DAY1 = datetime(2026, 6, 30).date()


def read(rel: str) -> list[dict]:
    with (ROOT / rel).open(encoding="utf-8-sig", newline="") as f:
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


def config_index() -> tuple[set[str], dict[tuple[str, str, str], list[dict]]]:
    case_products: set[str] = set()
    by_pcs: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in read("data/config/config_anomaly.csv"):
        product = r["golden_group_id"]
        case_products.add(product)
        row = {
            "product": product,
            "channel": r["channel"],
            "source": r["source"],
            "aspect": r["aspect"],
            "start": int(r["window_start_day"]),
            "end": int(r["window_end_day"]),
            "intended": r["intended_answer"].strip().upper(),
            "scoring": r["scoring_included"].strip().upper(),
        }
        row["excluded"] = row["scoring"] == "N" or not row["intended"]
        by_pcs[(product, r["channel"], r["source"])].append(row)
    return case_products, by_pcs


def windows_for(product: str, channel: str, source: str, day: int, by_pcs: dict) -> list[dict]:
    return [
        r for r in by_pcs.get((product, channel, source), [])
        if r["start"] <= day <= r["end"]
    ]


def slices_for(product: str, channel: str, source: str, day: int, case_products: set[str], by_pcs: dict) -> list[str]:
    windows = windows_for(product, channel, source, day, by_pcs)
    slices = []
    if product not in case_products:
        slices.append("pure_background_products")
    elif not windows:
        slices.append("case_products_outside_windows")
    else:
        if any(w["excluded"] for w in windows):
            slices.append("scoring_excluded_windows")
        if any(w["intended"] == "TRUE" and not w["excluded"] for w in windows):
            slices.append("scored_true_windows")
        if any(w["intended"] == "FALSE" and not w["excluded"] for w in windows):
            slices.append("scored_false_windows")
    return slices


def allowed_aspects(source: str) -> list[str]:
    if source == "review":
        return [a for a in ALL_ASPECTS if a in REVIEW_ASPECTS]
    return ALL_ASPECTS


def golden_negative_aspects(label: dict) -> list[str]:
    if label.get("true_aspect") and label.get("true_sentiment") == "-1":
        return [label["true_aspect"]]
    return []


def overlay_negative_aspects(item_id: str, cache: dict, fallback: list[str]) -> list[str]:
    preds = cache.get(item_id)
    if preds is None:
        return fallback
    return [
        p["aspect"] for p in preds
        if p.get("sentiment") == -1 and p.get("aspect") in ALL_ASPECTS
    ]


def add_doc(
    acc: dict,
    slice_name: str,
    label_mode: str,
    source: str,
    channel: str,
    neg_aspects: list[str],
    cache_hit: bool,
) -> None:
    neg_set = set(neg_aspects)
    for aspect in allowed_aspects(source):
        key = (slice_name, label_mode, source, channel, aspect)
        acc[key]["total"] += 1
        acc[key]["cache_docs"] += int(cache_hit)
        if aspect in neg_set:
            acc[key]["neg"] += 1


def wilson(p_num: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 0.0
    p = p_num / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def collect() -> dict:
    product_of = product_mapping()
    case_products, by_pcs = config_index()
    cache = json.loads(CACHE.read_text(encoding="utf-8"))
    labels = {
        "cs": {r["inquiry_id"]: r for r in read("data/golden/golden_cs_labels.csv")},
        "review": {r["review_id"]: r for r in read("data/golden/golden_review_labels.csv")},
    }
    inputs = [
        ("cs", "data/input/input_cs_inquiries.csv", "inquiry_id", "inquired_at"),
        ("review", "data/input/input_reviews.csv", "review_id", "created_at"),
    ]
    acc = defaultdict(lambda: {"neg": 0, "total": 0, "cache_docs": 0})

    for source, path, id_key, time_key in inputs:
        for row in read(path):
            product = product_of.get((row["channel"], row["channel_product_id"]))
            if product is None:
                continue
            day = day_number(row[time_key])
            slice_names = slices_for(product, row["channel"], source, day, case_products, by_pcs)
            if not slice_names:
                continue

            item_id = row[id_key]
            golden_neg = golden_negative_aspects(labels[source].get(item_id, {}))
            cache_hit = item_id in cache
            overlay_neg = overlay_negative_aspects(item_id, cache, golden_neg)
            for slice_name in slice_names:
                add_doc(acc, slice_name, "golden", source, row["channel"], golden_neg, cache_hit)
                add_doc(acc, slice_name, "classifier_overlay", source, row["channel"], overlay_neg, cache_hit)
    return acc


def rows_from_counts(acc: dict) -> list[dict]:
    rows = []
    for (slice_name, label_mode, source, channel, aspect), counts in sorted(acc.items()):
        n = counts["total"]
        neg = counts["neg"]
        cache_docs = counts["cache_docs"]
        observed = neg / n if n else 0.0
        lo, hi = wilson(neg, n)
        expected = BASELINE_RATE.get(aspect, {}).get(channel)
        rows.append(
            {
                "slice": slice_name,
                "label_mode": label_mode,
                "source": source,
                "channel": channel,
                "aspect": aspect,
                "neg": neg,
                "total": n,
                "cache_docs": cache_docs,
                "cache_coverage": round(cache_docs / n, 5) if n else 0.0,
                "observed_rate": round(observed, 5),
                "wilson_low": round(lo, 5),
                "wilson_high": round(hi, 5),
                "configured_baseline": round(expected, 5) if expected is not None else "",
                "observed_minus_config": round(observed - expected, 5) if expected is not None else "",
                "config_inside_ci": (lo <= expected <= hi) if expected is not None else "",
            }
        )
    return rows


def by_key(rows: list[dict]) -> dict[tuple[str, str, str, str, str], dict]:
    return {
        (r["slice"], r["label_mode"], r["source"], r["channel"], r["aspect"]): r
        for r in rows
    }


def fmt_pct(value: float | str) -> str:
    if value == "":
        return ""
    return f"{float(value):.3%}"


def table_rows(rows: list[dict], slice_name: str, label_mode: str, source: str, aspects: set[str]) -> list[str]:
    out = []
    for r in rows:
        if r["slice"] != slice_name or r["label_mode"] != label_mode or r["source"] != source:
            continue
        if r["aspect"] not in aspects:
            continue
        out.append(
            f"| {r['channel']} | {r['aspect']} | {r['neg']}/{r['total']} | "
            f"{r['observed_rate']:.3%} | {r['wilson_low']:.3%}-{r['wilson_high']:.3%} | "
            f"{fmt_pct(r['configured_baseline'])} | {fmt_pct(r['observed_minus_config'])} | "
            f"{r['config_inside_ci']} |"
        )
    return out


def write_outputs(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    indexed = by_key(rows)
    pure_focus = [
        r for r in rows
        if r["slice"] == "pure_background_products"
        and r["label_mode"] == "golden"
        and r["source"] == "cs"
        and r["aspect"] in {"색상", "사이즈", "소재"}
    ]
    pure_offenders = [
        r for r in pure_focus
        if r["configured_baseline"] != "" and not r["config_inside_ci"]
    ]

    lines = [
        "# Mock baseline audit, 2026-08-06",
        "",
        "This audit separates pure generated background, case products outside configured windows, scoring-excluded windows, and classifier overlay effects.",
        "It does not use detection false-alert outcomes to choose any baseline.",
        "",
        "## Slices",
        "",
        "- `pure_background_products`: products never listed in `config_anomaly.csv`.",
        "- `case_products_outside_windows`: case products on days with no configured window for that product/channel/source.",
        "- `scoring_excluded_windows`: configured windows excluded by `scoring_included=N` or blank `intended_answer`.",
        "- `scored_true_windows` / `scored_false_windows`: configured scored windows, for diagnostic contrast only.",
        "",
        "## Pure Background, Golden Labels",
        "",
        "| Channel | Aspect | Neg/Total | Observed | 95% CI | Config | Diff | Config in CI |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(table_rows(rows, "pure_background_products", "golden", "cs", {"색상", "사이즈", "소재"}))

    lines.extend([
        "",
        f"Pure-background CS 색상/사이즈/소재 rows outside config CI: {len(pure_offenders)}/{len(pure_focus)}",
        "",
        "## Classifier Overlay Delta, Pure Background",
        "",
        "| Channel | Aspect | Golden | Overlay | Delta | Cache coverage |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for g in pure_focus:
        o = indexed.get(("pure_background_products", "classifier_overlay", g["source"], g["channel"], g["aspect"]))
        if not o:
            continue
        delta = float(o["observed_rate"]) - float(g["observed_rate"])
        lines.append(
            f"| {g['channel']} | {g['aspect']} | {g['observed_rate']:.3%} | "
            f"{o['observed_rate']:.3%} | {delta:+.3%} | {o['cache_coverage']:.3%} |"
        )

    lines.extend([
        "",
        "## Scoring-Excluded Windows, Golden Labels",
        "",
        "| Channel | Aspect | Neg/Total | Observed | 95% CI | Config | Diff | Config in CI |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    lines.extend(table_rows(rows, "scoring_excluded_windows", "golden", "cs", {"색상", "사이즈", "소재", "파손", "오배송", "기타"}))

    lines.extend([
        "",
        "## Interpretation",
        "",
        "- If pure background is near `config/aspect_count`, the generator is applying the table as an aspect-internal rate.",
        "- If classifier overlay is much higher than golden, prompt/classifier bias can create extra detection numerator mass.",
        "- Scoring-excluded windows should not be counted as false alerts in demo performance, because they were explicitly excluded before scoring.",
        "- Official `data/input` and `data/golden` were not regenerated by this audit.",
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    rows = rows_from_counts(collect())
    write_outputs(rows)
    print(f"rows: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
