"""Audit false published alerts in the demo simulation.

This script focuses on the current best raw candidate:

    product x source family + real classification

It writes a row-level CSV for false alerts and a compact markdown summary. The
goal is to decide whether false alerts come from mock background drift, missing
golden labels, timing mismatch, or actual detection over-sensitivity.
"""

from __future__ import annotations

import asyncio
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))

import app.detection.statistics as stats_mod
from app.batch.daily import STATE_RETENTION_DAYS, CountingClient
from app.detection.service import detect_anomaly
from demo_sim import (
    CACHE,
    FAMILIES,
    _ORIGINAL_DECIDE,
    classify_alert,
    day_date,
    load_truth_sets,
    make_decide,
    swap_real,
)
from scripts.golden_inputs import load_golden_inputs as load_inputs
from validate_anomaly import BASELINE_RATE


OUT_CSV = ROOT / "eval/results/demo_false_alert_audit_20260806.csv"
OUT_MD = ROOT / "eval/results/demo_false_alert_audit_20260806.md"


def truth_index(truth: dict) -> dict[tuple[str, str, str], list[tuple[str, int, int]]]:
    by_pas: dict[tuple[str, str, str], list[tuple[str, int, int]]] = defaultdict(list)
    for (product, aspect, channel, source), (ws, we) in truth.items():
        by_pas[(product, aspect, source)].append((channel, ws, we))
    return by_pas


def false_reason(alert, day_n: int, by_pas: dict, ignored: dict) -> str:
    source = alert.stats.source.value
    aspect = alert.main_aspect.value
    channels = {c.value for c in alert.significant_channels} or {alert.channel.value}
    for ch in channels:
        span = ignored.get((alert.product_group_id, aspect, ch, source))
        if span and span[0] <= day_n <= span[1] + 6:
            return "scoring_excluded_window"

    spans = by_pas.get((alert.product_group_id, alert.main_aspect.value, alert.stats.source.value), [])
    if not spans:
        return "no_golden_case_same_product_aspect_source"

    channels = {c.value for c in alert.significant_channels} or {alert.channel.value}
    for true_channel, ws, we in spans:
        if true_channel in channels and day_n < ws:
            return "early_same_case"
        if true_channel in channels and day_n > we:
            return "late_same_case"
    return "same_product_aspect_source_but_other_channel"


async def collect_false_alerts():
    gold_items, documents = load_inputs()
    cache = json.loads(CACHE.read_text(encoding="utf-8"))
    truth, ignored = load_truth_sets()
    real_items, swapped = swap_real(gold_items, cache)

    family = "상품x source"
    stats_mod.decide_fires = make_decide(FAMILIES[family])
    published = []
    rows = []
    by_pas = truth_index(truth)

    for day_n in range(29, 61):
        wend = day_date(day_n)
        cutoff = date.fromordinal(wend.toordinal() - STATE_RETENTION_DAYS)
        prior = [a for a in published if a.window_end >= cutoff]

        alerts, _suppressed = await detect_anomaly(
            real_items,
            documents=documents,
            window_end=wend,
            prior_alerts=prior,
            resolved_alert_ids=set(),
            client=CountingClient(),
        )

        for alert in alerts:
            kind = classify_alert(alert, truth, day_n, ignored)
            if kind not in {"false", "ignored"}:
                continue

            aspect = alert.main_aspect.value
            channel = alert.channel.value
            source = alert.stats.source.value
            baseline = BASELINE_RATE.get(aspect, {}).get(channel)
            rows.append(
                {
                    "day": day_n,
                    "product": alert.product_group_id,
                    "source": source,
                    "aspect": aspect,
                    "channel": channel,
                    "significant_channels": ",".join(c.value for c in alert.significant_channels),
                    "verdict": alert.verdict.value,
                    "confidence": alert.detection_confidence.value,
                    "interpretation": alert.source_signals.interpretation,
                    "recommended_action": alert.recommended_action.value,
                    "cur_total": alert.stats.cur_total,
                    "cur_rate": round(alert.stats.cur_rate, 4),
                    "past_rate": round(alert.stats.past_rate, 4),
                    "delta": round(alert.stats.delta, 4),
                    "baseline_rate": round(baseline, 4) if baseline is not None else "",
                    "cur_minus_baseline": round(alert.stats.cur_rate - baseline, 4) if baseline is not None else "",
                    "past_minus_baseline": round(alert.stats.past_rate - baseline, 4) if baseline is not None else "",
                    "p_value": alert.stats.p_value,
                    "bh_significant": alert.stats.bh_significant,
                    "kind": kind,
                    "reason_hint": false_reason(alert, day_n, by_pas, ignored),
                }
            )
        published.extend(alerts)

    stats_mod.decide_fires = _ORIGINAL_DECIDE
    return rows, len(documents), len(cache), swapped


def write_outputs(rows: list[dict], n_documents: int, n_cache: int, n_swapped: int) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0]) if rows else []
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    by_reason = Counter(r["reason_hint"] for r in rows)
    by_kind = Counter(r["kind"] for r in rows)
    by_aspect = Counter(r["aspect"] for r in rows)
    by_source = Counter(r["source"] for r in rows)
    by_channel = Counter(r["channel"] for r in rows)
    by_day = Counter(int(r["day"]) for r in rows)
    by_product = Counter(r["product"] for r in rows)

    cs_color = [r for r in rows if r["source"] == "cs" and r["aspect"] == "색상"]
    avg_cur = sum(float(r["cur_rate"]) for r in cs_color) / len(cs_color) if cs_color else 0.0
    avg_past = sum(float(r["past_rate"]) for r in cs_color) / len(cs_color) if cs_color else 0.0
    avg_base = sum(float(r["baseline_rate"]) for r in cs_color if r["baseline_rate"] != "") / len(cs_color) if cs_color else 0.0

    md = [
        "# Demo false alert audit, 2026-08-06",
        "",
        "Scope: product x source family + real classification, raw demo publishing.",
        "",
        f"- Documents: {n_documents:,}",
        f"- Classification cache rows: {n_cache:,}",
        f"- Swapped with real classification: {n_swapped:,}",
        f"- False/ignored published alerts audited: {len(rows)}",
        "",
        "## Summary",
        "",
        f"- By kind: {dict(by_kind.most_common())}",
        f"- By reason hint: {dict(by_reason.most_common())}",
        f"- By source: {dict(by_source.most_common())}",
        f"- By aspect: {dict(by_aspect.most_common())}",
        f"- By channel: {dict(by_channel.most_common())}",
        f"- Top products: {dict(by_product.most_common(10))}",
        f"- Top false days: {dict(by_day.most_common(10))}",
        "",
        "## CS Color Concentration",
        "",
        f"- CS color false alerts: {len(cs_color)}/{len(rows)}",
        f"- Average current rate: {avg_cur:.4f}",
        f"- Average past rate: {avg_past:.4f}",
        f"- Average configured baseline rate: {avg_base:.4f}",
        "",
        "## Interpretation Notes",
        "",
        "- `ignored` means the alert overlaps a scenario window that was explicitly excluded from scoring (`scoring_included=N`) or has no intended answer.",
        "- If `past_rate` is far below `baseline_rate`, the alert may be caused by an unusually low past window rather than an unusually high current window.",
        "- If `cur_rate` is close to the configured baseline but `delta` is large, the mock background baseline is likely unstable.",
        "- `no_golden_case_same_product_aspect_source` means the alert is outside the current golden anomaly definitions. It can be a true mock false alert or a missing golden case; it needs review.",
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")


async def main() -> None:
    rows, n_documents, n_cache, n_swapped = await collect_false_alerts()
    write_outputs(rows, n_documents, n_cache, n_swapped)
    print(f"false/ignored alerts: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    asyncio.run(main())
