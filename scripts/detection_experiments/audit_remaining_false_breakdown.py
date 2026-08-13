"""Break down remaining demo false alerts after scoring exclusions.

Scope:
    product x source family + real classification

For every remaining `false` alert, this script records:

- whether an oracle/golden run publishes a matching alert,
- how far it is from the next TRUE window for the same product/aspect/source,
- whether the relation is same-channel, other-channel, or no TRUE case.

This is a diagnostic audit only. It does not change thresholds or regenerate
mock data.
"""

from __future__ import annotations

import asyncio
import csv
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))

from demo_sim import (
    _ORIGINAL_DECIDE,
    FAMILIES,
    classify_alert,
    day_date,
    load_truth_sets,
    make_decide,
    require_full_real_cache,
)
from validate_anomaly import BASELINE_RATE

import app.detection.statistics as stats_mod
from app.batch.daily import STATE_RETENTION_DAYS, CountingClient
from app.detection.service import detect_anomaly
from scripts.golden_inputs import load_golden_inputs as load_inputs

OUT_CSV = ROOT / "eval/results/remaining_false_breakdown_20260807.csv"
OUT_MD = ROOT / "eval/results/remaining_false_breakdown_20260807.md"


def channels_of(alert) -> set[str]:
    return {c.value for c in alert.significant_channels} or {alert.channel.value}


def truth_by_pas(truth: dict) -> dict[tuple[str, str, str], list[dict]]:
    out: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for (product, aspect, channel, source), (ws, we) in truth.items():
        out[(product, aspect, source)].append({
            "channel": channel,
            "start": ws,
            "end": we,
        })
    return out


def ignored_by_pas(ignored: dict) -> dict[tuple[str, str, str], list[dict]]:
    out: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for (product, aspect, channel, source), (ws, we) in ignored.items():
        out[(product, aspect, source)].append({
            "channel": channel,
            "start": ws,
            "end": we,
        })
    return out


def relation_to_truth(alert, day_n: int, by_pas: dict) -> dict:
    key = (alert.product_group_id, alert.main_aspect.value, alert.stats.source.value)
    spans = by_pas.get(key, [])
    chs = channels_of(alert)
    same_channel = [s for s in spans if s["channel"] in chs]
    other_channel = [s for s in spans if s["channel"] not in chs]

    future_same = [s for s in same_channel if day_n < s["start"]]
    future_any = [s for s in spans if day_n < s["start"]]
    if future_same:
        nearest = min(future_same, key=lambda s: s["start"])
        return {
            "truth_relation": "early_same_channel",
            "days_to_true_start": nearest["start"] - day_n,
            "nearest_true_start": nearest["start"],
            "nearest_true_end": nearest["end"],
            "nearest_true_channel": nearest["channel"],
            "true_channels_same_pas": "|".join(sorted(s["channel"] for s in spans)),
        }
    if future_any:
        nearest = min(future_any, key=lambda s: s["start"])
        relation = "early_other_channel" if nearest in other_channel else "early_same_pas"
        return {
            "truth_relation": relation,
            "days_to_true_start": nearest["start"] - day_n,
            "nearest_true_start": nearest["start"],
            "nearest_true_end": nearest["end"],
            "nearest_true_channel": nearest["channel"],
            "true_channels_same_pas": "|".join(sorted(s["channel"] for s in spans)),
        }
    if same_channel:
        nearest = min(same_channel, key=lambda s: abs(day_n - s["end"]))
        return {
            "truth_relation": "after_same_channel_tail_or_late",
            "days_to_true_start": "",
            "nearest_true_start": nearest["start"],
            "nearest_true_end": nearest["end"],
            "nearest_true_channel": nearest["channel"],
            "true_channels_same_pas": "|".join(sorted(s["channel"] for s in spans)),
        }
    if spans:
        nearest = min(spans, key=lambda s: abs(day_n - s["start"]))
        return {
            "truth_relation": "same_pas_other_channel_only",
            "days_to_true_start": nearest["start"] - day_n,
            "nearest_true_start": nearest["start"],
            "nearest_true_end": nearest["end"],
            "nearest_true_channel": nearest["channel"],
            "true_channels_same_pas": "|".join(sorted(s["channel"] for s in spans)),
        }
    return {
        "truth_relation": "no_true_same_product_aspect_source",
        "days_to_true_start": "",
        "nearest_true_start": "",
        "nearest_true_end": "",
        "nearest_true_channel": "",
        "true_channels_same_pas": "",
    }


def relation_to_ignored(alert, day_n: int, by_pas: dict) -> str:
    key = (alert.product_group_id, alert.main_aspect.value, alert.stats.source.value)
    chs = channels_of(alert)
    for span in by_pas.get(key, []):
        if span["channel"] in chs and span["start"] <= day_n <= span["end"] + 6:
            return "overlaps_ignored_window"
    return ""


def alert_matches(a, b) -> bool:
    if a.product_group_id != b.product_group_id:
        return False
    if a.main_aspect.value != b.main_aspect.value:
        return False
    if a.stats.source.value != b.stats.source.value:
        return False
    return bool(channels_of(a) & channels_of(b))


async def collect_alerts(items, documents, label: str, truth: dict, ignored: dict) -> list[dict]:
    family = "상품x source"
    stats_mod.decide_fires = make_decide(FAMILIES[family])
    published = []
    records = []

    for day_n in range(29, 61):
        wend = day_date(day_n)
        cutoff = date.fromordinal(wend.toordinal() - STATE_RETENTION_DAYS)
        prior = [a for a in published if a.window_end >= cutoff]
        alerts, _suppressed = await detect_anomaly(
            items,
            documents=documents,
            window_end=wend,
            prior_alerts=prior,
            resolved_alert_ids=set(),
            client=CountingClient(),
        )
        for alert in alerts:
            records.append({
                "label": label,
                "day": day_n,
                "kind": classify_alert(alert, truth, day_n, ignored),
                "alert": alert,
            })
        published.extend(alerts)

    stats_mod.decide_fires = _ORIGINAL_DECIDE
    return records


def row_from_false(rec: dict, oracle_records: list[dict], by_truth: dict, by_ignored: dict) -> dict:
    alert = rec["alert"]
    day_n = rec["day"]
    aspect = alert.main_aspect.value
    source = alert.stats.source.value
    channel = alert.channel.value
    significant_channels = "|".join(sorted(channels_of(alert)))
    baseline = BASELINE_RATE.get(aspect, {}).get(channel)

    oracle_same_day = [
        o for o in oracle_records
        if o["day"] == day_n and alert_matches(alert, o["alert"])
    ]
    oracle_near = [
        o for o in oracle_records
        if abs(o["day"] - day_n) <= 1 and alert_matches(alert, o["alert"])
    ]
    relation = relation_to_truth(alert, day_n, by_truth)
    ignored_relation = relation_to_ignored(alert, day_n, by_ignored)
    oracle_kinds = "|".join(sorted({o["kind"] for o in oracle_same_day}))

    if oracle_same_day:
        likely_bucket = "detector_or_data_definition"
    elif oracle_near:
        likely_bucket = "timing_or_suppression_difference"
    else:
        likely_bucket = "real_classifier_or_real_only_path"

    return {
        "day": day_n,
        "product": alert.product_group_id,
        "source": source,
        "aspect": aspect,
        "channel": channel,
        "significant_channels": significant_channels,
        "truth_relation": relation["truth_relation"],
        "days_to_true_start": relation["days_to_true_start"],
        "nearest_true_start": relation["nearest_true_start"],
        "nearest_true_end": relation["nearest_true_end"],
        "nearest_true_channel": relation["nearest_true_channel"],
        "true_channels_same_pas": relation["true_channels_same_pas"],
        "ignored_relation": ignored_relation,
        "oracle_same_day_match": bool(oracle_same_day),
        "oracle_near_day_match": bool(oracle_near),
        "oracle_same_day_kinds": oracle_kinds,
        "likely_bucket": likely_bucket,
        "cur_total": alert.stats.cur_total,
        "cur_rate": round(alert.stats.cur_rate, 4),
        "past_rate": round(alert.stats.past_rate, 4),
        "delta": round(alert.stats.delta, 4),
        "baseline_rate": round(baseline, 4) if baseline is not None else "",
        "p_value": alert.stats.p_value,
    }


def write_outputs(rows: list[dict], real_count: int, oracle_count: int) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    by_bucket = Counter(r["likely_bucket"] for r in rows)
    by_relation = Counter(r["truth_relation"] for r in rows)
    by_oracle = Counter("oracle_match" if r["oracle_same_day_match"] else "real_only" for r in rows)
    by_day = Counter(int(r["day"]) for r in rows)
    by_product = Counter(r["product"] for r in rows)

    early = [r for r in rows if r["truth_relation"] == "early_same_channel"]
    early_days = [int(r["days_to_true_start"]) for r in early if r["days_to_true_start"] != ""]
    avg_early = sum(early_days) / len(early_days) if early_days else 0.0

    lines = [
        "# Remaining false alert breakdown, 2026-08-07",
        "",
        "Scope: product x source family + real classification after scoring-excluded alerts are ignored.",
        "",
        f"- Real published alerts collected: {real_count}",
        f"- Oracle published alerts collected: {oracle_count}",
        f"- Remaining false alerts analyzed: {len(rows)}",
        "",
        "## Summary",
        "",
        f"- By likely bucket: {dict(by_bucket.most_common())}",
        f"- By truth relation: {dict(by_relation.most_common())}",
        f"- By oracle match: {dict(by_oracle.most_common())}",
        f"- Top false days: {dict(by_day.most_common(10))}",
        f"- Top products: {dict(by_product.most_common(10))}",
        f"- Early same-channel average days before TRUE start: {avg_early:.2f}",
        "",
        "## Interpretation",
        "",
        "- `detector_or_data_definition`: a matching oracle alert also appears, so this is unlikely to be caused by the real classifier alone.",
        "- `real_classifier_or_real_only_path`: no matching oracle alert appears on the same or adjacent day, so classifier/cache effects or real-only suppression path differences are plausible.",
        "- `early_same_channel`: same product/aspect/source/channel has a future TRUE window; this may be early signal, a too-late golden window, or detector sensitivity.",
        "- This audit does not change thresholds and does not regenerate mock data.",
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def main() -> None:
    gold_items, documents = load_inputs()
    _path, _cache, real_items, _swapped, _coverage = require_full_real_cache(
        gold_items
    )
    truth, ignored = load_truth_sets()
    by_truth = truth_by_pas(truth)
    by_ignored = ignored_by_pas(ignored)

    oracle_records = await collect_alerts(gold_items, documents, "oracle", truth, ignored)
    real_records = await collect_alerts(real_items, documents, "real", truth, ignored)
    false_records = [r for r in real_records if r["kind"] == "false"]
    rows = [row_from_false(r, oracle_records, by_truth, by_ignored) for r in false_records]
    write_outputs(rows, len(real_records), len(oracle_records))
    print(f"remaining false alerts: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    asyncio.run(main())
