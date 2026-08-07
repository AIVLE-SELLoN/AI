"""Audit whether case-past jumps and baseline dilution are the same issue.

This script does not tune detection. It checks the generated golden labels
against config_anomaly.csv:

1. TRUE rows' ws-28..ws-1 case-past rates vs config past_rate / BASELINE_RATE.
2. Pure-background rates for the same source/channel/aspect denominator.
3. Distribution of case-past start days.
4. Details of ignored demo alerts.
"""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT / "scripts"))

from validate_anomaly import BASELINE_RATE

DAY1 = datetime(2026, 6, 30).date()
OUT_CSV = ROOT / "eval/results/case_past_baseline_link_20260807.csv"
OUT_MD = ROOT / "eval/results/case_past_baseline_link_20260807.md"


def read_csv(rel: str) -> list[dict]:
    with (ROOT / rel).open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def day_number(raw: str) -> int:
    return 1 + (datetime.fromisoformat(raw).date() - DAY1).days


def product_mapping() -> dict[tuple[str, str], str]:
    variant_group = {
        r["variant_row_id"]: r["golden_group_id"]
        for r in read_csv("data/golden/golden_mapping.csv")
    }
    out: dict[tuple[str, str], str] = {}
    for r in read_csv("data/input/input_channel_products.csv"):
        group = variant_group.get(r["variant_row_id"])
        if group:
            out[(r["channel"], r["channel_product_id"])] = group
    return out


def load_docs() -> list[dict]:
    product_of = product_mapping()
    docs: list[dict] = []
    specs = [
        (
            "cs",
            "data/input/input_cs_inquiries.csv",
            "data/golden/golden_cs_labels.csv",
            "inquiry_id",
            "inquired_at",
        ),
        (
            "review",
            "data/input/input_reviews.csv",
            "data/golden/golden_review_labels.csv",
            "review_id",
            "created_at",
        ),
    ]
    for source, input_rel, label_rel, id_key, time_key in specs:
        labels = {r[id_key]: r for r in read_csv(label_rel)}
        for row in read_csv(input_rel):
            product = product_of.get((row["channel"], row["channel_product_id"]))
            if not product:
                continue
            label = labels.get(row[id_key], {})
            docs.append(
                {
                    "id": row[id_key],
                    "product": product,
                    "channel": row["channel"],
                    "source": source,
                    "day": day_number(row[time_key]),
                    "aspect": label.get("true_aspect", ""),
                    "sentiment": label.get("true_sentiment", ""),
                }
            )
    return docs


def count_rate(
    docs: list[dict],
    *,
    product: str | None,
    source: str,
    channel: str,
    aspect: str,
    start_day: int | None = None,
    end_day: int | None = None,
    exclude_products: set[str] | None = None,
) -> dict:
    total = 0
    neg = 0
    for doc in docs:
        if product is not None and doc["product"] != product:
            continue
        if exclude_products and doc["product"] in exclude_products:
            continue
        if doc["source"] != source or doc["channel"] != channel:
            continue
        if start_day is not None and doc["day"] < start_day:
            continue
        if end_day is not None and doc["day"] > end_day:
            continue
        total += 1
        if doc["aspect"] == aspect and doc["sentiment"] == "-1":
            neg += 1
    return {"neg": neg, "total": total, "rate": neg / total if total else 0.0}


def fmt_pct(value: float) -> str:
    return f"{value:.2%}"


def main() -> None:
    docs = load_docs()
    config = read_csv("data/config/config_anomaly.csv")
    case_products = {r["golden_group_id"] for r in config}
    true_rows = [r for r in config if r["intended_answer"].strip().upper() == "TRUE"]

    rows: list[dict] = []
    exact_config_matches = 0
    exact_baseline_matches = 0
    ratios = []
    background_ratios = []

    for r in true_rows:
        ws = int(r["window_start_day"])
        past_start = ws - 28
        past_end = ws - 1
        aspect = r["aspect"]
        channel = r["channel"]
        source = r["source"]
        product = r["golden_group_id"]
        observed = count_rate(
            docs,
            product=product,
            source=source,
            channel=channel,
            aspect=aspect,
            start_day=past_start,
            end_day=past_end,
        )
        pure = count_rate(
            docs,
            product=None,
            source=source,
            channel=channel,
            aspect=aspect,
            exclude_products=case_products,
        )
        config_rate = int(r["past_neg"]) / int(r["past_total"])
        csv_rate = float(r["past_rate"])
        baseline = BASELINE_RATE[aspect][channel]
        observed_rate = observed["rate"]
        pure_rate = pure["rate"]
        config_match = abs(observed_rate - config_rate) < 0.00001
        baseline_match = abs(config_rate - baseline) < 0.0001
        exact_config_matches += int(config_match)
        exact_baseline_matches += int(baseline_match)
        ratios.append(observed_rate / config_rate if config_rate else 0.0)
        background_ratios.append(config_rate / pure_rate if pure_rate else 0.0)
        rows.append(
            {
                "case_id": r["case_id"],
                "product": product,
                "source": source,
                "channel": channel,
                "aspect": aspect,
                "window": f"{r['window_start_day']}-{r['window_end_day']}",
                "case_past_window": f"{past_start}-{past_end}",
                "observed_neg_total": f"{observed['neg']}/{observed['total']}",
                "observed_rate": round(observed_rate, 5),
                "config_past_rate": round(config_rate, 5),
                "csv_past_rate": round(csv_rate, 5),
                "baseline_rate": round(baseline, 5),
                "pure_background_neg_total": f"{pure['neg']}/{pure['total']}",
                "pure_background_rate": round(pure_rate, 5),
                "config_vs_background_ratio": round(config_rate / pure_rate, 2) if pure_rate else "",
                "observed_matches_config": config_match,
                "config_matches_baseline": baseline_match,
            }
        )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    start_counts = Counter(int(r["window_start_day"]) - 28 for r in config)
    start_focus = {day: count for day, count in sorted(start_counts.items()) if day in {23, 24, 26}}
    focus_rows = [r for r in config if int(r["window_start_day"]) - 28 in {23, 24, 26}]
    focus_source = Counter(r["source"] for r in focus_rows)
    focus_aspect = Counter(r["aspect"] for r in focus_rows)
    focus_scored = Counter(r["intended_answer"].strip().upper() or "blank" for r in focus_rows)

    excluded_config = [
        r for r in config
        if r["scoring_included"].strip().upper() == "N" or not r["intended_answer"].strip()
    ]
    excluded_delta_ge_3pp = [
        r for r in excluded_config
        if float(r["cur_rate"]) - float(r["past_rate"]) >= 0.03
    ]

    ignored_path = ROOT / "eval/results/demo_false_alert_audit_20260806.csv"
    ignored_rows = []
    if ignored_path.exists():
        with ignored_path.open(encoding="utf-8-sig", newline="") as f:
            ignored_rows = [r for r in csv.DictReader(f) if r.get("kind") == "ignored"]
    for alert in ignored_rows:
        channels = set((alert.get("significant_channels") or alert["channel"]).split(","))
        if alert["channel"] != "ALL":
            channels.add(alert["channel"])
        matches = []
        for r in excluded_config:
            if r["golden_group_id"] != alert["product"]:
                continue
            if r["source"] != alert["source"] or r["aspect"] != alert["aspect"]:
                continue
            if r["channel"] not in channels:
                continue
            day = int(alert["day"])
            start = int(r["window_start_day"])
            end = int(r["window_end_day"]) + 6
            if start <= day <= end:
                delta = float(r["cur_rate"]) - float(r["past_rate"])
                matches.append(
                    f"{r['case_id']} {r['channel']} {r['window_start_day']}-{r['window_end_day']} "
                    f"config_delta={delta:.2%} intended={r['intended_answer'] or 'blank'}"
                )
        alert["matched_config"] = " / ".join(matches)

    avg_observed = sum(float(r["observed_rate"]) for r in rows) / len(rows)
    avg_config = sum(float(r["config_past_rate"]) for r in rows) / len(rows)
    avg_pure = sum(float(r["pure_background_rate"]) for r in rows) / len(rows)
    avg_ratio = sum(ratios) / len(ratios)
    avg_bg_ratio = sum(background_ratios) / len(background_ratios)

    sample_rows = rows[:4]
    lines = [
        "# Case-past와 baseline 희석 연결 감사, 2026-08-07",
        "",
        "## 요약",
        "",
        f"- TRUE config 행: {len(rows)}",
        f"- case-past 관측률이 config `past_neg/past_total`과 일치: {exact_config_matches}/{len(rows)}",
        f"- config `past_rate`가 `BASELINE_RATE`와 일치: {exact_baseline_matches}/{len(rows)}",
        f"- 평균 case-past 관측률: {avg_observed:.2%}",
        f"- 평균 config past_rate: {avg_config:.2%}",
        f"- 평균 순수 배경률: {avg_pure:.2%}",
        f"- 평균 observed/config 비율: {avg_ratio:.2f}x",
        f"- 평균 config/순수배경 비율: {avg_bg_ratio:.2f}x",
        "",
        "## 해석",
        "",
        "TRUE case-past 구간은 config 명세대로 생성되어 있다. 단차의 직접 원인은 case-past를 특별 이상 구간으로 잘못 심은 것이 아니라, 일반 배경이 `BASELINE_RATE`보다 낮게 희석되어 있다는 점이다.",
        "",
        "따라서 기존 보고서의 가설 4(순수 배경 baseline 희석)와 가설 6(case-past 조기 발화)은 별도 원인이라기보다 같은 생성기 baseline 해석 문제의 두 현상으로 보는 것이 더 정확하다.",
        "",
        "## TRUE case-past 샘플",
        "",
        "| Case | Product | Source | Channel | Aspect | Case-past | Config | Pure background | Config/Pure |",
        "|---|---|---|---|---|---:|---:|---:|---:|",
    ]
    for r in sample_rows:
        lines.append(
            f"| {r['case_id']} | {r['product']} | {r['source']} | {r['channel']} | {r['aspect']} | "
            f"{fmt_pct(float(r['observed_rate']))} | {fmt_pct(float(r['config_past_rate']))} | "
            f"{fmt_pct(float(r['pure_background_rate']))} | {r['config_vs_background_ratio']}x |"
        )
    lines.extend(
        [
            "",
            "## Case-past 시작일 분포",
            "",
            f"- 전체 config 행: {len(config)}",
            f"- ws-28 in 23/24/26: {sum(start_focus.values())}/{len(config)}",
            f"- 시작일별: {dict(start_focus)}",
            f"- 해당 행 source 구성: {dict(focus_source)}",
            f"- 해당 행 aspect 구성: {dict(focus_aspect)}",
            f"- 해당 행 intended 구성: {dict(focus_scored)}",
            "",
            "## scoring 제외 config 행",
            "",
            f"- 로컬 config 기준 scoring 제외 또는 intended blank 행: {len(excluded_config)}",
            f"- 그중 config 설계 상승폭이 3%p 이상인 행: {len(excluded_delta_ge_3pp)}",
            "",
            "## ignored 알림 6건",
            "",
            "| Day | Product | Source | Aspect | Channel | Current | Past | Delta | Matched config |",
            "|---:|---|---|---|---|---:|---:|---:|---|",
        ]
    )
    for r in ignored_rows:
        lines.append(
            f"| {r['day']} | {r['product']} | {r['source']} | {r['aspect']} | {r['channel']} | "
            f"{float(r['cur_rate']):.2%} | {float(r['past_rate']):.2%} | {float(r['delta']):.2%} | "
            f"{r.get('matched_config', '')} |"
        )
    lines.extend(["", f"CSV: `{OUT_CSV.relative_to(ROOT)}`"])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
