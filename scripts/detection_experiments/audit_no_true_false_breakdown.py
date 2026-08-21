"""Audit false alerts with no TRUE case for the same product/aspect/source.

Input: `remaining_false_breakdown_20260807.csv`

Scope:
    rows whose truth_relation == no_true_same_product_aspect_source

The goal is to distinguish true background false alerts from cases caused by
configured FALSE / scoring-excluded / other-source / other-aspect scenario
windows. This script does not tune thresholds and does not regenerate data.
"""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.console import force_utf8_output

IN_CSV = ROOT / "eval/results/remaining_false_breakdown_20260807.csv"
OUT_CSV = ROOT / "eval/results/no_true_false_breakdown_20260807.csv"
OUT_MD = ROOT / "eval/results/no_true_false_breakdown_20260807.md"
PAST_DAYS = 28
CURRENT_DAYS = 7


def read(path: Path | str) -> list[dict]:
    p = Path(path)
    if not p.is_absolute():
        p = ROOT / p
    with p.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def parse_channels(raw: str) -> set[str]:
    if not raw:
        return set()
    return {p for p in raw.replace(",", "|").split("|") if p}


def overlap_len(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start) + 1)


def config_rows() -> list[dict]:
    rows = []
    for r in read("data/config/config_anomaly.csv"):
        rows.append({
            "case_id": r["case_id"],
            "product": r["golden_group_id"],
            "channel": r["channel"],
            "source": r["source"],
            "aspect": r["aspect"],
            "start": int(r["window_start_day"]),
            "end": int(r["window_end_day"]),
            "case_past_start": int(r["window_start_day"]) - PAST_DAYS,
            "case_past_end": int(r["window_start_day"]) - 1,
            "scoring": r["scoring_included"].strip().upper(),
            "intended": r["intended_answer"].strip().upper(),
            "note": r.get("note", ""),
            "past_rate": r["past_rate"],
            "cur_rate": r["cur_rate"],
        })
    return rows


def row_relation(false_row: dict, cfg: list[dict]) -> dict:
    day = int(false_row["day"])
    cur_start = day - CURRENT_DAYS + 1
    cur_end = day
    product = false_row["product"]
    source = false_row["source"]
    aspect = false_row["aspect"]
    sig_channels = parse_channels(false_row["significant_channels"]) or {false_row["channel"]}

    same_pas = [
        r for r in cfg
        if r["product"] == product and r["source"] == source and r["aspect"] == aspect
    ]
    same_product_aspect = [
        r for r in cfg
        if r["product"] == product and r["aspect"] == aspect
    ]
    same_product = [r for r in cfg if r["product"] == product]

    def enrich(matches: list[dict]) -> list[dict]:
        out = []
        for r in matches:
            channel_match = r["channel"] in sig_channels
            current_overlap_case_past = overlap_len(cur_start, cur_end, r["case_past_start"], r["case_past_end"])
            current_overlap_window = overlap_len(cur_start, cur_end, r["start"], r["end"])
            distance_to_window = r["start"] - day
            out.append({
                **r,
                "channel_match": channel_match,
                "current_overlap_case_past": current_overlap_case_past,
                "current_overlap_window": current_overlap_window,
                "distance_to_window": distance_to_window,
            })
        return out

    same_pas_e = enrich(same_pas)
    same_product_aspect_e = enrich(same_product_aspect)
    same_product_e = enrich(same_product)

    same_pas_channel = [r for r in same_pas_e if r["channel_match"]]
    same_pas_case_past = [r for r in same_pas_channel if r["current_overlap_case_past"] > 0]
    same_pas_future = [r for r in same_pas_channel if day < r["start"]]
    same_pas_scored_false = [r for r in same_pas_case_past if r["intended"] == "FALSE" and r["scoring"] == "Y"]
    same_pas_excluded = [r for r in same_pas_case_past if r["scoring"] == "N" or not r["intended"]]

    other_source_same_aspect = [
        r for r in same_product_aspect_e
        if r["source"] != source and r["current_overlap_case_past"] > 0
    ]
    other_aspect_same_product = [
        r for r in same_product_e
        if r["aspect"] != aspect and r["current_overlap_case_past"] > 0
    ]

    def choose_primary(matches: list[dict]) -> dict | None:
        if not matches:
            return None
        alert_channel = false_row["channel"]
        return sorted(
            matches,
            key=lambda r: (
                r["channel"] != alert_channel,
                r["channel"] not in sig_channels,
                r["source"] != source,
                -int(r["current_overlap_case_past"]),
                abs(int(r["distance_to_window"])),
            ),
        )[0]

    if same_pas_scored_false:
        category = "future_scored_FALSE_same_pas_case_past"
        evidence = same_pas_scored_false
    elif same_pas_excluded:
        category = "future_scoring_excluded_same_pas_case_past"
        evidence = same_pas_excluded
    elif other_source_same_aspect:
        category = "other_source_same_aspect_case_past"
        evidence = other_source_same_aspect
    elif other_aspect_same_product:
        category = "other_aspect_same_product_case_past"
        evidence = other_aspect_same_product
    elif false_row["oracle_same_day_match"] == "False":
        category = "real_classifier_only_no_config_overlap"
        evidence = []
    elif same_pas_future:
        category = "future_same_pas_no_current_overlap"
        evidence = same_pas_future
    else:
        category = "unexplained_by_config_overlap"
        evidence = []

    nearest = min(same_product_e, key=lambda r: abs(r["start"] - day), default=None)
    primary = choose_primary(evidence) if evidence else nearest
    if primary:
        evidence_text = (
            f"{primary['case_id']} {primary['channel']} {primary['source']} {primary['aspect']} "
            f"{primary['start']}-{primary['end']} scoring={primary['scoring']} intended={primary['intended'] or 'blank'} "
            f"case_past={primary['case_past_start']}-{primary['case_past_end']} "
            f"overlap={primary['current_overlap_case_past']}d note={primary['note']}"
        )
        nearest_case_id = primary["case_id"]
        nearest_window = f"{primary['start']}-{primary['end']}"
        nearest_case_past = f"{primary['case_past_start']}-{primary['case_past_end']}"
        overlap = primary["current_overlap_case_past"]
    else:
        evidence_text = ""
        nearest_case_id = ""
        nearest_window = ""
        nearest_case_past = ""
        overlap = 0

    return {
        "day": day,
        "product": product,
        "source": source,
        "aspect": aspect,
        "channel": false_row["channel"],
        "significant_channels": false_row["significant_channels"],
        "category": category,
        "oracle_same_day_match": false_row["oracle_same_day_match"],
        "nearest_case_id": nearest_case_id,
        "nearest_window": nearest_window,
        "nearest_case_past": nearest_case_past,
        "current_window": f"{max(1, cur_start)}-{cur_end}",
        "case_past_overlap_days": overlap,
        "evidence": evidence_text,
        "cur_rate": false_row["cur_rate"],
        "past_rate": false_row["past_rate"],
        "delta": false_row["delta"],
    }


def write_outputs(rows: list[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    by_category = Counter(r["category"] for r in rows)
    by_oracle = Counter("oracle_match" if r["oracle_same_day_match"] == "True" else "real_only" for r in rows)
    overlap_rows = [r for r in rows if int(r["case_past_overlap_days"]) > 0]

    lines = [
        "# TRUE 없는 false 알림 분해, 2026-08-07",
        "",
        "범위: 남은 false 중 `no_true_same_product_aspect_source` 12건.",
        "",
        "## 요약",
        "",
        f"- 분석 대상: {len(rows)}",
        f"- oracle/golden에서도 같은 날 발생: {by_oracle.get('oracle_match', 0)}",
        f"- real classification에서만 발생: {by_oracle.get('real_only', 0)}",
        f"- 현재 window가 관련 config의 case-past와 겹친 row: {len(overlap_rows)}/{len(rows)}",
        f"- category별: {dict(by_category.most_common())}",
        "",
        "## 해석",
        "",
        "- `future_scored_FALSE_same_pas_case_past`: 같은 product/aspect/source/channel에 미래 configured FALSE window가 있고, 그 case-past가 현재 window와 겹친다.",
        "- `future_scoring_excluded_same_pas_case_past`: 같은 product/aspect/source/channel에 미래 채점 제외 window가 있고, 그 case-past가 현재 window와 겹친다.",
        "- `other_aspect_same_product_case_past`: 같은 상품의 다른 aspect 시나리오 case-past가 현재 window와 겹친다.",
        "- `real_classifier_only_no_config_overlap`: oracle에서는 안 뜨고 real classification에서만 뜬다.",
        "",
        "이 결과도 threshold 튜닝 근거가 아니라 mock 시나리오/time-axis 정의 감사 결과다.",
        "",
        "## 상세",
        "",
        "| Day | Product | Source | Aspect | Channel | Category | Evidence |",
        "|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['day']} | {r['product']} | {r['source']} | {r['aspect']} | {r['channel']} | "
            f"{r['category']} | {r['evidence']} |"
        )
    lines.extend([
        "",
        f"CSV: `{OUT_CSV.relative_to(ROOT)}`",
    ])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    # 첫 문장이어야 한다 — 사설 `sys.stdout.reconfigure()` 를 대체한다(stderr 미변경 ·
    # `contextlib.suppress` 부재). 사유 전문은 `app/core/console.py`.
    force_utf8_output()

    cfg = config_rows()
    false_rows = [
        r for r in read(IN_CSV)
        if r["truth_relation"] == "no_true_same_product_aspect_source"
    ]
    rows = [row_relation(r, cfg) for r in false_rows]
    write_outputs(rows)
    print(f"no-true false rows: {len(rows)}")
    print(f"csv: {OUT_CSV.relative_to(ROOT)}")
    print(f"summary: {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
