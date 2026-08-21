"""Sweep demo-time alert policies for recall vs false-alert ratio.

This is an experiment script, not production logic. It keeps the detection
statistics unchanged and compares post-detection publishing policies under the
same demo setup used by demo_sim.py.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
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

import app.detection.statistics as stats_mod
from app.batch.daily import STATE_RETENTION_DAYS, CountingClient
from app.core.console import force_utf8_output
from app.detection.service import detect_anomaly
from scripts.golden_inputs import load_golden_inputs as load_inputs


@dataclass(frozen=True)
class Policy:
    name: str
    warmup_end_day: int | None = None
    shadow_warmup: bool = False
    cs_color_consecutive_days: int = 1


POLICIES = [
    Policy("baseline"),
    Policy("warmup_skip_d29_32", warmup_end_day=32),
    Policy("warmup_shadow_d29_32", warmup_end_day=32, shadow_warmup=True),
    Policy("cs_color_2day", cs_color_consecutive_days=2),
    Policy(
        "warmup_skip_d29_32__cs_color_2day",
        warmup_end_day=32,
        cs_color_consecutive_days=2,
    ),
]

WARMUP_SWEEP = [
    Policy(f"shadow_until_d{day}", warmup_end_day=day, shadow_warmup=True)
    for day in range(29, 40)
]


def alert_key(alert) -> tuple:
    return (
        alert.product_group_id,
        alert.main_aspect.value,
        alert.channel.value,
        alert.stats.source.value,
    )


def needs_color_gate(alert) -> bool:
    return alert.stats.source.value == "cs" and alert.main_aspect.value == "색상"


async def run_policy(family: str, keyfn, policy: Policy, items, documents, truth, ignored) -> dict:
    stats_mod.decide_fires = make_decide(keyfn)
    state_alerts = []
    user_alerts = []
    suppressed_total = 0
    color_seen: dict[tuple, tuple[int, int]] = {}

    for day_n in range(29, 61):
        wend = day_date(day_n)
        cutoff = date.fromordinal(wend.toordinal() - STATE_RETENTION_DAYS)
        prior = [a for a in state_alerts if a.window_end >= cutoff]

        alerts, suppressed = await detect_anomaly(
            items,
            documents=documents,
            window_end=wend,
            prior_alerts=prior,
            resolved_alert_ids=set(),
            client=CountingClient(),
        )
        suppressed_total += len(suppressed)

        for alert in alerts:
            in_warmup = policy.warmup_end_day is not None and day_n <= policy.warmup_end_day
            if in_warmup:
                if policy.shadow_warmup:
                    state_alerts.append(alert)
                continue

            if policy.cs_color_consecutive_days > 1 and needs_color_gate(alert):
                key = alert_key(alert)
                prev_day, prev_streak = color_seen.get(key, (None, 0))
                streak = prev_streak + 1 if prev_day == day_n - 1 else 1
                color_seen[key] = (day_n, streak)
                if streak < policy.cs_color_consecutive_days:
                    continue

            user_alerts.append((day_n, alert))
            state_alerts.append(alert)

    stats_mod.decide_fires = _ORIGINAL_DECIDE

    true = echo = ignored_count = false = 0
    first_hit = {}
    for day_n, alert in user_alerts:
        kind = classify_alert(alert, truth, day_n, ignored)
        if kind == "true":
            true += 1
            first_hit.setdefault(
                (alert.product_group_id, alert.main_aspect.value, alert.stats.source.value),
                day_n,
            )
        elif kind == "echo":
            echo += 1
        elif kind == "ignored":
            ignored_count += 1
        else:
            false += 1

    published = true + echo + ignored_count + false
    scored = true + echo + false
    cases = {(p, a, s) for (p, a, _c, s) in truth}
    return {
        "family": family,
        "policy": policy.name,
        "published": published,
        "scored": scored,
        "true": true,
        "echo": echo,
        "ignored": ignored_count,
        "false": false,
        "false_ratio": false / scored if scored else 0.0,
        "alerts_per_day": published / 32,
        "cases_hit": len(first_hit),
        "cases_total": len(cases),
        "suppressed": suppressed_total,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--families", nargs="*", default=["상품별", "상품x source"])
    parser.add_argument("--policies", nargs="*", default=[p.name for p in POLICIES])
    parser.add_argument("--skip-warmup-sweep", action="store_true")
    args = parser.parse_args()

    gold_items, documents = load_inputs()
    path, cache, real_items, swapped, coverage = require_full_real_cache(gold_items)
    truth, ignored = load_truth_sets()
    print(
        f"문서 {len(documents):,} / 캐시 {len(cache):,} ({path.name})"
        f" / 실제분류로 덮음 {swapped:,} ({coverage:.1%})"
    )

    policy_by_name = {p.name: p for p in POLICIES}
    selected_policies = [policy_by_name[name] for name in args.policies]

    targets = args.families
    rows = []
    for family in targets:
        keyfn = FAMILIES[family]
        for policy in selected_policies:
            result = await run_policy(family, keyfn, policy, real_items, documents, truth, ignored)
            rows.append(result)
            print(f"  ...{family} / {policy.name} done")

    print("\n" + "=" * 112)
    print("데모 정책 튜닝 스윕 — real classification, 32일, detect_anomaly 기반")
    print("=" * 112)
    print(
        f"{'family':14s} {'policy':34s} {'발행':>6s} {'채점':>6s} {'참':>5s} "
        f"{'ignored':>7s} {'헛':>5s} {'헛비율':>8s} {'알림/일':>8s} "
        f"{'케이스도달':>10s} {'억제':>6s}"
    )
    print("-" * 112)
    for row in rows:
        print(
            f"{row['family']:14s} {row['policy']:34s} "
            f"{row['published']:5d} {row['scored']:5d} {row['true']:5d} "
            f"{row['ignored']:7d} {row['false']:5d} "
            f"{row['false_ratio']:7.1%} {row['alerts_per_day']:7.2f} "
            f"{row['cases_hit']:4d}/{row['cases_total']:<5d} {row['suppressed']:5d}"
        )

    print("\n주의: warmup/shadow/color consecutive는 실험용 발행 정책이다.")
    print("통계 검정 임계값(BH_FDR_Q, MIN_DELTA)은 변경하지 않았다.")

    if args.skip_warmup_sweep:
        return

    print("\n" + "=" * 112)
    print("상품x source shadow warmup 기간 스윕")
    print("=" * 112)
    print(
        f"{'policy':24s} {'발행':>6s} {'채점':>6s} {'참':>5s} "
        f"{'ignored':>7s} {'헛':>5s} {'헛비율':>8s} {'알림/일':>8s} {'케이스도달':>10s}"
    )
    print("-" * 112)
    family = "상품x source"
    keyfn = FAMILIES[family]
    for policy in WARMUP_SWEEP:
        row = await run_policy(family, keyfn, policy, real_items, documents, truth, ignored)
        print(
            f"{policy.name:24s} {row['published']:5d} {row['scored']:5d} {row['true']:5d} "
            f"{row['ignored']:7d} {row['false']:5d} "
            f"{row['false_ratio']:7.1%} {row['alerts_per_day']:7.2f} "
            f"{row['cases_hit']:4d}/{row['cases_total']:<5d}"
        )


if __name__ == "__main__":
    # 첫 문장이어야 한다. `async def main()` 이라 `main()` 안이 아니라 **여기**다 — 가드가
    # `AsyncFunctionDef` 를 못 찾아 `__main__` 블록을 진입 지점으로 삼는다. `--help` 는
    # `main()` 안에서 파싱하므로 이 호출이 그보다 앞선다.
    force_utf8_output()

    asyncio.run(main())
