"""데모 재현 — `detect_anomaly()` 를 하루씩 32일 이어 돌린다. LLM 0회.

앞선 daily_exp2.py 는 BH 를 직접 돌려 **슬롯×날짜**를 셌다. 데모에서 셀러가 실제로
받는 건 그게 아니라 `combine_sources` 를 거쳐 발행된 **DetectionAlert** 이고,
`prior_alerts` 억제(RENOTIFY_BLOCK_DAYS=7)로 같은 조합의 연속 알림이 합쳐진다.

그래서 여기서는 운영 진입점(app.batch.daily.run_batch)이 하는 것과 같은 순서로 간다.

    load_inputs() → 실제분류 캐시로 덮기
    for day in 29..60:
        prior  = 최근 STATE_RETENTION_DAYS 안에 **발행된** 알림
        alerts, suppressed = await detect_anomaly(..., prior_alerts=prior)
        발행분만 prior 에 누적          ← save_published 규칙과 동일

⚠️ [6] 원인분류는 detect_anomaly 안에서 LLM 을 부른다. daily.py 의 CountingClient
   스텁을 주입해 과금 0 을 보장한다(daily.py:357 과 같은 이유).

family 변형은 statistics.decide_fires 를 교체해서 넣는다 — run_detection 이 모듈
전역으로 조회하므로 이 자리만 바꾸면 [2-B] 전체가 바뀐다. q·MIN_DELTA 는 불변.
"""
import asyncio
import csv
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))

from statsmodels.stats.multitest import multipletests

import app.detection.statistics as stats_mod
from app.batch.daily import STATE_RETENTION_DAYS, CountingClient
from app.core.constants import BH_FDR_Q, CURRENT_WINDOW_DAYS
from app.core.schemas import AspectSentiment
from app.detection.service import detect_anomaly
from scripts.golden_inputs import load_golden_inputs as load_inputs

ANCHOR = date(2026, 8, 28)  # day 60
CACHE = (
    ROOT / "data/eval_cache/pipeline_full_batch_classify_aspect_v5-15290041_run1.json"
)

FAMILIES = {
    "현재(전체)": None,
    "상품별": lambda k: k[0],
    "상품x source": lambda k: (k[0], k[3]),
}

_ORIGINAL_DECIDE = stats_mod.decide_fires


def make_decide(keyfn):
    """decide_fires 를 family 별 BH 로 교체. keyfn=None 이면 원본 그대로."""
    if keyfn is None:
        return _ORIGINAL_DECIDE

    def decide(batch, q=BH_FDR_Q):
        if not batch:
            return batch
        groups = defaultdict(list)
        for t in batch:
            groups[keyfn(t["key"])].append(t)
        for g in groups.values():
            rej, _, _, _ = multipletests(
                [t["p_value"] for t in g], alpha=q, method="fdr_bh"
            )
            for t, sig in zip(g, rej):
                t["bh_significant"] = bool(sig)
                t["fired"] = t["bh_significant"] and t["meaningful"]
        return batch

    return decide


def day_date(n: int) -> date:
    return ANCHOR - timedelta(days=60 - n)


def load_truth() -> dict:
    truth, _ignored = load_truth_sets()
    return truth


def load_truth_sets() -> tuple[dict, dict]:
    with (ROOT / "data/config/config_anomaly.csv").open(encoding="utf-8-sig") as f:
        cfg = list(csv.DictReader(f))
    truth = {}
    ignored = {}
    for r in cfg:
        key = (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
        span = (
            int(r["window_start_day"]),
            int(r["window_end_day"]),
        )
        if r["intended_answer"].strip().upper() == "TRUE":
            truth[key] = span
        elif r["scoring_included"].strip().upper() == "N" or not r["intended_answer"].strip():
            ignored[key] = span
    return truth, ignored


def swap_real(items, cache):
    out, n = [], 0
    for it in items:
        preds = cache.get(it.item_id)
        if preds is None:
            out.append(it)
            continue
        n += 1
        out.append(
            it.model_copy(
                update={
                    "aspects": [
                        AspectSentiment(aspect=p["aspect"], sentiment=p["sentiment"])
                        for p in preds
                    ]
                }
            )
        )
    return out, n


def classify_alert(alert, truth, day_n: int, ignored: dict | None = None) -> str:
    """알림을 참 / 여진 / 채점제외 / 헛알림 으로 가른다.

    발행 알림은 (상품, 채널, main_aspect, stats.source) 로 식별한다. 전역형은 채널이
    여러 개라 significant_channels 중 하나라도 맞으면 그 슬롯으로 본다.

    ⚠️ **여진을 헛알림과 섞으면 안 된다.** 케이스 기간이 [ws, we] 여도 그 데이터는
       현재 윈도우(7일)에 we+6 까지 남아 있다. we+1~we+6 에 뜬 알림은 진짜 이상을
       보고 있는 것이지 가짜가 아니다. 다만 '늦은 알림'이라 참으로도 안 센다.
    """
    source = alert.stats.source
    aspect = alert.main_aspect.value
    channels = {c.value for c in alert.significant_channels} or {alert.channel.value}
    tail = CURRENT_WINDOW_DAYS - 1
    for ch in channels:
        span = truth.get((alert.product_group_id, aspect, ch, source))
        if not span:
            continue
        if span[0] <= day_n <= span[1]:
            return "true"
        if span[1] < day_n <= span[1] + tail:
            return "echo"
    if ignored:
        for ch in channels:
            span = ignored.get((alert.product_group_id, aspect, ch, source))
            if not span:
                continue
            if span[0] <= day_n <= span[1] + tail:
                return "ignored"
    return "false"


async def run_family(name, keyfn, items, documents, truth, ignored, label) -> dict:
    stats_mod.decide_fires = make_decide(keyfn)
    published: list = []
    n_true = n_echo = n_ignored = n_false = n_suppressed = 0
    first_hit: dict = {}

    for n in range(29, 61):
        wend = day_date(n)
        cutoff = date.fromordinal(wend.toordinal() - STATE_RETENTION_DAYS)
        prior = [a for a in published if a.window_end >= cutoff]

        alerts, suppressed = await detect_anomaly(
            items,
            documents=documents,
            window_end=wend,
            prior_alerts=prior,
            resolved_alert_ids=set(),
            client=CountingClient(),
        )
        n_suppressed += len(suppressed)
        for a in alerts:
            kind = classify_alert(a, truth, n, ignored)
            if kind == "true":
                n_true += 1
                first_hit.setdefault(
                    (a.product_group_id, a.main_aspect.value, a.stats.source), n
                )
            elif kind == "echo":
                n_echo += 1
            elif kind == "ignored":
                n_ignored += 1
            else:
                n_false += 1
        published.extend(alerts)

    stats_mod.decide_fires = _ORIGINAL_DECIDE
    total = n_true + n_echo + n_ignored + n_false
    scored_total = n_true + n_echo + n_false
    cases = {(p, a, s) for (p, a, _c, s) in truth}
    return {
        "family": name,
        "label": label,
        "published": total,
        "scored_published": scored_total,
        "true": n_true,
        "echo": n_echo,
        "ignored": n_ignored,
        "false": n_false,
        "fa_ratio": n_false / scored_total if scored_total else 0.0,
        "per_day": total / 32,
        "suppressed": n_suppressed,
        "cases_hit": len(first_hit),
        "cases_total": len(cases),
    }


async def main() -> None:
    items, documents = load_inputs()
    cache = json.loads(CACHE.read_text(encoding="utf-8"))
    truth, ignored = load_truth_sets()
    real_items, swapped = swap_real(items, cache)
    print(f"문서 {len(documents):,} / 캐시 {len(cache):,} / 실제분류로 덮음 {swapped:,}")

    rows = []
    for name, keyfn in FAMILIES.items():
        for label, its in (("oracle", items), ("real", real_items)):
            r = await run_family(name, keyfn, its, documents, truth, ignored, label)
            rows.append(r)
            print(f"  ...{name}/{label} done  published={r['published']}")

    print("\n" + "=" * 100)
    print("데모 재현 — detect_anomaly 32일 연속 (억제 + combine_sources 포함)")
    print("발행 1건 = 셀러가 실제로 받는 알림 1개")
    print("=" * 100)
    head = (
        f"{'family':14s} {'label':8s} {'발행':>6s} {'채점':>6s} {'참':>6s} {'여진':>6s} "
        f"{'제외':>6s} {'헛알림':>7s} {'헛알림비율':>10s} {'알림/일':>8s} {'억제':>7s} {'케이스도달':>10s}"
    )
    print(head)
    print("-" * 100)
    for r in rows:
        print(
            f"{r['family']:14s} {r['label']:8s} {r['published']:5d}건 "
            f"{r['scored_published']:5d}건 {r['true']:5d}건 {r['echo']:5d}건 "
            f"{r['ignored']:5d}건 {r['false']:6d}건 "
            f"{r['fa_ratio']:9.1%} {r['per_day']:7.2f}건 "
            f"{r['suppressed']:6d}건 {r['cases_hit']:4d}/{r['cases_total']:<5d}"
        )
    print()
    print("  참     = 케이스 기간 [ws, we] 안에 뜬 알림")
    print("  여진   = we+1~we+6. 데이터가 아직 현재 윈도우에 남아 진짜 이상을 보고")
    print("           있는 것 — 가짜는 아니나 '늦은 알림'이라 참으로도 안 센다")
    print("  제외   = scoring_included=N 또는 intended_answer 빈칸인 설계 제외 구간")
    print("  헛알림 = 그 외 전부. 헛알림비율은 제외를 분자/분모에서 뺀 채점 기준이다")


if __name__ == "__main__":
    asyncio.run(main())
