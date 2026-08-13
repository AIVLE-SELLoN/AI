"""이상화가 케이스 신호를 보존했는지 검산 — config 의도 vs 실제 vs 이상화."""
import csv
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from demo_sim import ANCHOR, require_full_real_cache
from ideal_bg_sim import case_regions, idealize

from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS
from app.detection.aggregate import count_window
from app.detection.loader import build_rows
from scripts.golden_inputs import load_golden_inputs as load_inputs


def ordinal(n: int) -> int:
    return ANCHOR.toordinal() - (60 - n)


gold_items, documents = load_inputs()
_path, _cache, real_items, _swapped, _coverage = require_full_real_cache(gold_items)
regions = case_regions()
ideal_items, kept, redrawn = idealize(
    gold_items, real_items, regions, random.Random(11)
)
print(f"보존 {kept:,} / 재추첨 {redrawn:,}\n")

with (ROOT / "data/config/config_anomaly.csv").open(encoding="utf-8-sig") as f:
    cfg = [r for r in csv.DictReader(f) if r["intended_answer"].strip().upper() == "TRUE"]

sets = {
    "골든": build_rows(documents, gold_items),
    "실제분류": build_rows(documents, real_items),
    "이상화": build_rows(documents, ideal_items),
}

print(f"{'슬롯':38s} {'라벨':10s} {'현재':>12s} {'과거':>12s} {'delta':>8s}")
print("-" * 88)
for r in cfg[:6]:
    key = (r["golden_group_id"], r["aspect"], r["channel"], r["source"])
    we = int(r["window_end_day"])
    cur_s, cur_e = ordinal(we - CURRENT_WINDOW_DAYS + 1), ordinal(we)
    past_s, past_e = ordinal(we - CURRENT_WINDOW_DAYS - PAST_WINDOW_DAYS + 1), ordinal(
        we - CURRENT_WINDOW_DAYS
    )
    slot = f"{key[0]}/{key[1]}/{key[2]}/{key[3]}"
    print(
        f"{slot:38s} {'config':10s} "
        f"{int(r['cur_neg']):5d}/{int(r['cur_total']):<6d} "
        f"{int(r['past_neg']):5d}/{int(r['past_total']):<6d} "
        f"{100 * (int(r['cur_neg']) / int(r['cur_total']) - int(r['past_neg']) / int(r['past_total'])):7.1f}p"
    )
    for label, rows in sets.items():
        ct, cn = count_window(rows, cur_s, cur_e)
        pt, pn = count_window(rows, past_s, past_e)
        c_t = ct.get((key[0], key[2], key[3]), 0)
        c_n = cn.get(key, 0)
        p_t = pt.get((key[0], key[2], key[3]), 0)
        p_n = pn.get(key, 0)
        d = (c_n / c_t if c_t else 0) - (p_n / p_t if p_t else 0)
        print(
            f"{'':38s} {label:10s} {c_n:5d}/{c_t:<6d} {p_n:5d}/{p_t:<6d} {100 * d:7.1f}p"
        )
    print()
