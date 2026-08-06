"""실험② 캐시를 일별 슬라이딩용으로 **이틀 앞까지** 넓힌다.

지금 캐시는 케이스마다 [we-6, we] 7일치다(we = window_end_day). 일별 배치에서
연속 2일 발화를 보려면 전날 윈도우 [we-7, we-1] 이 필요하고, 연속 3일이면 [we-8, we-2]
까지 필요하다. 새로 드는 것은 we-7, we-8 **이틀분뿐**이다.

같은 캐시 파일에 덧붙인다 — 키가 늘어도 실험②는 자기 7일치 id 만 조회하므로 영향 없다.
"""
import asyncio
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "eval"))

from run_pipeline_eval import (
    CONFIG_ANOMALY,
    DAY1,
    INPUT_INQUIRIES,
    SOURCE_CS,
    _product_of,
    classify_cached,
    read,
)

from app.core.constants import CURRENT_WINDOW_DAYS

EXTRA_DAYS = 2  # 연속 3일까지 보려면 2일 앞까지


def collect_wide() -> list[dict]:
    """케이스 상품의 [we-6-EXTRA, we] 구간 CS 문의."""
    windows: dict[str, tuple] = {}
    for r in read(CONFIG_ANOMALY):
        if r["source"] != SOURCE_CS:
            continue
        end = DAY1 + timedelta(days=int(r["window_end_day"]) - 1)
        windows[r["golden_group_id"]] = (
            end - timedelta(days=CURRENT_WINDOW_DAYS - 1 + EXTRA_DAYS),
            end,
        )

    product_of = _product_of()
    out: list[dict] = []
    for r in read(INPUT_INQUIRIES):
        product = product_of.get((r["channel"], r["channel_product_id"]))
        if product not in windows:
            continue
        from datetime import datetime

        created = datetime.fromisoformat(r["inquired_at"])
        start, end = windows[product]
        if not (start <= created.date() <= end):
            continue
        out.append(
            {
                "id": r["inquiry_id"],
                "product": product,
                "channel": r["channel"],
                "source": SOURCE_CS,
                "created_at": created,
                "text": r["content"],
            }
        )
    return out


async def main() -> None:
    docs = collect_wide()
    print(f"넓힌 구간 CS 문의 {len(docs):,}건 (기존 7일치 + {EXTRA_DAYS}일)")
    await classify_cached(docs, run=1, tag="full", mode="batch", concurrency=12)
    print("완료 — 같은 캐시 파일에 덧붙임")


if __name__ == "__main__":
    asyncio.run(main())
