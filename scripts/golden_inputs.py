"""골든 CSV → 탐지 배치 입력. **평가·재현 전용. `app/` 에서 import 하지 말 것.**

왜 `app/batch/daily.py` 밖에 있나
---------------------------------
`eval/README.md` §232 가 **"`data/golden/` 은 `eval/` 만 읽는다. `app/` 코드가 import
하면 컨닝이다"** 라고 정했다. daily.py 가 운영 진입점이면서 골든 라벨을 읽고 있어서
그 원칙을 어기고 있었고, 2026-08-06 에 이리로 옮겼다.

무엇을 주는가
-------------
`(items, documents)` — `detect_anomaly()` 가 먹는 형태 그대로다.

    items:     분자의 출처 (ClassifiedItem). **골든 라벨이 그대로 들어간다 = 분류 오차 0.**
    documents: 분모의 출처 (원본 문서). 리뷰는 aspect 0개면 classified_item 에 행이
               아예 없어서, items 로 분모를 세면 그 문서가 통째로 빠진다.

🔻 **이걸로 낸 숫자는 "탐지 성능"이 아니다.** 분류가 100% 정확하다고 가정한 값이라
   `eval/README.md` §68 이 실험① 에 붙인 경고가 그대로 적용된다. 실제 분류를 섞으려면
   `data/eval_cache/pipeline_*.json` 으로 덮어써야 한다
   (`scripts/detection_experiments/demo_sim.py` 의 `swap_real()` 참고).

운영에서는
----------
이 파일을 쓰지 않는다. 워커가 분류해 `classified_item` 에 넣은 것을 읽는 로더가
`app/batch/daily.py` 안에 생기고, 그게 기본값이 된다.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from app.core.schemas import AspectSentiment, ClassifiedItem

ROOT = Path(__file__).resolve().parent.parent


def _read_csv(rel: str) -> list[dict]:
    with (ROOT / rel).open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_golden_inputs(
    window_end: date | None = None,
) -> tuple[list[ClassifiedItem], list[dict]]:
    """골든 라벨(oracle)로 (items, documents) 를 만든다. LLM 0회.

    Args:
        window_end: 운영 로더와 시그니처를 맞추기 위한 인자. 골든은 60일치가
            전부라 무시한다 — DB 로더는 이 값으로 35일 범위조회를 건다.

    Returns:
        items:     분자의 출처 (ClassifiedItem)
        documents: **분모의 출처** (원본 문서)
    """
    variant_group = {
        r["variant_row_id"]: r["golden_group_id"]
        for r in _read_csv("data/golden/golden_mapping.csv")
    }
    product_of: dict[tuple[str, str], str] = {}
    for r in _read_csv("data/input/input_channel_products.csv"):
        group = variant_group.get(r["variant_row_id"])
        if group:
            product_of.setdefault((r["channel"], r["channel_product_id"]), group)

    items: list[ClassifiedItem] = []
    documents: list[dict] = []

    # ⚠️ **CS 와 리뷰를 둘 다 읽어야 한다.** 한쪽만 넣으면 [8] 종합이 성립하지 않아
    #    source_signals 한쪽이 영원히 null 이 되고, BH family 도 절반으로 줄어
    #    컷오프가 달라진다(실측: CS 만 756검정 / 둘 다 1,464검정).
    sources = [
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

    for source, input_csv, golden_csv, id_key, time_key in sources:
        labels = {r[id_key]: r for r in _read_csv(golden_csv)}
        for r in _read_csv(input_csv):
            product = product_of.get((r["channel"], r["channel_product_id"]))
            if product is None:
                continue

            label = labels.get(r[id_key], {})
            aspects = []
            if label.get("true_aspect"):
                aspects.append(
                    AspectSentiment(
                        aspect=label["true_aspect"],
                        sentiment=int(label["true_sentiment"]),
                    )
                )
            items.append(
                ClassifiedItem(
                    item_id=r[id_key],
                    source=source,
                    channel=r["channel"],
                    product_group_id=product,
                    raw_text=r["content"],
                    aspects=aspects,
                    created_at=r[time_key],
                )
            )
            documents.append(
                {
                    "id": r[id_key],
                    "product": product,
                    "channel": r["channel"],
                    "source": source,
                    "created_at": r[time_key],
                    "text": r["content"],
                }
            )
    return items, documents
