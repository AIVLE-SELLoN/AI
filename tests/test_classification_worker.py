"""분류 워커(`scripts/classification_worker.py`)의 실패 격리 회귀 테스트.

여기서 재는 것 하나: **분류 실패 1건이 그 건에만 국한되는가.**

이게 깨지면 조용히 깨진다 — 예외 객체가 성공 목록에 섞여 들어가고, 실패는
dead-letter 에 안 남고, persist 단계에서 배치가 통째로 터진다. 그 건은 커서만
전진한 채 어디에도 남지 않아 **영구 유실**된다(분모 합의의 전제인 커버리지가 깨진다).

`classify_aspect()` 는 계약상 **raise 하지 않고** 실패를 결과 자리에 담아 돌려준다
(길이·순서가 요청과 동일). 워커가 이걸 gather 로 한 번 더 감싸면 바깥 gather 는
예외를 볼 일이 없어 실패 판정이 영원히 False 가 된다. 아래 테스트가 그 형태를 막는다.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

# scripts/ 는 패키지가 아니라 저장소 루트의 형제 폴더다.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import classification_worker as worker

from app.classification.service import ClassifyRequestItem
from app.core.exceptions import LlmParseError
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    Sentiment,
    Source,
)

FAILING_ID = "I-2"


def _request(item_id: str) -> ClassifyRequestItem:
    return ClassifyRequestItem(
        item_id=item_id,
        source=Source.REVIEW,
        channel=Channel.NAVER,
        product_group_id="P001",
        raw_text="색상이 사진과 달라요",
        created_at=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
    )


async def _fake_classify_aspect(
    items: list[ClassifyRequestItem],
) -> list[ClassifiedItem | Exception]:
    """계약대로 동작하는 대역 — raise 하지 않고 실패를 그 자리에 담아 돌려준다."""
    outcomes: list[ClassifiedItem | Exception] = []
    for item in items:
        if item.item_id == FAILING_ID:
            outcomes.append(LlmParseError(f"ClassifiedItem 검증 실패 [item_id={item.item_id}]"))
            continue
        outcomes.append(
            ClassifiedItem(
                item_id=item.item_id,
                source=item.source,
                channel=item.channel,
                product_group_id=item.product_group_id,
                raw_text=item.raw_text,
                aspects=[AspectSentiment(aspect=Aspect.COLOR, sentiment=Sentiment.NEGATIVE)],
                created_at=item.created_at,
            )
        )
    return outcomes


@pytest.fixture
def classify_items():
    """`classify_items()` 만 쓰는 워커. DB 는 열지 않는다(__init__ 이 연결을 만들지 않는다)."""
    instance = worker.ClassificationWorker(dry_run=True)
    instance.loop = asyncio.new_event_loop()
    try:
        yield instance.classify_items
    finally:
        instance.loop.close()


def test_failed_item_is_isolated_to_dead_letter(classify_items) -> None:
    """실패 1건은 dead-letter 로 빠지고 나머지는 정상 적재된다."""
    items = [_request("I-1"), _request(FAILING_ID), _request("I-3")]

    with patch.object(worker, "classify_aspect", _fake_classify_aspect):
        results, failures = classify_items(items)

    assert [r.item_id for r in results] == ["I-1", "I-3"]
    assert [(f[0], f[1]) for f in failures] == [(FAILING_ID, "classify")]
    assert "검증 실패" in failures[0][2]


def test_success_list_never_contains_exceptions(classify_items) -> None:
    """성공 목록에는 ClassifiedItem 만 들어간다.

    ⚠️ 이 테스트가 막는 것: 워커가 `classify_aspect([item])` 를 건별로 부르고 그 바깥을
       다시 `gather(return_exceptions=True)` 로 감싸는 형태. 그러면 outcome 이
       `[LlmParseError(...)]` 라는 **리스트**로 와서 `isinstance(outcome, BaseException)`
       이 False 가 되고, 예외 객체가 그대로 results 에 섞인다. 아래 `.aspects` 접근이
       그 시점에 AttributeError 로 터진다 — persist 단계에서 나던 바로 그 오류다.
    """
    items = [_request("I-1"), _request(FAILING_ID)]

    with patch.object(worker, "classify_aspect", _fake_classify_aspect):
        results, _ = classify_items(items)

    assert all(isinstance(r, ClassifiedItem) for r in results)
    # persist 가 실제로 읽는 속성 — 예외 객체가 섞였다면 여기서 AttributeError 다
    assert results[0].aspects[0].aspect is Aspect.COLOR


def test_classify_aspect_called_once_for_whole_batch(classify_items) -> None:
    """배치 1건당 classify_aspect 호출도 1번이다 — 건별로 쪼개 부르지 않는다.

    쪼개 부르면 위 실패 판정이 무력화되고, 프롬프트가 1건씩 처리하므로 LLM 호출 수도
    줄지 않는다(격리는 classify_aspect 가 이미 해준다).
    """
    items = [_request("I-1"), _request(FAILING_ID), _request("I-3")]
    calls: list[int] = []

    async def _counting(reqs: list[ClassifyRequestItem]) -> list[ClassifiedItem | Exception]:
        calls.append(len(reqs))
        return await _fake_classify_aspect(reqs)

    with patch.object(worker, "classify_aspect", _counting):
        classify_items(items)

    assert calls == [3]


def test_empty_batch_skips_llm(classify_items) -> None:
    """빈 배치는 LLM 을 아예 부르지 않는다."""
    with patch.object(worker, "classify_aspect") as mock_classify:
        results, failures = classify_items([])

    mock_classify.assert_not_called()
    assert (results, failures) == ([], [])
