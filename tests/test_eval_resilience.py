"""평가 도구의 무응답·캐시 누락 회귀 테스트. LLM 실호출 없음."""

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "eval"))

import run_cause_eval as cause_eval
import run_pipeline_eval as pipeline_eval


def test_pipeline_to_items_excludes_missing_cache_key():
    documents = [
        {
            "id": "INQ-1",
            "text": "완료 문의",
            "source": "cs",
            "product": "P001",
            "channel": "COUPANG",
            "created_at": datetime(2026, 8, 28, tzinfo=UTC),
        },
        {
            "id": "INQ-2",
            "text": "무응답 문의",
            "source": "cs",
            "product": "P001",
            "channel": "COUPANG",
            "created_at": datetime(2026, 8, 28, tzinfo=UTC),
        },
    ]

    items = pipeline_eval._to_items(documents, {"INQ-1": []})

    assert [item.item_id for item in items] == ["INQ-1"]


def test_cause_eval_isolates_failed_batch(monkeypatch):
    rows = [
        {"cs_id": "OK", "raw_text": "정상", "aspect": "색상"},
        {"cs_id": "FAIL", "raw_text": "실패", "aspect": "소재"},
    ]

    async def fake_classify(aspect, items, *, trace_key):
        if aspect == "소재":
            raise RuntimeError("일시적 API 오류")
        return [
            {
                "cs_id": item["cs_id"],
                "cause": "기타",
                "confidence": 0.3,
                "evidence": item["raw_text"],
                "aspect_match": True,
            }
            for item in items
        ]

    monkeypatch.setattr(cause_eval, "classify_cause", fake_classify)

    predictions, failures = asyncio.run(
        cause_eval.run_batches(rows, batch_size=1, concurrency=2)
    )

    assert set(predictions) == {"OK"}
    assert len(failures) == 1
    assert failures[0]["aspect"] == "소재"
    assert failures[0]["cs_ids"] == ["FAIL"]
    assert "일시적 API 오류" in failures[0]["error"]


def test_cause_eval_main_exits_nonzero_when_a_batch_failed(monkeypatch):
    async def fake_main_async(_args):
        return 1

    monkeypatch.setattr(cause_eval, "main_async", fake_main_async)
    monkeypatch.setattr(cause_eval.sys, "argv", ["run_cause_eval.py"])

    with pytest.raises(SystemExit) as exc_info:
        cause_eval.main()

    assert exc_info.value.code == 1
