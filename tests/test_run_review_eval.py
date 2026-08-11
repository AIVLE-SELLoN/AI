"""실험④ 평가기의 경로·재시도·스크럽 회귀 테스트."""

import asyncio
import json

import eval.run_review_eval as review_eval
from eval.run_review_eval import _dataset_split


def test_dataset_split_accepts_posix_training_path() -> None:
    assert _dataset_split("/tmp/71630/Training/labels/sample.json") == "Training"


def test_dataset_split_accepts_windows_validation_path() -> None:
    path = r"C:\data\71630\Validation\labels\sample.json"
    assert _dataset_split(path) == "Validation"


def test_dataset_split_requires_an_exact_path_segment() -> None:
    assert _dataset_split("/tmp/71630/TrainingBackup/sample.json") == "Unknown"


def test_target_map_matches_prompt_scope() -> None:
    assert review_eval.TARGET_MAP["신축성"] == "소재"
    assert review_eval.TARGET_MAP["마감"] == "소재"
    assert review_eval.TARGET_MAP["길이"] == "사이즈"


def test_run_with_retries_retries_only_missing_rows(monkeypatch) -> None:
    rows = [
        {"review_id": "R1", "raw_text": "a", "gold": {}},
        {"review_id": "R2", "raw_text": "b", "gold": {}},
    ]
    requested: list[list[str]] = []

    async def fake_runner(part, chunk_size, concurrency, *, trace_prefix=""):
        ids = [row["review_id"] for row in part]
        requested.append(ids)
        if len(requested) == 1:
            return {"R1": []}, ["R2"]
        return {"R2": []}, []

    monkeypatch.setattr(review_eval, "run_batch_chunks", fake_runner)
    predictions, failed, attempts = asyncio.run(
        review_eval.run_with_retries(rows, 20, 4, "batch", run_number=1, retries=2)
    )

    assert requested == [["R1", "R2"], ["R2"]]
    assert list(predictions) == ["R1", "R2"]
    assert failed == []
    assert [a["remaining"] for a in attempts] == [1, 0]


def test_score_scrubs_raw_text_but_keeps_full_prediction_audit() -> None:
    rows = [
        {
            "review_id": "R1",
            "raw_text": "공개 저장소에 들어가면 안 되는 원문",
            "gold": {"소재": {"sentiment": -1, "mixed_signal": False}},
        }
    ]
    predictions = {"R1": [{"aspect": "사이즈", "sentiment": -1, "mixed_signal": False}]}

    scores = review_eval.score(rows, predictions)

    assert "raw_text" not in json.dumps(scores, ensure_ascii=False)
    assert scores["aspect_counts"] == {"tp": 0, "fp": 1, "fn": 1}
    assert scores["mismatches"][0]["review_id"] == "R1"


def test_summary_reports_mean_range_and_unanswered() -> None:
    def run(value: float, unanswered: int) -> dict:
        scores = {key: value for key in review_eval.SUMMARY_METRICS}
        scores["n_unanswered"] = unanswered
        return {"scores": scores}

    summary = review_eval.summarize_runs([run(0.8, 0), run(0.9, 0), run(0.7, 0)])

    assert summary["metrics"]["aspect_f1"] == {
        "mean": 0.8,
        "min": 0.7,
        "max": 0.9,
        "half_range": 0.1,
        "values": [0.8, 0.9, 0.7],
    }
    assert summary["all_runs_zero_unanswered"] is True


def test_paired_summary_isolates_target_map_delta() -> None:
    def scores(value: float) -> dict:
        result = {key: value for key in review_eval.SUMMARY_METRICS}
        result["n_unanswered"] = 0
        return result

    runs = [
        {"scores": scores(0.87), "legacy_scores": scores(0.84)},
        {"scores": scores(0.88), "legacy_scores": scores(0.85)},
    ]

    comparison = review_eval.summarize_paired_runs(runs)

    assert comparison is not None
    assert comparison["legacy_summary"]["metrics"]["aspect_f1"]["mean"] == 0.845
    assert comparison["delta_current_minus_legacy"]["aspect_f1"] == {
        "mean": 0.03,
        "min": 0.03,
        "max": 0.03,
        "values": [0.03, 0.03],
    }
