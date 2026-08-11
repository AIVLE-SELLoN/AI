"""실험④ 평가기의 운영체제별 데이터 경로 처리 회귀 테스트."""

from eval.run_review_eval import _dataset_split


def test_dataset_split_accepts_posix_training_path() -> None:
    assert _dataset_split("/tmp/71630/Training/labels/sample.json") == "Training"


def test_dataset_split_accepts_windows_validation_path() -> None:
    path = r"C:\data\71630\Validation\labels\sample.json"
    assert _dataset_split(path) == "Validation"


def test_dataset_split_requires_an_exact_path_segment() -> None:
    assert _dataset_split("/tmp/71630/TrainingBackup/sample.json") == "Unknown"
