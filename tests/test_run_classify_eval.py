"""담당: 현진 — eval/run_classify_eval.py의 score()·sample_rows() 테스트.

PR 리뷰(2026-08-05) 지적사항 반영: "신규 지표 3종에 테스트가 없다 — tests/가 eval/을
임포트하지 않아 1번(FPR 구조적 0건) 같은 회귀가 안 잡힌다." LLM 호출 없는 순수 함수만
테스트 대상(score()·sample_rows() 자체는 네트워크 호출이 없다 — 예측 결과를 입력으로
받아 채점만 함).
"""

from __future__ import annotations

import sys
from pathlib import Path

# eval/은 app/과 달리 패키지가 아니라 스크립트 폴더라 경로를 직접 추가해야 임포트된다.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eval.run_classify_eval import sample_rows, score


def _row(iid: str, aspect: str, sentiment: int) -> dict:
    return {"inquiry_id": iid, "raw_text": f"텍스트-{iid}", "true_aspect": aspect, "true_sentiment": sentiment}


def _pred(aspect: str, sentiment: int) -> list[dict]:
    return [{"aspect": aspect, "sentiment": sentiment}]


class TestNegativeDetectionFPR:
    """지표① — PR 리뷰 blocking 이슈 1번(오탐 구조적 0건 문제) 회귀 방지."""

    def test_degenerate_model_that_always_predicts_negative_shows_high_fpr(self):
        """모든 문의를 무조건 부정으로 찍는 최악의 모델은 FPR이 정직하게 높게 나와야 한다.

        PR 리뷰 이전 버전(--only-negative가 부정만 뽑던 시절)에서는 비부정 표본이
        구조적으로 0건이라 fp·tn이 항상 0이었고, 이런 모델도 precision 100%가
        나오는 함정이 있었다. 이 테스트가 그 함정의 재발을 막는다.
        """
        rows = [_row(f"N{i}", "색상", -1) for i in range(5)] + [_row(f"P{i}", "색상", 0) for i in range(5)]
        predictions = {r["inquiry_id"]: _pred("색상", -1) for r in rows}  # 전부 부정으로 뭉갬

        result = score(rows, predictions)
        nd = result["negative_detection"]

        assert nd["fp"] == 5, "비부정 5건을 전부 부정으로 잘못 찍었으니 fp=5여야 함"
        assert nd["tn"] == 0
        assert nd["fpr"] == 1.0, "FPR 100% — 뭉개는 모델이 정직하게 최악으로 드러나야 함"
        assert nd["precision"] == 0.5, "tp=5, fp=5 → 50%"

    def test_precision_and_f1_are_null_when_no_nonnegative_sample(self):
        """비부정 표본이 0건(fp+tn=0)이면 precision·f1은 100%가 아니라 None(측정불가)이어야 한다.

        --only-negative가 옛날처럼 부정만 뽑아온 극단적 상황을 시뮬레이션 — 이때
        "측정 안 됐다"를 "완벽하다"로 착각하면 안 된다.
        """
        rows = [_row(f"N{i}", "색상", -1) for i in range(4)]
        predictions = {r["inquiry_id"]: _pred("색상", -1) for r in rows}  # 전부 정답

        result = score(rows, predictions)
        nd = result["negative_detection"]

        assert nd["fp"] == 0 and nd["tn"] == 0, "비부정 표본이 아예 없는 상황 셋업 확인"
        assert nd["precision"] is None, "fp+tn=0이면 precision은 None이어야 함(가짜 100% 방지)"
        assert nd["f1"] is None
        assert nd["fpr"] is None
        assert nd["recall"] == 1.0, "recall은 fp+tn과 무관하니 정상 계산돼야 함"

    def test_perfect_model_has_zero_fpr_and_full_recall(self):
        """정상 모델(전부 정답)은 FPR 0%, recall 100%가 나와야 한다 — 지표 자체의 정상 동작 확인."""
        rows = [_row(f"N{i}", "색상", -1) for i in range(3)] + [_row(f"P{i}", "색상", 0) for i in range(3)]
        predictions = {}
        for r in rows:
            predictions[r["inquiry_id"]] = _pred("색상", r["true_sentiment"] if r["true_sentiment"] == -1 else 0)

        nd = score(rows, predictions)["negative_detection"]
        assert nd["fp"] == 0
        assert nd["fpr"] == 0.0
        assert nd["recall"] == 1.0
        assert nd["precision"] == 1.0


class TestNegativeScopedAspectAccuracy:
    """지표② — 부정 한정 aspect 정확도가 단일 accuracy 값으로 통합됐는지."""

    def test_accuracy_field_replaces_precision_recall_f1(self):
        """단일 aspect 출력에서는 fp==fn이 구조적으로 성립해 P=R=F1이 항상 같은 값이므로,
        PR 리뷰 반영 후 accuracy 하나로 통합됐다(§ score() 주석 참고). precision/recall/f1
        키는 이제 없어야 한다.
        """
        rows = [_row("N1", "색상", -1), _row("N2", "사이즈", -1)]
        predictions = {
            "N1": _pred("색상", -1),  # 정답
            "N2": _pred("소재", -1),  # aspect 오답(사이즈인데 소재로)
        }
        na = score(rows, predictions)["negative_scoped_aspect"]

        assert "precision" not in na and "recall" not in na and "f1" not in na
        assert na["tp"] == 1 and na["fp"] == 1 and na["fn"] == 1
        assert na["accuracy"] == 0.5

    def test_only_counts_true_negative_rows(self):
        """비부정(true_sentiment != -1) 문항은 이 지표에 전혀 안 들어가야 한다."""
        rows = [_row("N1", "색상", -1), _row("P1", "색상", 0)]
        predictions = {"N1": _pred("색상", -1), "P1": _pred("소재", 0)}  # P1은 aspect도 틀림

        na = score(rows, predictions)["negative_scoped_aspect"]
        assert na["n_true_negative"] == 1, "비부정 문항(P1)은 분모에 안 들어가야 함"
        assert na["accuracy"] == 1.0, "부정 문항(N1)만 봤을 때는 100% 정답"


class TestMultiOutputDiagnostic:
    """지표③ — 다중 aspect 출력 진단(explode 계약 근거)."""

    def test_counts_items_with_two_or_more_predicted_aspects(self):
        rows = [_row("A1", "색상", -1), _row("A2", "색상", -1), _row("A3", "색상", -1)]
        predictions = {
            "A1": [{"aspect": "색상", "sentiment": -1}, {"aspect": "사이즈", "sentiment": -1}],  # 다중
            "A2": _pred("색상", -1),  # 단일
            "A3": _pred("색상", -1),  # 단일
        }
        md = score(rows, predictions)["multi_output_diagnostic"]
        assert md["count"] == 1
        assert md["rate"] == round(1 / 3, 4)


class TestSampleRowsBalancedNegativeSampling:
    """PR 리뷰 반영 — --only-negative가 부정만이 아니라 부정N+비부정N을 균형 추출하는지."""

    def _build_rows(self, n_neg: int, n_nonneg: int) -> list[dict]:
        rows = [_row(f"N{i}", "색상", -1) for i in range(n_neg)]
        rows += [_row(f"P{i}", "색상", 0) for i in range(n_nonneg)]
        return rows

    def test_only_negative_splits_limit_roughly_in_half(self):
        rows = self._build_rows(n_neg=1000, n_nonneg=1000)
        sampled = sample_rows(rows, limit=300, seed=42, only_negative=True)

        n_neg_sampled = sum(1 for r in sampled if r["true_sentiment"] == -1)
        n_nonneg_sampled = len(sampled) - n_neg_sampled

        assert len(sampled) == 300
        assert n_neg_sampled == 150, "limit=300이면 부정 150(절반)이어야 함"
        assert n_nonneg_sampled == 150

    def test_odd_limit_gives_extra_to_negative_side(self):
        rows = self._build_rows(n_neg=1000, n_nonneg=1000)
        sampled = sample_rows(rows, limit=301, seed=42, only_negative=True)
        n_neg_sampled = sum(1 for r in sampled if r["true_sentiment"] == -1)
        assert n_neg_sampled == 151, "홀수 limit이면 부정 쪽에 1건 더 줘야 함"

    def test_limit_zero_returns_all_negative_and_all_nonnegative(self):
        """--limit 0이면 표본추출 없이 부정 전체+비부정 전체가 그대로 나와야 한다."""
        rows = self._build_rows(n_neg=7, n_nonneg=13)
        sampled = sample_rows(rows, limit=0, seed=1, only_negative=True)
        assert len(sampled) == 20
        assert sum(1 for r in sampled if r["true_sentiment"] == -1) == 7

    def test_without_only_negative_flag_behaves_as_plain_stratified_sample(self):
        """only_negative=False(기본값)면 sentiment 필터링 없이 기존 층화추출 그대로."""
        rows = self._build_rows(n_neg=100, n_nonneg=900)  # 전체 1000건, 부정 10%
        sampled = sample_rows(rows, limit=100, seed=42, only_negative=False)
        n_neg_sampled = sum(1 for r in sampled if r["true_sentiment"] == -1)
        # 균형(50)이 아니라 원본 비율(~10%)에 가까워야 함 — 느슨하게 범위로 확인
        assert 5 <= n_neg_sampled <= 20, f"층화추출이면 원본 비율(~10)에 가까워야 하는데 {n_neg_sampled}"