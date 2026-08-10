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

from eval.run_classify_eval import (
    compute_leak_map,
    operational_negative_rate,
    parse_few_shot_examples,
    sample_rows,
    score,
    tag_leaked_rows,
)


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

    def test_precision_operational_reproduces_jiin_example(self):
        """운영비율 환산 precision — 지인님 PR리뷰 예시(recall95%·FPR5%→~61%) 재현
        (Notion A안, 2026-08-06 반영). 균형표본(50:50) precision은 95%로 높게 나오지만,
        실제 운영 부정비율(7.4%)로 환산하면 60%대로 뚝 떨어진다는 게 이 지표의 핵심.

        p를 인자로 **명시해서** 넘긴다(2026-08-10). 예전엔 score() 안에 0.074가 박혀
        있어서 이 테스트가 골든 파일 상태에 묶여 있었다 — 골든을 재생성하면 실제 p가
        움직이는데 이 기대값은 안 움직여서, 공식이 맞는지 데이터가 맞는지 못 가린다.
        여기서 검증할 것은 **베이즈 공식**뿐이므로 p를 고정한다.
        """
        rows = [_row(f"N{i}", "색상", -1) for i in range(20)] + [_row(f"P{i}", "색상", 0) for i in range(20)]
        predictions = {}
        for i in range(19):
            predictions[f"N{i}"] = _pred("색상", -1)  # 부정 19/20 정답 → recall 95%
        predictions["N19"] = _pred("색상", 0)
        predictions["P0"] = _pred("색상", -1)  # 비부정 1/20 오탐 → FPR 5%
        for i in range(1, 20):
            predictions[f"P{i}"] = _pred("색상", 0)

        nd = score(rows, predictions, operational_rate=0.074)["negative_detection"]

        assert nd["recall"] == 0.95
        assert nd["fpr"] == 0.05
        assert nd["precision"] == 0.95, "표본기준 precision은 균형표본이라 95%로 높게 나옴"
        assert abs(nd["precision_operational"] - 0.603) < 0.005, (
            f"운영환산 precision은 60%대여야 하는데 {nd['precision_operational']}"
        )
        assert nd["precision_operational_p"] == 0.074, "어느 p로 환산했는지 결과에 남아야 함"
        assert nd["precision"] != nd["precision_operational"], "표본기준과 운영환산은 반드시 달라야 함(그게 이 지표의 존재 이유)"

    def test_precision_operational_is_null_without_p(self):
        """p를 안 넘기면 환산값은 None이다 — 추정해서 채우면 안 된다 (2026-08-10).

        예전엔 0.074가 박혀 있어서, 골든이 재생성돼 실제 부정비율이 3배 넘게 움직인
        뒤에도 옛 p로 계산한 숫자가 아무 경고 없이 나왔다. 못 재는 건 None으로 낸다.
        """
        rows = [_row(f"N{i}", "색상", -1) for i in range(2)] + [_row(f"P{i}", "색상", 0) for i in range(2)]
        predictions = {r["inquiry_id"]: _pred("색상", r["true_sentiment"]) for r in rows}

        nd = score(rows, predictions)["negative_detection"]
        assert nd["fpr"] is not None, "FPR은 계산돼야 한다 — None인 건 p 쪽 사유여야 함"
        assert nd["precision_operational"] is None
        assert nd["precision_operational_p"] is None

    def test_operational_rate_counts_population_not_sample(self):
        """p는 골든 전량에서 센다 — 층화 표본의 비율이 아니다.

        표본은 aspect 층화(또는 --only-negative)라 부정비율이 설계상 왜곡돼 있다.
        표본으로 재면 환산 precision 이 표본 precision 과 같아져 지표가 무의미해진다.
        """
        population = [_row(f"N{i}", "색상", -1) for i in range(10)] + [
            _row(f"P{i}", "색상", 0) for i in range(90)
        ]
        assert operational_negative_rate(population) == 0.10

        balanced_sample = population[:10] + population[10:20]  # 50:50 으로 뽑힌 표본
        assert operational_negative_rate(balanced_sample) == 0.50, (
            "표본으로 세면 0.5 — 그래서 호출자가 sample_rows() 이전 행을 넘겨야 한다"
        )
        assert operational_negative_rate([]) is None

    def test_precision_operational_is_null_when_fpr_is_null(self):
        """fpr이 None이면(비부정 표본 0건) precision_operational도 연쇄적으로 None이어야 한다
        — 베이즈 환산 자체가 FPR값을 입력으로 쓰니, FPR을 모르면 환산도 불가능."""
        rows = [_row(f"N{i}", "색상", -1) for i in range(4)]
        predictions = {r["inquiry_id"]: _pred("색상", -1) for r in rows}

        nd = score(rows, predictions)["negative_detection"]
        assert nd["fpr"] is None
        assert nd["precision_operational"] is None, "fpr이 None인데 환산값만 계산되면 안 됨"

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


class TestFewShotLeakFilter:
    """§6 B안(2026-08-06, 지인님 리뷰) — few-shot 유출 방어. PR #30 전량 실행에서
    오탐 65건이 전부 예시20 템플릿 하나였던 것(4,058건 완전일치)이 계기.
    """

    def test_parse_few_shot_examples_finds_all_v5_inputs(self):
        """v5의 '입력:' 문장이 전부(46개) 파싱돼야 한다 — 하드코딩이 아니라 실제 파일 파싱
        확인용(§6 B안 1번: '예시가 늘어도 자동 반영').

        40 → 46: v5 3차 수정(2026-08-09)에서 감성 정책 예시 6개 추가.
          20-6  배송 미도착 주장 → -1 (일정만 문의는 0을 같은 예시에서 대비)
          22-1  "문제 없음" + 명시적 칭찬 → 1
          22-1b 완만한 칭찬도 긍정(리뷰 문체)
          22-2  가정형 문의 → 0
          22-3  포장 결함 관측 + 가정형 → -1 (22-2와 대비)
          24-1  담백한 오배송 서술 → -1
        """
        texts = parse_few_shot_examples("classify_aspect_v5")
        assert len(texts) == 46
        assert "배송 조회가 안 되는데 확인 부탁드려요." in texts

    def test_similarity_reproduces_notion_reported_numbers(self):
        """difflib.SequenceMatcher가 지인님이 노션에 기록한 예시20(1.00)·25(0.88)·
        20-2(0.79) 유사도를 정확히 재현하는지 — 알고리즘 검증(2026-08-06)."""
        few_shot = parse_few_shot_examples("classify_aspect_v5")

        exact_match_text = "배송 조회가 안 되는데 확인 부탁드려요."
        rows = [{"inquiry_id": "X", "raw_text": exact_match_text}]
        leak_map = compute_leak_map(rows, few_shot, threshold=0.0)  # 임계 0 = 전부 유사도만 확인
        assert leak_map[exact_match_text]["max_similarity"] == 1.0

    def test_exact_match_is_flagged_leaked(self):
        rows = [
            {"inquiry_id": "A", "raw_text": "배송 조회가 안 되는데 확인 부탁드려요.",
             "true_aspect": "기타", "true_sentiment": 0},
        ]
        few_shot = parse_few_shot_examples("classify_aspect_v5")
        leak_map = compute_leak_map(rows, few_shot, threshold=0.75)
        tag_leaked_rows(rows, leak_map)
        assert rows[0]["is_leaked"] is True

    def test_unrelated_text_not_flagged(self):
        rows = [
            {"inquiry_id": "A", "raw_text": "이 문장은 few-shot 예시 어디와도 안 겹치는 완전히 새로운 내용입니다",
             "true_aspect": "색상", "true_sentiment": -1},
        ]
        few_shot = parse_few_shot_examples("classify_aspect_v5")
        leak_map = compute_leak_map(rows, few_shot, threshold=0.75)
        tag_leaked_rows(rows, leak_map)
        assert rows[0]["is_leaked"] is False

    def test_zero_threshold_means_check_disabled_at_cli_level(self):
        """--leak-threshold 0이면 main_async()가 few_shot_texts를 아예 안 채워서(빈 리스트)
        검사가 완전히 꺼진다 — compute_leak_map 자체에 threshold=0을 넘기면 오히려 거의
        전부 유출로 잡히므로(SequenceMatcher.ratio()>=0이 사실상 항상 참), CLI 레벨에서
        분기해야 한다는 걸 이 테스트로 고정한다(구현 중 실제로 이 버그를 잡았음, 2026-08-06).
        """
        rows = [{"inquiry_id": "A", "raw_text": "아무 문장"}]
        # "검사 꺼짐" 상태 = few_shot_texts가 빈 리스트로 넘어온 상황을 그대로 재현
        leak_map = compute_leak_map(rows, few_shot_texts=[], threshold=0.75)
        assert leak_map == {}, "few_shot_texts가 비었으면 leak_map도 빈 딕셔너리여야 함(검사 꺼짐)"

    def test_leak_filter_recomputes_metrics_excluding_leaked_rows(self):
        """score()의 leak_filter가 유출 제외 후 aspect_f1·sentiment_accuracy를 실제로
        재계산하는지 — LLM 재호출 없이 predictions만으로(§6 B안: '검증에 LLM 재실행 불필요')."""
        few_shot = parse_few_shot_examples("classify_aspect_v5")
        rows = [
            {"inquiry_id": "A", "raw_text": "배송 조회가 안 되는데 확인 부탁드려요.",
             "true_aspect": "기타", "true_sentiment": 0},  # 유출, 아래서 오답으로 만듦
            {"inquiry_id": "B", "raw_text": "완전히 새로운 문장 하나 더 추가해봄",
             "true_aspect": "색상", "true_sentiment": -1},  # 정답
        ]
        leak_map = compute_leak_map(rows, few_shot, threshold=0.75)
        tag_leaked_rows(rows, leak_map)
        predictions = {
            "A": [{"aspect": "기타", "sentiment": -1}],  # 오답(뒤집힘 재현)
            "B": [{"aspect": "색상", "sentiment": -1}],  # 정답
        }
        result = score(rows, predictions)
        lf = result["leak_filter"]

        assert lf["applied"] is True
        assert lf["n_excluded_rows_in_sample"] == 1
        assert lf["leak_excluded_n_scored"] == 1
        # A(오답) 빠지고 B(정답)만 남으므로 유출제외 sentiment_accuracy는 100%여야 함
        assert lf["leak_excluded_sentiment_accuracy"] == 1.0
        # 전체(A+B) 기준은 A가 오답이라 100% 미만이어야 함
        assert result["sentiment_accuracy"] < 1.0

    def test_no_leak_map_computed_returns_not_applied(self):
        """tag_leaked_rows()를 호출 안 하면(is_leaked 키가 없으면) leak_filter가
        하위호환으로 조용히 'applied: False'를 반환해야 한다 — 기존 호출부 안 깨짐."""
        rows = [{"inquiry_id": "A", "raw_text": "x", "true_aspect": "색상", "true_sentiment": -1}]
        predictions = {"A": [{"aspect": "색상", "sentiment": -1}]}
        result = score(rows, predictions)
        assert result["leak_filter"]["applied"] is False

    def test_zero_leaked_rows_in_sample_reports_cleanly(self):
        """표본에 유출 문항이 하나도 없으면(우연히) applied=True, excluded=0으로
        깔끔하게 나와야 한다(에러 없이)."""
        few_shot = parse_few_shot_examples("classify_aspect_v5")
        rows = [{"inquiry_id": "A", "raw_text": "완전히 안 겹치는 새 문장입니다 정말로",
                 "true_aspect": "색상", "true_sentiment": -1}]
        leak_map = compute_leak_map(rows, few_shot, threshold=0.75)
        tag_leaked_rows(rows, leak_map)
        predictions = {"A": [{"aspect": "색상", "sentiment": -1}]}
        result = score(rows, predictions)
        assert result["leak_filter"]["applied"] is True
        assert result["leak_filter"]["n_excluded_rows_in_sample"] == 0