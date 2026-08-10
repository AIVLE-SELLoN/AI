"""배경 baseline 이 **전체 문의 분모**로 생성되는지 고정한다.

이 테스트가 지키는 것은 성능이 아니라 **정합성**이다. 확정 스펙 세 곳이 분모를
(상품,채널,source) 총문의(aspect 무관)로 못박고 있다.

    1. 「이상탐지 시나리오 정의서[확정]」 §1 — "(상품,채널) 총문의(= 분모, aspect 무관)"
    2. data/config/config_anomaly.csv — SC-001 past_neg=40 / past_total=800 (=28일 x 28건)
    3. app/detection/aggregate.py — 탐지 분모도 aspect 무관

2026-08-09 이전 배경 경로는 볼륨을 aspect 수로 먼저 나눈 뒤 그 안에서 BASELINE_RATE 를
적용해서, 전체 분모로 보면 CS 1/6·리뷰 1/3 로 희석됐다. 케이스 경로는 past_neg/past_total
로 전체 분모에 정확 건수를 심고 있었으므로, 한 생성기 안에 규약이 두 개인 상태였다.

⚠️ **이 테스트를 관측률 임계로 느슨하게 풀지 말 것.** 희석은 6배짜리라 웬만한 허용오차로는
   안 잡힌다. 여기서는 확률 자체(`_negative_rate`)를 직접 고정하고, 표본 검증은 넉넉한
   허용오차로 따로 둔다 — 시드 고정이라 재현되지만 이항 변동은 남기 때문이다.
"""

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from generate_cs_review_data import (
    ASPECTS,
    BASELINE_DENOMINATOR_ASPECT,
    BASELINE_DENOMINATOR_TOTAL,
    BASELINE_RATE,
    _negative_rate,
)

REVIEW_ASPECTS = ["색상", "사이즈", "소재"]


# ── 확률 자체 고정 ──────────────────────────────────────────────


@pytest.mark.parametrize("aspect", ASPECTS)
@pytest.mark.parametrize("channel", ["COUPANG", "NAVER", "ZIGZAG"])
def test_total_denominator_undoes_aspect_split_cs(aspect, channel):
    """CS(aspect 6개): 전체 분모 기준 부정률이 config 값과 같아야 한다."""
    per_doc = _negative_rate(aspect, channel, len(ASPECTS), BASELINE_DENOMINATOR_TOTAL)
    # 문서를 aspect 수로 쪼갠 뒤 per_doc 로 뽑으므로, 전체 분모 기준 비율은 per_doc / n_aspects
    observed_over_total = per_doc / len(ASPECTS)
    assert observed_over_total == pytest.approx(BASELINE_RATE[aspect][channel])


@pytest.mark.parametrize("aspect", REVIEW_ASPECTS)
def test_total_denominator_undoes_aspect_split_review(aspect):
    """리뷰(aspect 3개): 희석 배수가 6이 아니라 3이라 따로 고정한다."""
    per_doc = _negative_rate(aspect, "NAVER", len(REVIEW_ASPECTS), BASELINE_DENOMINATOR_TOTAL)
    observed_over_total = per_doc / len(REVIEW_ASPECTS)
    assert observed_over_total == pytest.approx(BASELINE_RATE[aspect]["NAVER"])


def test_aspect_denominator_reproduces_old_dilution():
    """옛 동작(aspect 분모)은 전체 분모로 보면 정확히 1/6 로 희석된다 — 재현용 경로 보존."""
    per_doc = _negative_rate("색상", "COUPANG", len(ASPECTS), BASELINE_DENOMINATOR_ASPECT)
    observed_over_total = per_doc / len(ASPECTS)
    assert observed_over_total == pytest.approx(BASELINE_RATE["색상"]["COUPANG"] / len(ASPECTS))
    # 2026-08-07 감사 실측 0.903% 와 같은 자리수인지 (희석분 0.833%)
    assert 0.005 < observed_over_total < 0.012


def test_two_modes_differ_by_aspect_count():
    """두 모드의 비는 aspect 수다 — 감사에서 나온 6.04배의 정체."""
    total = _negative_rate("색상", "COUPANG", len(ASPECTS), BASELINE_DENOMINATOR_TOTAL)
    aspect = _negative_rate("색상", "COUPANG", len(ASPECTS), BASELINE_DENOMINATOR_ASPECT)
    assert total / aspect == pytest.approx(len(ASPECTS))


def test_rate_is_clamped_to_one():
    """표를 올렸을 때 확률이 1을 넘어 조용히 깨지지 않는다."""
    assert _negative_rate("사이즈", "NAVER", 100, BASELINE_DENOMINATOR_TOTAL) == 1.0


def test_current_table_stays_below_clamp():
    """지금 표는 clamp 에 걸리지 않는다 — 걸리면 위 보정이 무의미해지므로 감시한다."""
    for aspect in ASPECTS:
        for channel in ("COUPANG", "NAVER", "ZIGZAG"):
            raw = BASELINE_RATE[aspect][channel] * len(ASPECTS)
            assert raw < 1.0, f"{aspect}/{channel} 이 clamp 에 걸린다 ({raw})"


# ── 표본 수준 검증 ──────────────────────────────────────────────


def test_sampled_rate_matches_config_on_total_denominator():
    """생성 루프와 같은 구조로 뽑았을 때 전체 분모 부정률이 config 근처로 온다.

    허용오차가 넉넉한 이유: 시드는 고정이지만 이항 변동이 남는다. 이 테스트는 정밀도가
    아니라 **1/6 희석이 돌아왔는지**를 잡는 용도다 — 희석이 남아 있으면 0.83% 라 절대
    통과하지 못한다.
    """
    rng = random.Random(11)
    channel, target = "COUPANG", "색상"
    docs_per_aspect = 2000

    negatives = 0
    for aspect in ASPECTS:
        rate = _negative_rate(aspect, channel, len(ASPECTS), BASELINE_DENOMINATOR_TOTAL)
        for _ in range(docs_per_aspect):
            if aspect == target and rng.random() < rate:
                negatives += 1

    total_docs = docs_per_aspect * len(ASPECTS)
    observed = negatives / total_docs
    expected = BASELINE_RATE[target][channel]

    assert observed == pytest.approx(expected, abs=0.008), (
        f"전체 분모 기준 {target}/{channel} 부정률 {observed:.4f}, 기대 {expected:.4f}"
    )
