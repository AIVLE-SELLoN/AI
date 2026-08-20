"""월간 리포트 정량 지표 계산. 계약은 `docs/reporting_schema.md`.

여기서 만든 값이 그대로 MonthlyReportInput 이 된다(생성 주체는 FastAPI reporting 노드).
LLM 은 이 수치를 문장으로 옮기기만 하고 계산은 전부 이 모듈이 한다.

판정 순서가 중요하다 — 게이트 -> 순열검정 -> BH-FDR -> severity 순이며, BH-FDR 을
severity 산출보다 먼저 돌려야 한다. 유의성이 확정되기 전에 단계를 매기면 다중검정
보정이 무의미해진다.
"""

from __future__ import annotations

import numpy as np
from statsmodels.stats.multitest import multipletests

from app.core import constants
from app.core.schemas import (
    ChannelDivergencePair,
    DriftStatus,
    HoldReason,
    MonthlyChannelDivergenceInput,
    Severity,
)

# JSD 를 bits 로 환산할 때 쓰는 상수. 스키마가 log₂(bits) 기준이라 nats → bits 변환이 필요하다.
_LN2 = float(np.log(2.0))


# ── 감성 분포·드리프트 ────────────────────────────────────────────────────


def calculate_sentiment_ratios(
    pos_count: int,
    neu_count: int,
    neg_count: int,
) -> tuple[float, float, float]:
    """3대 감성 지분율. 분모는 세 건수의 합(= aspect_distributions.total_count)으로 통일한다."""
    total = pos_count + neu_count + neg_count
    if total == 0:
        return 0.0, 0.0, 0.0
    return pos_count / total, neu_count / total, neg_count / total


def calculate_sentiment_drift(
    current_neg_ratio: float,
    previous_neg_ratio: float,
) -> tuple[float, DriftStatus]:
    """ΔP_neg 와 RISK/NORMAL 판정. RISK 는 drift >= DRIFT_RISK_THRESHOLD 일 때만."""
    drift = current_neg_ratio - previous_neg_ratio
    status = (
        DriftStatus.RISK if drift >= constants.DRIFT_RISK_THRESHOLD else DriftStatus.NORMAL
    )
    return drift, status


# ── JSD (bits) ───────────────────────────────────────────────────────────


def calculate_jsd_bits(p_counts: list[int] | np.ndarray, q_counts: list[int] | np.ndarray) -> float:
    """두 감성 분포의 Jensen-Shannon Divergence (단위: bits, log₂ 기준).

    스키마가 `jsd_score: 0.00–1.00 (log₂, bits)` 로 못박아서 nats 가 아니라 bits 다.
    bits 기준 JSD 는 두 분포가 완전히 겹치지 않을 때 최대 1.0 이라 범위와도 맞는다.
    (제곱근을 씌운 JS distance 가 아니다 — divergence 그대로 쓴다.)
    """
    p = np.asarray(p_counts, dtype=np.float64)
    q = np.asarray(q_counts, dtype=np.float64)

    if p.sum() <= 0 or q.sum() <= 0:
        return 0.0

    p = p / p.sum()
    q = q / q.sum()
    m = 0.5 * (p + q)

    # 0 * log0 = 0 으로 처리 (np.where 로 0 지점을 건너뛴다)
    def _kl(a: np.ndarray, b: np.ndarray) -> float:
        mask = a > 0
        return float(np.sum(a[mask] * np.log(a[mask] / b[mask])))

    jsd_nats = 0.5 * _kl(p, m) + 0.5 * _kl(q, m)
    return float(np.clip(jsd_nats / _LN2, 0.0, 1.0))


def permutation_test_jsd(
    p_counts: list[int],
    q_counts: list[int],
    *,
    n_permutations: int = constants.PERMUTATION_B,
    seed: int = 42,
) -> tuple[float, float, float]:
    """관측 JSD · 귀무 기댓값 E[JSD|H0] · 순열검정 p값을 함께 반환한다.

    두 채널의 부정 문서를 한 통에 붓고 원래 크기대로 다시 나누는 것을 B회 반복한다
    (라벨 순열). 관측값 이상이 나온 비율이 p값이고, 순열 분포의 평균이 baseline 이다.
    baseline 을 빼는 이유: 표본이 작으면 같은 분포에서 나눠도 JSD 가 0 이 아니라서,
    그 기저값을 빼지 않으면 소표본 채널이 항상 위기로 보인다(excess).

    Returns:
        (jsd_score, jsd_baseline, p_value) — 전부 bits 기준.
    """
    observed = calculate_jsd_bits(p_counts, q_counts)

    n_a, n_b = int(sum(p_counts)), int(sum(q_counts))
    if n_a == 0 or n_b == 0:
        return observed, 0.0, 1.0

    # 카테고리 라벨 배열로 펼친 뒤 섞어서 (n_a, n_b) 로 다시 자른다
    pooled = np.repeat(
        np.arange(len(p_counts)),
        np.asarray(p_counts, dtype=np.int64) + np.asarray(q_counts, dtype=np.int64),
    )
    n_categories = len(p_counts)
    rng = np.random.default_rng(seed)

    null_scores = np.empty(n_permutations, dtype=np.float64)
    for i in range(n_permutations):
        shuffled = rng.permutation(pooled)
        a_counts = np.bincount(shuffled[:n_a], minlength=n_categories)
        b_counts = np.bincount(shuffled[n_a:], minlength=n_categories)
        null_scores[i] = calculate_jsd_bits(a_counts, b_counts)

    baseline = float(null_scores.mean())
    # +1 보정: p값이 0 이 되지 않게 한다(B회로는 "0" 을 증명할 수 없다)
    p_value = float((np.sum(null_scores >= observed) + 1) / (n_permutations + 1))

    return observed, baseline, p_value


def apply_bh_fdr(p_values: list[float], q: float = constants.BH_FDR_Q) -> list[bool]:
    """BH-FDR 다중검정 보정. 전 (상품 × 채널쌍) p값을 한 family 로 묶어 돌린다.

    severity 산출보다 **먼저** 호출해야 한다. 여기서 떨어진 쌍은 excess 가 커도 SAFE 다.
    """
    if not p_values:
        return []
    rejected, _, _, _ = multipletests(p_values, alpha=q, method="fdr_bh")
    return [bool(r) for r in rejected]


# ── 게이트·severity 판정 ──────────────────────────────────────────────────


def check_divergence_gate(n_a: int, n_b: int) -> HoldReason | None:
    """[게이트] min(n_A, n_B) >= 1 AND N >= 30. 미충족이면 보류 사유를 돌려준다."""
    if min(n_a, n_b) < 1:
        return HoldReason.EMPTY_CHANNEL
    if (n_a + n_b) < constants.JSD_GATE_MIN_TOTAL:
        return HoldReason.INSUFFICIENT_SAMPLE
    return None


def decide_severity(
    jsd_score: float,
    jsd_baseline: float,
    *,
    bh_significant: bool,
) -> tuple[Severity, bool]:
    """excess 와 유의성으로 severity·is_crisis 를 결정한다.

    유의하지 않으면 excess 가 아무리 커도 SAFE 다 — 다중검정을 통과하지 못한 차이는
    우연으로 본다.
    """
    excess = jsd_score - jsd_baseline
    delta_min = constants.JSD_DELTA_MIN

    if not bh_significant or excess < delta_min:
        return Severity.SAFE, False
    if excess < 2 * delta_min:
        return Severity.CAUTION, True
    return Severity.CRISIS, True


def build_channel_divergence_pair(
    comparison_pair: str,
    p_counts: list[int],
    q_counts: list[int],
    *,
    bh_significant: bool | None = None,
    n_permutations: int = constants.PERMUTATION_B,
    seed: int = 42,
) -> tuple[ChannelDivergencePair, float | None]:
    """채널쌍 1건을 계산해 (pair, p_value) 로 돌려준다.

    bh_significant 를 나중에 채워야 하므로 2단계로 쓴다:
      1) 전 쌍에 대해 이 함수를 돌려 p값을 모은다 (bh_significant=None → 임시로 False)
      2) apply_bh_fdr() 결과로 finalize_pair() 를 호출해 severity 를 확정한다
    게이트 미충족이면 판정 6개 값이 전부 null 인 보류 pair 를 돌려주고 p값은 None 이다.
    """
    n_a, n_b = int(sum(p_counts)), int(sum(q_counts))
    hold_reason = check_divergence_gate(n_a, n_b)

    if hold_reason is not None:
        return (
            ChannelDivergencePair(
                comparison_pair=comparison_pair,
                sample_size=n_a + n_b,
                hold_reason=hold_reason,
            ),
            None,
        )

    jsd_score, jsd_baseline, p_value = permutation_test_jsd(
        p_counts, q_counts, n_permutations=n_permutations, seed=seed
    )
    significant = bool(bh_significant) if bh_significant is not None else False
    severity, is_crisis = decide_severity(jsd_score, jsd_baseline, bh_significant=significant)

    return (
        ChannelDivergencePair(
            comparison_pair=comparison_pair,
            sample_size=n_a + n_b,
            jsd_score=jsd_score,
            jsd_baseline=jsd_baseline,
            p_value=p_value,
            bh_significant=significant,
            is_crisis=is_crisis,
            severity=severity,
        ),
        p_value,
    )


def finalize_pair(pair: ChannelDivergencePair, *, bh_significant: bool) -> ChannelDivergencePair:
    """BH-FDR 결과를 반영해 severity·is_crisis 를 다시 매긴다(보류 pair 는 그대로 통과)."""
    if pair.hold_reason is not None or pair.jsd_score is None or pair.jsd_baseline is None:
        return pair

    severity, is_crisis = decide_severity(
        pair.jsd_score, pair.jsd_baseline, bh_significant=bh_significant
    )
    return pair.model_copy(
        update={
            "bh_significant": bh_significant,
            "severity": severity,
            "is_crisis": is_crisis,
        }
    )


def _worst_rank(pair: ChannelDivergencePair) -> tuple[int, float]:
    """worst_pair 정렬 키 — (severity 등급, excess).

    jsd_score 최댓값으로 고르면 안 된다. severity 는 `excess = jsd − baseline` 과
       유의성으로 정해지는데 baseline 은 쌍마다 다르다(표본이 작을수록 크다). 그래서
       jsd 가 가장 큰 쌍이 SAFE 이고 다른 쌍이 CRISIS 인 상황이 실제로 생기고,
       그때 리포트 제목에는 "안정 단계"가 박히면서 is_crisis=true 로 나간다.
       판정과 대표 쌍 선정이 **같은 기준**을 봐야 한다.
    """
    rank = {Severity.CRISIS: 3, Severity.CAUTION: 2, Severity.SAFE: 1}
    severity_rank = rank.get(pair.severity, 0)  # 보류(severity=None)는 최하위
    excess = (
        pair.jsd_score - pair.jsd_baseline
        if pair.jsd_score is not None and pair.jsd_baseline is not None
        else float("-inf")
    )
    return severity_rank, excess


def build_channel_divergence(
    pairs: list[ChannelDivergencePair],
    calculated_at,
) -> MonthlyChannelDivergenceInput:
    """채널쌍들을 묶어 MonthlyChannelDivergenceInput 을 만든다(worst_pair·is_crisis 롤업).

    worst_pair 는 **가장 위험한 쌍**(severity → excess 순)이다. 전 쌍이 보류면 첫 쌍
    라벨을 쓴다(스키마가 worst_pair 를 필수로 요구하므로 라벨 자체는 있어야 한다).
    is_crisis 는 판정된 쌍이 하나도 없으면 null 이다.
    """
    if not pairs:
        raise ValueError("channel_divergence 는 최소 1개 채널쌍이 필요합니다")

    judged = [p for p in pairs if p.severity is not None]
    worst_pair = (
        max(judged, key=_worst_rank).comparison_pair if judged else pairs[0].comparison_pair
    )

    crisis_flags = [p.is_crisis for p in pairs if p.is_crisis is not None]
    is_crisis = any(crisis_flags) if crisis_flags else None

    return MonthlyChannelDivergenceInput(
        calculated_at=calculated_at,
        worst_pair=worst_pair,
        is_crisis=is_crisis,
        pairs=pairs,
    )
