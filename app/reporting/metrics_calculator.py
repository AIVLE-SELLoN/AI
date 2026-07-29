from __future__ import annotations

import numpy as np
from scipy.special import rel_entr

from app.core import constants


def calculate_sentiment_ratios(
    pos_count: int,
    neu_count: int,
    neg_count: int,
) -> tuple[float, float, float]:
    """3대 감정(긍정, 중립, 부정) 지분율 연산"""
    total = pos_count + neu_count + neg_count
    if total == 0:
        return 0.0, 0.0, 0.0
    return pos_count / total, neu_count / total, neg_count / total


def calculate_sentiment_drift(
    current_neg_ratio: float,
    previous_neg_ratio: float,
) -> tuple[float, str]:
    """속성별 감성 드리프트 ΔP_neg(a) 계산 및 RISK / STABLE 판정"""
    drift = current_neg_ratio - previous_neg_ratio
    status = "RISK" if drift >= constants.DRIFT_THRESHOLD else "STABLE"
    return drift, status


def calculate_jensen_shannon_divergence(
    p_dist: list[float],
    q_dist: list[float],
) -> float:
    """두 감정 확률 분포 간 Jensen-Shannon Divergence(JSD) 연산"""
    p = np.asarray(p_dist, dtype=np.float64)
    q = np.asarray(q_dist, dtype=np.float64)

    p = np.clip(p, 1e-12, 1.0)
    q = np.clip(q, 1e-12, 1.0)

    p /= p.sum()
    q /= q.sum()

    m = 0.5 * (p + q)
    jsd = 0.5 * np.sum(rel_entr(p, m)) + 0.5 * np.sum(rel_entr(q, m))
    return float(np.sqrt(np.maximum(jsd, 0.0)))


def calculate_multichannel_divergence(
    channel_distributions: list[tuple[list[float], list[float]]],
) -> tuple[float, bool]:
    """다채널 분열 지수 M_JSD 통합 계산 및 오퍼레이션 장애(CRISIS) 판정"""
    if not channel_distributions:
        return 0.0, False

    jsd_scores = [
        calculate_jensen_shannon_divergence(p, q)
        for p, q in channel_distributions
    ]
    max_jsd = max(jsd_scores) if jsd_scores else 0.0
    is_crisis = max_jsd >= constants.JSD_CRISIS_THRESHOLD

    return max_jsd, is_crisis