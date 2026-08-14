"""config_anomaly 검산기가 운영 통계 로직과 같은 BH family를 쓰는지 고정한다."""

import pytest

from scripts import validate_anomaly


def test_validator_bh_family_is_per_product(monkeypatch):
    """다른 상품의 귀무 검정이 상품 A의 BH 임계를 희석하지 않아야 한다."""
    p_values = {1: 0.02, **{n: 0.9 for n in range(2, 11)}}

    def fake_fisher(cur_neg, _cur_total, _past_neg, _past_total):
        return p_values[cur_neg], 0.10

    monkeypatch.setattr(validate_anomaly, "run_fisher", fake_fisher)
    batch = [
        {
            "product": "A",
            "cur_neg": 1,
            "cur_total": 100,
            "past_neg": 0,
            "past_total": 100,
        }
    ]
    batch.extend(
        {
            "product": "B",
            "cur_neg": n,
            "cur_total": 100,
            "past_neg": 0,
            "past_total": 100,
        }
        for n in range(2, 11)
    )

    result = validate_anomaly.apply_pipeline(batch)

    assert result[0]["bh_significant"] is True
    assert result[0]["fired"] is True
    assert not any(item["fired"] for item in result[1:])


def test_robustness_check_recalculates_only_the_changed_product_family(monkeypatch):
    """다른 상품의 귀무 검정이 강건성 재계산의 BH 임계를 희석하지 않아야 한다."""
    monkeypatch.setattr(
        validate_anomaly,
        "run_fisher",
        lambda *_args: (0.02, 0.10),
    )
    batch = [
        {
            "case_id": "SC-001",
            "product": "A",
            "channel": "COUPANG",
            "aspect": "색상",
            "source": "cs",
            "cur_neg": 2,
            "cur_total": 100,
            "past_neg": 0,
            "past_total": 100,
            "p_value": 0.02,
            "intended_answer": "TRUE",
        }
    ]
    batch.extend(
        {
            "case_id": f"BG-{n}",
            "product": "B",
            "channel": "NAVER",
            "aspect": "소재",
            "source": "review",
            "cur_neg": n,
            "cur_total": 100,
            "past_neg": 0,
            "past_total": 100,
            "p_value": 0.9,
            "intended_answer": "",
        }
        for n in range(2, 11)
    )

    result = validate_anomaly.robustness_check(batch)

    assert result["흔들린_케이스"] == []


def test_robustness_check_reports_a_real_one_count_flip():
    """실제 Fisher 경계 사례가 -1건에서 뒤집히면 취약 케이스로 보고한다.

    현재 3/100 대 과거 0/400은 p=0.00781, delta=3%p라 단일검정 family에서
    발화한다. 현재 부정을 2건으로 낮추면 p=0.03968이지만 delta=2%p라 관문③에서
    미발화로 뒤집힌다. 빈 결과만 확인하면 함수가 항상 []를 반환해도 통과하므로,
    검출 능력 자체를 이 양성 사례로 고정한다.
    """
    batch = [
        {
            "case_id": "SC-ROBUSTNESS-POSITIVE",
            "product": "A",
            "channel": "COUPANG",
            "aspect": "색상",
            "source": "cs",
            "cur_neg": 3,
            "cur_total": 100,
            "past_neg": 0,
            "past_total": 400,
            "p_value": 0.007808387860057466,
            "intended_answer": "TRUE",
        }
    ]

    result = validate_anomaly.robustness_check(batch)

    assert len(result["흔들린_케이스"]) == 1
    unstable = result["흔들린_케이스"][0]
    assert unstable["case_id"] == "SC-ROBUSTNESS-POSITIVE"
    assert len(unstable["flips"]) == 1
    flip = unstable["flips"][0]
    assert flip["변화"] == "-1건"
    assert flip["p"] == pytest.approx(0.0396793587)
    assert flip["delta"] == pytest.approx(0.02)
    assert flip["새_fired"] is False
