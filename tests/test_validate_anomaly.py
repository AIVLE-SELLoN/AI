"""config_anomaly 검산기가 운영 통계 로직과 같은 BH family를 쓰는지 고정한다."""

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
