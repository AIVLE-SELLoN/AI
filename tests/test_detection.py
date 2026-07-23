"""담당: 서영 (Agent2) — 통계 검정 유닛테스트.

정답지: 이상탐지 시나리오 문서 부록 A (scipy 3중 검산으로 소수점 12자리 확인된 값).
수제 숫자만 쓴다 — config CSV·LLM·DB 없이 함수만 검증한다.
"""

import pytest

from app.detection.statistics import (
    build_batch,
    decide_fires,
    run_detection,
    run_one_test,
)

# 부록 A 검산값: (케이스, cur_neg, cur_total, past_neg, past_total, delta, p)
APPENDIX_A = [
    ("SC-001", 26, 200, 40, 800, 0.0800, 0.0001),   # 참양성 — 확실 발화
    ("SC-013", 13, 200, 8, 800, 0.0550, 0.0000),    # 참양성(오배송)
    ("SC-019", 5, 60, 10, 240, 0.0417, 0.159),      # 저사건 함정 — Fisher가 거름
    ("SC-020", 4, 55, 6, 220, 0.0455, 0.117),       # 저사건 함정
    ("SC-021", 3, 50, 3, 200, 0.0450, 0.096),       # 저사건 함정
    ("SC-023", 12, 200, 40, 800, 0.0100, 0.338),    # 잡음 — min_delta가 거름
    ("SC-026", 23, 200, 40, 800, 0.0650, 0.001224), # G 경계 — 확정 컷오프
]


# ── 관문② Fisher: 부록 A p값 재현 ────────────────────────────────
@pytest.mark.parametrize("case,cn,ct,pn,pt,delta,p", APPENDIX_A)
def test_fisher_p_matches_appendix_a(case, cn, ct, pn, pt, delta, p):
    r = run_one_test(cn, ct, pn, pt)
    assert r["p_value"] == pytest.approx(p, abs=5e-4), f"{case} p 불일치"
    assert r["delta"] == pytest.approx(delta, abs=5e-4), f"{case} delta 불일치"


# ── 관문③ min_delta: 상승폭 3%p 미만이면 meaningful=False ──────────
def test_min_delta_gate():
    # SC-001: +8%p → 실질적
    assert run_one_test(26, 200, 40, 800)["meaningful"] is True
    # SC-023: +1%p → 실질적이지 않음 (통계와 무관하게 여기서 걸림)
    assert run_one_test(12, 200, 40, 800)["meaningful"] is False
    # 경계 정확히 3%p: 8% → 11% = +3%p → 통과 (>=)
    assert run_one_test(22, 200, 64, 800)["meaningful"] is True


# ── 관문① 최소표본: 총문의 <10 이면 보류(채널 단위), batch 미진입 ──
def test_small_sample_is_held():
    combos = [
        ("P001", "색상", "COUPANG", "cs", (3, 8, 2, 32)),    # 총문의 8 < 10 → 보류
        ("P001", "색상", "NAVER", "cs", (26, 200, 40, 800)),  # 정상 판정 대상
    ]
    batch, held = build_batch(combos)
    assert ("P001", "COUPANG") in held
    assert len(batch) == 1
    assert batch[0]["key"] == ("P001", "색상", "NAVER", "cs")


def test_hold_is_channel_level():
    """보류는 채널 단위 — 그 (상품,채널)의 모든 aspect 가 함께 빠진다."""
    combos = [
        ("P036", "색상", "COUPANG", "cs", (3, 8, 2, 32)),
        ("P036", "사이즈", "COUPANG", "cs", (1, 8, 1, 32)),
        ("P036", "소재", "COUPANG", "cs", (0, 8, 1, 32)),
    ]
    batch, held = build_batch(combos)
    assert batch == []                       # 세 aspect 전부 보류
    assert held == [("P036", "COUPANG")] * 3  # 채널 단위로 잡힘


# ── 관문② BH-FDR: step-up 절차 (설명의 k 예시 재현) ──────────────
def test_bh_step_up_toy():
    """검정 5개, q=0.05. 순위 k의 통과 기준 (k/5)*0.05.

    p = [0.001, 0.008, 0.030, 0.045, 0.060]
    k=3(0.030) 이 (3/5)*0.05=0.030 에 딱 맞아 통과 → 1,2,3 발화 / 4,5 탈락.
    (전부 delta 충분하다고 두고 BH 만 본다.)
    """
    ps = [0.001, 0.008, 0.030, 0.045, 0.060]
    batch = [{"p_value": p, "meaningful": True} for p in ps]
    decide_fires(batch, q=0.05)
    fired = [t["fired"] for t in batch]
    assert fired == [True, True, True, False, False]


def test_bh_empty_batch():
    assert decide_fires([]) == []


# ── 3관문 통합: 참양성만 발화, 함정·잡음은 안 함 ─────────────────
def test_three_gate_integration():
    combos = [
        ("P019", "색상", "COUPANG", "cs", (26, 200, 40, 800)),  # 참양성 → 발화
        ("P020", "색상", "COUPANG", "cs", (5, 60, 10, 240)),    # 함정 → Fisher/BH 컷
        ("P024", "색상", "COUPANG", "cs", (12, 200, 40, 800)),  # 잡음 → min_delta 컷
        ("P036", "색상", "COUPANG", "cs", (3, 8, 2, 32)),       # 소표본 → 보류
    ]
    batch, held = run_detection(combos)

    fired = {t["key"][0]: t["fired"] for t in batch}
    assert fired["P019"] is True     # 참양성만 발화
    assert fired["P020"] is False    # 함정
    assert fired["P024"] is False    # 잡음
    assert "P036" not in fired       # 보류라 batch 에 없음
    assert ("P036", "COUPANG") in held
