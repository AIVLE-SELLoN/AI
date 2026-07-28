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
from app.detection.verdict import classify_pattern, run_verdict

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
    assert ("P001", "COUPANG", "cs") in held
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
    assert batch == []                              # 세 aspect 전부 보류
    assert held == [("P036", "COUPANG", "cs")] * 3  # (채널,source) 단위로 잡힘


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
    assert ("P036", "COUPANG", "cs") in held


# ── [3] 편중·전역 판정 (로직 §[3] classify_pattern) ─────────────────
def _ch(testable: bool, fired: bool) -> dict:
    return {"testable": testable, "fired": fired}


def test_verdict_normal_when_no_fire():
    """발화 0개 → 정상 (알림 없음)."""
    cs = {"COUPANG": _ch(True, False), "NAVER": _ch(True, False)}
    assert classify_pattern(cs)["verdict"] == "정상"


def test_verdict_global_all_testable_fired():
    """판정가능 전부 발화 · 보류 없음 → 전역형."""
    cs = {
        "COUPANG": _ch(True, True),
        "NAVER": _ch(True, True),
        "ZIGZAG": _ch(True, True),
    }
    r = classify_pattern(cs)
    assert r["verdict"] == "전역형"
    assert r["held"] == []


def test_verdict_tentative_global_when_held_exists():
    """판정가능 전부 발화 · 보류 채널 있음 → 잠정 전역형."""
    cs = {
        "COUPANG": _ch(True, True),
        "NAVER": _ch(True, True),
        "ZIGZAG": _ch(False, False),  # 표본 부족 보류
    }
    r = classify_pattern(cs)
    assert r["verdict"] == "잠정 전역형"
    assert r["held"] == ["ZIGZAG"]


def test_verdict_biased_partial_fire():
    """일부만 발화 → 편중형 (1개든 2개든)."""
    cs = {
        "COUPANG": _ch(True, True),
        "NAVER": _ch(True, False),
        "ZIGZAG": _ch(True, False),
    }
    r = classify_pattern(cs)
    assert r["verdict"] == "편중형"
    assert r["channels"] == ["COUPANG"]

    cs["NAVER"] = _ch(True, True)  # 2개 발화도 편중형
    assert classify_pattern(cs)["verdict"] == "편중형"


def test_verdict_indeterminate_single_testable():
    """판정가능 채널 1개 + 발화 → 구분불가 ('전부 발화'보다 먼저 걸림)."""
    cs = {
        "COUPANG": _ch(True, True),
        "NAVER": _ch(False, False),
        "ZIGZAG": _ch(False, False),
    }
    r = classify_pattern(cs)
    assert r["verdict"] == "구분불가"
    assert r["channels"] == ["COUPANG"]
    assert set(r["held"]) == {"NAVER", "ZIGZAG"}


def test_verdict_single_testable_no_fire_is_normal():
    """판정순서 검증 — 1채널이라도 발화 0이면 구분불가 아니라 정상."""
    cs = {"COUPANG": _ch(True, False), "NAVER": _ch(False, False)}
    assert classify_pattern(cs)["verdict"] == "정상"


# ── [3] run_verdict 배치 래퍼 (source별 독립) ──────────────────────
def _fired(product, aspect, channel, source, fired):
    return {"key": (product, aspect, channel, source), "fired": fired}


def test_run_verdict_biased_single_group():
    """한 채널만 발화 → 편중형, 그룹 1개."""
    batch = [
        _fired("P019", "색상", "COUPANG", "cs", True),
        _fired("P019", "색상", "NAVER", "cs", False),
        _fired("P019", "색상", "ZIGZAG", "cs", False),
    ]
    res = run_verdict(batch, held=[])
    assert len(res) == 1
    assert res[0]["verdict"] == "편중형"
    assert res[0]["product"] == "P019"
    assert res[0]["aspect"] == "색상"
    assert res[0]["source"] == "cs"


def test_run_verdict_held_makes_tentative_global():
    """배치의 채널 전부 발화 + 그 상품 보류 채널 있음 → 잠정 전역형."""
    batch = [
        _fired("P020", "소재", "COUPANG", "cs", True),
        _fired("P020", "소재", "NAVER", "cs", True),
    ]
    res = run_verdict(batch, held=[("P020", "ZIGZAG", "cs")])
    assert res[0]["verdict"] == "잠정 전역형"
    assert res[0]["held"] == ["ZIGZAG"]


def test_run_verdict_is_per_source():
    """같은 (상품,aspect)라도 source별로 독립 판정 — cs 전역형, review 편중형."""
    batch = [
        _fired("P019", "색상", "COUPANG", "cs", True),
        _fired("P019", "색상", "NAVER", "cs", True),
        _fired("P019", "색상", "COUPANG", "review", True),
        _fired("P019", "색상", "NAVER", "review", False),
    ]
    res = run_verdict(batch, held=[])
    by_source = {r["source"]: r["verdict"] for r in res}
    assert by_source["cs"] == "전역형"       # cs 둘 다 발화
    assert by_source["review"] == "편중형"   # review 일부만


def test_run_verdict_held_channel_in_batch_not_double_counted():
    """held 목록에 있어도 이 그룹 배치에 있으면 testable — held로 중복 처리 안 함."""
    batch = [
        _fired("P021", "색상", "COUPANG", "cs", True),
        _fired("P021", "색상", "NAVER", "cs", True),  # NAVER는 cs 배치에 있음(=testable)
    ]
    # NAVER가 held 목록에 있으나(같은 cs) cs 배치엔 존재 → 배치 우선
    res = run_verdict(batch, held=[("P021", "NAVER", "cs")])
    assert res[0]["verdict"] == "전역형"   # 둘 다 testable·발화, 보류 없음
    assert res[0]["held"] == []


def test_run_verdict_held_is_source_specific():
    """[검증-A] held는 source별 — cs에서 보류된 채널이 review 그룹엔 안 낀다.

    ZIGZAG가 cs에서만 보류(review 데이터 없음)인데, source-무관 held였다면
    review 그룹에 held로 잘못 끼어 전역형→잠정전역형으로 뒤집혔다. 이 버그 재현·방지.
    """
    batch = [
        _fired("P022", "색상", "COUPANG", "review", True),
        _fired("P022", "색상", "NAVER", "review", True),
    ]
    res = run_verdict(batch, held=[("P022", "ZIGZAG", "cs")])
    assert res[0]["source"] == "review"
    assert res[0]["verdict"] == "전역형"   # ZIGZAG(cs 보류)가 review 판정에 안 낌
    assert res[0]["held"] == []
