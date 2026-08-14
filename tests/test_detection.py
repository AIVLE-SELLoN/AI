"""담당: 서영 (Agent2) — 통계 검정 유닛테스트.

정답지: 이상탐지 시나리오 문서 부록 A (scipy 3중 검산으로 소수점 12자리 확인된 값).
수제 숫자만 쓴다 — config CSV·LLM·DB 없이 함수만 검증한다.
"""

import pytest

from app.detection.aggregate import (
    build_baseline,
    build_combinations,
    collect_texts,
    count_window,
)
from app.detection.scope import is_in_scope, pick_main_aspect
from app.detection.statistics import (
    build_batch,
    decide_fires,
    run_detection,
    run_one_test,
)
from app.detection.verdict import classify_pattern, run_verdict

# 부록 A 검산값: (케이스, cur_neg, cur_total, past_neg, past_total, delta, p)
APPENDIX_A = [
    ("SC-001", 26, 200, 40, 800, 0.0800, 0.0001),  # 참양성 — 확실 발화
    ("SC-013", 13, 200, 8, 800, 0.0550, 0.0000),  # 참양성(오배송)
    ("SC-019", 5, 60, 10, 240, 0.0417, 0.159),  # 저사건 함정 — Fisher가 거름
    ("SC-020", 4, 55, 6, 220, 0.0455, 0.117),  # 저사건 함정
    ("SC-021", 3, 50, 3, 200, 0.0450, 0.096),  # 저사건 함정
    ("SC-023", 12, 200, 40, 800, 0.0100, 0.338),  # 잡음 — min_delta가 거름
    ("SC-026", 23, 200, 40, 800, 0.0650, 0.001224),  # G 경계 — 확정 컷오프
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
        ("P001", "색상", "COUPANG", "cs", (3, 8, 2, 32)),  # 총문의 8 < 10 → 보류
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
    assert batch == []  # 세 aspect 전부 보류
    assert held == [("P036", "COUPANG", "cs")]  # aspect 수만큼 중복되지 않는다


def test_past_total_zero_holds_only_that_aspect():
    """과거표본 0 은 그 aspect 슬롯만 보류한다 — 같은 채널의 다른 aspect 는 살아야 한다.

    past_total 은 aspect 마다 따로 센다(aggregate §97·§181). 색상의 기준선이
    비었다고 같은 채널의 사이즈·소재까지 판정을 접으면, 그 이상이 held 표기도
    없이 조용히 사라진다. 최소표본 보류(채널 단위)와 범위를 섞지 말 것.
    """
    combos = [
        ("P100", "색상", "NAVER", "cs", (10, 50, 0, 0)),  # 과거표본 0 → 이 슬롯만 보류
        ("P100", "사이즈", "NAVER", "cs", (20, 50, 5, 200)),  # 살아야 한다
        ("P100", "소재", "NAVER", "cs", (18, 50, 4, 200)),  # 살아야 한다
    ]
    batch, held = build_batch(combos)
    assert [t["key"][1] for t in batch] == ["사이즈", "소재"]
    assert held == [("P100", "NAVER", "cs")]  # 보류 사실은 계속 보고된다


def test_unreliable_denominator_slot_is_excluded_before_testing():
    """분류 커버리지 미달 슬롯은 **검정 전에** family 에서 빠진다.

    분모가 깎인 슬롯은 부정률이 부풀려져 p값이 실제보다 작게 나온다. BH 는
    step-up 이라 가짜로 작은 p값 하나가 기각 개수를 늘려 나머지 검정의 임계까지
    완화시킨다 — 한 상품의 데이터 결함이 다른 상품을 오탐시킨다.
    """
    combos = [
        ("P001", "색상", "COUPANG", "cs", (26, 200, 40, 800)),  # 커버리지 미달로 가정
        ("P001", "색상", "COUPANG", "review", (5, 40, 4, 160)),  # 같은 채널 다른 source
        ("P002", "색상", "COUPANG", "cs", (26, 200, 40, 800)),  # 무관한 상품
    ]
    batch, held = build_batch(
        combos, unreliable_denominators={("P001", "COUPANG", "cs")}
    )

    keys = [t["key"] for t in batch]
    assert ("P001", "색상", "COUPANG", "cs") not in keys  # 검정 자체를 안 한다
    assert ("P001", "색상", "COUPANG", "review") in keys  # 분모는 source 마다 따로다
    assert ("P002", "색상", "COUPANG", "cs") in keys
    assert ("P001", "COUPANG", "cs") in held  # 보류로 보고된다


def test_hold_propagates_across_sources():
    """CS 가 표본 부족이면 그 (상품,채널)은 리뷰까지 함께 보류된다.

    로직 §5·§215, config 보고 §304: "한 채널이 보류되면 그 (상품,채널)의
    모든 aspect·source 12검정이 통째로 family 에서 빠진다."
    source 별로 따로 보류하면 family 크기(m)가 달라져 BH 컷오프가 어긋난다.
    """
    combos = [
        ("P036", "색상", "COUPANG", "cs", (3, 8, 2, 32)),  # CS 총문의 8 < 10
        ("P036", "색상", "COUPANG", "review", (5, 40, 4, 160)),  # 리뷰는 표본 충분
        ("P036", "색상", "NAVER", "cs", (26, 200, 40, 800)),  # 다른 채널은 무관
    ]
    batch, held = build_batch(combos)
    assert set(held) == {("P036", "COUPANG", "cs"), ("P036", "COUPANG", "review")}
    assert [t["key"] for t in batch] == [("P036", "색상", "NAVER", "cs")]


# ── 관문② BH-FDR: step-up 절차 (설명의 k 예시 재현) ──────────────
def test_bh_step_up_toy():
    """검정 5개, q=0.05. 순위 k의 통과 기준 (k/5)*0.05.

    p = [0.001, 0.008, 0.030, 0.045, 0.060]
    k=3(0.030) 이 (3/5)*0.05=0.030 에 딱 맞아 통과 → 1,2,3 발화 / 4,5 탈락.
    (전부 delta 충분하다고 두고 BH 만 본다.)

    ⚠️ 5개를 **같은 상품**에 둔다 — family 가 상품별이라 그래야 한 family 안의
       step-up 을 보는 원래 의도가 유지된다.
    """
    ps = [0.001, 0.008, 0.030, 0.045, 0.060]
    batch = [
        {"p_value": p, "meaningful": True, "key": ("P001", a, "COUPANG", "cs")}
        for p, a in zip(ps, ["색상", "사이즈", "소재", "파손", "오배송"])
    ]
    decide_fires(batch, q=0.05)
    fired = [t["fired"] for t in batch]
    assert fired == [True, True, True, False, False]


def test_bh_family_is_per_product():
    """family 가 **상품별**이라는 것 자체를 고정한다.

    상품 A 는 검정 1개(p=0.02), 상품 B 는 귀무 검정 9개(p=0.9).

        배치 전체 family : m=10 → 순위1 임계 0.05/10 = 0.005 → A 탈락
        상품별 family    : A 의 m=1 → 임계 0.05/1 = 0.05    → A 발화

    즉 **상품별 그룹핑을 빼면 이 테스트는 실패한다.** 그게 이 테스트의 목적이다.
    """
    batch = [{"p_value": 0.02, "meaningful": True, "key": ("A", "색상", "COUPANG", "cs")}]
    batch += [
        {"p_value": 0.9, "meaningful": True, "key": ("B", a, "NAVER", "cs")}
        for a in ["색상", "사이즈", "소재", "파손", "오배송", "기타"]
    ]
    batch += [
        {"p_value": 0.9, "meaningful": True, "key": ("B", a, "ZIGZAG", "cs")}
        for a in ["색상", "사이즈", "소재"]
    ]
    decide_fires(batch, q=0.05)

    fired = {t["key"]: t["fired"] for t in batch}
    assert fired[("A", "색상", "COUPANG", "cs")] is True
    assert not any(f for k, f in fired.items() if k[0] == "B")


def test_bh_missing_key_raises():
    """key 가 없으면 조용히 옛 동작(전체 family)으로 폴백하지 않고 세운다.

    폴백을 두면 "key 를 안 넘긴 호출부만 옛 보정"이 되는데, 그건 알림이 덜 나가는
    방향이라 **미탐이고 조용하다.**
    """
    with pytest.raises(KeyError):
        decide_fires([{"p_value": 0.01, "meaningful": True}], q=0.05)


def test_bh_empty_batch():
    assert decide_fires([]) == []


# ── 3관문 통합: 참양성만 발화, 함정·잡음은 안 함 ─────────────────
def test_three_gate_integration():
    combos = [
        ("P019", "색상", "COUPANG", "cs", (26, 200, 40, 800)),  # 참양성 → 발화
        ("P020", "색상", "COUPANG", "cs", (5, 60, 10, 240)),  # 함정 → Fisher/BH 컷
        ("P024", "색상", "COUPANG", "cs", (12, 200, 40, 800)),  # 잡음 → min_delta 컷
        ("P036", "색상", "COUPANG", "cs", (3, 8, 2, 32)),  # 소표본 → 보류
    ]
    batch, held = run_detection(combos)

    fired = {t["key"][0]: t["fired"] for t in batch}
    assert fired["P019"] is True  # 참양성만 발화
    assert fired["P020"] is False  # 함정
    assert fired["P024"] is False  # 잡음
    assert "P036" not in fired  # 보류라 batch 에 없음
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
    assert by_source["cs"] == "전역형"  # cs 둘 다 발화
    assert by_source["review"] == "편중형"  # review 일부만


def test_run_verdict_held_channel_in_batch_not_double_counted():
    """held 목록에 있어도 이 그룹 배치에 있으면 testable — held로 중복 처리 안 함."""
    batch = [
        _fired("P021", "색상", "COUPANG", "cs", True),
        _fired(
            "P021", "색상", "NAVER", "cs", True
        ),  # NAVER는 cs 배치에 있음(=testable)
    ]
    # NAVER가 held 목록에 있으나(같은 cs) cs 배치엔 존재 → 배치 우선
    res = run_verdict(batch, held=[("P021", "NAVER", "cs")])
    assert res[0]["verdict"] == "전역형"  # 둘 다 testable·발화, 보류 없음
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
    assert res[0]["verdict"] == "전역형"  # ZIGZAG(cs 보류)가 review 판정에 안 낌
    assert res[0]["held"] == []


# ── [4] 주 aspect 선택 (로직 §[4] pick_main_aspect) ────────────────
def test_pick_main_aspect_max_delta():
    """delta 최대가 main, 나머지는 subs (버리지 않음)."""
    main, subs = pick_main_aspect({"색상": 0.080, "파손": 0.070})
    assert main == "색상"
    assert subs == ["파손"]


def test_pick_main_aspect_single():
    """발화 aspect 1개면 그게 main, subs 비어있음."""
    main, subs = pick_main_aspect({"소재": 0.05})
    assert main == "소재"
    assert subs == []


def test_pick_main_aspect_three():
    main, subs = pick_main_aspect({"색상": 0.03, "사이즈": 0.09, "소재": 0.05})
    assert main == "사이즈"
    assert set(subs) == {"색상", "소재"}


# ── [5] 스코프 필터 (로직 §[5] is_in_scope) ────────────────────────
def test_is_in_scope_true_for_recommendable():
    for aspect in ("색상", "사이즈", "소재"):
        assert is_in_scope(aspect) is True


def test_is_in_scope_false_for_alert_only():
    for aspect in ("파손", "오배송", "기타"):
        assert is_in_scope(aspect) is False


# ── [0] 집계 (로직 §[0] count_window / collect_texts) ─────────────
def _row(product, channel, source, aspect, neg, day, rid="x", text="t"):
    return {
        "product": product,
        "channel": channel,
        "source": source,
        "aspect": aspect,
        "is_negative": neg,
        "day": day,
        "id": rid,
        "text": text,
    }


def test_count_window_denominator_is_aspect_agnostic():
    """분모(총문의)는 aspect·감성 무관 전체, 분자는 부정+aspect 만. (문서 §129)"""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 30),  # 부정 색상
        _row("P1", "COUPANG", "cs", "색상", True, 31),  # 부정 색상
        _row("P1", "COUPANG", "cs", "사이즈", True, 32),  # 부정 사이즈
        _row("P1", "COUPANG", "cs", "색상", False, 33),  # 색상 문의지만 긍정 → 분모만
        _row("P1", "COUPANG", "cs", None, False, 34),  # aspect 없음 → 분모만
    ]
    totals, negs = count_window(rows, 29, 35)
    assert totals[("P1", "COUPANG", "cs")] == 5  # 전부 분모
    assert negs[("P1", "색상", "COUPANG", "cs")] == 2
    assert negs[("P1", "사이즈", "COUPANG", "cs")] == 1


def test_count_window_boundary_inclusive_and_excludes_outside():
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 29),  # 시작 경계 포함
        _row("P1", "COUPANG", "cs", "색상", True, 35),  # 끝 경계 포함
        _row("P1", "COUPANG", "cs", "색상", True, 28),  # 구간 밖(과거) 제외
        _row("P1", "COUPANG", "cs", "색상", True, 36),  # 구간 밖(미래) 제외
    ]
    totals, negs = count_window(rows, 29, 35)
    assert totals[("P1", "COUPANG", "cs")] == 2
    assert negs[("P1", "색상", "COUPANG", "cs")] == 2


def test_count_window_source_separated():
    """cs 와 review 는 분모를 합치지 않는다. (문서 §136)"""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 30),
        _row("P1", "COUPANG", "review", "색상", True, 30),
        _row("P1", "COUPANG", "review", "색상", False, 31),
    ]
    totals, _ = count_window(rows, 29, 35)
    assert totals[("P1", "COUPANG", "cs")] == 1
    assert totals[("P1", "COUPANG", "review")] == 2


def test_collect_texts_gathers_negative_only_by_key():
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 30, "INQ-1", "색이 달라요"),
        _row("P1", "COUPANG", "cs", "색상", False, 31, "INQ-2", "색 만족"),  # 긍정 제외
        _row(
            "P1", "COUPANG", "cs", "색상", True, 40, "INQ-3", "구간 밖"
        ),  # 구간 밖 제외
    ]
    texts = collect_texts(rows, 29, 35)
    key = ("P1", "색상", "COUPANG", "cs")
    assert texts[key] == [{"cs_id": "INQ-1", "raw_text": "색이 달라요"}]


# ── [1] 과거 기준 (로직 §[1] build_baseline) ──────────────────────
def test_build_baseline_uses_preceding_28_days():
    """과거 윈도우 = [cur_start-28, cur_start-1]. cur_start=29 → 과거 [1,28]."""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 1),  # 과거 시작 경계
        _row("P1", "COUPANG", "cs", "색상", True, 28),  # 과거 끝 경계
        _row("P1", "COUPANG", "cs", "색상", True, 29),  # 현재 → 과거 아님
        _row("P1", "COUPANG", "cs", "색상", True, 0),  # 28일 밖 → 제외
    ]
    totals, negs, _ = build_baseline(rows, cur_start=29, aspects=["색상"])
    assert totals[("P1", "색상", "COUPANG", "cs")] == 2
    assert negs[("P1", "색상", "COUPANG", "cs")] == 2


def test_build_baseline_excludes_alert_days():
    """알림 구간 날짜(상품,aspect,채널,day)는 과거 집계에서 제외 — 기준선 오염 방지(§150)."""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 10),
        _row("P1", "COUPANG", "cs", "색상", True, 11),  # 이 날이 알림 구간
        _row("P1", "COUPANG", "cs", "색상", True, 12),
    ]
    totals, negs, unfiltered = build_baseline(
        rows, cur_start=29, aspects=["색상"], alert_days={("P1", "색상", "COUPANG", 11)}
    )
    assert (
        totals[("P1", "색상", "COUPANG", "cs")] == 2
    )  # day11 통째로 빠짐 (분자·분모 모두)
    assert negs[("P1", "색상", "COUPANG", "cs")] == 2
    assert unfiltered[("P1", "COUPANG", "cs")] == 3  # 제외 전 = 폴백 판정 재료


def test_build_baseline_exclusion_is_per_aspect():
    """색상 알림 구간을 빼도 같은 날의 사이즈 집계는 남는다 — 제외 단위가 aspect별(§150)."""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 10),
        _row(
            "P1", "COUPANG", "cs", "사이즈", True, 11
        ),  # 색상 알림 구간이지만 사이즈 문의
    ]
    totals, negs, _ = build_baseline(
        rows,
        cur_start=29,
        aspects=["색상", "사이즈"],
        alert_days={("P1", "색상", "COUPANG", 11)},
    )
    assert totals[("P1", "색상", "COUPANG", "cs")] == 1  # day11 빠짐
    assert totals[("P1", "사이즈", "COUPANG", "cs")] == 2  # 사이즈는 그대로
    assert negs[("P1", "사이즈", "COUPANG", "cs")] == 1


# ── [0]+[1] 조합 빌더 (build_combinations) ────────────────────────
def test_build_combinations_emits_full_grid_with_zero_fill():
    """관측된 (상품,채널,source)마다 aspects 전 슬롯 방출 — 부정 없는 aspect 는 0으로."""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 30),  # 현재 부정
        _row("P1", "COUPANG", "cs", "색상", True, 5),  # 과거 부정
        _row("P1", "COUPANG", "cs", "사이즈", False, 31),  # 분모만
    ]
    combos, _ = build_combinations(rows, 29, 35, aspects=["색상", "사이즈", "소재"])
    by_aspect = {c[1]: c[4] for c in combos if c[0] == "P1"}
    assert set(by_aspect) == {"색상", "사이즈", "소재"}  # 전 aspect 슬롯 존재
    assert by_aspect["색상"] == (1, 2, 1, 1)  # cur_neg,cur_total,past_neg,past_total
    assert by_aspect["사이즈"] == (0, 2, 0, 1)  # 부정 0 이어도 슬롯은 나옴
    assert by_aspect["소재"] == (0, 2, 0, 1)  # 관측조차 없어도 그리드엔 포함


def test_baseline_fallback_when_past_halved_by_exclusion():
    """알림 구간 제외로 과거 표본이 절반 이하로 줄면 설정값 부정률로 대체 (§152).

    과거 4건 중 3건이 알림 구간 → 1건만 남음(절반 이하) → 설정값 10% × 1건 = 0건.
    """
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 10),
        _row("P1", "COUPANG", "cs", "색상", True, 11),
        _row("P1", "COUPANG", "cs", "색상", True, 12),
        _row("P1", "COUPANG", "cs", "색상", False, 13),
        _row("P1", "COUPANG", "cs", "색상", True, 30),  # 현재 윈도우
    ]
    alert_days = {("P1", "색상", "COUPANG", d) for d in (10, 11, 12)}
    combos, _ = build_combinations(
        rows,
        29,
        35,
        aspects=["색상"],
        alert_days=alert_days,
        past_rate_fallback={("COUPANG", "색상"): 0.10},
    )
    past_neg, past_total = combos[0][4][2], combos[0][4][3]
    assert past_total == 1  # 3일 제외 후 1건만 남음
    assert past_neg == round(0.10 * 1)  # 실측(0건) 대신 설정값 적용


def test_baseline_fallback_initial_period_assumes_window_ratio_n():
    """과거 표본이 아예 없으면(초기 구간) 현재 볼륨을 윈도우 길이 비로 늘려 N 으로 쓴다.

    현재 7일 20건 · 과거 28일 → N = 20 × 28/7 = 80, 설정값 5% → 부정 4건. (§153·§137)
    """
    rows = [
        _row("P1", "COUPANG", "cs", "색상", i < 3, 29 + (i % 7), rid=f"r{i}")
        for i in range(20)
    ]
    combos, _ = build_combinations(
        rows, 29, 35, aspects=["색상"], past_rate_fallback={("COUPANG", "색상"): 0.05}
    )
    _, cur_total, past_neg, past_total = combos[0][4]
    assert cur_total == 20
    assert past_total == 80  # 20 × (28/7)
    assert past_neg == 4  # round(0.05 × 80)


def test_baseline_fallback_skipped_without_config_rate():
    """설정값이 주입되지 않으면 폴백을 못 타고 past_total=0 → [2] 가 보류로 보낸다."""
    rows = [_row("P1", "COUPANG", "cs", "색상", True, 30)]
    combos, _ = build_combinations(rows, 29, 35, aspects=["색상"])
    assert combos[0][4][3] == 0
    batch, held = run_detection(combos)
    assert batch == []
    assert held == [("P1", "COUPANG", "cs")]


def test_baseline_fallback_not_applied_when_sample_survives():
    """절반 초과로 남았으면 실측치를 그대로 쓴다 — 폴백은 표본이 무너졌을 때만."""
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 10),  # 알림 구간
        _row("P1", "COUPANG", "cs", "색상", True, 11),
        _row("P1", "COUPANG", "cs", "색상", True, 12),
        _row("P1", "COUPANG", "cs", "색상", True, 30),
    ]
    combos, _ = build_combinations(
        rows,
        29,
        35,
        aspects=["색상"],
        alert_days={("P1", "색상", "COUPANG", 10)},
        past_rate_fallback={("COUPANG", "색상"): 0.99},
    )
    assert combos[0][4][2] == 2  # 실측 2건 유지 (설정값 99% 무시)


def test_build_combinations_texts_only_current_negatives():
    rows = [
        _row("P1", "COUPANG", "cs", "색상", True, 30, "INQ-1", "현재부정"),
        _row("P1", "COUPANG", "cs", "색상", True, 5, "INQ-2", "과거부정"),
    ]
    _, texts = build_combinations(rows, 29, 35, aspects=["색상"])
    key = ("P1", "색상", "COUPANG", "cs")
    assert texts[key] == [{"cs_id": "INQ-1", "raw_text": "현재부정"}]
