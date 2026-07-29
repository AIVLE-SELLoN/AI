"""담당: 서영 (Agent2) — [6] 원인 분류 테스트.

judge_cause 는 수제 숫자로(순수), classify_cause·diagnose_cause 는 LLM 을 목킹해 검증한다.
OpenAI 실호출 없음 (비용 0).
"""

import pytest

from app.detection.cause import classify_cause, diagnose_cause, judge_cause


# ── judge_cause (순수, 로직 §[6]) ─────────────────────────────────
def test_judge_cause_consistent():
    """최다 원인 70%(≥50%) · 14건(≥5) → 일관, 주원인 특정."""
    causes = ["사진_색감_오차"] * 14 + ["조명_보정_차이"] * 3 + ["실물_염색_편차"] * 3
    top, consistent, freq = judge_cause(causes)
    assert top == "사진_색감_오차"
    assert consistent is True
    assert freq["사진_색감_오차"] == 14


def test_judge_cause_not_consistent_by_ratio():
    """최다 40% < 50% → 흩어짐(원인 미특정)."""
    causes = ["A"] * 8 + ["B"] * 6 + ["C"] * 6
    top, consistent, _ = judge_cause(causes)
    assert top is None
    assert consistent is False


def test_judge_cause_not_consistent_by_count():
    """비율 50%는 넘어도 건수 4 < 5 → 저건수 우연 방지로 미특정."""
    causes = ["A"] * 4 + ["B"] * 4
    top, consistent, _ = judge_cause(causes)
    assert top is None
    assert consistent is False


def test_judge_cause_boundary_exactly_threshold():
    """정확히 50% & 5건 → 경계 통과(>=)."""
    causes = ["A"] * 5 + ["B", "C", "D", "E", "F"]
    top, consistent, _ = judge_cause(causes)
    assert top == "A"
    assert consistent is True


def test_judge_cause_empty():
    assert judge_cause([]) == (None, False, {})


# ── classify_cause / diagnose_cause (LLM 목킹) ────────────────────
class _FakeClient:
    """complete_json 을 미리 정한 payload 로 답하는 가짜 LlmClient."""

    def __init__(self, payload):
        self._payload = payload
        self.calls = 0
        self.last_prompt = ""

    async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
        self.calls += 1
        self.last_prompt = prompt
        return self._payload


@pytest.mark.asyncio
async def test_classify_cause_returns_results():
    payload = {"results": [
        {"cs_id": "a1", "cause": "사진_색감_오차", "confidence": 0.9,
         "evidence": "사진이랑 색이", "aspect_match": True},
    ]}
    client = _FakeClient(payload)
    items = [{"cs_id": "a1", "raw_text": "사진이랑 색이 달라요"}]

    results = await classify_cause("색상", items, client=client)

    assert results == payload["results"]
    assert client.calls == 1
    assert "색상" in client.last_prompt   # aspect 가 프롬프트에 실림


@pytest.mark.asyncio
async def test_classify_cause_empty_skips_llm():
    """items 가 비면 LLM 호출 없이 [] — 비용 0."""
    client = _FakeClient({"results": []})
    results = await classify_cause("색상", [], client=client)
    assert results == []
    assert client.calls == 0


@pytest.mark.asyncio
async def test_diagnose_cause_consistent():
    payload = {"results": (
        [{"cs_id": str(i), "cause": "사진_색감_오차", "aspect_match": True} for i in range(14)]
        + [{"cs_id": f"x{i}", "cause": "조명_보정_차이", "aspect_match": True} for i in range(6)]
    )}
    client = _FakeClient(payload)
    items = [{"cs_id": str(i), "raw_text": "t"} for i in range(20)]

    r = await diagnose_cause("색상", items, client=client)

    assert r["label"] == "사진_색감_오차"
    assert r["consistent"] is True
    assert r["count"] == 14
    assert r["total"] == 20


@pytest.mark.asyncio
async def test_diagnose_cause_excludes_aspect_mismatch():
    """aspect_match=false(오라우팅)는 집계에서 제외 — total·분포에서 빠진다."""
    payload = {"results": [
        {"cs_id": "1", "cause": "사진_색감_오차", "aspect_match": True},
        {"cs_id": "2", "cause": "사진_색감_오차", "aspect_match": True},
        {"cs_id": "3", "cause": "기타", "aspect_match": False},  # 배송불만 오라우팅
    ]}
    client = _FakeClient(payload)
    items = [{"cs_id": str(i), "raw_text": "t"} for i in range(3)]

    r = await diagnose_cause("색상", items, client=client)

    assert r["total"] == 2          # false 1건 제외
    # 인용 경계도 같은 집합이어야 한다 (스키마 §3: inquiry_ids = root_cause.total 건).
    # 걷어낸 문의가 남으면 Agent3 가 다른 aspect 불만을 근거로 인용할 수 있다.
    assert r["cs_ids"] == ["1", "2"]
    assert len(r["cs_ids"]) == r["total"]
    assert "기타" not in r["freq"]