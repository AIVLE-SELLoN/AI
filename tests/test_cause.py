"""담당: 서영 (Agent2) — [6] 원인 분류 테스트.

judge_cause 는 수제 숫자로(순수), classify_cause·diagnose_cause 는 LLM 을 목킹해 검증한다.
OpenAI 실호출 없음 (비용 0).
"""

import re

import pytest

import app.detection.cause as cause_module
from app.config import get_settings
from app.core.prompts import load_prompt
from app.detection.cause import (
    CAUSE_CHUNK_SIZE,
    CAUSE_MAX_PROMPT_CHARS,
    CAUSE_TAXONOMY,
    CauseValidationError,
    classify_cause,
    diagnose_cause,
    judge_cause,
)
from app.detection.scope import SCOPE_ASPECTS


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
async def test_classify_cause_uses_dedicated_cause_model(monkeypatch):
    """Agent2 [6]만 CAUSE_LLM_MODEL을 쓰고 기본 LLM 모델과 섞이지 않는다."""
    payload = {"results": [
        {"cs_id": "a1", "cause": "사진_색감_오차", "confidence": 0.9,
         "evidence": "사진이랑 색이", "aspect_match": True},
    ]}
    fake_client = _FakeClient(payload)
    requested_models: list[str | None] = []
    settings = get_settings()
    monkeypatch.setattr(settings, "cause_llm_model", "gpt-4o")

    def fake_get_llm_client(*, model=None):
        requested_models.append(model)
        return fake_client

    monkeypatch.setattr(cause_module, "get_llm_client", fake_get_llm_client)

    await cause_module.classify_cause(
        "색상", [{"cs_id": "a1", "raw_text": "사진이랑 색이 달라요"}]
    )

    assert requested_models == ["gpt-4o"]


@pytest.mark.asyncio
async def test_diagnose_cause_consistent():
    ids = [str(i) for i in range(14)] + [f"x{i}" for i in range(6)]
    payload = {"results": [
        {
            "cs_id": cs_id,
            "cause": "사진_색감_오차" if index < 14 else "조명_보정_차이",
            "confidence": 0.9,
            "evidence": "원문",
            "aspect_match": True,
        }
        for index, cs_id in enumerate(ids)
    ]}
    client = _FakeClient(payload)
    items = [{"cs_id": cs_id, "raw_text": "원문"} for cs_id in ids]

    r = await diagnose_cause("색상", items, client=client)

    assert r["label"] == "사진_색감_오차"
    assert r["consistent"] is True
    assert r["count"] == 14
    assert r["total"] == 20


@pytest.mark.asyncio
async def test_diagnose_cause_excludes_aspect_mismatch():
    """aspect_match=false(오라우팅)는 집계에서 제외 — total·분포에서 빠진다."""
    payload = {"results": [
        {"cs_id": "1", "cause": "사진_색감_오차", "confidence": 0.9,
         "evidence": "원문", "aspect_match": True},
        {"cs_id": "2", "cause": "사진_색감_오차", "confidence": 0.9,
         "evidence": "원문", "aspect_match": True},
        {"cs_id": "3", "cause": "기타", "confidence": 0.3,
         "evidence": "", "aspect_match": False},  # 배송불만 오라우팅
    ]}
    client = _FakeClient(payload)
    items = [{"cs_id": str(i), "raw_text": "원문"} for i in range(1, 4)]

    r = await diagnose_cause("색상", items, client=client)

    assert r["total"] == 2          # false 1건 제외
    # 인용 경계도 같은 집합이어야 한다 (스키마 §3: inquiry_ids = root_cause.total 건).
    # 걷어낸 문의가 남으면 Agent3 가 다른 aspect 불만을 근거로 인용할 수 있다.
    assert r["cs_ids"] == ["1", "2"]
    assert len(r["cs_ids"]) == r["total"]
    assert "기타" not in r["freq"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"results": []},
        {"results": [{
            "cs_id": "unknown", "cause": "사진_색감_오차", "confidence": 0.9,
            "evidence": "사진", "aspect_match": True,
        }]},
    ],
)
async def test_classify_cause_rejects_missing_or_unknown_ids(payload):
    client = _FakeClient(payload)
    with pytest.raises(CauseValidationError, match="ID"):
        await classify_cause(
            "색상", [{"cs_id": "a1", "raw_text": "사진이 달라요"}], client=client
        )


@pytest.mark.asyncio
async def test_classify_cause_drops_taxonomy_mismatch_item():
    client = _FakeClient({"results": [{
        "cs_id": "a1", "cause": "실제_원단_문제", "confidence": 0.9,
        "evidence": "사진", "aspect_match": True,
    }]})
    results = await classify_cause(
        "색상", [{"cs_id": "a1", "raw_text": "사진이 달라요"}], client=client
    )

    assert results == []


@pytest.mark.asyncio
async def test_classify_cause_drops_evidence_not_in_source_item():
    client = _FakeClient({"results": [{
        "cs_id": "a1", "cause": "사진_색감_오차", "confidence": 0.9,
        "evidence": "실물과 완전히 다름", "aspect_match": True,
    }]})
    results = await classify_cause(
        "색상", [{"cs_id": "a1", "raw_text": "사진이 달라요"}], client=client
    )

    assert results == []


@pytest.mark.asyncio
async def test_classify_cause_drops_empty_evidence_when_aspect_matches():
    client = _FakeClient({"results": [{
        "cs_id": "a1", "cause": "사진_색감_오차", "confidence": 0.9,
        "evidence": "", "aspect_match": True,
    }]})

    results = await classify_cause(
        "색상", [{"cs_id": "a1", "raw_text": "사진이 달라요"}], client=client
    )

    assert results == []


@pytest.mark.asyncio
async def test_invalid_items_do_not_inflate_consistency_ratio():
    """검증 탈락 6건을 분모에서 지워 5/5 일관으로 만드는 회귀를 막는다."""
    ids = [f"a{i}" for i in range(11)]
    payload = {"results": [
        {
            "cs_id": cs_id,
            "cause": "사진_색감_오차",
            "confidence": 0.9,
            "evidence": "원문" if index < 5 else "원문에 없는 인용",
            "aspect_match": True,
        }
        for index, cs_id in enumerate(ids)
    ]}
    items = [{"cs_id": cs_id, "raw_text": "원문"} for cs_id in ids]

    result = await diagnose_cause("색상", items, client=_FakeClient(payload))

    assert result["total"] == 11
    assert result["attempted_total"] == 11
    assert result["invalid_count"] == 6
    assert result["consistent"] is False
    assert result["label"] is None
    assert result["cs_ids"] == ids


@pytest.mark.asyncio
async def test_classify_cause_rejects_duplicate_input_ids_before_llm():
    client = _FakeClient({"results": []})
    with pytest.raises(CauseValidationError, match="중복"):
        await classify_cause(
            "색상",
            [
                {"cs_id": "a1", "raw_text": "첫 문의"},
                {"cs_id": "a1", "raw_text": "둘째 문의"},
            ],
            client=client,
        )
    assert client.calls == 0


@pytest.mark.asyncio
async def test_classify_cause_rejects_single_item_over_prompt_limit():
    client = _FakeClient({"results": []})
    with pytest.raises(CauseValidationError, match="요청 크기 상한"):
        await classify_cause(
            "색상",
            [{"cs_id": "a1", "raw_text": "x" * CAUSE_MAX_PROMPT_CHARS}],
            client=client,
        )
    assert client.calls == 0


def test_cause_taxonomy_matches_scope_and_prompt():
    """코드·스코프·프롬프트 중 한 곳만 수정되는 taxonomy 드리프트를 막는다."""
    assert set(CAUSE_TAXONOMY) == {aspect.value for aspect in SCOPE_ASPECTS}

    prompt = load_prompt("detection", "classify_cause_v1")
    taxonomy_block = prompt.split("[원인 후보 정의와 판별 단서]", 1)[1].split(
        "[입력 형식]", 1
    )[0]
    sections = re.findall(
        r"^■ (색상|사이즈|소재)\s*$([\s\S]*?)(?=^■ |\Z)",
        taxonomy_block,
        flags=re.MULTILINE,
    )
    prompt_taxonomy = {
        aspect: frozenset(
            label.strip()
            for label in re.findall(r"^- ([^:\n]+?)\s*:", body, flags=re.MULTILINE)
        )
        for aspect, body in sections
    }

    assert prompt_taxonomy == CAUSE_TAXONOMY


class _SequenceClient:
    def __init__(self, payloads):
        self.payloads = iter(payloads)
        self.calls = 0

    async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
        self.calls += 1
        return next(self.payloads)


@pytest.mark.asyncio
async def test_classify_cause_chunks_large_batches_in_order():
    items = [
        {"cs_id": f"a{i}", "raw_text": f"사진이 달라요 {i}"}
        for i in range(CAUSE_CHUNK_SIZE + 1)
    ]

    def payload(rows):
        return {"results": [
            {
                "cs_id": row["cs_id"],
                "cause": "사진_색감_오차",
                "confidence": 0.9,
                "evidence": row["raw_text"],
                "aspect_match": True,
            }
            for row in rows
        ]}

    client = _SequenceClient(
        [payload(items[:CAUSE_CHUNK_SIZE]), payload(items[CAUSE_CHUNK_SIZE:])]
    )
    results = await classify_cause("색상", items, client=client)

    assert client.calls == 2
    assert [row["cs_id"] for row in results] == [row["cs_id"] for row in items]
