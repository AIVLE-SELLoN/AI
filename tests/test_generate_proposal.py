"""담당: 지인 — pipeline.generate_proposal() 테스트.

실제 OpenAI 호출(과금)은 안 쓴다. get_llm_client()만 모킹해서 프롬프트 조립·응답
파싱·Proposal 조립 로직을 검증한다. copy_draft/image_guide 두 경로 모두 확인 —
서로 다른 프롬프트 파일을 쓰는지, detailpage_grounded가 타입별로 맞게 갈리는지가 핵심.

current_text는 이제 LLM이 만든다(2026-07-27부터) — "근거 원문이라고 주장하는 인용"을
LLM이 내고, evaluate()가 사후에 진짜인지 대조한다. 그래서 여기 있는 assert들은
"LLM이 응답한 대로 Proposal에 그대로 들어가는지"만 본다 — 진짜/할루시네이션 판정은
test_evaluate.py가 담당.
"""

import pytest

from app.core.schemas import ProposalType
from app.recommendation import pipeline


class _FakeLlmClient:
    def __init__(self, response: dict):
        self._response = response
        self.last_prompt: str | None = None
        self.last_temperature: float | None = None

    async def complete_json(self, prompt: str, *, trace_key: str = "-", temperature: float = 0.0) -> dict:
        self.last_prompt = prompt
        self.last_temperature = temperature
        return self._response


def _context(**overrides):
    base = {"detail_text": "아이보리 컬러", "cs_summary": "CS 20건 중 14건이 '사진_색감_오차' 관련 언급", "similar_case": None}
    base.update(overrides)
    return base


@pytest.mark.asyncio
async def test_copy_draft_builds_proposal_from_llm_response(monkeypatch, biased_alert):
    fake_client = _FakeLlmClient(
        {
            "current_text": "아이보리 컬러",
            "proposed_text": "상세페이지에 소재 안내를 추가하세요.",
            "rationale": "원인 분류: 표기_오타",
        }
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    proposal = await pipeline.generate_proposal(biased_alert, ProposalType.COPY_DRAFT, _context())

    assert proposal.type == ProposalType.COPY_DRAFT
    assert proposal.target_field == biased_alert.main_aspect
    assert proposal.current_text == "아이보리 컬러"
    assert proposal.proposed_text == "상세페이지에 소재 안내를 추가하세요."
    assert proposal.detailpage_grounded is True
    assert "아이보리 컬러" in fake_client.last_prompt
    assert "색상" in fake_client.last_prompt


@pytest.mark.asyncio
async def test_copy_draft_marks_ungrounded_when_no_detail_text(monkeypatch, biased_alert):
    fake_client = _FakeLlmClient(
        {"current_text": pipeline.NO_DETAIL_TEXT, "proposed_text": "이미지 확인이 필요합니다.", "rationale": "근거 부족"}
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    context = _context(detail_text=pipeline.NO_DETAIL_TEXT)
    proposal = await pipeline.generate_proposal(biased_alert, ProposalType.COPY_DRAFT, context)

    assert proposal.detailpage_grounded is False


@pytest.mark.asyncio
async def test_image_guide_builds_proposal_using_image_guide_prompt(monkeypatch, biased_alert):
    fake_client = _FakeLlmClient(
        {
            "current_text": "CS 20건 중 14건이 '사진_색감_오차' 관련 언급",
            "proposed_text": "자연광에서 재촬영을 진행하세요.",
            "rationale": "원인 분류: 사진_색감_오차",
        }
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    proposal = await pipeline.generate_proposal(biased_alert, ProposalType.IMAGE_GUIDE, _context())

    assert proposal.type == ProposalType.IMAGE_GUIDE
    assert proposal.current_text == "CS 20건 중 14건이 '사진_색감_오차' 관련 언급"
    assert proposal.detailpage_grounded is False, "image_guide는 상세페이지 근거가 아니므로 항상 False"
    assert "CS 20건 중 14건" in fake_client.last_prompt
    assert "촬영" in fake_client.last_prompt, "image_guide_v1.md 고유 문구 — copy_draft 프롬프트와 구분"


@pytest.mark.asyncio
async def test_retry_includes_previous_failure_and_raised_temperature(monkeypatch, biased_alert):
    """2026-07-27 버그 수정 확인: 재시도가 온도 0으로 같은 프롬프트를 그대로 반복하면
    같은 답이 나올 수밖에 없었다. 실패 사유를 프롬프트에 넣고 temperature도 올려야
    재시도가 실질적인 의미를 가진다.
    """
    fake_client = _FakeLlmClient(
        {"current_text": "아이보리 컬러", "proposed_text": "확인해보세요.", "rationale": "원인 분류: 표기_오타"}
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    await pipeline.generate_proposal(
        biased_alert,
        ProposalType.COPY_DRAFT,
        _context(),
        previous_failure="근거를 원문에서 찾을 수 없습니다: '완전히 다른 색상'",
        temperature=0.4,
    )

    assert fake_client.last_temperature == 0.4
    assert "완전히 다른 색상" in fake_client.last_prompt
    assert "이전 시도" in fake_client.last_prompt


@pytest.mark.asyncio
async def test_first_attempt_has_no_feedback_and_default_temperature(monkeypatch, biased_alert):
    fake_client = _FakeLlmClient(
        {"current_text": "아이보리 컬러", "proposed_text": "확인해보세요.", "rationale": "원인 분류: 표기_오타"}
    )
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    await pipeline.generate_proposal(biased_alert, ProposalType.COPY_DRAFT, _context())

    assert fake_client.last_temperature == 0.0
    assert "이전 시도" not in fake_client.last_prompt
