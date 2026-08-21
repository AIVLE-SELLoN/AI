"""담당: 지인 — 근거없음 경로(fallback_guide) 통합 테스트.

grounding이 MAX_RETRY번 다 실패하면 run()이 generate_fallback_proposal()로
넘어가는지 오케스트레이션 레벨에서 검증한다. LLM(fake)이 매번 근거와 안 맞는
current_text를 내서 실제로 재시도가 소진되게 만든 뒤, 마지막에 일반 가이드로
대체되는지 확인 — 이 테스트가 통과하려면 evaluate()가 진짜로 실패할 수 있어야
하므로(할루시네이션 검증, test_evaluate.py) 그 수정이 선행돼야 의미가 있다.
"""

import pytest

from app.core.schemas import EvaluatorChecks, HitlStatus, RecommendationConfidence
from app.recommendation import pipeline


class _FakeHallucinatingClient:
    """라우팅은 항상 copy_draft, 생성은 근거와 안 맞는 current_text만 계속 낸다.

    MAX_ATTEMPTS(=MAX_RETRY+1)번째 complete_json 호출까지는 할루시네이션 응답,
    그 다음 호출(=fallback_guide 프롬프트)부터는 정상적인 일반 가이드 응답을 낸다.
    """

    def __init__(self):
        self.complete_json_call_count = 0

    async def choose_tool(self, prompt: str, *, tools, trace_key: str = "-", temperature: float = 0.0):
        return {"name": "use_copy_draft", "arguments": {"reason": "테스트"}}

    async def complete_json(self, prompt: str, *, trace_key: str = "-", temperature: float = 0.0) -> dict:
        self.complete_json_call_count += 1
        if self.complete_json_call_count <= pipeline.MAX_ATTEMPTS:
            return {
                "current_text": "근거 원문과 전혀 다른 할루시네이션 문구",
                "proposed_text": "테스트 제안",
                "rationale": "테스트",
            }
        return {"proposed_text": "상세페이지 내용과 실제 상품이 일치하는지 확인해보세요.", "rationale": "근거를 특정하지 못해 일반 가이드로 대체"}


def _stub_context(alert, inquiries=()):
    return {
        "detail_text": "아이보리 컬러",
        "cs_quotes": "무관",
        "cs_summary": "무관",
        "similar_case": None,
    }


@pytest.mark.asyncio
async def test_run_falls_back_to_general_guide_after_grounding_exhausted(monkeypatch, biased_alert):
    fake_client = _FakeHallucinatingClient()
    monkeypatch.setattr(pipeline, "retrieve_context", _stub_context)
    monkeypatch.setattr(pipeline, "get_llm_client", lambda: fake_client)

    result = await pipeline.run(biased_alert)

    # MAX_ATTEMPTS번 생성 시도 + fallback 1번 = 총 MAX_ATTEMPTS+1번 complete_json 호출
    assert fake_client.complete_json_call_count == pipeline.MAX_ATTEMPTS + 1

    assert result.proposal.current_text == pipeline.NO_DETAIL_TEXT
    assert result.proposal.detailpage_grounded is False
    assert result.proposal.proposed_text == "상세페이지 내용과 실제 상품이 일치하는지 확인해보세요."

    # passed=True(더 이상 재시도 안 함)이지만 checks는 실제로 계산해서 정직하게 기록한다.
    # fallback 프롬프트는 원인 라벨을 일부러 인용하지 말라고 시키므로 consistency는
    # 실제로 False가 나오는 게 맞다(예전엔 True로 하드코딩돼있던 버그).
    assert result.evaluator.passed is True
    assert result.evaluator.checks == EvaluatorChecks(grounding=False, consistency=False, actionability=True)
    assert "일반 가이드" in result.evaluator.failure_reason

    # 근거도 없고 similar_case도 없어서 확신도는 낮음으로 자연히 떨어진다.
    assert result.recommendation_confidence == RecommendationConfidence.LOW
    assert result.hitl_status == HitlStatus.PENDING
