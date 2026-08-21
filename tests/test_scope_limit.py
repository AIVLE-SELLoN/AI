"""담당: 지인 — 스코프 한계 원인(`docs/agent3_logic.md` §4-3) 처리 테스트.

실물_염색_편차·실제_원단_문제는 텍스트·이미지 어느 쪽으로도 해결 안 되는 원인이라
run()이 라우팅·생성을 아예 건너뛰고 고정 문구로 조립한다. LLM을 한 번도 안 부르는지
(get_llm_client가 호출되면 즉시 실패하도록 모킹) + retrieve_context도 안 타는지까지
같이 확인한다.
"""

import pytest

from app.core.schemas import (
    EvaluatorChecks,
    ProposalType,
    RecommendationConfidence,
    RootCause,
)
from app.recommendation import pipeline


def _fail(*args, **kwargs):
    raise AssertionError("스코프 한계 경로는 이 함수를 호출하면 안 됨")


@pytest.mark.asyncio
async def test_run_skips_llm_entirely_for_scope_limit_label(monkeypatch, biased_alert):
    alert = biased_alert.model_copy(
        update={"root_cause": RootCause(label="실물_염색_편차", count=5, total=10, consistent=True)}
    )
    monkeypatch.setattr(pipeline, "get_llm_client", _fail)
    monkeypatch.setattr(pipeline, "retrieve_context", _fail)

    result = await pipeline.run(alert)

    assert result.proposal.type == ProposalType.COPY_DRAFT
    assert result.proposal.proposed_text == pipeline.SCOPE_LIMIT_PROPOSED_TEXT
    assert result.proposal.detailpage_grounded is False
    assert result.recommendation_confidence == RecommendationConfidence.LOW
    assert result.evaluator.passed is True

    # grounding=False가 정직한 기록(대조할 근거 자체가 없음, 예전엔 True로
    # 잘못 기록되던 버그). consistency/actionability는 고정 문구를 실제로 검사한 값 —
    # 우연이 아니라 SCOPE_LIMIT_PROPOSED_TEXT·rationale이 실제로 그 기준을 만족해서 True.
    assert result.evaluator.checks == EvaluatorChecks(grounding=False, consistency=True, actionability=True)
    assert "근거 검증 대상 자체가 없음" in result.evaluator.failure_reason


@pytest.mark.asyncio
async def test_run_skips_llm_for_other_scope_limit_label(monkeypatch, biased_alert):
    alert = biased_alert.model_copy(
        update={"root_cause": RootCause(label="실제_원단_문제", count=5, total=10, consistent=True)}
    )
    monkeypatch.setattr(pipeline, "get_llm_client", _fail)
    monkeypatch.setattr(pipeline, "retrieve_context", _fail)

    result = await pipeline.run(alert)

    assert result.recommendation_confidence == RecommendationConfidence.LOW
