"""담당: 지인 — pipeline.record_hitl_outcome() 테스트.

실제 ChromaDB는 안 쓴다. get_rejection_reasons()가 반환하는 컬렉션 객체의
upsert() 호출 인자만 모킹해서 확인한다.
"""

import pytest

from app.core.schemas import (
    Citation,
    Evaluator,
    EvaluatorChecks,
    HitlFeedback,
    HitlStatus,
    Proposal,
    ProposalType,
    Recommendation,
    RejectionReason,
    RejectionReasonCode,
)
from app.recommendation import pipeline


class _FakeCollection:
    def __init__(self):
        self.upsert_calls = []

    def upsert(self, *, ids, documents, metadatas):
        self.upsert_calls.append({"ids": ids, "documents": documents, "metadatas": metadatas})


def _recommendation(alert_id: str, hitl_status, hitl_feedback=None) -> Recommendation:
    return Recommendation(
        recommendation_id="REC-HITL-TEST",
        alert_id=alert_id,
        created_at="2026-05-28T10:31:40",
        proposal=Proposal(
            type=ProposalType.IMAGE_GUIDE,
            target_field="색상",
            current_text="CS 20건 중 14건이 '사진_색감_오차' 관련 언급",
            proposed_text="자연광에서 재촬영을 진행하세요.",
            rationale="원인 분류: 사진_색감_오차",
            detailpage_grounded=False,
        ),
        citations=[Citation(inquiry_id="INQ-000412", quote="발췌")],
        evaluator=Evaluator(
            passed=True, attempts=1, checks=EvaluatorChecks(grounding=True, consistency=True, actionability=True)
        ),
        hitl_status=hitl_status,
        hitl_feedback=hitl_feedback,
    )


def test_records_approved_outcome(monkeypatch, biased_alert):
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.APPROVED,
        hitl_feedback=HitlFeedback(processed_at="2026-05-29T09:00:00", processed_by="seller-001"),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    assert len(fake_collection.upsert_calls) == 1
    call = fake_collection.upsert_calls[0]
    assert call["ids"] == ["REC-HITL-TEST"]
    assert "사진_색감_오차" in call["documents"][0]
    # §4-2 스펙: "원인 라벨 + CS 요약 + 개선안 본문" — CS 요약이 실제로 들어가는지 확인
    # (2026-07-27 이전엔 CS 요약 대신 aspect가 들어가던 버그).
    assert "CS 20건 중 14건" in call["documents"][0]
    assert call["metadatas"][0]["outcome"] == "승인"
    assert call["metadatas"][0]["channel"] == "COUPANG"
    assert "rejection_reason_code" not in call["metadatas"][0]


def test_records_rejected_outcome_with_reason(monkeypatch, biased_alert):
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.REJECTED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00",
            processed_by="seller-001",
            rejection_reason=RejectionReason(
                reason_code=RejectionReasonCode.INSUFFICIENT_GROUNDS, reason_text="근거가 약함"
            ),
        ),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    metadata = fake_collection.upsert_calls[0]["metadatas"][0]
    assert metadata["outcome"] == "반려"
    assert metadata["rejection_reason_code"] == "근거부족"
    assert metadata["rejection_reason_text"] == "근거가 약함"


def test_edited_approved_records_the_edited_text(monkeypatch, biased_alert):
    """수정후승인이면 셀러가 고쳐 쓴 문장이 적재된다 — 원래 제안문은 안 들어간다.

    실제 승인 내용과 다른 문장이 "승인" 사례로 쌓이면 컬렉션2가 학습 자료로서
    거짓이 된다. 원래 제안문 부재까지 같이 검사하는 이유: 둘을 이어 붙이는 식으로
    고치면 "수정문이 들어간다"만으로는 통과해버린다.
    """
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.EDITED_APPROVED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00",
            processed_by="seller-001",
            edited_text="자연광에서 재촬영하고 색상 보정은 하지 마세요.",
        ),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    document = fake_collection.upsert_calls[0]["documents"][0]
    assert "색상 보정은 하지 마세요" in document
    assert "자연광에서 재촬영을 진행하세요." not in document
    # 스코프: metadata에 수정후승인 구분값은 이번에 안 넣는다(스키마 확장은 별건).
    assert fake_collection.upsert_calls[0]["metadatas"][0]["outcome"] == "승인"


@pytest.mark.parametrize("hitl_status", [HitlStatus.APPROVED, HitlStatus.REJECTED])
def test_records_proposed_text_when_no_edited_text(monkeypatch, biased_alert, hitl_status):
    """edited_text가 없는 기존 승인·반려 경로는 동작이 안 바뀐다."""
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=hitl_status,
        hitl_feedback=HitlFeedback(processed_at="2026-05-29T09:00:00", processed_by="seller-001"),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    assert "자연광에서 재촬영을 진행하세요." in fake_collection.upsert_calls[0]["documents"][0]


def test_edited_approved_without_edited_text_falls_back_to_proposed_text(monkeypatch, biased_alert):
    """수정후승인인데 백엔드가 edited_text를 안 실어주면 제안문으로 폴백한다.

    edited_text는 선택 필드라 도달 가능한 상태다. 여기서 던지거나 빈 문서를 넣으면
    승인 사례 1건이 통째로 유실된다 — 원문에 가장 가까운 값을 남기는 쪽이 낫다.
    """
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.EDITED_APPROVED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00",
            processed_by="seller-001",
            edited_text=None,
        ),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    assert "자연광에서 재촬영을 진행하세요." in fake_collection.upsert_calls[0]["documents"][0]


def test_raises_when_hitl_status_still_pending(biased_alert):
    recommendation = _recommendation(biased_alert.alert_id, hitl_status=HitlStatus.PENDING)

    with pytest.raises(ValueError, match="PENDING"):
        pipeline.record_hitl_outcome(biased_alert, recommendation)


def test_raises_when_alert_id_mismatch(biased_alert):
    recommendation = _recommendation("ALT-DIFFERENT", hitl_status=HitlStatus.APPROVED)

    with pytest.raises(ValueError, match="다릅니다"):
        pipeline.record_hitl_outcome(biased_alert, recommendation)
