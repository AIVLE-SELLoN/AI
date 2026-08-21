"""담당: 지인 — pipeline.record_hitl_outcome() 테스트.

실제 ChromaDB는 안 쓴다. get_rejection_reasons()가 반환하는 컬렉션 객체의
upsert() 호출 인자만 모킹해서 확인한다.
"""

import pytest

from app.config import get_settings
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

from .conftest import TEST_COMPANY_ID


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
    # 회사 축이 붙는다(vectordb.scoped_document_id). 값은 conftest 의 `pin_company_id`
    # 가 고정한다 — 개발자 `.env` 를 보면 사람마다 결과가 갈린다(그 픽스처 docstring).
    assert call["ids"] == [f"{TEST_COMPANY_ID}:REC-HITL-TEST"]
    assert call["metadatas"][0]["company_id"] == TEST_COMPANY_ID
    assert "사진_색감_오차" in call["documents"][0]
    # `docs/agent3_logic.md` §4-2: "원인 라벨 + CS 요약 + 개선안 본문" — CS 요약 확인
    # (예전엔 CS 요약 대신 aspect가 들어가던 버그).
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


@pytest.mark.parametrize("hitl_status", [HitlStatus.EDITED_APPROVED, HitlStatus.APPROVED])
def test_approved_records_the_edited_text(monkeypatch, biased_alert, hitl_status):
    """승인 계열이면 셀러가 고쳐 쓴 문장이 적재된다 — 원래 제안문은 안 들어간다.

    실제 승인 내용과 다른 문장이 "승인" 사례로 쌓이면 컬렉션2가 학습 자료로서
    거짓이 된다. 원래 제안문 부재까지 같이 검사하는 이유: 둘을 이어 붙이는 식으로
    고치면 "수정문이 들어간다"만으로는 통과해버린다.

    `승인`도 같이 도는 이유: 정예시 자리에 들어갈 건 "셀러가 최종 승인한 문장"이지
    상태 이름이 아니다(백엔드가 수정본을 `수정후승인` 아닌 `승인`으로 표시해도 그
    문장을 살려야 한다). 이 파라미터가 없으면 가드를 `== EDITED_APPROVED`로 좁히는
    변경이 아무 테스트도 안 물고 통과한다.
    """
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=hitl_status,
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


@pytest.mark.parametrize("edited_text", [None, "", "   "])
def test_edited_approved_without_usable_edited_text_falls_back(monkeypatch, biased_alert, edited_text):
    """수정후승인인데 쓸 수 있는 edited_text가 없으면 제안문으로 폴백한다.

    edited_text는 선택 필드에 길이 제약도 없어 셋 다 도달 가능하다(백엔드 미전달 /
    셀러가 입력칸을 비우고 저장). 여기서 던지거나 빈 문서를 넣으면 승인 사례 1건이
    통째로 유실된다 — 원문에 가장 가까운 값을 남기는 쪽이 낫다.

    공백 케이스를 같이 도는 이유: truthy 판정만으로는 "   "가 통과해 문서에서
    개선안 본문이 통째로 빠진다(CS 요약에서 끝난다). 이 파라미터가 그 정규화를 고정한다.
    """
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.EDITED_APPROVED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00",
            processed_by="seller-001",
            edited_text=edited_text,
        ),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    assert "자연광에서 재촬영을 진행하세요." in fake_collection.upsert_calls[0]["documents"][0]


def test_rejected_keeps_proposed_text_even_if_edited_text_arrives(monkeypatch, biased_alert):
    """반려는 edited_text가 실려 와도 우리 제안문을 적재한다.

    부예시의 뜻은 "이런 제안이 거절당했다"라 본문이 우리 제안문이어야 한다. 셀러가
    승인 의도로 쓴 문장이 반려 사례로 들어가면, 이 파일이 고친 버그(승인 사례에 실제
    승인 안 된 문장이 들어감)와 같은 모양이 반려 쪽에 생긴다.
    스키마상 반려 시 edited_text는 null이지만(recommendation_schema.md §3) 계약 위반이
    실제로 오면 조용히 오염되므로 코드에서 막는다.
    """
    fake_collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: fake_collection)

    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.REJECTED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00",
            processed_by="seller-001",
            edited_text="셀러가 승인 의도로 고쳐 쓴 문장",
            rejection_reason=RejectionReason(reason_code=RejectionReasonCode.DIFFERENT_CAUSE),
        ),
    )

    pipeline.record_hitl_outcome(biased_alert, recommendation)

    document = fake_collection.upsert_calls[0]["documents"][0]
    assert "자연광에서 재촬영을 진행하세요." in document
    assert "셀러가 승인 의도로" not in document
    assert fake_collection.upsert_calls[0]["metadatas"][0]["outcome"] == "반려"


def test_raises_when_hitl_status_still_pending(biased_alert):
    recommendation = _recommendation(biased_alert.alert_id, hitl_status=HitlStatus.PENDING)

    with pytest.raises(ValueError, match="PENDING"):
        pipeline.record_hitl_outcome(biased_alert, recommendation)


def test_raises_when_alert_id_mismatch(biased_alert):
    recommendation = _recommendation("ALT-DIFFERENT", hitl_status=HitlStatus.APPROVED)

    with pytest.raises(ValueError, match="다릅니다"):
        pipeline.record_hitl_outcome(biased_alert, recommendation)


# ── 회사 범위 격리 ─────────────────────────────────────────────────
def _two_company_upserts(monkeypatch, biased_alert, companies):
    """같은 논리 알림을 회사만 바꿔 두 번 적재하고 upsert 인자를 돌려준다.

    `recommendation_id` 는 `alert_id` 파생이라 **회사 안에서만 유일**하다 — 두 회사가
    같은 (window_end, 상품, aspect, 채널) 조합을 만들면 같은 값이 나온다. 그래서 그
    값을 벡터DB 문서 ID 로 **그대로** 쓰면 나중 회사가 앞의 회사를 덮는다.
    """
    collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: collection)
    recommendation = _recommendation(
        biased_alert.alert_id,
        hitl_status=HitlStatus.APPROVED,
        hitl_feedback=HitlFeedback(
            processed_at="2026-05-29T09:00:00", processed_by="seller-001"
        ),
    )

    for company in companies:
        monkeypatch.setattr(pipeline, "current_tenant", lambda c=company: c)
        pipeline.record_hitl_outcome(biased_alert, recommendation)

    return collection.upsert_calls


def test_two_companies_with_the_same_alert_do_not_overwrite_each_other(
    monkeypatch, biased_alert
):
    """회사가 다르면 **같은 `recommendation_id` 라도 문서가 안 겹친다.**

    `product_group_id` 가 회사별 시퀀스라 A사에도 `P001`, B사에도 `P001` 이 있다.
    백엔드는 `(companyId, alert_id)` 복합 유니크로 흡수하지만 **벡터DB엔 그 축이
    없었다** — 이 테스트가 없으면 회사 축을 지워도 조용히 통과한다(문서가 1건이 되고,
    그게 곧 앞 회사 데이터 유실이다).
    """
    calls = _two_company_upserts(monkeypatch, biased_alert, ["SLN-aaa", "SLN-bbb"])

    ids = [call["ids"][0] for call in calls]
    assert ids == [
        "SLN-aaa:REC-HITL-TEST",
        "SLN-bbb:REC-HITL-TEST",
    ], "회사 축이 빠져 두 회사 문서가 같은 ID 를 받았습니다 — 나중 것이 앞엣것을 덮습니다"
    assert len(set(ids)) == 2

    # metadata 로도 갈린다 — 조회 필터가 이 키를 본다(retrieve_context).
    assert [call["metadatas"][0]["company_id"] for call in calls] == [
        "SLN-aaa",
        "SLN-bbb",
    ]


def test_same_company_reupsert_stays_one_document(monkeypatch, biased_alert):
    """반대편 — **같은 회사**의 재전달은 문서가 하나여야 한다.

    회사 축을 넣었다고 멱등성이 깨지면 안 된다. 컨슈머가 중복 제거를 안 하고
    `recommendation_id` upsert 에 기대고 있으므로(`mq_consumer` docstring), 같은 회사
    같은 개선안이 두 번 와도 ID 가 같아야 한다.
    """
    calls = _two_company_upserts(monkeypatch, biased_alert, ["SLN-aaa", "SLN-aaa"])

    ids = [call["ids"][0] for call in calls]
    assert ids == ["SLN-aaa:REC-HITL-TEST", "SLN-aaa:REC-HITL-TEST"]
    assert len(set(ids)) == 1  # 같은 ID → Chroma 에서 한 문서로 덮인다


def test_local_fallback_when_company_id_is_unset(monkeypatch, biased_alert):
    """`MQ_COMPANY_ID` 가 비어 있으면 `_local` 로 떨어진다 — 빈 접두어를 만들지 않는다.

    개발·테스트 환경의 기본값이 `""` 라(`config.mq_company_id`) 폴백이 없으면 문서 ID 가
    `:REC-…` 가 된다. 운영은 배포마다 실제 값이 박혀 이 경로를 안 탄다.

    conftest 의 `pin_company_id` 가 고정한 값을 **여기서만 덮어쓴다** — 폴백을 재려면
       설정이 비어 있어야 하기 때문이다.
    """
    monkeypatch.setattr(get_settings(), "mq_company_id", "")
    collection = _FakeCollection()
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: collection)

    pipeline.record_hitl_outcome(
        biased_alert,
        _recommendation(
            biased_alert.alert_id,
            hitl_status=HitlStatus.APPROVED,
            hitl_feedback=HitlFeedback(
                processed_at="2026-05-29T09:00:00", processed_by="seller-001"
            ),
        ),
    )

    call = collection.upsert_calls[0]
    assert call["ids"] == ["_local:REC-HITL-TEST"]
    assert call["metadatas"][0]["company_id"] == "_local"
