"""담당: 지인 — pipeline.retrieve_context() 조회 로직 테스트.

실제 ChromaDB(임베딩 모델 다운로드 필요)는 안 쓴다. vectordb 호출 지점만 모킹해서
"찾음/못찾음" 분기를 검증한다. 라우팅이 LLM 몫(route_proposal_type)으로 넘어가면서
retrieve_context는 어느 타입이 선택될지 모른 채 두 후보 근거(detail_text/cs_summary)를
항상 같이 가져온다. 실제 왕복 확인은 scripts/seed_vectordb.py를 로컬에서 수동
실행해서 한다(tests=비용 0 원칙과 분리).
"""

import logging

from app.recommendation import pipeline


class FakeCollection:
    """Chroma 컬렉션 대역. `count()` 만 있으면 되는 이유는 pipeline 이 조회 0건일 때
    "컬렉션이 통째로 비었는가"를 그것으로만 가르기 때문이다(_log_detail_page_miss)."""

    def __init__(self, count: int = 1) -> None:
        self._count = count

    def count(self) -> int:
        return self._count


def test_returns_detail_text_when_found(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == "아이보리 컬러"
    assert context["similar_case"] is None


def test_falls_back_when_detail_page_missing(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", lambda collection, where: [])
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT


def test_warns_when_detail_collection_is_empty(monkeypatch, biased_alert, caplog):
    """컬렉션이 통째로 비면(시딩 누락) 조회 0건과 같은 모양이라 로그로 갈라줘야 한다.

    `.chroma/` 가 gitignore 라 각자 로컬이 곧 환경이고, 시딩 전에는 전건이 0건으로
    나온다 — 상품 등록 문제로 오진하면 엉뚱한 데를 파게 된다.
    """
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: FakeCollection(count=0))
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", lambda collection, where: [])
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT
    assert "seed_vectordb" in caplog.text


def test_does_not_warn_when_only_this_product_is_unregistered(monkeypatch, biased_alert, caplog):
    """컬렉션에 다른 상품이 들어 있으면 시딩은 된 것이다 — 경고 대상이 아니다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: FakeCollection(count=504))
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", lambda collection, where: [])
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        pipeline.retrieve_context(biased_alert)

    assert caplog.text == ""


def test_always_includes_cs_summary_regardless_of_detail_page_result(monkeypatch, biased_alert):
    """라우팅 전이라 어느 타입이 뽑힐지 모른다 — cs_summary도 항상 같이 채워야 한다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["cs_summary"] == "CS 20건 중 14건이 '사진_색감_오차' 관련 언급"


def _stub_vectordb(monkeypatch):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])


def test_cs_quotes_and_cs_summary_are_separate_slots(monkeypatch, biased_alert, linked_inquiries):
    """🔴 둘을 한 문자열로 합치면 image_guide grounding 이 다시 자기참조가 된다.

    cs_summary 는 우리가 만든 문장이라 grounding 대조 대상에 섞이면 안 된다
    (2026-08-09 수정). 슬롯 분리를 계약으로 고정한다.
    """
    _stub_vectordb(monkeypatch)

    context = pipeline.retrieve_context(biased_alert, linked_inquiries)

    assert "사진이랑 색이 너무 달라요" in context["cs_quotes"]
    assert "조명 때문인지" in context["cs_quotes"]
    assert "CS 20건 중" not in context["cs_quotes"], "통계 문장이 근거에 섞이면 자기참조가 된다"
    assert "CS 20건 중" in context["cs_summary"]


def test_cs_quotes_is_no_detail_text_without_inquiries(monkeypatch, biased_alert):
    """원문이 없으면 '없음'을 정직하게 표시한다 — 통계 요약으로 대신 채우지 않는다."""
    _stub_vectordb(monkeypatch)

    context = pipeline.retrieve_context(biased_alert)

    assert context["cs_quotes"] == pipeline.NO_DETAIL_TEXT


def test_returns_top_similar_case_when_found(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(
        pipeline,
        "query_documents",
        lambda collection, **kwargs: [{"document": "지난 4월 미디원피스A, 재촬영 진행 → 정상화"}],
    )

    context = pipeline.retrieve_context(biased_alert)

    assert context["similar_case"] == "지난 4월 미디원피스A, 재촬영 진행 → 정상화"
