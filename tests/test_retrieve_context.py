"""담당: 지인 — pipeline.retrieve_context() 조회 로직 테스트.

실제 ChromaDB(임베딩 모델 다운로드 필요)는 안 쓴다. vectordb 호출 지점만 모킹해서
"찾음/못찾음" 분기를 검증한다. 라우팅이 LLM 몫(route_proposal_type)으로 넘어가면서
retrieve_context는 어느 타입이 선택될지 모른 채 두 후보 근거(detail_text/cs_summary)를
항상 같이 가져온다. 실제 왕복 확인은 scripts/seed_vectordb.py를 로컬에서 수동
실행해서 한다(tests=비용 0 원칙과 분리).
"""

from app.recommendation import pipeline


def test_returns_detail_text_when_found(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: "detail-collection")
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: "rejection-collection")
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == "아이보리 컬러"
    assert context["similar_case"] is None


def test_falls_back_when_detail_page_missing(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: "detail-collection")
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: "rejection-collection")
    monkeypatch.setattr(pipeline, "get_documents", lambda collection, where: [])
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT


def test_always_includes_cs_summary_regardless_of_detail_page_result(monkeypatch, biased_alert):
    """라우팅 전이라 어느 타입이 뽑힐지 모른다 — cs_summary도 항상 같이 채워야 한다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: "detail-collection")
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: "rejection-collection")
    monkeypatch.setattr(
        pipeline, "get_documents", lambda collection, where: [{"document": "아이보리 컬러"}]
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["cs_summary"] == "CS 20건 중 14건이 '사진_색감_오차' 관련 언급"


def test_returns_top_similar_case_when_found(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: "detail-collection")
    monkeypatch.setattr(pipeline, "get_rejection_reasons", lambda: "rejection-collection")
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
