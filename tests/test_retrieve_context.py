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


def fake_get_documents(*, product_docs, tenant_docs=({"document": "다른 상품"},)):
    """`get_documents` 대역. **컬렉션1 조회는 두 종류라 갈라줘야 한다.**

    | `where` 모양 | 무엇 | 부르는 곳 |
    |---|---|---|
    | `$and`(회사+상품+채널+aspect) | 상품 조회 | `_get_detail_page_text` |
    | `{company_id: "SLN-…"}` | 이 회사 문서가 있나 | `_log_detail_page_miss` |

    `tenant_docs` 기본값을 비우지 않는 이유: 대부분의 테스트가 재려는 건 "시딩은 정상인데
    이 상품만 없다" 라서, 회사 문서가 있는 쪽이 기본 전제여야 한다.

    ⚠️ **세 번째 조회(구형 판별)는 여기 없다 — 일부러 없앴다.** 그건 컬렉션 전체의 성질이라
       미스마다 다시 계산할 게 아니고, 지금은 `scripts/seed_vectordb.py` 가 시딩 직후
       전수로 본다(`tests/test_seed_vectordb.py::test_reports_legacy_documents…`).
    """

    def _inner(collection, where, limit=None, include=None):
        if "$and" in where:
            return [dict(doc) for doc in product_docs]
        return [dict(doc) for doc in tenant_docs]

    return _inner


def test_returns_detail_text_when_found(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", fake_get_documents(product_docs=[{"document": "아이보리 컬러"}])
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == "아이보리 컬러"
    assert context["similar_case"] is None


def test_falls_back_when_detail_page_missing(monkeypatch, biased_alert):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", fake_get_documents(product_docs=[]))
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
    monkeypatch.setattr(pipeline, "get_documents", fake_get_documents(product_docs=[]))
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        context = pipeline.retrieve_context(biased_alert)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT
    assert "seed_vectordb" in caplog.text


def test_does_not_warn_when_only_this_product_is_unregistered(monkeypatch, biased_alert, caplog):
    """컬렉션에 다른 상품이 들어 있으면 시딩은 된 것이다 — 경고 대상이 아니다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: FakeCollection(count=504))
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", fake_get_documents(product_docs=[]))
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        pipeline.retrieve_context(biased_alert)

    assert caplog.text == ""


def test_always_includes_cs_summary_regardless_of_detail_page_result(monkeypatch, biased_alert):
    """라우팅 전이라 어느 타입이 뽑힐지 모른다 — cs_summary도 항상 같이 채워야 한다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", fake_get_documents(product_docs=[{"document": "아이보리 컬러"}])
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    context = pipeline.retrieve_context(biased_alert)

    assert context["cs_summary"] == "CS 20건 중 14건이 '사진_색감_오차' 관련 언급"


def _stub_vectordb(monkeypatch):
    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", fake_get_documents(product_docs=[{"document": "아이보리 컬러"}])
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
        pipeline, "get_documents", fake_get_documents(product_docs=[{"document": "아이보리 컬러"}])
    )
    monkeypatch.setattr(
        pipeline,
        "query_documents",
        lambda collection, **kwargs: [{"document": "지난 4월 미디원피스A, 재촬영 진행 → 정상화"}],
    )

    context = pipeline.retrieve_context(biased_alert)

    assert context["similar_case"] == "지난 4월 미디원피스A, 재촬영 진행 → 정상화"


# ── 회사 범위 격리 — 컬렉션1 조회 경로 ─────────────────────────────
def test_detail_page_lookup_is_scoped_to_the_current_company(monkeypatch, biased_alert):
    """🔴 컬렉션1 조회가 **회사 축까지** 좁힌다.

    `product_group_id` 는 회사별 시퀀스라 A사에도 `P001`, B사에도 `P001` 이 있다. 이
    필터가 없으면 **다른 회사 상세페이지**가 개선안의 인용 근거가 되고, 그 문장이
    `citations` 에 박제돼 셀러 화면까지 나간다.

    ⚠️ 시딩 ID 격리만으로는 이걸 못 막는다 — ID 가 안 겹쳐도 조회는 그대로 뚫린다.
       그래서 **시딩 테스트와 별개로** `where` 인자 자체를 잡아서 본다
       (`tests/test_seed_vectordb.py` 가 쓰기 쪽 반쪽).
    """
    captured: dict = {}

    def capturing_get_documents(collection, where, limit=None):
        if "$and" in where:
            captured["where"] = where
        return [{"document": "아이보리 컬러"}]

    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", capturing_get_documents)
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])
    monkeypatch.setattr(pipeline, "current_tenant", lambda: "SLN-aaa")

    pipeline.retrieve_context(biased_alert)

    assert {"company_id": "SLN-aaa"} in captured["where"]["$and"], (
        "컬렉션1 조회에 회사 필터가 없으면 다른 회사 상세페이지를 근거로 인용합니다"
    )


def _run_with_no_tenant_documents(monkeypatch, biased_alert, caplog):
    """컬렉션엔 문서가 있는데 **현재 회사 것만 0건**인 상태를 만든다."""
    monkeypatch.setattr(pipeline, "get_detail_pages", lambda: FakeCollection(count=504))
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(
        pipeline, "get_documents", fake_get_documents(product_docs=[], tenant_docs=[])
    )
    monkeypatch.setattr(pipeline, "query_documents", lambda collection, **kwargs: [])

    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        return pipeline.retrieve_context(biased_alert)


def test_warns_when_collection_has_no_documents_for_this_company(
    monkeypatch, biased_alert, caplog
):
    """🔴 회사 축 도입이 만든 **새 실패 모드** — 축 없이 시딩된 컬렉션.

    옛 문서엔 `company_id` metadata 가 없어서 조회 필터가 **전건을 걸러낸다**. 504건이
    멀쩡히 들어 있으니 `count()` 는 0이 아니고, 그래서 옛 코드였다면 **"상세페이지
    미등록"(INFO)** 으로 조용히 오진했다 — 사람은 상품 등록 쪽을 파는데 실제 조치는
    시딩이다. 팀 전원이 시딩해야 하므로 누군가는 반드시 이 상태를 만난다.
    """
    context = _run_with_no_tenant_documents(monkeypatch, biased_alert, caplog)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT
    assert "seed_vectordb.py" in caplog.text, "시딩이 조치라는 걸 로그가 말해줘야 한다"


def test_never_recommends_reset_in_the_miss_path(monkeypatch, biased_alert, caplog):
    """🔴 **`--reset` 을 안내하면 안 된다 (서영님 #84 리뷰).**

    "현재 회사 문서 0건" 은 두 상태가 **같은 모양**이다 — ① 구형 문서만 있음 ② A사는
    정상이고 **새로 붙은 B사만** 아직 없음. ②에서 `--reset` 을 안내대로 실행하면
    `detail_pages` 뿐 아니라 **`rejection_reasons`(HITL 반려 이력)와 다른 회사 문서까지**
    지워진다. 이번 변경은 임베딩 모델 변경이 아니라 **일반 시딩이면 복구된다**(실측).

    ⚠️ 런타임에선 ①·②를 **구분하지 않는다**(구분은 시딩 스크립트가 전수로 한다). 그래서
       문구가 두 경우 모두에 참이어야 하고, 이 테스트가 그걸 고정한다 — 없으면 파괴적
       안내가 조용히 되돌아온다.
    """
    context = _run_with_no_tenant_documents(monkeypatch, biased_alert, caplog)

    assert context["detail_text"] == pipeline.NO_DETAIL_TEXT
    assert "seed_vectordb.py" in caplog.text
    # 두 상태 모두에 참인 문구여야 한다 — 한쪽을 단정하지 않는다.
    assert "회사 축 없이 시딩됐거나 이 회사가 처음입니다" in caplog.text
    # 🔴 핵심 단언 — 파괴적 명령을 "실행하라" 고 안내하지 않는다.
    assert "--reset` 으로" not in caplog.text
    assert "`--reset` 은 쓰지 마세요" in caplog.text


# ── 회사 범위 격리 — 조회 경로 (서영님 PR #77 리뷰) ────────────────
def test_similar_case_query_is_scoped_to_the_current_company(monkeypatch, biased_alert):
    """🔴 컬렉션2 조회가 **aspect 와 회사 축을 같이** 좁힌다.

    문서 ID 에 회사 접두어가 붙는 것만으로는 **조회가 안 막힌다** — 예전 필터는
    `where={"aspect": ...}` 하나뿐이라 다른 회사의 반려 사례가 `similar_case` 로
    새어 나왔다. ID 격리와 조회 격리는 **짝이고, 이건 그 나머지 반쪽**이다.

    ⚠️ 필터를 지워도 컬렉션2 가 비어 있으면(운영 현재 0건) 아무 테스트도 안 깨진다 —
       그래서 `where` 인자 자체를 잡아서 본다.
    """
    captured: dict = {}

    monkeypatch.setattr(pipeline, "get_detail_pages", FakeCollection)
    monkeypatch.setattr(pipeline, "get_rejection_reasons", FakeCollection)
    monkeypatch.setattr(pipeline, "get_documents", fake_get_documents(product_docs=[]))
    monkeypatch.setattr(pipeline, "current_tenant", lambda: "SLN-aaa")

    def fake_query(collection, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(pipeline, "query_documents", fake_query)

    pipeline.retrieve_context(biased_alert)

    assert captured["where"] == {
        "$and": [
            {"aspect": "색상"},
            {"company_id": "SLN-aaa"},
        ]
    }, "조회에 회사 필터가 없으면 다른 회사 반려 사례가 similar_case 로 새어 나옵니다"
