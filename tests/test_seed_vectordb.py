"""담당: 지인 — scripts/seed_vectordb.py 회사 범위 격리 테스트.

실제 ChromaDB·임베딩 API 는 안 쓴다(테스트 비용 0 원칙). `upsert_documents` 인자만
잡아서 **회사 축이 문서 ID 와 metadata 에 실제로 실리는지** 본다.

조회 쪽 반쪽은 `tests/test_retrieve_context.py` 에 있다
   (`test_detail_page_lookup_is_scoped_to_the_current_company`). **둘은 짝이고 서로를
   대신하지 못한다** — ID 만 격리하면 조회가 새고, 조회만 막으면 시딩이 서로를 덮는다.
"""

import csv

from app.core.vectordb import TENANT_METADATA_KEY
from scripts import seed_vectordb

ROWS = [
    {
        "product_group_id": "P001",
        "channel": "COUPANG",
        "aspect": "색상",
        "detail_text": "아이보리 컬러",
    },
    {
        "product_group_id": "P002",
        "channel": "NAVER",
        "aspect": "사이즈",
        "detail_text": "총장 95cm",
    },
]


class _FakeCollection:
    name = "detail_pages"


def _seed(monkeypatch, tmp_path, company=None, foreign_rows=()):
    """시딩을 한 번 돌리고 `upsert_documents` 가 받은 인자를 돌려준다.

    `foreign_rows` 는 시딩 직후 구형 문서 리포트(`report_legacy_documents`)가 볼 값이다 —
    `{"metadata": {...}}` 형태이고 `company_id` 키가 **없으면** 구형이라는 뜻이다.
    """
    csv_path = tmp_path / "input_detail_fields.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(ROWS[0]))
        writer.writeheader()
        writer.writerows(ROWS)

    calls: list[dict] = []
    monkeypatch.setattr(seed_vectordb, "CSV_PATH", csv_path)
    monkeypatch.setattr(seed_vectordb, "get_detail_pages", _FakeCollection)
    monkeypatch.setattr(
        seed_vectordb, "upsert_documents", lambda collection, **kwargs: calls.append(kwargs)
    )
    monkeypatch.setattr(
        seed_vectordb,
        "get_documents",
        lambda collection, **kwargs: [dict(r) for r in foreign_rows],
    )
    if company is not None:
        monkeypatch.setattr(seed_vectordb, "current_tenant", lambda: company)

    seed_vectordb.main()
    return calls[0]


def test_seed_ids_carry_the_company_axis(monkeypatch, tmp_path):
    """문서 ID 에 회사 접두어가 붙는다.

    `{product_group_id}:{channel}:{aspect}` 는 **회사 안에서만** 유일하다 —
    `product_group_id` 가 회사별 시퀀스라 A사 P001 과 B사 P001 이 같은 ID 를 받는다.
    그러면 나중 시딩이 앞 회사 상세페이지를 **조용히 덮는다**(유실).
    """
    call = _seed(monkeypatch, tmp_path, company="SLN-aaa")

    assert call["ids"] == [
        "SLN-aaa:P001:COUPANG:색상",
        "SLN-aaa:P002:NAVER:사이즈",
    ], "회사 축이 빠지면 다른 회사 시딩이 서로를 덮습니다"


def test_seed_metadata_carries_the_company_axis(monkeypatch, tmp_path):
    """metadata 에도 같은 값이 실린다 — **조회 필터가 이 키를 본다.**

    ID 접두어만으로는 조회가 안 막힌다(`retrieve_context` 는 metadata `where` 로 좁힌다).
    그래서 ID 테스트와 **별개로** 잡는다 — 한쪽만 지웠을 때 다른 쪽이 통과하면 안 된다.
    """
    call = _seed(monkeypatch, tmp_path, company="SLN-aaa")

    assert [m[TENANT_METADATA_KEY] for m in call["metadatas"]] == ["SLN-aaa", "SLN-aaa"]
    # 기존 축은 그대로 남아야 한다 — 회사 축은 추가지 교체가 아니다.
    assert call["metadatas"][0]["product_group_id"] == "P001"
    assert call["metadatas"][0]["channel"] == "COUPANG"
    assert call["metadatas"][0]["aspect"] == "색상"


def test_two_companies_seeding_the_same_product_do_not_collide(monkeypatch, tmp_path):
    """같은 상품 카탈로그를 회사만 바꿔 시딩해도 문서가 안 겹친다."""
    a = _seed(monkeypatch, tmp_path, company="SLN-aaa")
    b = _seed(monkeypatch, tmp_path, company="SLN-bbb")

    assert set(a["ids"]).isdisjoint(b["ids"])
    assert a["documents"] == b["documents"]  # 같은 카탈로그인데 ID 만 갈린다


def test_seed_uses_the_configured_company_id(monkeypatch, tmp_path):
    """`current_tenant()` 를 실제로 경유한다 — 상수를 박아두지 않았다.

    conftest 의 `pin_company_id` 가 `MQ_COMPANY_ID` 를 `SLN-test` 로 못박으므로, 그 값이
    나오면 설정을 읽고 있다는 뜻이다(개발자 `.env` 값이 새면 이 assert 가 잡는다).
    """
    call = _seed(monkeypatch, tmp_path)

    assert call["ids"][0].startswith("SLN-test:")
    assert call["metadatas"][0][TENANT_METADATA_KEY] == "SLN-test"


# ── 구형 문서 리포트 ───────────────────────────────────────────────
def test_reports_legacy_documents_after_seeding(monkeypatch, tmp_path, capsys):
    """구형 문서 수를 **시딩 직후 전수로** 알려준다.

    런타임(`_log_detail_page_miss`)에서 하지 않는 이유: "이 컬렉션이 구형인가" 는
    알림별이 아니라 **컬렉션 전체의 성질**이라, 미스마다 다시 계산하면 같은 답을 수십 번
    구하고 핫 패스라 전수를 못 봐 표본으로 어림잡게 된다. 여기는 한 번만 돌고 전수를
    보며, 무엇보다 **사람이 이 콘솔 앞에 서 있는 시점**이다.
    """
    _seed(
        monkeypatch,
        tmp_path,
        company="SLN-aaa",
        foreign_rows=[{"metadata": {}}, {"metadata": {}}],
    )

    out = capsys.readouterr().out
    assert "구형 문서 2건" in out
    assert "--all-companies" in out, "확인 방법을 같이 알려줘야 한다"
    # 정리 수단으로 파괴적 명령을 권하지 않는다.
    assert "`--reset` 을 쓰지 마세요" in out


def test_separates_other_company_documents_from_legacy(monkeypatch, tmp_path, capsys):
    """다른 회사 문서는 **구형이 아니다** — 정상이고 조회에서 격리된다.

    `$nin` 이 두 종류를 같이 물어오므로(키 없음 + 다른 회사), metadata 로 갈라야 한다.
    안 가르면 멀쩡한 멀티테넌트 상태를 "구형" 이라고 잘못 보고한다.
    """
    _seed(
        monkeypatch,
        tmp_path,
        company="SLN-aaa",
        foreign_rows=[
            {"metadata": {"company_id": "SLN-bbb"}},
            {"metadata": {}},
        ],
    )

    out = capsys.readouterr().out
    assert "다른 회사 문서 1건" in out
    assert "구형 문서 1건" in out


def test_stays_quiet_when_there_are_no_legacy_documents(monkeypatch, tmp_path, capsys):
    """깨끗한 컬렉션에선 구형 경고를 띄우지 않는다 — 매번 뜨면 아무도 안 읽는다."""
    _seed(monkeypatch, tmp_path, company="SLN-aaa", foreign_rows=[])

    out = capsys.readouterr().out
    assert "구형" not in out


def test_legacy_report_asks_for_metadatas_only(monkeypatch, tmp_path):
    """본문 전송을 없앤다 — `include=["metadatas"]`.

    상세페이지가 건당 700자대라, 빼지 않으면 수십 건만 훑어도 수만 자가 오간다
    (실측: 50건 = 36,090자). 리포트는 metadata 만 있으면 되므로 본문은 낭비다.
    """
    captured: dict = {}

    monkeypatch.setattr(seed_vectordb, "current_tenant", lambda: "SLN-aaa")

    def fake_get_documents(collection, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(seed_vectordb, "get_documents", fake_get_documents)
    seed_vectordb.report_legacy_documents(_FakeCollection(), "SLN-aaa")

    assert captured["include"] == ["metadatas"], "본문까지 끌어오면 로그용으로 과합니다"
    assert captured["where"] == {TENANT_METADATA_KEY: {"$nin": ["SLN-aaa"]}}
