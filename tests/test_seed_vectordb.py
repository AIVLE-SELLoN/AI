"""담당: 지인 — scripts/seed_vectordb.py 회사 범위 격리 테스트.

실제 ChromaDB·임베딩 API 는 안 쓴다(테스트 비용 0 원칙). `upsert_documents` 인자만
잡아서 **회사 축이 문서 ID 와 metadata 에 실제로 실리는지** 본다.

🔴 조회 쪽 반쪽은 `tests/test_retrieve_context.py` 에 있다
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


def _seed(monkeypatch, tmp_path, company=None):
    """시딩을 한 번 돌리고 `upsert_documents` 가 받은 인자를 돌려준다."""
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
    if company is not None:
        monkeypatch.setattr(seed_vectordb, "current_tenant", lambda: company)

    seed_vectordb.main()
    return calls[0]


def test_seed_ids_carry_the_company_axis(monkeypatch, tmp_path):
    """🔴 문서 ID 에 회사 접두어가 붙는다.

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
    """🔴 metadata 에도 같은 값이 실린다 — **조회 필터가 이 키를 본다.**

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
