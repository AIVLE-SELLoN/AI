"""담당: 지인 — scripts/inspect_detail_pages.py 회사 범위 테스트 (서영님 #84 리뷰).

실제 ChromaDB 는 안 쓴다. `collection.get()` 이 받은 `where` 만 잡아서 **기본 덤프가
회사 범위를 지키는지** 본다.

🔴 왜 필요한가 — metadata 격리는 **권한 경계가 아니다**(`docs/vectordb_tenancy.md` §5).
   그래서 저장소가 주는 운영 도구가 기본으로 전건을 뜨면, 공유 컬렉션에서 **다른 회사
   상세페이지 원문이 한 CSV 로 새어 나간다.**
"""

import csv

from scripts import inspect_detail_pages


class _FakeCollection:
    """`get()` 호출 인자를 기록하고 고정 문서를 돌려준다."""

    def __init__(self):
        self.calls: list[dict | None] = []

    def get(self, where=None):
        self.calls.append(where)
        return {
            "ids": ["SLN-test:P001:COUPANG:색상"],
            "metadatas": [
                {
                    "company_id": "SLN-test",
                    "product_group_id": "P001",
                    "channel": "COUPANG",
                    "aspect": "색상",
                }
            ],
            "documents": ["아이보리 컬러"],
        }


def _dump(monkeypatch, tmp_path, *, all_companies):
    collection = _FakeCollection()
    monkeypatch.setattr(inspect_detail_pages, "get_detail_pages", lambda: collection)
    out = tmp_path / "dump.csv"
    inspect_detail_pages.dump(out, all_companies=all_companies)
    return collection, out


def test_default_dump_is_scoped_to_the_current_company(monkeypatch, tmp_path):
    """🔴 기본 덤프는 `company_id` 로 좁힌다 — 안 좁히면 남의 원문이 CSV 로 나간다."""
    collection, _ = _dump(monkeypatch, tmp_path, all_companies=False)

    assert collection.calls == [{"company_id": "SLN-test"}], (
        "기본 덤프에 회사 필터가 없으면 다른 회사 상세페이지 원문이 함께 나갑니다"
    )


def test_all_companies_flag_opens_the_full_dump(monkeypatch, tmp_path):
    """전사 덤프는 **명시적 옵션으로만** 열린다(구형 문서 확인 경로이기도 하다)."""
    collection, _ = _dump(monkeypatch, tmp_path, all_companies=True)

    assert collection.calls == [None], "--all-companies 는 필터 없이 전건을 떠야 한다"


def test_dump_writes_company_id_column(monkeypatch, tmp_path):
    """`company_id` 컬럼이 비어 있으면 구형 문서라는 신호다 — 컬럼 자체를 고정한다."""
    _, out = _dump(monkeypatch, tmp_path, all_companies=False)

    with out.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == inspect_detail_pages.COLUMNS
    assert rows[1][1] == "SLN-test"


def test_cli_defaults_to_scoped_dump(monkeypatch, tmp_path):
    """🔴 배선 확인 — `main()` 이 인자 없이 불리면 **회사 범위**로 돈다.

    `dump()` 만 테스트하면 CLI 기본값이 `--all-companies` 로 뒤집혀도 안 걸린다.
    """
    collection = _FakeCollection()
    monkeypatch.setattr(inspect_detail_pages, "get_detail_pages", lambda: collection)

    inspect_detail_pages.main([str(tmp_path / "cli.csv")])

    assert collection.calls == [{"company_id": "SLN-test"}]
