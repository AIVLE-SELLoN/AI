"""`scripts/build_mapped_data_input.py` — 변환기의 입력 계약.

이 변환기가 막으려는 실패는 **부분 매핑**이다. 매핑이 한 행이라도 빠지면 그 채널상품은
`mock_producer._resolve_group` 에서 채널상품 ID 로 폴백되고, 한 상품이 채널마다 다른
그룹으로 갈려 채널 간 비교가 사라진다. 파일은 멀쩡해 보여서 아무도 모른다.

그래서 여기서 고정하는 건 "정상 입력이 변환되는가"보다 **"불완전한 입력에서 부분 파일이
안 나오는가"** 다. 조용히 성공하는 경우가 하나라도 있으면 변환기가 원래 문제를 다시
만든다(2026-08-13 서영님 지적 — 실제로 그랬다).
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts import build_mapped_data_input as builder


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _golden(tmp_path: Path, body: str, header: str = "variant_row_id,golden_group_id") -> Path:
    return _write(tmp_path / "golden_mapping.csv", f"{header}\n{body}")


def _catalog(tmp_path: Path, variants: list[str]) -> Path:
    body = "\n".join(f"{v},COUPANG,C{i:04d}" for i, v in enumerate(variants))
    return _write(
        tmp_path / "input_channel_products.csv",
        f"variant_row_id,channel,channel_product_id\n{body}\n",
    )


# ── 정상 변환 ────────────────────────────────────────────────────


def test_renames_only_the_group_column(tmp_path: Path) -> None:
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\nV2,P1\nV3,P2\n"))

    assert rows == [
        {"variant_row_id": "V1", "product_group_id": "P1"},
        {"variant_row_id": "V2", "product_group_id": "P1"},
        {"variant_row_id": "V3", "product_group_id": "P2"},
    ]


def test_scenario_metadata_is_not_carried_over(tmp_path: Path) -> None:
    """🔴 `canonical_option`·`mock_scenario_tag` 는 운영 테이블에 있을 수 없는 값이다.

    들고 오면 목이 운영보다 많이 아는 상태가 된다.
    """
    src = _golden(
        tmp_path,
        "V1,P1,블랙/M,SC-030\n",
        header="variant_row_id,golden_group_id,canonical_option,mock_scenario_tag",
    )

    assert set(builder.build_rows(src)[0]) == {"variant_row_id", "product_group_id"}


# ── 🔴 불완전한 입력은 부분 파일을 만들지 않는다 ────────────────


@pytest.mark.parametrize(
    "body, blank_column",
    [
        ("V1,P1\nV2,\n", "golden_group_id"),
        ("V1,P1\n,P2\n", "variant_row_id"),
        ("V1,P1\nV2,   \n", "golden_group_id"),  # 공백만 있는 것도 빈 값이다
    ],
)
def test_blank_field_fails_with_line_number(tmp_path: Path, body: str, blank_column: str) -> None:
    """🔴 예전엔 조건절에서 조용히 걸러 종료코드 0 으로 부분 파일이 나왔다.

    빠진 variant 는 producer 에서 폴백되므로, 변환기가 원래 문제를 다시 만드는 셈이었다.
    """
    with pytest.raises(SystemExit) as exc:
        builder.build_rows(_golden(tmp_path, body))

    message = str(exc.value)
    assert blank_column in message
    assert ":3 —" in message  # 헤더가 1행이라 문제의 데이터는 3행


def test_short_row_is_treated_as_blank(tmp_path: Path) -> None:
    """컬럼이 아예 없는 짧은 행도 빈 값과 같이 다룬다 — DictReader 가 None 을 준다."""
    with pytest.raises(SystemExit):
        builder.build_rows(_golden(tmp_path, "V1,P1\nV2\n"))


# ── 헤더 ─────────────────────────────────────────────────────────


def test_missing_required_column_names_it(tmp_path: Path) -> None:
    """🔴 헤더가 틀리면 전 행이 '빈 값'으로 보여 사유가 엉뚱해진다 — 먼저 잡는다.

    ⚠️ **헤더 검사 고유의 신호를 본다.** 처음엔 메시지에 `golden_group_id` 가 들어가는지만
       봤는데, 그건 헤더 검사를 통째로 지워도 통과한다 — 컬럼이 없으면 모든 행이 빈 값이
       되어 아래 빈 값 검사가 같은 컬럼명을 찍기 때문이다(변이 검증에서 새어 나갔다).
       두 경로를 가르는 건 "몇 행이 틀렸다"가 아니라 "헤더가 틀렸다"는 사유와 실제 헤더
       목록이다.
    """
    src = _golden(tmp_path, "V1,P1\n", header="variant_row_id,product_group_id")

    with pytest.raises(SystemExit) as exc:
        builder.build_rows(src)

    message = str(exc.value)
    assert "필수 컬럼" in message
    assert "헤더:" in message  # 실제로 들어온 헤더를 같이 보여준다
    assert "golden_group_id" in message
    assert "product_group_id" in message


def test_header_only_file_fails(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="0건"):
        builder.build_rows(_golden(tmp_path, ""))


# ── 중복 ─────────────────────────────────────────────────────────


def test_duplicate_variant_with_different_group_fails(tmp_path: Path) -> None:
    """PRIMARY KEY 라 적재 시 INSERT OR REPLACE 로 덮어써진다 — 어느 쪽이 남는지 모른다."""
    with pytest.raises(SystemExit) as exc:
        builder.build_rows(_golden(tmp_path, "V1,P1\nV1,P2\n"))

    assert "V1" in str(exc.value)
    assert "P1" in str(exc.value) and "P2" in str(exc.value)


def test_duplicate_variant_with_same_group_also_fails(tmp_path: Path) -> None:
    """🔴 그룹이 같아도 죽인다.

    지금 결과가 같다고 넘기면 원본이 1:1 이라는 전제가 깨진 걸 아무도 모르고, 나중에
    한쪽만 고쳐져 갈라질 때 비로소 드러난다. 그때는 어느 행이 맞는지 알 수 없다.
    """
    with pytest.raises(SystemExit) as exc:
        builder.build_rows(_golden(tmp_path, "V1,P1\nV1,P1\n"))

    assert "중복" in str(exc.value)


# ── products 대본과 variant 집합 대조 ───────────────────────────


def test_variant_missing_from_mapping_fails(tmp_path: Path) -> None:
    """🔴 products 에 있는데 매핑에 없으면 그 채널상품이 폴백된다 — 이게 원래 그 실패다."""
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\n"))

    with pytest.raises(SystemExit) as exc:
        builder.check_against_catalog(rows, _catalog(tmp_path, ["V1", "V2"]))

    assert "V2" in str(exc.value)


def test_extra_mapping_only_warns(tmp_path: Path, capsys) -> None:
    """반대 방향은 경고로 끝낸다 — producer 가 products 기준으로 조인해 폴백을 안 만든다."""
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\nV2,P1\n"))

    builder.check_against_catalog(rows, _catalog(tmp_path, ["V1"]))

    assert "경고" in capsys.readouterr().out


def test_exact_match_is_reported(tmp_path: Path, capsys) -> None:
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\nV2,P2\n"))

    builder.check_against_catalog(rows, _catalog(tmp_path, ["V1", "V2"]))

    assert "일치 확인" in capsys.readouterr().out


def test_catalog_with_wrong_header_fails(tmp_path: Path) -> None:
    """🔴 파일이 **있는데 헤더가 틀린** 경우는 건너뛰지 않고 죽는다.

    `variant_row_id` 컬럼이 없으면 전 행이 빈 값으로 읽혀 `catalog_variants` 가 공집합이
    되고, 그러면 대조가 조용히 통과한다(2026-08-14 지적 — 재현하면 종료코드 0 에
    출력 파일까지 기록됐다). 게다가 그때 나가는 경고가 "매핑에만 있는 variant"라
    **원인과 반대쪽을 지목한다** — 문제는 매핑이 아니라 catalog 헤더다.

    같은 파일을 producer 가 읽으면 `build_channel_product_map` 의 조인이 통째로 비어
    채널 비교가 사라진다. 이 변환기가 막으려는 바로 그 실패다.
    """
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\nV2,P1\n"))
    bad = _write(
        tmp_path / "input_channel_products.csv", "wrong_header,channel\nV2,COUPANG\n"
    )

    with pytest.raises(SystemExit) as exc:
        builder.check_against_catalog(rows, bad)

    message = str(exc.value)
    assert "필수 컬럼" in message
    assert "variant_row_id" in message
    assert "wrong_header" in message  # 실제로 들어온 헤더를 같이 보여준다


def test_catalog_header_check_runs_before_the_set_comparison(tmp_path: Path, capsys) -> None:
    """헤더 검사가 **집합 대조보다 먼저** 돈다 — 순서가 뒤집히면 경고가 원인을 가린다.

    "매핑에만 있는 variant" 경고가 먼저 나가면 읽는 사람이 매핑을 고치러 간다.
    """
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\n"))
    bad = _write(tmp_path / "input_channel_products.csv", "wrong_header\nV1\n")

    with pytest.raises(SystemExit):
        builder.check_against_catalog(rows, bad)

    assert "경고" not in capsys.readouterr().out


def test_absent_catalog_is_skipped_not_fatal(tmp_path: Path, capsys) -> None:
    """`data/**` 가 gitignore 라 products 가 없는 환경이 정상으로 있다."""
    rows = builder.build_rows(_golden(tmp_path, "V1,P1\n"))

    builder.check_against_catalog(rows, tmp_path / "없는파일.csv")

    assert "건너뜀" in capsys.readouterr().out


# ── 정본 데이터 (있을 때만) ─────────────────────────────────────


@pytest.mark.skipif(
    not builder.DEFAULT_SRC.exists(), reason="golden 매핑은 data/** 라 저장소에 없다"
)
def test_real_golden_mapping_converts_and_matches_catalog(tmp_path: Path) -> None:
    """정본으로 실제 변환이 되는지. **데이터 번들이 있는 사람만 돈다.**

    새 클론에는 입력이 없어 이 테스트는 건너뛴다 — 그 사실 자체가 "스크립트만으로는
    같은 상태를 만들 수 없다"는 전제를 보여준다(2026-08-13 지적).
    """
    rows = builder.build_rows(builder.DEFAULT_SRC)

    assert len({r["variant_row_id"] for r in rows}) == len(rows)  # 중복 없음
    builder.check_against_catalog(rows, builder.DEFAULT_CATALOG)

    out = tmp_path / "input_mapped_data.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=builder.FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    with out.open(encoding="utf-8-sig", newline="") as f:
        reloaded = list(csv.DictReader(f))

    assert reloaded == rows  # 쓰고 다시 읽어도 같다
