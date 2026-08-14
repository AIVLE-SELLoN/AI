"""데모 real 측정이 불완전한 분류 캐시를 섞지 않는지 검증한다."""

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "scripts" / "detection_experiments"),
)

import demo_sim


def test_legacy_global_family_is_an_explicit_grouping_function():
    """구정책 대조군이 현재 운영 기본값(None)으로 조용히 바뀌면 안 된다."""
    keyfn = demo_sim.FAMILIES["전체(구정책)"]

    assert keyfn is not None
    assert keyfn(("P001", "색상", "COUPANG", "cs")) == keyfn(
        ("P002", "소재", "NAVER", "review")
    )


def test_check_ideal_import_has_no_execution_side_effect(capsys):
    """도구를 import만 해도 전량 데이터·캐시 검사가 시작되면 안 된다."""
    sys.modules.pop("check_ideal", None)

    module = importlib.import_module("check_ideal")

    assert hasattr(module, "main")
    assert capsys.readouterr().out == ""


def test_require_full_real_cache_rejects_partial_coverage(monkeypatch):
    items = [SimpleNamespace(item_id="INQ-1")]
    candidate = (Path("partial.json"), {}, items, 1, 0.989)
    monkeypatch.setattr(demo_sim, "pick_cache", lambda _items: candidate)

    with pytest.raises(RuntimeError, match="99%"):
        demo_sim.require_full_real_cache(items)


def test_require_full_real_cache_returns_verified_candidate(monkeypatch):
    items = [SimpleNamespace(item_id="INQ-1"), SimpleNamespace(item_id="INQ-2")]
    cache = {"INQ-1": [], "INQ-2": []}
    candidate = (Path("full.json"), cache, items, 2, 1.0)
    monkeypatch.setattr(demo_sim, "pick_cache", lambda _items: candidate)

    path, loaded, real_items, swapped, coverage = demo_sim.require_full_real_cache(
        items
    )

    assert path == Path("full.json")
    assert loaded is cache
    assert real_items is items
    assert swapped == 2
    assert coverage == 1.0


def test_pick_cache_returns_one_consistent_count_and_coverage(tmp_path, monkeypatch):
    """선택 시 계산한 swapped를 호출부가 다시 세지 않고 그대로 사용한다."""
    items = [SimpleNamespace(item_id="INQ-1"), SimpleNamespace(item_id="INQ-2")]
    cache_path = tmp_path / "pipeline_test_run1.json"
    cache_path.write_text('{"INQ-1": []}', encoding="utf-8")
    monkeypatch.setattr(demo_sim, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(demo_sim, "swap_real", lambda given, _cache: (given, 1))

    candidate = demo_sim.pick_cache(items)

    assert candidate is not None
    _path, _cache, _real_items, swapped, coverage = candidate
    assert swapped == 1
    assert coverage == 0.5
