"""데모 real 측정이 불완전한 분류 캐시를 섞지 않는지 검증한다."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "scripts" / "detection_experiments"),
)

import demo_sim


def test_require_full_real_cache_rejects_partial_coverage(monkeypatch):
    items = [SimpleNamespace(item_id="INQ-1")]
    candidate = (Path("partial.json"), {}, items, 0.989)
    monkeypatch.setattr(demo_sim, "pick_cache", lambda _items: candidate)

    with pytest.raises(RuntimeError, match="99%"):
        demo_sim.require_full_real_cache(items)


def test_require_full_real_cache_returns_verified_candidate(monkeypatch):
    items = [SimpleNamespace(item_id="INQ-1"), SimpleNamespace(item_id="INQ-2")]
    cache = {"INQ-1": [], "INQ-2": []}
    candidate = (Path("full.json"), cache, items, 1.0)
    monkeypatch.setattr(demo_sim, "pick_cache", lambda _items: candidate)

    path, loaded, real_items, swapped, coverage = demo_sim.require_full_real_cache(
        items
    )

    assert path == Path("full.json")
    assert loaded is cache
    assert real_items is items
    assert swapped == 2
    assert coverage == 1.0
