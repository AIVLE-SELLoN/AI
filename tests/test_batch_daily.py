"""담당: 서영 (Agent2) — 일 1회 배치의 상태 저장·게이트·종료코드.

지인님 PR 리뷰(2026-08-06)가 짚은 1·3·5번이 전부 이 구간이라, 그 회귀를 고정한다.
LLM 은 부르지 않는다 — 발행·개선안·가이드라인을 전부 주입/몽키패치로 막는다.
"""

import json
from datetime import date, datetime

import pytest

from app.batch import daily
from app.core.constants import CURRENT_WINDOW_DAYS, PAST_WINDOW_DAYS
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    RecommendedAction,
    Source,
    SourceSignals,
    Verdict,
)


def _alert(alert_id: str, window_end: date, action=RecommendedAction.LOGISTICS_CHECK):
    return DetectionAlert(
        alert_id=alert_id,
        detected_at=datetime(2026, 8, 28, 9, 0),
        product_group_id="P001",
        channel=Channel.COUPANG,
        window_start=date.fromordinal(window_end.toordinal() - CURRENT_WINDOW_DAYS + 1),
        window_end=window_end,
        verdict=Verdict.BIASED,
        significant_channels=[Channel.COUPANG],
        main_aspect=Aspect.DAMAGE,
        stats=DetectionStats(
            source=Source.CS,
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=1e-4,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(cs=True, review=None, interpretation="CS 선행"),
        detection_confidence=DetectionConfidence.MEDIUM,
        scope_in=False,
        recommended_action=action,
        evidence=Evidence(inquiry_ids=[]),
    )


# ── 상태 저장 왕복 · 보관 기간 ───────────────────────────────────


def test_state_roundtrip_drops_outside_retention(tmp_path):
    """STATE_RETENTION_DAYS 밖의 알림은 로드에서 빠진다.

    보관 기간이 짧으면 그 경계에서 조용히 억제가 풀리고 기준선이 오염된다.
    """
    path = tmp_path / "state.json"
    now = date(2026, 8, 28)
    old = date.fromordinal(now.toordinal() - daily.STATE_RETENTION_DAYS - 1)

    daily.save_published([_alert("ALT-NEW", now), _alert("ALT-OLD", old)], now, path)
    loaded = {a.alert_id for a in daily.load_prior_alerts(now, path)}

    assert loaded == {"ALT-NEW"}
    assert daily.STATE_RETENTION_DAYS == CURRENT_WINDOW_DAYS + PAST_WINDOW_DAYS


def test_state_read_survives_corrupted_file(tmp_path, caplog):
    """쓰다 만 JSON 이 있어도 배치는 뜬다 — 유일한 상태 저장소라 자가복구가 필요하다."""
    path = tmp_path / "state.json"
    path.write_text('[{"alert_id": "ALT-1", "detec', encoding="utf-8")

    assert daily.load_prior_alerts(date(2026, 8, 28), path) == []
    assert any("손상" in r.message for r in caplog.records)


def test_state_read_skips_schema_mismatched_item(tmp_path, caplog):
    """스키마가 어긋난 항목 하나가 배치 전체를 죽이지 않는다.

    DetectionAlert 에 필수 필드가 하나 추가되는 것만으로 이 상태가 된다.
    """
    path = tmp_path / "state.json"
    good = _alert("ALT-OK", date(2026, 8, 28)).model_dump(mode="json")
    path.write_text(
        json.dumps([good, {"alert_id": "ALT-BROKEN"}], ensure_ascii=False),
        encoding="utf-8",
    )

    loaded = daily.load_prior_alerts(date(2026, 8, 28), path)
    assert [a.alert_id for a in loaded] == ["ALT-OK"]
    assert any("스키마" in r.message for r in caplog.records)


def test_atomic_write_leaves_no_partial_file(tmp_path):
    """임시파일 경유라 원본이 반쪽으로 안 남는다."""
    path = tmp_path / "state.json"
    daily.save_published([_alert("ALT-1", date(2026, 8, 28))], date(2026, 8, 28), path)

    assert json.loads(path.read_text(encoding="utf-8"))[0]["alert_id"] == "ALT-1"
    assert not (tmp_path / "state.json.tmp").exists()


# ── 배치 본체 ────────────────────────────────────────────────────


def _stub_inputs(window_end=None):
    """[6] 도 Agent3 도 안 타는 최소 입력 — 파손(스코프 밖) 1슬롯만 발화시킨다.

    로더는 `window_end` 를 받는다(운영 로더가 35일치만 읽게 하기 위한 시그니처).
    이 스텁은 규모가 작아 인자를 무시한다.
    """
    docs, items = [], []
    base = date(2026, 8, 28).toordinal()
    for day_offset in range(-34, 1):
        day = date.fromordinal(base + day_offset)
        current = day_offset >= -(CURRENT_WINDOW_DAYS - 1)
        neg = 8 if current else 1
        for i in range(40):
            doc_id = f"INQ-{day:%m%d}-{i:03d}"
            aspects = (
                [AspectSentiment(aspect=Aspect.DAMAGE, sentiment=-1)] if i < neg else []
            )
            items.append(
                ClassifiedItem(
                    item_id=doc_id,
                    source=Source.CS,
                    channel=Channel.COUPANG,
                    product_group_id="P001",
                    raw_text="x",
                    aspects=aspects
                    or [AspectSentiment(aspect=Aspect.ETC, sentiment=0)],
                    created_at=datetime.combine(day, datetime.min.time()),
                )
            )
            docs.append(
                {
                    "id": doc_id,
                    "product": "P001",
                    "channel": "COUPANG",
                    "source": "cs",
                    "created_at": datetime.combine(day, datetime.min.time()),
                    "text": "x",
                }
            )
    return items, docs


@pytest.mark.asyncio
async def test_max_alerts_does_not_cache_untouched_alerts(tmp_path, monkeypatch):
    """⚠️ 상한으로 잘린 알림은 캐시에 안 들어간다.

    들어가면 다음 배치가 그걸 직전 알림으로 보고 RENOTIFY_BLOCK_DAYS 만큼 억제해서
    **셀러가 그 알림을 영영 못 본다.** resolved_alert_ids 가 빈 집합이라 조기 해제
    경로도 없다. (지인님 PR 리뷰 §1)
    """
    path = tmp_path / "state.json"
    summary = await daily.run_batch(
        max_alerts=0, state_path=path, load_inputs=_stub_inputs
    )

    assert summary["published"] >= 1, "이 시나리오는 알림이 떠야 한다(테스트 전제)"
    assert summary["processed"] == 0
    assert summary["delivered"] == 0
    assert summary["state_cached"] == 0
    assert not path.exists()


@pytest.mark.asyncio
async def test_publish_failure_is_not_cached(tmp_path, monkeypatch):
    """발행이 터진 알림도 캐시에 안 들어간다 — MQ 가 잠깐 죽었다고 7일 침묵하면 안 된다."""
    path = tmp_path / "state.json"

    async def boom(alert, rec, trace_id):
        raise RuntimeError("MQ down")

    monkeypatch.setattr(daily, "publish_anomaly_analyzed", boom)
    summary = await daily.run_batch(state_path=path, load_inputs=_stub_inputs)

    assert summary["published"] >= 1
    assert summary["delivered"] == 0
    assert summary["state_cached"] == 0
    assert summary["failures"], "실패가 요약에 남아야 한다"


@pytest.mark.asyncio
async def test_window_end_is_taken_from_data_not_clock(tmp_path):
    """로드·탐지·저장이 같은 window_end 를 쓴다.

    읽기가 실행 시각이면, 데이터가 뒤처진 상태에서 방금 저장한 캐시를 통째로 버려
    매 배치가 첫 실행처럼 굴러간다. (지인님 PR 리뷰 §5)
    """
    path = tmp_path / "state.json"
    await daily.run_batch(state_path=path, load_inputs=_stub_inputs)

    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved, "발행분이 캐시에 있어야 한다"
    assert saved[0]["window_end"] == "2026-08-28"

    # 같은 캐시를 데이터 시각 기준으로 다시 읽으면 살아 있다.
    assert daily.load_prior_alerts(date(2026, 8, 28), path)


@pytest.mark.asyncio
async def test_dry_run_skips_recommendation_when_gate_closed(tmp_path):
    """개선안 카운트는 should_generate 를 통과한 alert 만 센다.

    조치 7종 중 '개선안 생성' 은 1종뿐이라, 게이트를 안 태우면 Agent3 비용이 크게
    과대추정된다. (지인님 PR 리뷰 §2)
    """
    summary = await daily.run_batch(
        dry_run=True, state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["published"] >= 1
    assert summary["llm_calls"].get("개선안", 0) == 0, (
        "이 시나리오의 알림은 파손(물류 점검 권장)이라 Agent3 대상이 아니다"
    )
    assert summary["llm_calls"].get("가이드라인", 0) == summary["processed"]
    assert summary["state_cached"] == 0, "dry-run 은 캐시를 건드리지 않는다"
