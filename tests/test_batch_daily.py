"""담당: 서영 (Agent2) — 일 1회 배치의 상태 저장·게이트·종료코드.

지인님 PR 리뷰(2026-08-06)가 짚은 1·3·5번이 전부 이 구간이라, 그 회귀를 고정한다.
LLM 은 부르지 않는다 — 발행·개선안·가이드라인을 전부 주입/몽키패치로 막는다.
"""

import json
from datetime import date, datetime, timezone

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
    Evaluator,
    EvaluatorChecks,
    Evidence,
    Recommendation,
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
async def test_window_end_is_taken_from_data_not_clock(tmp_path, monkeypatch):
    """로드·탐지·저장이 같은 window_end 를 쓴다.

    읽기가 실행 시각이면, 데이터가 뒤처진 상태에서 방금 저장한 캐시를 통째로 버려
    매 배치가 첫 실행처럼 굴러간다. (지인님 PR 리뷰 §5)
    """
    path = tmp_path / "state.json"

    # 발행을 명시적으로 막는다. 예전엔 app.core.mq 가 없어서 import 폴백(no-op)이
    # 대신 막아줬는데, 발행기가 생기면 실물이 불려 이 테스트가 MQ 상태에 딸려간다.
    async def _sent(alert, rec, trace_id):
        return None

    monkeypatch.setattr(daily, "publish_anomaly_analyzed", _sent)
    await daily.run_batch(state_path=path, load_inputs=_stub_inputs)

    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved, "발행분이 캐시에 있어야 한다"
    assert saved[0]["window_end"] == "2026-08-28"

    # 같은 캐시를 데이터 시각 기준으로 다시 읽으면 살아 있다.
    assert daily.load_prior_alerts(date(2026, 8, 28), path)


@pytest.mark.asyncio
async def test_cs_inquiries_are_built_once_and_shared(tmp_path, monkeypatch):
    """개선안·가이드라인이 **같은** CS 원문 리스트를 받는다.

    각자 만들면 같은 매핑이 두 벌이 되고, C4(item_id ↔ cs/reviews PK)가 풀려 DB 조회로
    바뀔 때 고칠 곳이 두 곳이 된다. 예전 호출부는 가이드라인에 rec 를 넘기고 있었다.
    """
    seen: dict = {}

    async def fake_recommendation(alert, inquiries):
        seen["개선안"] = inquiries
        # None 을 돌려주면 "생성 실패"로 잡힌다(그건 아래 별도 테스트가 본다).
        return Recommendation(
            recommendation_id="REC-000000000001",
            alert_id=alert.alert_id,
            created_at=datetime(2026, 8, 28, 9, 0, tzinfo=timezone.utc),
            evaluator=Evaluator(
                passed=True,
                attempts=1,
                checks=EvaluatorChecks(
                    grounding=True, consistency=True, actionability=True
                ),
            ),
        )

    async def fake_guideline(alert, inquiries, *, product_name=None):
        seen["가이드라인"] = inquiries

    async def sent(alert, rec, trace_id):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_for_alert", fake_recommendation)
    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert not summary["failures"], summary["failures"]
    assert isinstance(seen["개선안"], list)
    assert seen["개선안"] is seen["가이드라인"]


@pytest.mark.asyncio
async def test_silent_recommendation_failure_still_shows_up(tmp_path, monkeypatch):
    """⚠️ 개선안이 조용히 실패해도 요약·종료코드에 남는다.

    `generate_for_alert` 는 계약상 예외를 안 던지고 None 을 돌려준다. except 만 믿으면
    개선안이 하나도 안 붙은 배치가 "성공"으로 끝나서 아무도 못 알아챈다.
    알림 자체는 그대로 발행된다 — 개선안 없는 것과 알림이 안 가는 건 다르다.
    """

    async def always_fails(alert, inquiries):
        return None

    async def sent(alert, rec, trace_id):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_for_alert", always_fails)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["failures"], "조용한 실패가 요약에 남아야 한다"
    assert all(f["stage"] == "개선안" for f in summary["failures"])
    assert summary["delivered"] >= 1, "개선안이 없어도 알림은 발행된다"


@pytest.mark.asyncio
async def test_raised_recommendation_failure_is_counted_once(tmp_path, monkeypatch):
    """⚠️ 실패 1건이 요약에 1건으로 잡힌다.

    `generate_for_alert` 는 계약상 안 던지지만 던지는 날엔, except 와 뒤따르는
    `rec is None` 검사가 **둘 다** 타서 실패가 2배로 보고됐다. 그러면 배치 요약의
    실패 건수를 못 믿게 된다. (2026-08-07 재검토)
    """

    async def blows_up(alert, inquiries):
        raise RuntimeError("LLM 폭발")

    async def sent(alert, rec, trace_id):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_for_alert", blows_up)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    rec_failures = [f for f in summary["failures"] if f["stage"] == "개선안"]
    assert len(rec_failures) == summary["processed"], (
        f"alert 당 1건이어야 하는데 {len(rec_failures)}건 / "
        f"alert {summary['processed']}건"
    )
    assert "LLM 폭발" in rec_failures[0]["error"], "실제 사유가 남아야 한다"


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


@pytest.mark.asyncio
async def test_mq_connection_is_closed_even_when_batch_blows_up(tmp_path, monkeypatch):
    """⚠️ 루프 도중 터져도 MQ 연결을 닫는다.

    app/core/mq.py 가 프로세스당 연결을 재사용하는데, 안 닫고 이벤트 루프가 내려가면
    connect_robust 의 재연결 태스크가 남아 "Task was destroyed but it is pending" 이
    뜬다. 나중에 배치가 장수 프로세스에 얹히면 연결이 샌다. (서영님 PR 리뷰 §1)
    """
    closed: list[bool] = []

    async def spy_close():
        closed.append(True)

    async def boom(alert, rec, trace_id):
        raise KeyboardInterrupt  # 배치 격리 except 를 통과해 밖으로 나가는 예외

    monkeypatch.setattr(daily, "close_mq", spy_close)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", boom)

    with pytest.raises(KeyboardInterrupt):
        await daily.run_batch(
            state_path=tmp_path / "state.json", load_inputs=_stub_inputs
        )

    assert closed, "예외가 나가도 close_mq() 가 불려야 한다"


@pytest.mark.asyncio
async def test_mq_connection_is_closed_on_the_normal_path(tmp_path, monkeypatch):
    """정상 종료에서도 닫는다."""
    closed: list[bool] = []

    async def spy_close():
        closed.append(True)

    async def sent(alert, rec, trace_id):
        return None

    monkeypatch.setattr(daily, "close_mq", spy_close)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    await daily.run_batch(state_path=tmp_path / "state.json", load_inputs=_stub_inputs)

    assert closed


def test_output_streams_are_switched_to_utf8(monkeypatch):
    """⚠️ 요약을 내기 전에 출력 스트림을 UTF-8 로 돌린다.

    윈도우 기본 콘솔(cp949)에는 `⚠️`(U+26A0)·`ℹ️`(U+2139) 가 없어서 print_summary 가
    UnicodeEncodeError 로 터졌다. 탐지·발행이 다 끝난 **뒤에** 죽는 게 더 나쁘다 —
    main() 끝의 sys.exit(1) 에 도달을 못 해 성공한 배치도 비-0 으로 끝나고, 종료코드로
    성패를 판정할 수 없게 된다. (2026-08-07)
    """

    class _Reconfigurable:
        def __init__(self) -> None:
            self.kwargs: dict = {}

        def reconfigure(self, **kwargs) -> None:
            self.kwargs = kwargs

    out, err = _Reconfigurable(), _Reconfigurable()
    monkeypatch.setattr(daily.sys, "stdout", out)
    monkeypatch.setattr(daily.sys, "stderr", err)

    daily._force_utf8_output()

    # stderr 도 같이 — 로그 레코드가 그쪽으로 나간다.
    for stream in (out, err):
        assert stream.kwargs["encoding"] == "utf-8"
        assert stream.kwargs["errors"] == "replace"


def test_utf8_switch_is_safe_on_streams_that_cannot_reconfigure(monkeypatch):
    """pytest 캡처 스트림처럼 reconfigure 가 없는 곳에서도 터지지 않는다.

    여기서 예외가 새면 배치가 **아무 일도 하기 전에** 죽는다 — 인코딩 편의 때문에
    운영 진입점을 못 띄우는 건 원래 문제보다 나쁘다.
    """
    monkeypatch.setattr(daily.sys, "stdout", object())
    monkeypatch.setattr(daily.sys, "stderr", object())

    daily._force_utf8_output()  # 예외가 나면 이 줄에서 실패한다


def test_main_switches_encoding_before_printing(monkeypatch):
    """⚠️ `main()` 이 실제로 `_force_utf8_output()` 을 부른다 — 배선까지 고정한다.

    헬퍼만 테스트하면 호출 한 줄이 빠져도 아무것도 안 깨진다(2026-08-07 리뷰 지적).
    `main()` 은 인자 파싱·늦은 import·로깅 설정이 몰려 있어 손이 자주 가는 함수라,
    빠지면 윈도우에서 **성공한 배치가 다시 비-0 으로 끝나고** 종료코드 판정이 죽는다.
    """
    calls: list[str] = []

    async def fake_run_batch(**_kwargs):
        calls.append("run_batch")
        return {
            "trace_id": "trace-1",
            "dry_run": True,
            "input_source": "load_golden_inputs",  # ⚠️ 줄을 태운다
            "elapsed_sec": 1.0,
            "items": 0,
            "documents": 0,
            "prior_alerts": 0,
            "published": 0,
            "suppressed": 0,
            "processed": 0,
            "delivered": 0,
            "llm_calls": {},
            "cause_calls": 0,
            "failures": [],
            "state_cached": 0,
        }

    monkeypatch.setattr(daily, "_force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(daily, "run_batch", fake_run_batch)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    daily.main()

    assert "utf8" in calls, "main() 이 _force_utf8_output() 을 부르지 않았습니다"
    # 출력·로깅보다 먼저 불려야 한다.
    assert calls.index("utf8") < calls.index("run_batch")
