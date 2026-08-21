"""담당: 서영 (Agent2) — 일 1회 배치의 상태 저장·게이트·종료코드.

상태 저장·게이트·종료코드가 한 구간에 모여 있어 그 회귀를 여기서 고정한다.
LLM 은 부르지 않는다 — 발행·개선안·가이드라인을 전부 주입/몽키패치로 막는다.
"""

import ast
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
import pytest

from app.batch import daily, inputs
from app.core import exit_codes, logging_setup
from tests.conftest import bad_log_level_settings, pin_settings, unloadable_settings

ROOT = Path(__file__).resolve().parents[1]
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
from app.detection.cause import CAUSE_TAXONOMY, classify_cause
from app.recommendation.pipeline import RecommendationOutcome, SkipReason


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


@pytest.mark.asyncio
@pytest.mark.parametrize("aspect", ["색상", "사이즈", "소재"])
async def test_counting_client_stub_cause_is_valid_for_every_supported_aspect(aspect):
    """dry-run 스텁이 특정 aspect의 taxonomy에서 탈락해 후속 게이트를 닫으면 안 된다."""
    items = [{"cs_id": f"CS-{i}", "raw_text": f"문의 {i}"} for i in range(3)]

    results = await classify_cause(aspect, items, client=daily.CountingClient())

    assert len(results) == len(items)
    assert {result["cause"] for result in results} == {daily.STUB_CAUSE}
    assert daily.STUB_CAUSE in CAUSE_TAXONOMY[aspect]


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


def test_classifier_versions_only_when_the_filter_guaranteed_them():
    """payload 의 분류기 신원은 **필터가 보장할 때만** 값이 있다.

    실을 수 있는 근거가 `_ASPECT_SQL` 의 활성 버전 필터뿐이다 — 그 필터가 "이 알림에
    기여한 모든 행의 버전 3종이 활성 값"임을 쿼리로 강제하므로 주장이 아니라 관측이다.
    골든 입력(`--input-source golden`)은 CSV 를 그대로 읽어 그 필터를 안 타므로, 같은 값을
    실으면 **검증한 적 없는 것을 검증된 것처럼 보고**하게 된다. 골든은 분류 오차가 0 인
    oracle 이라 애초에 분류기를 안 거쳤다 — `null` 이 정확한 답이다.
    """
    versions = daily.classifier_versions_for(daily.load_inputs_from_db)

    assert set(versions) == {"prompt_cs", "prompt_review", "model", "pipeline"}
    # 필터가 쓰는 값과 **같은 값**이어야 한다. 따로 조립하면 payload 가 실제로 읽은 것과
    # 다른 버전을 말하게 된다.
    assert tuple(versions.values()) == inputs._active_version_params()

    assert daily.classifier_versions_for(_stub_inputs) is None


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


def _stub_in_scope_inputs(window_end=None):
    """색상 편중형 1슬롯 — dry-run 스텁이 원인분류 계약과 실제로 대면한다."""
    docs, items = [], []
    base = date(2026, 8, 28).toordinal()
    for channel in (Channel.COUPANG, Channel.NAVER, Channel.ZIGZAG):
        for day_offset in range(-34, 1):
            day = date.fromordinal(base + day_offset)
            current = day_offset >= -(CURRENT_WINDOW_DAYS - 1)
            neg = 8 if current and channel == Channel.COUPANG else 1
            for i in range(40):
                doc_id = f"INQ-{channel.value}-{day:%m%d}-{i:03d}"
                aspects = (
                    [AspectSentiment(aspect=Aspect.COLOR, sentiment=-1)]
                    if i < neg
                    else [AspectSentiment(aspect=Aspect.ETC, sentiment=0)]
                )
                items.append(
                    ClassifiedItem(
                        item_id=doc_id,
                        source=Source.CS,
                        channel=channel,
                        product_group_id="P001",
                        raw_text="화면과 실물 색상이 달라요",
                        aspects=aspects,
                        created_at=datetime.combine(day, datetime.min.time()),
                    )
                )
                docs.append(
                    {
                        "id": doc_id,
                        "product": "P001",
                        "channel": channel.value,
                        "source": "cs",
                        "created_at": datetime.combine(day, datetime.min.time()),
                        "text": "화면과 실물 색상이 달라요",
                    }
                )
    return items, docs


@pytest.mark.asyncio
async def test_max_alerts_does_not_cache_untouched_alerts(tmp_path, monkeypatch):
    """상한으로 잘린 알림은 캐시에 안 들어간다.

    들어가면 다음 배치가 그걸 직전 알림으로 보고 RENOTIFY_BLOCK_DAYS 만큼 억제해서
    **셀러가 그 알림을 영영 못 본다.** resolved_alert_ids 가 빈 집합이라 조기 해제
    경로도 없다.
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

    async def boom(alert, rec, trace_id, versions=None):
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
    매 배치가 첫 실행처럼 굴러간다.
    """
    path = tmp_path / "state.json"

    # 발행을 명시적으로 막는다. 예전엔 app.core.mq 가 없어서 import 폴백(no-op)이
    # 대신 막아줬는데, 발행기가 생기면 실물이 불려 이 테스트가 MQ 상태에 딸려간다.
    async def _sent(alert, rec, trace_id, versions=None):
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
        # 개선안 없이 돌려주면 "생성 실패"로 잡힌다(그건 아래 별도 테스트가 본다).
        return RecommendationOutcome(
            Recommendation(
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
        )

    async def fake_guideline(alert, inquiries, *, product_name=None):
        seen["가이드라인"] = inquiries

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_outcome_for_alert", fake_recommendation)
    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert not summary["failures"], summary["failures"]
    assert isinstance(seen["개선안"], list)
    assert seen["개선안"] is seen["가이드라인"]


@pytest.mark.asyncio
async def test_cs_mapping_failure_does_not_kill_the_batch(tmp_path, monkeypatch):
    """CS 원문 매핑이 터져도 배치가 끝까지 돈다 — 알림 발행도 캐시 저장도 살아 있다.

    이 호출은 알림별 try/except **바깥**에 있었다. 루프를 감싸는 try 엔 except 가 없어서
    (`finally: close_mq()` 뿐) 여기서 던지면 `run_batch` 밖으로 나가고, `save_published()`
    가 try/finally **뒤**라 같이 건너뛴다 — **이미 발행에 성공한 앞쪽 알림이 캐시에 안
    들어가서 다음 배치가 같은 알림을 다시 만들고 LLM 비용을 또 쓴다.**

    `state_cached` 를 같이 보는 이유가 그것이다. 실패 항목만 확인하면 캐시 유실은 안 잡힌다.
    """

    def boom(alert, documents):
        raise ValueError("documents 한 행의 형식이 이상함")

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "build_linked_inquiries", boom)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    mapping = [f for f in summary["failures"] if f["stage"] == "CS 원문 매핑"]
    assert mapping, "매핑 실패가 요약에 남아야 한다"
    assert mapping[0]["target_key"].startswith("ALT-")
    assert "alert_id" not in mapping[0], "failures 식별자 키는 target_key 하나다"
    assert "형식이 이상함" in mapping[0]["error"], "실제 사유가 남아야 한다"
    assert summary["delivered"] >= 1, "알림은 통계로 서므로 CS 원문과 무관하게 발행된다"
    assert summary["state_cached"] >= 1, "발행분이 캐시에 들어가야 한다 — 안 그러면 재과금"


@pytest.mark.asyncio
async def test_silent_recommendation_failure_still_shows_up(tmp_path, monkeypatch):
    """개선안이 조용히 실패해도 요약·종료코드에 남는다.

    `generate_outcome_for_alert` 는 계약상 예외를 안 던지고 개선안 없는 결과를 돌려준다.
    except 만 믿으면 개선안이 하나도 안 붙은 배치가 "성공"으로 끝나서 아무도 못 알아챈다.
    알림 자체는 그대로 발행된다 — 개선안 없는 것과 알림이 안 가는 건 다르다.

    실패로 남는 사유는 `ERROR` 뿐이다(데이터 갭·라우팅 미스는 아래 두 테스트 참고).
    """

    async def always_fails(alert, inquiries):
        return RecommendationOutcome(
            reason=SkipReason.ERROR, detail="RuntimeError('Chroma 접속 실패')"
        )

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_outcome_for_alert", always_fails)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["failures"], "조용한 실패가 요약에 남아야 한다"
    assert all(f["stage"] == "개선안" for f in summary["failures"])
    assert "Chroma" in summary["failures"][0]["error"], (
        "사유를 값으로 받았으니 요약에도 그대로 남아야 한다"
    )
    assert summary["no_evidence"] == 0
    assert summary["routing_miss"] == 0
    assert summary["delivered"] >= 1, "개선안이 없어도 알림은 발행된다"


@pytest.mark.asyncio
async def test_routing_miss_is_counted_but_not_a_failure(tmp_path, monkeypatch):
    """라우팅 미스도 **실패가 아니다** — 건수로만 센다.

    처음엔 실패로 뒀는데, 그러면 `NO_EVIDENCE` 를 실패에서 뺀 이유가 옆문으로 그대로
    돌아온다. **근본 원인이 같기 때문**이다(상세페이지 미등록 — mock 504행 중 489행이
    "정보 없음"). 갈리는 건 모델이 그 빈 쪽을 골랐느냐뿐이고, 그 선택을 코드로 강제하지
    않기로 한 것도 우리 결정이다. 우리가 안 고치기로 한 걸 매일 실패로 세면 배치가 상시
    종료코드 1 로 끝나 진짜 장애가 묻힌다.

    대신 **요약에는 남아야 한다** — 여기가 조용해지면 프롬프트 v3 를 손볼 근거가 사라진다.
    """

    async def routed_wrong(alert, inquiries):
        return RecommendationOutcome(
            reason=SkipReason.ROUTED_WITHOUT_EVIDENCE,
            detail="copy_draft 로 라우팅됐으나 그쪽 근거가 없음",
        )

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_outcome_for_alert", routed_wrong)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["failures"] == [], "라우팅 미스는 배치 실패가 아니다"
    assert summary["routing_miss"] == summary["processed"] >= 1
    assert summary["no_evidence"] == 0, "데이터 갭과 섞이면 안 된다"
    # 라우팅까지는 갔으므로 LLM 을 썼다 — 근거 0건과 달리 비용 집계에 들어간다.
    assert summary["llm_calls"].get("개선안", 0) == summary["processed"]


@pytest.mark.asyncio
async def test_no_evidence_is_counted_but_not_a_failure(tmp_path, monkeypatch):
    """근거 0건은 **실패가 아니다** — 건수로만 세고 종료코드에 안 싣는다.

    상세페이지 미등록은 흔한 데이터 갭이라(mock 기준 504행 중 489행이 "정보 없음"),
    이걸 실패로 세면 배치가 상시 종료코드 1 로 끝나 **진짜 장애 신호가 무뎌진다.**
    반대로 아예 안 세면 근거 파이프라인이 통째로 끊긴 걸 아무도 못 본다.
    """

    async def no_evidence(alert, inquiries):
        return RecommendationOutcome(
            reason=SkipReason.NO_EVIDENCE, detail="상세페이지·CS 원문이 둘 다 없음"
        )

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_outcome_for_alert", no_evidence)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["failures"] == [], "데이터 갭은 배치 실패가 아니다"
    assert summary["no_evidence"] == summary["processed"]
    # 라우팅 전에 걸러지므로 LLM 은 한 번도 안 돈다 — 비용 집계에 넣으면 과대추정이다.
    assert summary["llm_calls"].get("개선안", 0) == 0
    assert summary["delivered"] >= 1, "개선안이 없어도 알림은 발행된다"


@pytest.mark.asyncio
async def test_raised_recommendation_failure_is_counted_once(tmp_path, monkeypatch):
    """실패 1건이 요약에 1건으로 잡힌다.

    `generate_outcome_for_alert` 는 계약상 안 던지지만 던지는 날엔, except 와 뒤따르는
    `rec is None` 검사가 **둘 다** 타서 실패가 2배로 보고됐다. 그러면 배치 요약의
    실패 건수를 못 믿게 된다.
    """

    async def blows_up(alert, inquiries):
        raise RuntimeError("LLM 폭발")

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "should_generate", lambda _alert: True)
    monkeypatch.setattr(daily, "generate_outcome_for_alert", blows_up)
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
    과대추정된다.
    """
    summary = await daily.run_batch(
        dry_run=True, state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["published"] >= 1
    assert summary["llm_calls"].get("개선안", 0) == 0, (
        "이 시나리오의 알림은 파손(물류 점검 권장)이라 Agent3 대상이 아니다"
    )
    # 가이드라인도 게이트를 태운다 — 이 알림은 `evidence.inquiry_ids` 가 비어 있어
    # (스코프 밖이라 [6] 원인분류를 안 탄다) `is_guideline_target()` 이 거르고 LLM 을
    # 아예 안 부른다. 예전엔 알림 수만큼 세서 **비용 추정이 위로 어긋났다**.
    assert summary["llm_calls"].get("가이드라인", 0) == 0
    assert summary["state_cached"] == 0, "dry-run 은 캐시를 건드리지 않는다"


@pytest.mark.asyncio
async def test_dry_run_counts_in_scope_cause_and_recommendation(tmp_path):
    """인스코프 편중형에서 스텁 응답이 검증을 통과하고 비용 게이트를 연다.

    기존 dry-run 테스트는 파손만 넣어 원인분류를 호출하지 않았다. 그래서 스텁이 필수
    필드를 빼거나 few-shot ID를 섞어도 전부 통과했고, 실제 실행에서만 개선안 추정이
    0건으로 무너졌다.
    """
    summary = await daily.run_batch(
        dry_run=True,
        state_path=tmp_path / "state.json",
        load_inputs=_stub_in_scope_inputs,
    )

    assert summary["published"] >= 1
    assert summary["cause_calls"] >= 1
    assert summary["cause_failures"] == 0
    assert summary["failures"] == []
    assert summary["llm_calls"].get("개선안", 0) == summary["processed"] >= 1
    assert summary["llm_calls"].get("가이드라인", 0) == summary["processed"]


@pytest.mark.asyncio
async def test_guideline_not_counted_when_it_was_not_a_target(tmp_path, monkeypatch):
    """실제 경로도 dry-run 과 **같은 것**을 센다 — `None`(대상 아님)은 안 센다.

    두 경로가 다른 걸 세면 `--dry-run` 으로 잡은 비용 추정이 실제와 안 맞는다. 그게
    dry-run 을 두는 이유 전부다. `None` 은 실패도 아니다(콜백을 돌려주는 FAILED_* 와 구분).
    """

    async def not_a_target(alert, inquiries, *, product_name=None):
        return None

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "generate_guideline", not_a_target)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["llm_calls"].get("가이드라인", 0) == 0
    assert not summary["failures"], "생성 대상이 아닌 건 실패가 아니다"


@pytest.mark.asyncio
async def test_dry_run_counts_guideline_when_gate_open(tmp_path, monkeypatch):
    """게이트를 통과하는 알림은 그대로 센다 — 위 테스트가 "항상 0" 으로 굳지 않게.

    가이드라인은 발화한 알림 **거의 전부**에 돌아서 건수가 그대로 비용이다. 한쪽만
    고정하면 세는 쪽이 통째로 죽어도 테스트가 통과한다.
    """
    monkeypatch.setattr(daily, "is_guideline_target", lambda _alert: True)

    summary = await daily.run_batch(
        dry_run=True, state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["llm_calls"].get("가이드라인", 0) == summary["processed"] >= 1


@pytest.mark.asyncio
async def test_mq_connection_is_closed_even_when_batch_blows_up(tmp_path, monkeypatch):
    """루프 도중 터져도 MQ 연결을 닫는다.

    app/core/mq.py 가 프로세스당 연결을 재사용하는데, 안 닫고 이벤트 루프가 내려가면
    connect_robust 의 재연결 태스크가 남아 "Task was destroyed but it is pending" 이
    뜬다. 나중에 배치가 장수 프로세스에 얹히면 연결이 샌다.
    """
    closed: list[bool] = []

    async def spy_close():
        closed.append(True)

    async def boom(alert, rec, trace_id, versions=None):
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

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "close_mq", spy_close)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    await daily.run_batch(state_path=tmp_path / "state.json", load_inputs=_stub_inputs)

    assert closed


@pytest.mark.asyncio
async def test_cause_failure_is_reported_as_batch_failure(
    tmp_path, monkeypatch, capsys
):
    """Agent2 보강 실패를 알림 발행 성공과 별개로 실패 종료 근거에 남긴다."""

    async def fake_detect(_items, *, diagnostics, **_kwargs):
        diagnostics.cause_failures.append(
            {
                "product": "P001",
                "aspect": "색상",
                "channel": "COUPANG",
                "source": "cs",
                "error": "CauseValidationError: 응답 ID 누락",
            }
        )
        return [], []

    monkeypatch.setattr(daily, "detect_anomaly", fake_detect)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["cause_failures"] == 1
    assert summary["failures"] == [
        {
            "target_key": "P001/색상/COUPANG/cs",
            "stage": "원인분류",
            "error": "CauseValidationError: 응답 ID 누락",
        }
    ]

    daily.print_summary(summary)
    assert "P001/색상/COUPANG/cs [원인분류]" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_injected_loader_reports_unknown_not_zero(tmp_path):
    """주입된 로더는 제외 건수가 **`None`(보고 안 함)** 이지 `0` 이 아니다.

    골든 로더는 매핑이 없는 행을 세지 않고 그냥 건너뛴다(`scripts/golden_inputs.py`).
    거기서 `{}` 나 `0` 을 실으면 배치가 **"제외 0건" 이라고 주장**하게 되는데, 그건
    관측이 아니라 무지다. `classifier_versions` 가 골든 입력에 `None` 을 싣는 것과 같은
    규칙이고, 같은 이유로 화면에도 줄이 안 나가야 한다.
    """
    summary = await daily.run_batch(
        state_path=tmp_path / "state.json", load_inputs=_stub_inputs
    )

    assert summary["input_dropped"] is None


def test_print_summary_shows_dropped_inputs_only_when_there_are_any(capsys):
    """화면에 나야 값을 한다 — 요약 dict 에만 있으면 사람은 못 본다.

    반대편(비었거나 관측 불가면 줄을 안 낸다)도 같이 잠근다. 매번 "0건" 을 찍으면
       눈에 안 띄는 줄이 하나 늘 뿐이고, 이 항목의 목적은 **늘었을 때 보이는 것**이다.
    """
    daily.print_summary(_fake_summary(input_dropped={"상품매핑 없음": 7}))
    assert "상품매핑 없음 7건" in capsys.readouterr().out

    for quiet in (None, {}):
        daily.print_summary(_fake_summary(input_dropped=quiet))
        assert "입력 제외" not in capsys.readouterr().out


def _fake_summary(**overrides) -> dict:
    """`run_batch()` 반환값의 가짜. **실제 계약과 키가 정확히 같다.**

    예전엔 리터럴이 두 벌이었고 그중 하나에 실제로는 없는 `window_end` 가 들어 있었다
       — 가짜가 계약과 갈리면 다음 사람이 `summary["window_end"]`
       를 쓰고 런타임에 `KeyError` 를 본다. **반대 방향(키 누락)도 같이 위험하다** —
       `print_summary` 가 `.get()` 으로 읽는 항목은 가짜에 없어도 조용히 통과해서, 그
       분기를 태운다고 믿는 테스트가 실제로는 안 태운다.

    아래 `test_fake_summary_matches_the_real_contract` 가 두 방향을 다 잠근다.
    """
    return {
        "trace_id": "trace-1",
        "dry_run": True,
        "input_source": "db",
        "elapsed_sec": 1.0,
        "items": 0,
        "documents": 0,
        # None = "이 입력원은 제외 건수를 보고하지 않는다"(골든·테스트 fake).
        # 0건과 다른 값이다 — `daily.read_inputs` 참고.
        "input_dropped": None,
        "coverage_gap_slots": 0,
        "coverage_missing_documents": 0,
        "prior_alerts": 0,
        "published": 0,
        "suppressed": 0,
        "processed": 0,
        "delivered": 0,
        "llm_calls": {},
        "cause_calls": 0,
        "cause_failures": 0,
        "no_evidence": 0,
        "routing_miss": 0,
        "guideline_retried": 0,
        "guideline_pending": 0,
        "guideline_retry_exhausted": 0,
        "failures": [],
        "state_cached": 0,
        **overrides,
    }


def test_fake_summary_matches_the_real_contract():
    """가짜 요약이 `run_batch()` 의 실제 반환 키와 **정확히 일치**하는지.

    한쪽으로만 검사하면 안 된다 —
      - 가짜에만 있는 키: 다음 사람이 그 키를 쓰고 런타임에 `KeyError` 를 본다
        (실제로 `window_end` 가 그랬다)
      - 실제에만 있는 키: `print_summary` 가 `.get()` 으로 읽는 분기를 **태운다고 믿는
        테스트가 안 태운다**

    `run_batch` 에 키가 하나 늘면 여기서 걸려서 가짜도 같이 갱신하게 된다.
    """
    source = (ROOT / "app" / "batch" / "daily.py").read_text(encoding="utf-8")
    real: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == "run_batch":
            for ret in ast.walk(node):
                if isinstance(ret, ast.Return) and isinstance(ret.value, ast.Dict):
                    real = {
                        k.value for k in ret.value.keys if isinstance(k, ast.Constant)
                    }

    assert real, "run_batch 의 반환 dict 를 못 찾았습니다 — 이 가드가 헛돌고 있습니다"
    assert set(_fake_summary()) == real, (
        f"가짜가 계약과 갈립니다 — 실제에만: {sorted(real - set(_fake_summary()))} / "
        f"가짜에만: {sorted(set(_fake_summary()) - real)}"
    )


def test_main_switches_encoding_before_printing(monkeypatch):
    """`main()` 이 실제로 `force_utf8_output()` 을 부른다 — 배선까지 고정한다.

    헬퍼만 테스트하면 호출 한 줄이 빠져도 아무것도 안 깨진다.
    `main()` 은 인자 파싱·늦은 import·로깅 설정이 몰려 있어 손이 자주 가는 함수라,
    빠지면 윈도우에서 **성공한 배치가 다시 비-0 으로 끝나고** 종료코드 판정이 죽는다.
    """
    calls: list[str] = []

    async def fake_run_batch(**_kwargs):
        calls.append("run_batch")
        # `input_source` 는 "golden 이면 경고" 줄을 태우려고 이 값을 쓴다.
        return _fake_summary(input_source="load_golden_inputs")

    pin_settings(monkeypatch)
    monkeypatch.setattr(daily, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(daily, "run_batch", fake_run_batch)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    daily.main()

    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    # 출력·로깅보다 먼저 불려야 한다.
    assert calls.index("utf8") < calls.index("run_batch")


@pytest.mark.parametrize(
    "fake_get_settings, why",
    [
        (bad_log_level_settings, "logging 이 거부하는 레벨 — 진짜 basicConfig 가 던진다"),
        (unloadable_settings, "설정 로딩 자체가 실패 — get_settings() 가 던진다"),
    ],
)
def test_config_error_exits_two_before_running_the_batch(
    monkeypatch, capsys, fake_get_settings, why
):
    """설정 오류는 exit **2** 이고, `run_batch` 에 **들어가지도 않는다.**

    예전엔 `get_settings()` 가 `run_batch` **안쪽**에서만 불려서
    (`_active_version_params()` · `load_inputs_from_db()`) `MQ_PORT=abc` 같은 값 오류가
    미포착 `ValidationError` 로 나갔다 — **exit 1 + raw traceback**. 그런데 1 은 이 파일이
    *"배치는 돌았는데 일부가 실패"* 로 쓰는 값이라(`sys.exit(EXIT_RUNTIME_ERROR)`),
    **"아예 못 떴다" 와 "돌다가 일부 실패" 가 종료코드로 구분이 안 됐다.**

    **두 갈래를 다 돈다.** 초안은 `get_settings` 를 **성공하는** 가짜로 바꿔 레벨 갈래만
       탔는데, 그러면 누가 `settings = get_settings()` 를 `try` 위로 올리는 "정리" 를 해도
       이 파일은 초록이고 배치의 `MQ_PORT=abc` 가 조용히 exit 1 로 돌아간다.

    `run_batch` 를 **안 탄다는 것까지** 본다. 종료코드만 보면, 배치를 끝까지 돌고 나서
       실패로 끝나도 통과한다 — LLM 을 태운 뒤 죽는 것과 아예 안 시작하는 것은 다르다.
    """
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(logging_setup, "get_settings", fake_get_settings)

    ran = []

    # **코루틴이어야 한다.** 평범한 lambda 면 `asyncio.run(None)` 이 `ValueError` 로
    #    죽어서, 가드가 사라지는 회귀에서 `SystemExit` 대신 엉뚱한 예외가 나고
    #    `assert not ran` 이 **우연히** 지켜진다.
    async def fake_run_batch(**_kwargs):
        ran.append(1)
        return _fake_summary()

    monkeypatch.setattr(daily, "run_batch", fake_run_batch)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    with pytest.raises(SystemExit) as exc:
        daily.main()

    assert exc.value.code == exit_codes.EXIT_CONFIG_ERROR, why
    assert "설정을 읽지 못해" in capsys.readouterr().err, why
    assert not ran, "설정이 틀렸는데 배치가 돌았습니다"


@pytest.mark.parametrize(
    "exc, why",
    [
        (
            FileNotFoundError("raw DB 가 없습니다: /nope/raw.db — 볼륨 마운트 확인"),
            "raw DB 경로 부재 — CronJob 에서 제일 잦다(볼륨 누락·경로 오타)",
        ),
        (
            RuntimeError("분류 결과 테이블이 없습니다: /x/raw.db — worker 를 먼저 돌리세요"),
            "스키마·분류결과 전제 — `_require_classified_tables` 계열",
        ),
        (
            # 실제 `_check_version_cutover` 가 이 모양이다 — **조치 안내가 뒷줄에 있다.**
            RuntimeError(
                "윈도우 안 분류 결과가 옛 분류기 기준입니다\n"
                "  섞인 채로는 돌리지 않습니다 — 기준선 부정률이 0 이 되어 오탐이 됩니다.\n"
                "  활성 값이 의도한 것인지 확인하세요(LLM_MODEL 오타면 설정을 고치세요)."
            ),
            "여러 줄 — 첫 줄만 남기면 조치 안내를 잃는다",
        ),
        # **Postgres 문.** 아래 둘은 `FileNotFoundError` 도 `RuntimeError` 도 아니라서
        #    `connection_error_types()` 가 빠지면 **미포착 → exit 1 + raw traceback** 이다.
        # **두 베이스를 일부러 다 넣었다** — 누가 `psycopg.OperationalError` 로 좁히면
        #       아래 `UndefinedTable`(= ProgrammingError) 이 혼자 실패해서 알려준다.
        (
            psycopg.OperationalError(
                'connection failed: could not translate host name "rawdb" to address'
            ),
            "DB 미기동·호스트 오타·비밀번호 틀림 — OperationalError 계열",
        ),
        (
            psycopg.errors.UndefinedTable(
                'relation "voc_document" does not exist\n'
                "  운영 스키마에 우리 읽기 모델이 아직 없습니다 —"
                " docker/postgres/init/02_ai_read_model.sql 을 인프라에 요청하세요."
            ),
            "뷰·테이블 없음, GRANT 누락, DSN 형식 오타 — ProgrammingError 계열",
        ),
    ],
)
def test_runtime_environment_failures_exit_two(monkeypatch, capsys, exc, why):
    """실행 중 드러나는 **환경 전제**도 exit 2 다 — 부팅 가드만으로는 안 덮인다.

    `configure_logging_or_exit()` 은 `Settings` 로 읽히는 값만 본다. 그런데 배치에서 제일
    자주 나는 실패는 그 다음이다 — raw DB 경로가 틀렸거나, 스키마가 옛 버전이거나, 분류
    결과 테이블이 없는 것. 전부 **재시작해도 같은데** exit 1 로 나가면 k8s 가 영원히
    재시도한다.

    **메시지 전문이 나가야 한다.** 이 전제 검사들은 여러 줄로 조치 방법까지 담고 있어서
       (`LLM_MODEL 오타면 설정을 고치세요` 등) 첫 줄만 남기면 정작 필요한 안내를 잃는다 —
       부팅 경로(한 줄 축약)와 일부러 다르다.
    """
    pin_settings(monkeypatch)

    async def boom(**_kwargs):
        raise exc

    monkeypatch.setattr(daily, "run_batch", boom)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    with pytest.raises(SystemExit) as got:
        daily.main()

    assert got.value.code == exit_codes.EXIT_CONFIG_ERROR, why
    err = capsys.readouterr().err
    assert "환경이 준비되지 않아" in err, why
    # **전문**이 나가야 한다 — 첫 줄만 보면 위 여러 줄 케이스에서 조치 안내를 잃는다.
    for line in str(exc).splitlines():
        assert line.strip() in err, f"메시지가 잘렸습니다({why}): {line!r}"
    assert "Traceback" not in err, why


def test_runtime_error_stays_confined_to_preconditions():
    """위 분류의 전제 — `RuntimeError` 를 던지는 곳이 배치 전제검사뿐인지.

    `main()` 이 `(FileNotFoundError, RuntimeError)` 를 "환경 문제(=2)" 로 분류하는데,
    누군가 `app/` 어딘가에서 `RuntimeError` 를 **진짜 버그** 신호로 쓰기 시작하면 그게
    조용히 설정 오류로 오분류된다. 그때 이 테스트가 먼저 실패해서 분류를 다시 보게 한다.

    라이브러리가 던지는 `RuntimeError` 까지는 못 막는다. CronJob 이라 오분류 비용이
       비대칭이라(다음 예약 실행은 그대로 돈다) 감수한 선택이다 — `daily.main()` 주석 참고.
    """
    hits = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "app").rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Raise)
        and isinstance(node.exc, ast.Call)
        and isinstance(node.exc.func, ast.Name)
        and node.exc.func.id == "RuntimeError"
    }

    assert hits == {"app/batch/inputs.py"}, (
        "RuntimeError 를 던지는 곳이 배치 전제검사 밖으로 늘었습니다 — "
        f"daily.main() 의 (FileNotFoundError, RuntimeError) → exit 2 분류를 다시 보세요: {sorted(hits)}"
    )


def test_help_works_even_when_the_config_is_broken(monkeypatch):
    """`--help` 는 설정이 깨져 있어도 나와야 한다 — 그래서 설정 로딩이 `parse_args()` **뒤**다.

    순서를 앞으로 옮기면 **설정 오타 하나로 사용법조차 못 본다.** 하필 그때가 사용법이
       제일 필요한 순간이다(어떤 플래그로 고쳐 돌릴지 봐야 한다).
    반대 방향 제약도 있다 — `force_utf8_output()` 보다 뒤여야 한다(그건 첫 문장이어야
       하고 `tests/test_console_encoding.py` 가 강제한다). 두 제약 사이의 자리다.
    """
    pin_settings(monkeypatch, log_level="info")  # 설정이 깨진 상태
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--help"])

    with pytest.raises(SystemExit) as exc:
        daily.main()

    # argparse 의 정상 종료(0)여야 한다 — 설정 오류(2)로 먼저 죽으면 안 된다.
    assert exc.value.code == 0, "설정 로딩이 parse_args() 보다 앞으로 옮겨졌습니다"


def test_batch_failures_still_exit_one(monkeypatch):
    """반대편 — "돌았는데 일부 실패" 는 여전히 **1** 이다.

    위 테스트만 있으면 누가 `sys.exit(EXIT_CONFIG_ERROR)` 로 통일해도 안 걸린다.
       그러면 cron·k8s 가 **재시도해도 소용없는 실패**로 오해한다 — 이쪽은 다음 실행에
       나을 수 있는 실패다.
    """

    async def fake_run_batch(**_kwargs):
        return _fake_summary(
            failures=[{"stage": "발행", "target_key": "P001", "error": "boom"}]
        )

    pin_settings(monkeypatch)
    monkeypatch.setattr(daily, "run_batch", fake_run_batch)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    with pytest.raises(SystemExit) as exc:
        daily.main()

    # 두 코드가 서로 다르다는 단언은 여기 두지 않는다 —
    #    `test_main_entrypoint.py::test_the_contract_values_are_what_k8s_expects` 가
    #    이미 본다. 배치 동작 테스트 안에 두면 계약이 바뀔 때 두 곳이 서로 다른 메시지로
    #    실패한다.
    assert exc.value.code == exit_codes.EXIT_RUNTIME_ERROR


@pytest.mark.asyncio
async def test_window_end_fallback_uses_kst_today(tmp_path, monkeypatch):
    """문서가 0건이라 window_end 를 못 정할 때, 오늘 날짜를 **KST 로 정한다.**

    확정 문서의 KST 경계를 이 폴백에서도 지키는지만 본다. `date.today()` 는 호스트 로컬이라
    UTC 컨테이너에서는 KST 보다 하루 이른 날짜가 나온다.

    **retention 동작을 재는 테스트가 아니다**. 이 분기의
       `prior` 는 로그 건수에만 쓰인다 — documents 가 0건이면 `detect_anomaly` 가 즉시
       반환하고 `save_published` 도 건너뛴다. 컷오프 경계 자체는 위
       `test_state_roundtrip_drops_outside_retention` 이 이미 덮는다.

    UTC 호스트를 시계 monkeypatch 로 흉내낸다 — 서브프로세스가 필요 없다.
    **기준 순간을 오늘과 멀리 잡는 게 중요하다.** 오늘 근처로 잡으면 `date.today()`
       로 되돌려도 우연히 같은 값이 나와 뮤테이션이 안 물린다.
        UTC 2026-03-04 23:30  =  KST 2026-03-05 08:30  (두 날짜가 갈리는 순간)
    """

    class _UtcHostClock(datetime):
        """UTC 호스트의 시계. `tz` 를 안 주면 UTC 벽시계를 naive 로 돌려준다."""

        @classmethod
        def now(cls, tz=None):
            moment = datetime(2026, 3, 4, 23, 30, tzinfo=timezone.utc)
            return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(daily, "datetime", _UtcHostClock)

    seen: list[date] = []
    monkeypatch.setattr(
        daily, "load_prior_alerts", lambda window_end, path: seen.append(window_end) or []
    )

    await daily.run_batch(
        dry_run=True,
        state_path=tmp_path / "state.json",
        load_inputs=lambda window_end: ([], []),
    )

    assert seen == [date(2026, 3, 5)], "UTC 호스트에서 오늘 날짜가 하루 밀렸습니다"


# ── §4 가이드라인 재시도 대기열 ──────────────────────────────────
#
# "알림 발행은 성공했는데 가이드라인만 실패" 하면 알림이 억제 캐시에 들어가
# 그 건의 가이드라인이 영영 재시도되지 않던 구멍을 막는다. 대기열은
# published_alerts.json 이 아니라 **별도 파일**이다 — 백엔드 조회 API 가 붙으면
# 억제 캐시는 통째로 걷어내는데 "가이드라인을 받았는지"는 그 응답에 없어서다.


def _pending_path(tmp_path):
    return tmp_path / "pending.json"


class _FakeCallback:
    """generate_guideline 반환값 흉내. 배치는 내용을 안 보고 None 여부만 본다."""

    def __init__(self, alert_id):
        self.guideline_id = f"GD-{alert_id}"


@pytest.mark.asyncio
async def test_guideline_publish_failure_goes_to_pending_queue(tmp_path, monkeypatch):
    """발행 실패분이 대기열에 남고, 알림 자체는 억제 캐시에 들어간다.

    알림까지 캐시에서 빼면 다음 배치가 알림·개선안을 통째로 재생성한다(LLM 재지불).
    가이드라인만 대기열로 가는 것이 '상태 두 단계로 쪼개기'의 핵심이다.
    """

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("MQ down")

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    saved = json.loads(_pending_path(tmp_path).read_text(encoding="utf-8"))
    assert len(saved) == 1, "발행 실패분이 대기열에 있어야 한다"
    assert saved[0]["attempts"] == 0, "본배치 실패는 재시도를 아직 안 쓴 상태다"
    assert summary["guideline_pending"] == 1
    assert summary["delivered"] >= 1, (
        "알림은 발행·캐시돼야 한다 — 억제가 유지돼야 재과금이 없다"
    )
    assert any(f["stage"] == "발행:가이드" for f in summary["failures"])


@pytest.mark.asyncio
async def test_guideline_generation_exception_goes_to_pending_queue(
    tmp_path, monkeypatch
):
    """생성 예외도 대기열 대상이다 — 백엔드가 아무것도 못 들은 건 같아서다."""

    async def guideline_gen_fails(alert, inquiries, *, product_name=None):
        raise ValueError("대상 알림인데 원문 조회가 전부 실패")

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "generate_guideline", guideline_gen_fails)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    saved = json.loads(_pending_path(tmp_path).read_text(encoding="utf-8"))
    assert len(saved) == 1
    assert summary["guideline_pending"] == 1
    assert any(f["stage"] == "가이드라인" for f in summary["failures"])


@pytest.mark.asyncio
async def test_not_a_target_is_not_enqueued(tmp_path, monkeypatch):
    """None(대상 아님)은 대기열에 안 들어간다 — 스코프 밖 알림이 영구 적재되면 안 된다."""

    async def not_a_target(alert, inquiries, *, product_name=None):
        return None

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "generate_guideline", not_a_target)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    assert summary["guideline_pending"] == 0
    assert json.loads(_pending_path(tmp_path).read_text(encoding="utf-8")) == []


@pytest.mark.asyncio
async def test_failed_callback_that_published_is_not_enqueued(tmp_path, monkeypatch):
    """FAILED_* 콜백이 정상 발행됐으면 재시도하지 않는다 — 종결 상태다.

    generate_guideline docstring 이 FAILED_* 를 백엔드가 "생성 중" 에서 벗어나는
    종결 상태로 규정한다. 재시도하면 백엔드의 FAILED 행을 나중에 SUCCESS 로 덮는
    계약 변경이 된다. 배치는 콜백 내용을 안 보므로 발행 성공 여부가 유일한 기준이다.
    """

    async def failed_callback(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)  # status=FAILED_* 였다고 가정

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_ok(guideline, trace_id):
        return None

    monkeypatch.setattr(daily, "generate_guideline", failed_callback)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_ok)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    assert summary["guideline_pending"] == 0
    assert json.loads(_pending_path(tmp_path).read_text(encoding="utf-8")) == []


@pytest.mark.asyncio
async def test_retry_pass_republishes_and_dequeues(tmp_path, monkeypatch):
    """대기열의 건이 다음 배치에서 재생성 → 재발행되고 대기열에서 빠진다."""
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [{"alert": _alert("ALT-PENDING", date(2026, 8, 27)), "attempts": 0}],
        pending_path,
    )

    published: list[str] = []

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def record_publish(guideline, trace_id):
        published.append(guideline.guideline_id)

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", record_publish)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    assert "GD-ALT-PENDING" in published, "대기 건이 재발행돼야 한다"
    assert summary["guideline_retried"] == 1
    assert summary["guideline_pending"] == 0
    assert json.loads(pending_path.read_text(encoding="utf-8")) == []


@pytest.mark.asyncio
async def test_retry_failure_increments_attempts_and_keeps_entry(tmp_path, monkeypatch):
    """재시도가 또 실패하면 attempts 만 오르고 대기열에 남는다 + 요약에도 남는다."""
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [{"alert": _alert("ALT-PENDING", date(2026, 8, 27)), "attempts": 0}],
        pending_path,
    )

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("MQ still down")

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    saved = {
        item["alert"]["alert_id"]: item
        for item in json.loads(pending_path.read_text(encoding="utf-8"))
    }
    assert saved["ALT-PENDING"]["attempts"] == 1, "재시도 1회 소진이 기록돼야 한다"
    assert summary["guideline_retried"] == 0
    retry_failures = [f for f in summary["failures"] if f["stage"] == "가이드라인 재시도"]
    # 식별자 키는 target_key 하나다 — alert_id 로 넣으면 print_summary 가
    # "식별자 없음" 으로 찍는다.
    assert retry_failures and retry_failures[0]["target_key"] == "ALT-PENDING"
    # 이 배치 자신의 신규 실패도 attempts=0 으로 같이 들어간다 — 서로 안 겹친다.
    assert all(
        item["attempts"] == 0 for aid, item in saved.items() if aid != "ALT-PENDING"
    )


@pytest.mark.asyncio
async def test_retry_exhausted_is_dropped_not_retried_forever(tmp_path, monkeypatch):
    """상한을 소진하면 포기한다 — 영구 실패(S3 미구성)의 매일 재지불을 막는다."""
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [
            {
                "alert": _alert("ALT-PENDING", date(2026, 8, 27)),
                "attempts": daily.GUIDELINE_RETRY_MAX_ATTEMPTS - 1,
            }
        ],
        pending_path,
    )

    async def guideline_for_pending_only(alert, inquiries, *, product_name=None):
        # 메인 루프의 신규 알림은 대상 아님(None) — 재시도 경로만 검사한다.
        if alert.alert_id == "ALT-PENDING":
            return _FakeCallback(alert.alert_id)
        return None

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("S3 미구성 — 영구 실패")

    monkeypatch.setattr(daily, "generate_guideline", guideline_for_pending_only)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    assert summary["guideline_retry_exhausted"] == 1
    assert summary["guideline_pending"] == 0, "포기한 건은 대기열에서 빠져야 한다"
    assert json.loads(pending_path.read_text(encoding="utf-8")) == []


@pytest.mark.asyncio
async def test_dry_run_counts_pending_but_touches_nothing(tmp_path, monkeypatch):
    """dry-run 은 대기 건을 비용 추정에 더하되, 재시도도 저장도 하지 않는다."""
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [
            {"alert": _alert("ALT-P1", date(2026, 8, 27)), "attempts": 0},
            {"alert": _alert("ALT-P2", date(2026, 8, 26)), "attempts": 1},
        ],
        pending_path,
    )
    before = pending_path.read_text(encoding="utf-8")

    published: list = []

    async def record_publish(guideline, trace_id):
        published.append(guideline)

    monkeypatch.setattr(daily, "publish_guideline_generated", record_publish)

    summary = await daily.run_batch(
        dry_run=True,
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    # 스텁 알림은 inquiry_ids 가 비어 게이트가 닫히므로(0건) 대기 2건이 전부다.
    assert summary["llm_calls"].get("가이드라인") == 2, (
        "재시도 비용이 추정에 들어가야 한다"
    )
    assert published == [], "dry-run 은 발행하지 않는다"
    assert pending_path.read_text(encoding="utf-8") == before, (
        "dry-run 은 파일을 안 건드린다"
    )
    assert summary["guideline_pending"] == 2


def test_pending_entries_older_than_retention_are_dropped(tmp_path):
    """보관 기간(35일) 밖의 대기 건은 로드에서 포기된다 — 원문이 입력 창 밖이라 성공 불가."""
    path = _pending_path(tmp_path)
    now = date(2026, 8, 28)
    old = date.fromordinal(now.toordinal() - daily.STATE_RETENTION_DAYS - 1)
    daily.save_pending_guidelines(
        [
            {"alert": _alert("ALT-FRESH", now), "attempts": 1},
            {"alert": _alert("ALT-STALE", old), "attempts": 0},
        ],
        path,
    )

    loaded = daily.load_pending_guidelines(now, path)

    assert [e["alert"].alert_id for e in loaded] == ["ALT-FRESH"]
    assert loaded[0]["attempts"] == 1, "왕복에서 attempts 가 보존돼야 한다"


def test_corrupt_pending_file_degrades_to_empty(tmp_path):
    """깨진 대기열은 빈 대기열로 강등된다 — 파일 하나로 배치가 영구히 못 뜨면 안 된다."""
    path = _pending_path(tmp_path)
    path.write_text("{ not json", encoding="utf-8")
    assert daily.load_pending_guidelines(date(2026, 8, 28), path) == []

    path.write_text(
        json.dumps([{"alert": {"broken": True}, "attempts": 0}]), encoding="utf-8"
    )
    assert daily.load_pending_guidelines(date(2026, 8, 28), path) == []


@pytest.mark.asyncio
async def test_pending_path_derives_from_custom_state_path(tmp_path, monkeypatch):
    """--state-path 를 주면 대기열도 그 옆 파생 이름으로 따라간다.

    디렉토리가 아니라 **파일명에서** 파생한다 — 파일명만 바꿔 운영 디렉토리를 준
    디버그 실행이 운영 대기열을 소비·저장하는 사고를 막는다.
    """

    async def guideline_gen_fails(alert, inquiries, *, product_name=None):
        raise RuntimeError("boom")

    async def sent(alert, rec, trace_id, versions=None):
        return None

    monkeypatch.setattr(daily, "generate_guideline", guideline_gen_fails)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    await daily.run_batch(state_path=tmp_path / "debug.json", load_inputs=_stub_inputs)

    derived = tmp_path / "debug.pending_guidelines.json"
    assert derived.exists(), "대기열이 state_path 옆 파생 이름으로 생겨야 한다"
    assert len(json.loads(derived.read_text(encoding="utf-8"))) == 1


# ── 대기열 쓰기 순서와 조정 ──────────────────────────────────────


@pytest.mark.asyncio
async def test_pending_save_failure_does_not_suppress_the_alert(tmp_path, monkeypatch):
    """P1 — 대기열 저장이 실패하면 그 알림을 억제 캐시에도 넣지 않는다.

    억제 캐시를 먼저 저장하면 두 쓰기 사이에서 실패했을 때 알림은 억제되는데 대기
    항목이 없어 가이드라인이 영구 유실된다 — 이 PR 이 막으려는 구멍 그대로다.
    영구 유실 대신 다음 배치의 통째 재처리(중복 비용)를 택한다.
    """

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("MQ down")

    def pending_save_fails(entries, path):
        raise OSError("disk full")

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)
    monkeypatch.setattr(daily, "save_pending_guidelines", pending_save_fails)

    state_path = tmp_path / "state.json"
    summary = await daily.run_batch(
        state_path=state_path,
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    assert any(f["stage"] == "대기열 저장" for f in summary["failures"])
    assert summary["state_cached"] == 0, "대기 기록이 없는 알림은 억제되면 안 된다"
    assert not state_path.exists(), "억제 캐시에 들어가면 가이드라인이 영구 유실된다"


@pytest.mark.asyncio
async def test_alert_publish_failure_is_not_enqueued_for_retry(tmp_path, monkeypatch):
    """P2 — 알림 발행까지 실패한 건은 대기열에 안 넣는다.

    그 알림은 억제 캐시에 없어 다음 배치가 신규 target 으로 통째로 재처리한다
    (가이드라인도 그 경로에서 다시 만들어진다). 대기열에도 넣으면 같은
    guideline_id 가 두 경로에서 두 번 생성·발행된다.
    """

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def anomaly_publish_fails(alert, rec, trace_id, versions=None):
        raise RuntimeError("MQ down")

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("MQ down")

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", anomaly_publish_fails)
    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)

    summary = await daily.run_batch(
        state_path=tmp_path / "state.json",
        pending_path=_pending_path(tmp_path),
        load_inputs=_stub_inputs,
    )

    assert summary["delivered"] == 0
    assert summary["guideline_pending"] == 0, "알림째 재처리될 건이 대기열에 있으면 이중 생성"
    assert json.loads(_pending_path(tmp_path).read_text(encoding="utf-8")) == []


@pytest.mark.asyncio
async def test_max_alerts_zero_holds_pending_retries(tmp_path, monkeypatch):
    """P2 — 재시도가 --max-alerts 예산을 우회하면 안 된다.

    max_alerts=0 은 "이 실행에서 LLM·S3 비용 0" 이라는 뜻이다. 대기열이 그걸
    우회하면 장애 복구 직후 대기열 규모만큼 비용이 한 번에 나간다. 예산 밖 대기
    건은 attempts 를 쓰지 않고 그대로 다음 배치로 넘어간다.
    """
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [
            {"alert": _alert("ALT-P1", date(2026, 8, 27)), "attempts": 0},
            {"alert": _alert("ALT-P2", date(2026, 8, 26)), "attempts": 1},
        ],
        pending_path,
    )
    before = pending_path.read_text(encoding="utf-8")

    generated: list[str] = []

    async def record_generate(alert, inquiries, *, product_name=None):
        generated.append(alert.alert_id)
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def publish_ok(guideline, trace_id):
        return None

    monkeypatch.setattr(daily, "generate_guideline", record_generate)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", publish_ok)

    summary = await daily.run_batch(
        max_alerts=0,
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    assert generated == [], "예산 0 인데 재생성이 돌면 상한이 무의미하다"
    assert summary["guideline_retried"] == 0
    assert summary["guideline_pending"] == 2
    assert pending_path.read_text(encoding="utf-8") == before, "attempts 도 안 쓴다"

    # dry-run 비용 추정도 같은 예산을 적용한다 — 실측(위)과 어긋나면 추정을 못 믿는다.
    dry = await daily.run_batch(
        max_alerts=0,
        dry_run=True,
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )
    assert dry["llm_calls"].get("가이드라인", 0) == 0


@pytest.mark.asyncio
async def test_retry_budget_is_what_max_alerts_leaves_over(tmp_path, monkeypatch):
    """P2 — 예산은 신규 target 이 먼저 쓰고, 남는 만큼만 재시도한다."""
    pending_path = _pending_path(tmp_path)
    daily.save_pending_guidelines(
        [
            {"alert": _alert("ALT-P1", date(2026, 8, 27)), "attempts": 0},
            {"alert": _alert("ALT-P2", date(2026, 8, 26)), "attempts": 1},
        ],
        pending_path,
    )

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def publish_ok(guideline, trace_id):
        return None

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)
    monkeypatch.setattr(daily, "publish_guideline_generated", publish_ok)

    # 스텁 입력은 alert 1건을 낸다 → max_alerts=2 면 재시도 몫은 1.
    summary = await daily.run_batch(
        max_alerts=2,
        state_path=tmp_path / "state.json",
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    assert summary["processed"] == 1
    assert summary["guideline_retried"] == 1, "남은 예산 1 만큼만 재시도"
    saved = json.loads(pending_path.read_text(encoding="utf-8"))
    assert [(e["alert"]["alert_id"], e["attempts"]) for e in saved] == [("ALT-P2", 1)], (
        "예산 밖 건은 attempts 그대로 대기열에 남는다"
    )


@pytest.mark.asyncio
async def test_crash_between_pending_and_suppression_saves_single_path(
    tmp_path, monkeypatch
):
    """2회전 P1 — "대기열 저장 성공 + 억제 캐시 저장 실패" 크래시 창의 재실행.

    두 상태 파일은 원자적으로 같이 못 쓴다. 그 사이에서 죽으면 다음 실행에 같은
    알림이 신규 target(억제 안 됨)과 대기열 **양쪽**에 있다 — 조정 없이는 같은
    guideline_id 가 2회 생성·발행된다. 실행 초입에 겹치는 대기 항목을
    걷어내 메인 루프 한 경로만 태운다.
    """
    pending_path = _pending_path(tmp_path)
    state_path = tmp_path / "state.json"
    published: list[str] = []

    async def fake_guideline(alert, inquiries, *, product_name=None):
        return _FakeCallback(alert.alert_id)

    async def sent(alert, rec, trace_id, versions=None):
        return None

    async def guideline_publish_fails(guideline, trace_id):
        raise RuntimeError("MQ down")

    async def record_publish(guideline, trace_id):
        published.append(guideline.guideline_id)

    monkeypatch.setattr(daily, "generate_guideline", fake_guideline)
    monkeypatch.setattr(daily, "publish_anomaly_analyzed", sent)

    # 1차 실행: 가이드라인 발행 실패(→ 대기열 write-ahead 저장 성공) 직후
    # 억제 캐시 저장이 죽는다.
    real_save_published = daily.save_published

    def save_published_crashes(published_alerts, window_end, path):
        raise OSError("disk full")

    monkeypatch.setattr(daily, "publish_guideline_generated", guideline_publish_fails)
    monkeypatch.setattr(daily, "save_published", save_published_crashes)
    with pytest.raises(OSError):
        await daily.run_batch(
            state_path=state_path,
            pending_path=pending_path,
            load_inputs=_stub_inputs,
        )

    assert json.loads(pending_path.read_text(encoding="utf-8")), (
        "write-ahead 라 대기열은 남는다"
    )
    assert not state_path.exists(), "억제 캐시는 안 써졌다 — 불일치 상태 재현"

    # 2차 실행: 전부 정상. 같은 alert_id 가 target 과 대기열 양쪽에 있는 상태다.
    monkeypatch.setattr(daily, "save_published", real_save_published)
    monkeypatch.setattr(daily, "publish_guideline_generated", record_publish)
    summary = await daily.run_batch(
        state_path=state_path,
        pending_path=pending_path,
        load_inputs=_stub_inputs,
    )

    assert len(published) == 1, f"한 경로만 발행해야 한다 — 실제 {published}"
    assert summary["guideline_retried"] == 0, "대기 항목은 신규 target 에 밀려 걷힌다"
    assert json.loads(pending_path.read_text(encoding="utf-8")) == []
    assert state_path.exists(), "이번엔 억제 캐시까지 써져 불일치가 해소된다"
