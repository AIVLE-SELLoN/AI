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


def test_optional_wiring_is_actually_connected():
    """🔴 폴백은 **미구현용**이다. 실물이 있는데 폴백을 타면 배치가 조용히 no-op 이 된다.

    `_missing()` 은 모듈이 없을 때만 폴백하려는 것인데, `from X import Y` 에서 **Y 만**
    없어도 `exc.name` 이 모듈명이라 True 가 나온다(3.12 확인). 그래서 import 하는 심볼
    이름에 오타가 나면 폴백이 조용히 켜지고, 개선안·가이드라인이 **둘 다 no-op** 인데
    요약엔 "ℹ️ 미연결" 한 줄만 찍히고 배치는 정상 종료한다. 셀러에게 개선안이 하나도
    안 나가는 상태다.

    이 파일의 다른 테스트는 그 함수들을 전부 monkeypatch 하므로 이 끊김을 못 잡는다.
    (2026-08-11 리뷰 ②)
    """
    assert daily.MQ_AVAILABLE, "app.core.mq 가 있는데 폴백을 타고 있다"
    assert daily.RECOMMENDATION_AVAILABLE, "Agent3 가 있는데 폴백을 타고 있다"
    assert daily.GUIDELINE_AVAILABLE, "가이드라인이 있는데 폴백을 타고 있다"


def test_classifier_versions_only_when_the_filter_guaranteed_them():
    """🔴 payload 의 분류기 신원은 **필터가 보장할 때만** 값이 있다.

    실을 수 있는 근거가 `_ASPECT_SQL` 의 활성 버전 필터뿐이다 — 그 필터가 "이 알림에
    기여한 모든 행의 버전 3종이 활성 값"임을 쿼리로 강제하므로 주장이 아니라 관측이다.
    골든 입력(`--input-source golden`)은 CSV 를 그대로 읽어 그 필터를 안 타므로, 같은 값을
    실으면 **검증한 적 없는 것을 검증된 것처럼 보고**하게 된다. 골든은 분류 오차가 0 인
    oracle 이라 애초에 분류기를 안 거쳤다 — `null` 이 정확한 답이다.
    """
    versions = daily._classifier_versions_for(daily.load_inputs_from_db)

    assert set(versions) == {"prompt_cs", "prompt_review", "model", "pipeline"}
    # 필터가 쓰는 값과 **같은 값**이어야 한다. 따로 조립하면 payload 가 실제로 읽은 것과
    # 다른 버전을 말하게 된다.
    assert tuple(versions.values()) == daily._active_version_params()

    assert daily._classifier_versions_for(_stub_inputs) is None


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
    매 배치가 첫 실행처럼 굴러간다. (지인님 PR 리뷰 §5)
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
    """🔴 CS 원문 매핑이 터져도 배치가 끝까지 돈다 — 알림 발행도 캐시 저장도 살아 있다.

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
    """⚠️ 개선안이 조용히 실패해도 요약·종료코드에 남는다.

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
    """🔴 라우팅 미스도 **실패가 아니다** — 건수로만 센다 (2026-08-11 리뷰 반영).

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
    """🔴 근거 0건은 **실패가 아니다** — 건수로만 세고 종료코드에 안 싣는다 (2026-08-10).

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
    """⚠️ 실패 1건이 요약에 1건으로 잡힌다.

    `generate_outcome_for_alert` 는 계약상 안 던지지만 던지는 날엔, except 와 뒤따르는
    `rec is None` 검사가 **둘 다** 타서 실패가 2배로 보고됐다. 그러면 배치 요약의
    실패 건수를 못 믿게 된다. (2026-08-07 재검토)
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
    과대추정된다. (지인님 PR 리뷰 §2)
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
    # 아예 안 부른다. 예전엔 알림 수만큼 세서 **비용 추정이 위로 어긋났다**(2026-08-10).
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
    """⚠️ 루프 도중 터져도 MQ 연결을 닫는다.

    app/core/mq.py 가 프로세스당 연결을 재사용하는데, 안 닫고 이벤트 루프가 내려가면
    connect_robust 의 재연결 태스크가 남아 "Task was destroyed but it is pending" 이
    뜬다. 나중에 배치가 장수 프로세스에 얹히면 연결이 샌다. (서영님 PR 리뷰 §1)
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


def test_main_switches_encoding_before_printing(monkeypatch):
    """⚠️ `main()` 이 실제로 `force_utf8_output()` 을 부른다 — 배선까지 고정한다.

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

    monkeypatch.setattr(daily, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(daily, "run_batch", fake_run_batch)
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--dry-run"])

    daily.main()

    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    # 출력·로깅보다 먼저 불려야 한다.
    assert calls.index("utf8") < calls.index("run_batch")


@pytest.mark.asyncio
async def test_window_end_fallback_uses_kst_today(tmp_path, monkeypatch):
    """문서가 0건이라 window_end 를 못 정할 때, 오늘 날짜를 **KST 로 정한다.**

    §3(KST 경계)을 이 폴백에서도 지키는지만 본다. `date.today()` 는 호스트 로컬이라
    UTC 컨테이너에서는 KST 보다 하루 이른 날짜가 나온다.

    ⚠️ **retention 동작을 재는 테스트가 아니다** (서영님 사후 리뷰, PR #68). 이 분기의
       `prior` 는 로그 건수에만 쓰인다 — documents 가 0건이면 `detect_anomaly` 가 즉시
       반환하고 `save_published` 도 건너뛴다. 컷오프 경계 자체는 위
       `test_state_roundtrip_drops_outside_retention` 이 이미 덮는다.

    UTC 호스트를 시계 monkeypatch 로 흉내낸다 — 서브프로세스가 필요 없다.
    ⚠️ **기준 순간을 오늘과 멀리 잡는 게 중요하다.** 오늘 근처로 잡으면 `date.today()`
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
