"""담당: 서영 (Agent2) — 알림 발행 규칙 · 재알림 억제 · [0]~[8] 파이프라인 통합 테스트.

LLM 은 목킹한다 (비용 0). 통합 테스트는 "숫자를 넣으면 몇 건이 어떤 채널로 나가는가"를
본다 — 개별 판정 규칙은 test_detection.py·test_confidence.py 가 각각 담당.
"""

import itertools
from datetime import date, datetime

import pytest

from app.core.schemas import (
    Channel,
    ClassifiedItem,
    DetectionConfidence,
    DetectionStats,
    RecommendedAction,
    Source,
    Verdict,
)
from app.detection.alert import (
    UNSPECIFIED_CAUSE,
    build_alert,
    build_root_cause,
    resolve_channel,
)
from app.detection.service import detect_anomaly, normalize
from app.detection.suppression import filter_suppressed


class _FakeClient:
    """[6] 프롬프트3 응답을 고정하는 가짜 LlmClient."""

    def __init__(self, cause="사진_색감_오차"):
        self._cause = cause
        self.calls = 0

    async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
        self.calls += 1
        return {
            "results": [
                {"cs_id": f"C{i}", "cause": self._cause, "confidence": 0.9, "aspect_match": True}
                for i in range(12)
            ]
        }


# ── root_cause 2상태 (스키마 §5.2) ───────────────────────────────
def test_root_cause_none_when_step6_skipped():
    """[6] 자체를 안 돌렸으면 null — '봤는데 흩어졌다'와 구분된다."""
    assert build_root_cause(None) is None


def test_root_cause_unspecified_when_scattered():
    """[6] 수행했으나 분산 → label='미특정', consistent=False."""
    root = build_root_cause({"label": None, "consistent": False, "count": 0, "total": 20})
    assert root.label == UNSPECIFIED_CAUSE
    assert root.consistent is False
    assert root.total == 20


def test_root_cause_label_when_consistent():
    root = build_root_cause(
        {"label": "사진_색감_오차", "consistent": True, "count": 14, "total": 20}
    )
    assert root.label == "사진_색감_오차"
    assert root.count == 14


# ── 발행 채널 규칙 (스키마 §5.1) ──────────────────────────────────
def test_global_verdict_collapses_channel_to_all():
    assert resolve_channel(Verdict.GLOBAL, "COUPANG") == Channel.ALL
    assert resolve_channel(Verdict.TENTATIVE_GLOBAL, "COUPANG") == Channel.ALL


def test_biased_keeps_own_channel():
    assert resolve_channel(Verdict.BIASED, "COUPANG") == Channel.COUPANG
    assert resolve_channel(Verdict.INDETERMINATE, "NAVER") == Channel.NAVER


def _judgement(**overrides):
    base = {
        "product": "P001",
        "aspect": "색상",
        "channel": "COUPANG",
        "verdict": Verdict.BIASED,
        "significant_channels": ["COUPANG"],
        "excluded_channels": [],
        "stats": DetectionStats(
            source=Source.CS, cur_rate=0.13, past_rate=0.05, delta=0.08,
            p_value=0.0001, bh_significant=True, cur_total=200,
        ),
        "confidence": DetectionConfidence.MEDIUM,
        "interpretation": "CS 선행 신호 — 리뷰는 시차로 미반영 가능",
        "cs_signal": True,
        "review_signal": None,
        "root_cause": build_root_cause(
            {"label": "사진_색감_오차", "consistent": True, "count": 14, "total": 20}
        ),
        "inquiry_ids": ["INQ-1", "INQ-2"],
        "sub_aspects": [],
        "linked_change_id": None,
    }
    return {**base, **overrides}


def test_build_alert_biased_generates_recommendation():
    alert = build_alert(
        _judgement(),
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, 7),
        seq=itertools.count(1),
    )
    assert alert.alert_id == "ALT-20260707-0001"
    assert alert.channel == Channel.COUPANG
    assert alert.recommended_action == RecommendedAction.GENERATE_RECOMMENDATION
    assert alert.scope_in is True
    assert alert.evidence.inquiry_ids == ["INQ-1", "INQ-2"]


def test_build_alert_scattered_cause_downgrades_action():
    alert = build_alert(
        _judgement(
            root_cause=build_root_cause(
                {"label": None, "consistent": False, "count": 0, "total": 20}
            )
        ),
        detected_at=datetime(2026, 7, 7),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, 7),
        seq=itertools.count(1),
    )
    assert alert.recommended_action == RecommendedAction.CHANNEL_OPERATION_CHECK
    assert alert.root_cause.label == UNSPECIFIED_CAUSE


def test_build_alert_out_of_scope_aspect_keeps_scope_in_false():
    """파손은 scope_in=false 이고 조치는 물류 점검 — 전역형이 아니어도 개선안 대상 아님."""
    alert = build_alert(
        _judgement(aspect="파손", root_cause=None),
        detected_at=datetime(2026, 7, 7),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, 7),
        seq=itertools.count(1),
    )
    assert alert.scope_in is False
    assert alert.recommended_action == RecommendedAction.LOGISTICS_CHECK


# ── 재알림 억제 (로직 §6) ─────────────────────────────────────────
def _alert(product="P001", aspect="색상", channel="COUPANG", cur_rate=0.13, day=7, alert_id="A1"):
    return build_alert(
        _judgement(
            product=product,
            aspect=aspect,
            channel=channel,
            stats=DetectionStats(
                source=Source.CS, cur_rate=cur_rate, past_rate=0.05, delta=0.08,
                p_value=0.0001, bh_significant=True, cur_total=200,
            ),
        ),
        detected_at=datetime(2026, 7, day),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, day),
        seq=itertools.count(int(alert_id[1:])),
    )


def test_no_prior_alerts_passes_everything():
    alerts = [_alert()]
    published, suppressed = filter_suppressed(alerts, [])
    assert published == alerts
    assert suppressed == []


def test_same_combo_within_7_days_is_suppressed():
    prior = _alert(day=5, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.14)          # +1%p 뿐 → 갱신 조건 미달
    published, suppressed = filter_suppressed([current], [prior])
    assert published == []
    assert suppressed == [current]


def test_extra_5pp_rise_allows_update_alert():
    """억제 기간 중이라도 +5%p 추가 상승이면 갱신 — updates_alert_id 에 원본. (§6)"""
    prior = _alert(day=5, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.18)
    published, suppressed = filter_suppressed([current], [prior])
    assert suppressed == []
    assert published[0].updates_alert_id == prior.alert_id


def test_after_block_window_is_new_alert():
    prior = _alert(day=1, cur_rate=0.13)
    current = _alert(day=9, cur_rate=0.13)          # 8일 경과 → 억제 해제
    published, _ = filter_suppressed([current], [prior])
    assert published[0].updates_alert_id is None


def test_resolved_alert_releases_suppression():
    """승인/반려 처리가 끝난 건은 억제 근거가 아니다 — §6 "또는 ~ 처리 전까지"."""
    prior = _alert(day=5, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.13)
    published, suppressed = filter_suppressed(
        [current], [prior], resolved_alert_ids={prior.alert_id}
    )
    assert len(published) == 1
    assert published[0].updates_alert_id is None    # 갱신이 아니라 신규
    assert suppressed == []


def test_different_channel_is_not_suppressed():
    """억제 단위는 (상품, aspect, 채널) — 다른 채널은 별개 알림이다."""
    prior = _alert(channel="COUPANG", day=5)
    current = _alert(channel="NAVER", day=7)
    published, suppressed = filter_suppressed([current], [prior])
    assert len(published) == 1
    assert suppressed == []


# ── 입력 정규화 ──────────────────────────────────────────────────
def _item(item_id, channel, aspects, day, source="cs", product="P001"):
    return ClassifiedItem(
        item_id=item_id,
        source=source,
        channel=channel,
        product_group_id=product,
        raw_text="사진이랑 색이 너무 달라요",
        aspects=aspects,
        created_at=datetime(2026, 7, day, 12, 0),
    )


def test_normalize_keeps_one_row_per_inquiry():
    """한 문의가 2 aspect 부정이어도 행은 1개 — 쪼개면 분모가 부풀어 부정률이 반토막."""
    items = [
        _item("C1", "COUPANG", [
            {"aspect": "색상", "sentiment": -1},
            {"aspect": "사이즈", "sentiment": -1},
        ], day=3)
    ]
    rows = normalize(items)
    assert len(rows) == 1
    assert set(rows[0]["neg_aspects"]) == {"색상", "사이즈"}


def test_normalize_excludes_non_negative_from_numerator():
    """긍정·중립은 분모엔 들지만 neg_aspects 엔 안 들어간다."""
    items = [_item("C1", "COUPANG", [{"aspect": "색상", "sentiment": 1}], day=3)]
    rows = normalize(items)
    assert rows[0]["neg_aspects"] == []


# ── [0]~[8] 통합 ─────────────────────────────────────────────────
def _scenario_items():
    """쿠팡만 색상 부정이 5% → 30% 로 뛰는 편중형 시나리오.

    과거 28일: 채널마다 100건 중 색상 부정 5건(5%)
    현재 7일 : 쿠팡 40건 중 12건(30%) / 네이버·지그재그 40건 중 2건(5%)
    """
    items: list[ClassifiedItem] = []
    counter = itertools.count(1)
    neg = [{"aspect": "색상", "sentiment": -1}]
    pos = [{"aspect": "색상", "sentiment": 1}]

    for channel in ("COUPANG", "NAVER", "ZIGZAG"):
        # 과거 윈도우 (6/3 ~ 6/30) — 하루 약 3~4건씩 흩어 놓는다.
        for i in range(100):
            day = 3 + (i % 28)
            aspects = neg if i < 5 else pos
            items.append(
                ClassifiedItem(
                    item_id=f"P{next(counter)}",
                    source="cs",
                    channel=channel,
                    product_group_id="P001",
                    raw_text="사진이랑 색이 너무 달라요",
                    aspects=aspects,
                    created_at=datetime(2026, 6, day, 12, 0),
                )
            )
        # 현재 윈도우 (7/1 ~ 7/7)
        negatives = 12 if channel == "COUPANG" else 2
        for i in range(40):
            items.append(_item(f"C{next(counter)}", channel, neg if i < negatives else pos,
                               day=1 + (i % 7)))
    return items


@pytest.mark.asyncio
async def test_pipeline_emits_biased_alert_for_single_channel():
    client = _FakeClient()
    alerts, suppressed = await detect_anomaly(
        _scenario_items(),
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_end=date(2026, 7, 7),
        client=client,
    )

    assert suppressed == []
    assert len(alerts) == 1, [a.channel for a in alerts]

    alert = alerts[0]
    assert alert.verdict == Verdict.BIASED
    assert alert.channel == Channel.COUPANG          # 쿠팡만 발화 → 편중형
    assert alert.main_aspect == "색상"
    assert alert.stats.source == Source.CS
    assert alert.stats.cur_total == 40
    assert alert.stats.cur_rate == pytest.approx(0.30)
    assert alert.stats.past_rate == pytest.approx(0.05)
    assert alert.stats.bh_significant is True
    assert alert.root_cause.label == "사진_색감_오차"
    assert alert.recommended_action == RecommendedAction.GENERATE_RECOMMENDATION
    assert alert.detection_confidence == DetectionConfidence.MEDIUM  # 시점 미확인
    assert alert.source_signals.cs is True
    assert alert.source_signals.review is None      # 리뷰 데이터 없음 = 보류
    assert alert.window_start == date(2026, 7, 1)
    assert alert.window_end == date(2026, 7, 7)


@pytest.mark.asyncio
async def test_pipeline_change_log_raises_confidence_to_high():
    """상세페이지 수정 이력이 시점 일치하면 확신도 '높음' + linked_change_id 기록."""
    alerts, _ = await detect_anomaly(
        _scenario_items(),
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_end=date(2026, 7, 7),
        change_log={("P001", "색상"): "CHG-0009"},
        client=_FakeClient(),
    )
    assert alerts[0].detection_confidence == DetectionConfidence.HIGH
    assert alerts[0].evidence.linked_change_id == "CHG-0009"


@pytest.mark.asyncio
async def test_pipeline_scattered_cause_gives_low_confidence():
    """원인이 흩어지면 편중은 확실해도 확신도 '낮음' + 채널 운영 점검 권장."""

    class _ScatteredClient:
        calls = 0

        async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
            causes = ["사진_색감_오차", "조명_보정_차이", "실물_염색_편차", "기타"]
            return {
                "results": [
                    {"cs_id": f"C{i}", "cause": causes[i % 4], "aspect_match": True}
                    for i in range(12)
                ]
            }

    alerts, _ = await detect_anomaly(
        _scenario_items(),
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_end=date(2026, 7, 7),
        client=_ScatteredClient(),
    )
    assert alerts[0].detection_confidence == DetectionConfidence.LOW
    assert alerts[0].recommended_action == RecommendedAction.CHANNEL_OPERATION_CHECK
    assert alerts[0].root_cause.label == UNSPECIFIED_CAUSE


@pytest.mark.asyncio
async def test_pipeline_empty_input_returns_nothing():
    assert await detect_anomaly([]) == ([], [])
