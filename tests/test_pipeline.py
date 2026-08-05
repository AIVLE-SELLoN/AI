"""담당: 서영 (Agent2) — 알림 발행 규칙 · 재알림 억제 · [0]~[8] 파이프라인 통합 테스트.

LLM 은 목킹한다 (비용 0). 통합 테스트는 "숫자를 넣으면 몇 건이 어떤 채널로 나가는가"를
본다 — 개별 판정 규칙은 test_detection.py·test_confidence.py 가 각각 담당.
"""

import itertools
import logging
from datetime import date, datetime

import pytest

from app.core.schemas import (
    AspectSentiment,
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
from app.detection.loader import build_rows, check_coverage, unreliable_slots
from app.detection.service import _build_candidates, detect_anomaly, normalize
from app.detection.suppression import filter_suppressed
from app.detection.verdict import run_verdict


class _FakeClient:
    """[6] 프롬프트3 응답을 고정하는 가짜 LlmClient."""

    def __init__(self, cause="사진_색감_오차"):
        self._cause = cause
        self.calls = 0

    async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
        self.calls += 1
        return {
            "results": [
                {
                    "cs_id": f"C{i}",
                    "cause": self._cause,
                    "confidence": 0.9,
                    "aspect_match": True,
                }
                for i in range(12)
            ]
        }


# ── root_cause 2상태 (스키마 §5.2) ───────────────────────────────
def test_root_cause_none_when_step6_skipped():
    """[6] 자체를 안 돌렸으면 null — '봤는데 흩어졌다'와 구분된다."""
    assert build_root_cause(None) is None


def test_root_cause_unspecified_when_scattered():
    """[6] 수행했으나 분산 → label='미특정', consistent=False."""
    root = build_root_cause(
        {"label": None, "consistent": False, "count": 0, "total": 20}
    )
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
            source=Source.CS,
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=0.0001,
            bh_significant=True,
            cur_total=200,
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


@pytest.mark.parametrize(
    "verdict", [Verdict.GLOBAL, Verdict.TENTATIVE_GLOBAL, Verdict.INDETERMINATE]
)
def test_scope_in_ignores_verdict(verdict):
    """scope_in 은 **순수 aspect 속성** — verdict 를 섞지 않는다 (스키마 §3).

    문서가 서로 어긋난 지점이라 회귀가 조용히 들어올 수 있다: §3 필드정의는
    "개선안 생성 여부와 별개"인데 §5.1·§3.2 표는 전역형·구분불가 행을
    scope_in=false 로 적어놨다. scope.is_in_scope 가 §3 을 따른다고 선언했으므로
    여기서 못박는다 — 색상은 어떤 verdict 에서도 scope_in=true 다.

    recommended_action 은 반대로 verdict 를 본다. 둘을 같이 검사해서, 조치가
    verdict 에 반응하는데도 scope_in 은 흔들리지 않는다는 걸 한 테스트에 남긴다.
    """
    alert = build_alert(
        _judgement(verdict=verdict, root_cause=None),
        detected_at=datetime(2026, 7, 7),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, 7),
        seq=itertools.count(1),
    )
    assert alert.scope_in is True
    assert alert.recommended_action != RecommendedAction.GENERATE_RECOMMENDATION


# ── 재알림 억제 (로직 §6) ─────────────────────────────────────────
def _alert(
    product="P001",
    aspect="색상",
    channel="COUPANG",
    cur_rate=0.13,
    day=7,
    alert_id="A1",
    run_at=None,
):
    """day = 데이터 시각(window_end). run_at 을 주면 실행 시각만 따로 움직인다."""
    return build_alert(
        _judgement(
            product=product,
            aspect=aspect,
            channel=channel,
            stats=DetectionStats(
                source=Source.CS,
                cur_rate=cur_rate,
                past_rate=0.05,
                delta=0.08,
                p_value=0.0001,
                bh_significant=True,
                cur_total=200,
            ),
        ),
        detected_at=run_at or datetime(2026, 7, day),
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
    current = _alert(day=7, cur_rate=0.14)  # +1%p 뿐 → 갱신 조건 미달
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
    current = _alert(day=9, cur_rate=0.13)  # 8일 경과 → 억제 해제
    published, _ = filter_suppressed([current], [prior])
    assert published[0].updates_alert_id is None


def test_suppression_counts_data_time_not_wall_clock():
    """경과일은 window_end(데이터 시각)로 센다 — detected_at(실행 시각) 아님.

    데모는 60일치를 몇 분에 압축 재생한다. 실행 시각으로 세면 detected_at 차이가
    항상 0일이라 억제가 영영 안 풀리고 조합당 알림이 1건만 뜨고 끝난다
    (시스템 워크플로우 §6). 아래는 그 상황 그대로 — 데이터는 8일 벌어졌는데
    실행 시각은 10초 차이다.
    """
    run1 = datetime(2026, 8, 3, 14, 0, 0)
    prior = _alert(day=1, cur_rate=0.13, run_at=run1)
    current = _alert(day=9, cur_rate=0.13, run_at=run1.replace(second=10))

    published, suppressed = filter_suppressed([current], [prior])

    assert suppressed == []  # 데이터 기준 8일 경과 → 억제 해제
    assert published[0].updates_alert_id is None  # 갱신이 아니라 신규


def test_suppression_still_blocks_when_data_time_is_close():
    """반대 방향 — 실행 시각이 멀어도 데이터가 가까우면 계속 억제한다."""
    prior = _alert(day=5, cur_rate=0.13, run_at=datetime(2026, 8, 1))
    current = _alert(day=7, cur_rate=0.14, run_at=datetime(2026, 8, 20))

    published, suppressed = filter_suppressed([current], [prior])

    assert published == []  # 데이터 기준 2일 → 아직 억제 기간
    assert suppressed == [current]


def test_resolved_alert_releases_suppression():
    """승인/반려 처리가 끝난 건은 억제 근거가 아니다 — §6 "또는 ~ 처리 전까지"."""
    prior = _alert(day=5, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.13)
    published, suppressed = filter_suppressed(
        [current], [prior], resolved_alert_ids={prior.alert_id}
    )
    assert len(published) == 1
    assert published[0].updates_alert_id is None  # 갱신이 아니라 신규
    assert suppressed == []


def test_different_channel_is_not_suppressed():
    """억제 단위는 (상품, aspect, 채널) — 다른 채널은 별개 알림이다."""
    prior = _alert(channel="COUPANG", day=5)
    current = _alert(channel="NAVER", day=7)
    published, suppressed = filter_suppressed([current], [prior])
    assert len(published) == 1
    assert suppressed == []


# ── [3]~[5] 후보 접기 — aspect별 보류 채널 귀속 (PR #14 리뷰) ────
def _test_result(product, aspect, channel, source, fired, delta=0.08):
    return {
        "key": (product, aspect, channel, source),
        "fired": fired,
        "delta": delta,
        "p_value": 0.0001,
        "bh_significant": fired,
        "meaningful": True,
    }


def test_excluded_channels_belong_to_own_aspect():
    """보류 채널은 그 alert 의 main_aspect 판정에서 나온 것만 붙는다.

    한 상품에 판정 대상 aspect 가 2개 이상이면, 예전 구현은 상품 단위로 held 를
    덮어써서 **다른 aspect 가 보류시킨 채널이 엉뚱한 alert 에 병기**됐다.
    (past_total==0 폴백이 aspect 슬롯별로 걸리므로 held 는 실제로 aspect 마다 다르다.)

    색상: 3채널 전부 판정 가능, 쿠팡만 발화 → 보류 없음
    사이즈: 네이버 발화, 지그재그 보류      → 보류 [지그재그]
    """
    batch = [
        _test_result("P1", "색상", "COUPANG", "cs", True),
        _test_result("P1", "색상", "NAVER", "cs", False),
        _test_result("P1", "색상", "ZIGZAG", "cs", False),
        _test_result("P1", "사이즈", "NAVER", "cs", True, delta=0.09),
        _test_result("P1", "사이즈", "COUPANG", "cs", False),
    ]
    held = [("P1", "ZIGZAG", "cs")]  # 사이즈 슬롯만 보류 (색상은 배치에 있음)

    verdicts = run_verdict(batch, held)
    counts = {t["key"]: (26, 200, 40, 800) for t in batch}
    tests = {t["key"]: t for t in batch}

    candidates = _build_candidates(verdicts, "cs", tests, counts)

    color = candidates[("P1", "COUPANG")]
    size = candidates[("P1", "NAVER")]
    assert color["aspect"] == "색상"
    assert color["excluded_channels"] == []  # 다른 aspect 의 보류가 새면 안 된다
    assert size["aspect"] == "사이즈"
    assert size["excluded_channels"] == ["ZIGZAG"]  # 자기 판정의 보류는 유지


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
        _item(
            "C1",
            "COUPANG",
            [
                {"aspect": "색상", "sentiment": -1},
                {"aspect": "사이즈", "sentiment": -1},
            ],
            day=3,
        )
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
            items.append(
                _item(
                    f"C{next(counter)}",
                    channel,
                    neg if i < negatives else pos,
                    day=1 + (i % 7),
                )
            )
    return items


@pytest.mark.asyncio
async def test_unreliable_denominator_suppresses_that_slot_end_to_end():
    """분류 커버리지 미달 슬롯은 알림이 안 나간다 — detect_anomaly 까지 배선 확인.

    build_batch 에 인자만 만들어두고 호출부에서 안 넘기면 아무 일도 안 일어난다.
    실제로 그 실수를 했었기 때문에, 파이프라인 끝에서 결과가 달라지는지로 검증한다.
    """
    items = _scenario_items()
    fired_slot = ("P001", Channel.COUPANG.value, Source.CS.value)

    baseline, _ = await detect_anomaly(
        items,
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_end=date(2026, 7, 7),
        client=_FakeClient(),
    )
    assert len(baseline) == 1  # 원래는 발화한다

    alerts, _ = await detect_anomaly(
        items,
        detected_at=datetime(2026, 7, 7, 9, 0),
        window_end=date(2026, 7, 7),
        unreliable_denominators={fired_slot},
        client=_FakeClient(),
    )
    assert alerts == []  # 분모를 믿을 수 없으므로 검정 자체를 안 한다


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
    assert alert.channel == Channel.COUPANG  # 쿠팡만 발화 → 편중형
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
    assert alert.source_signals.review is None  # 리뷰 데이터 없음 = 보류
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


# ── documents 경유(로더) — 리뷰 분모 (탐지 분모 산출 방식 §1) ──────
def _review_scenario():
    """**부정률은 그대로인데 무관 리뷰만 늘어난** 리뷰 시나리오.

    무관 리뷰(aspect 0개)는 classified_item 에 행이 없다 — explode_to_rows 가 aspect
    마다 1행을 만들기 때문이다. 그래서 items 만으로 분모를 세면 그 문서가 통째로 빠진다.

        과거 28일  리뷰 200건 = 색상부정 40 + 색상긍정 160 + 무관 0
        현재 7일   리뷰 100건 = 색상부정 20 + 색상긍정 30  + 무관 50

        원본으로 세면(정답)   과거 40/200=20%  →  현재 20/100=20%   변화 없음
        items 로 세면(버그)   과거 40/200=20%  →  현재 20/50 =40%   +20%p 급등

    즉 **"칭찬 리뷰가 늘었다"가 "불만이 두 배로 늘었다"로 둔갑**한다. 채널마다 같은
    모양으로 깔아 3채널 전부 이 착시를 겪게 한다.
    """
    documents: list[dict] = []
    items: list[ClassifiedItem] = []
    counter = itertools.count(1)

    def add(day: datetime, channel: str, aspects: list | None) -> None:
        doc_id = f"RVW-{next(counter)}"
        documents.append(
            {
                "id": doc_id,
                "product": "P001",
                "channel": channel,
                "source": "review",
                "created_at": day,
                "text": "리뷰 원문",
            }
        )
        if aspects is None:  # 무관 리뷰 — ClassifiedItem 자체가 안 만들어진다
            return
        items.append(
            ClassifiedItem(
                item_id=doc_id,
                source="review",
                channel=channel,
                product_group_id="P001",
                raw_text="리뷰 원문",
                aspects=aspects,
                created_at=day,
            )
        )

    neg = [AspectSentiment(aspect="색상", sentiment=-1)]
    pos = [AspectSentiment(aspect="색상", sentiment=1)]

    for channel in ("COUPANG", "NAVER", "ZIGZAG"):
        for i in range(200):  # 과거 28일 (6/3 ~ 6/30)
            add(datetime(2026, 6, 3 + (i % 28), 12, 0), channel, neg if i < 40 else pos)
        for i in range(100):  # 현재 7일 (7/1 ~ 7/7)
            day = datetime(2026, 7, 1 + (i % 7), 12, 0)
            add(day, channel, neg if i < 20 else pos if i < 50 else None)
    return documents, items


@pytest.mark.asyncio
async def test_documents_prevent_review_false_positive_end_to_end():
    """무관 리뷰가 늘었을 뿐인데 알림이 나가면 안 된다 — documents 배선 확인.

    ⚠️ build_rows 를 직접 부르는 것만으로는 검증이 안 된다. detect_anomaly 가 실제로
       그 경로를 타는지 봐야 한다(인자만 만들어두고 안 쓰는 실수를 이미 한 적 있다).
       그래서 **파이프라인 끝에서 알림 유무가 갈리는지**로 확인한다.
    """
    documents, items = _review_scenario()
    kwargs = {
        "detected_at": datetime(2026, 7, 7, 9, 0),
        "window_end": date(2026, 7, 7),
        "client": _FakeClient(),
    }

    # items 만 주면 분모가 깎여 20% → 40% 로 보이고 오탐이 난다.
    wrong, _ = await detect_anomaly(items, **kwargs)
    assert wrong, "이 시나리오는 items 만 주면 오탐이 나야 한다(테스트 전제)"

    # documents 를 주면 분모가 원본 기준이라 부정률이 그대로 20% → 알림 없음.
    right, _ = await detect_anomaly(items, documents=documents, **kwargs)
    assert right == []


def test_documents_row_count_is_document_count():
    """분모의 출처가 원본이라는 것 자체를 숫자로 못박는다."""
    documents, items = _review_scenario()
    assert len(build_rows(documents, items)) == len(documents) == 900
    assert len(normalize(items)) == len(items) == 750  # 무관 150건이 빠진다


@pytest.mark.asyncio
async def test_documents_auto_coverage_ignores_review():
    """documents 를 주면 커버리지를 자동 계산하는데, 리뷰는 대상이 아니다.

    check_coverage 가 리뷰까지 봤다면 무관 리뷰 150건 때문에 이 슬롯들이 통째로
    검정에서 빠졌을 것이다(지인 리뷰 2026-08-04).
    """
    documents, items = _review_scenario()
    assert unreliable_slots(check_coverage(documents, items)) == set()


# ── POST /detect 의 documents 패스스루 (지인님 결선 정리 2026-08-05) ──────
#
# /detect 는 운영 경로가 아니라 **재현·디버깅 창구**다(운영은 app/batch/daily.py).
# 그래서 운영과 같은 분모 경로를 타야 한다 — 다른 분모를 쓰면 결과가 이상할 때
# 로직 문제인지 경로 차이인지 구분할 수 없다.
#
# ⚠️ detect_anomaly 에 documents 인자가 있어도 **라우터가 안 넘기면 소용없다.**
#    실제로 그런 상태로 하루 넘게 있었다(2026-08-04~05). 그래서 서비스 함수가 아니라
#    **HTTP 요청으로** 알림 발행이 갈리는지 확인한다.


def _detect_via_http(payload: dict, monkeypatch) -> dict:
    """POST /detect 를 실제로 태운다. [6] 만 스텁으로 막아 LLM 호출 0."""
    from fastapi.testclient import TestClient

    import app.detection.service as svc
    from app.main import app as fastapi_app

    async def _stub_cause(aspect, items, *, client=None, trace_key=""):
        return {
            "label": "사진_색감_오차", "consistent": True,
            "count": len(items), "total": len(items), "freq": {}, "cs_ids": [],
        }

    monkeypatch.setattr(svc, "diagnose_cause", _stub_cause)
    response = TestClient(fastapi_app).post("/api/v1/detect", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def _review_payload() -> tuple[list[dict], list[dict]]:
    """_review_scenario() 를 JSON 직렬화해 요청 본문으로 만든다."""
    documents, items = _review_scenario()
    docs = [{**d, "created_at": d["created_at"].isoformat()} for d in documents]
    return docs, [i.model_dump(mode="json") for i in items]


def test_detect_endpoint_passes_documents_through(monkeypatch):
    """documents 를 실으면 라우터가 그걸 분모로 쓴다 — 오탐이 사라져야 한다."""
    docs, items = _review_payload()
    base = {"items": items, "window_end": "2026-07-07"}

    # documents 없이 = 옛 경로. 무관 리뷰가 분모에서 빠져 오탐이 난다.
    without = _detect_via_http(base, monkeypatch)
    assert without["alerts"], "이 시나리오는 documents 가 없으면 오탐이 나야 한다(테스트 전제)"

    # documents 를 실으면 분모가 원본 기준 → 알림 0건.
    with_docs = _detect_via_http({**base, "documents": docs}, monkeypatch)
    assert with_docs["alerts"] == []
    assert with_docs["suppressed"] == []


def test_detect_endpoint_warns_when_review_without_documents(monkeypatch, caplog):
    """documents 없이 리뷰가 섞이면 경고를 남긴다 (조용히 틀리면 안 된다)."""
    _docs, items = _review_payload()

    with caplog.at_level(logging.WARNING, logger="app.detection.router"):
        _detect_via_http({"items": items, "window_end": "2026-07-07"}, monkeypatch)
    assert any("documents" in r.message for r in caplog.records)
