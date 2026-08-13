"""담당: 서영 (Agent2) — 알림 발행 규칙 · 재알림 억제 · [0]~[8] 파이프라인 통합 테스트.

LLM 은 목킹한다 (비용 0). 통합 테스트는 "숫자를 넣으면 몇 건이 어떤 채널로 나가는가"를
본다 — 개별 판정 규칙은 test_detection.py·test_confidence.py 가 각각 담당.
"""

import itertools
import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.constants import CURRENT_WINDOW_DAYS, KST, RENOTIFY_BLOCK_DAYS
from app.core.schemas import (
    Aspect,
    AspectSentiment,
    Channel,
    ClassifiedItem,
    DetectionConfidence,
    DetectionStats,
    RecommendedAction,
    Source,
    Verdict,
)
from app.detection import service
from app.detection.alert import (
    UNSPECIFIED_CAUSE,
    build_alert,
    build_root_cause,
    make_alert_id,
    resolve_channel,
)
from app.detection.loader import build_rows, check_coverage, unreliable_slots
from app.detection.service import (
    DetectionDiagnostics,
    _build_candidates,
    detect_anomaly,
    normalize,
)
from app.detection.suppression import filter_suppressed
from app.detection.verdict import run_verdict

ROOT = Path(__file__).resolve().parents[1]


class _FakeClient:
    """[6] 프롬프트3 응답을 고정하는 가짜 LlmClient."""

    def __init__(self, cause="사진_색감_오차"):
        self._cause = cause
        self.calls = 0

    async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
        self.calls += 1
        input_data = json.loads(prompt.rsplit("입력:", 1)[1].split("\n출력:", 1)[0])
        return {
            "results": [
                {
                    "cs_id": item["cs_id"],
                    "cause": self._cause,
                    "confidence": 0.9,
                    "evidence": item["raw_text"],
                    "aspect_match": True,
                }
                for item in input_data["items"]
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
        "channel_rates": [
            {"channel": "COUPANG", "rate": 0.13, "excluded": False},
            {"channel": "NAVER", "rate": 0.05, "excluded": False},
            {"channel": "ZIGZAG", "rate": None, "excluded": True},
        ],
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
    )
    assert alert.alert_id == "ALT-20260707-P001-COLOR-COUPANG"
    assert alert.channel == Channel.COUPANG
    assert alert.recommended_action == RecommendedAction.GENERATE_RECOMMENDATION
    assert alert.scope_in is True
    assert alert.evidence.inquiry_ids == ["INQ-1", "INQ-2"]
    assert alert.channel_rates[0].rate == pytest.approx(0.13)
    assert alert.channel_rates[2].excluded is True


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
    run_at=None,
):
    """day = 데이터 시각(window_end). run_at 을 주면 실행 시각만 따로 움직인다.

    ⚠️ `alert_id` 를 따로 못박는 인자가 없다 — `alert_id` 는 이제 (window_end, 상품,
       aspect, 채널)에서 **결정론적으로 나온다.** 그래서 `day` 를 바꾸면 ID 도 같이
       바뀌고, `day` 를 같게 두면 ID 도 같아진다. 아래 억제 테스트들이 그 성질에 기댄다.
    """
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


# ── 결정론적 alert_id 와 갱신 체인 (백엔드 멱등 upsert 계약) ──────
def test_same_window_update_reuses_id_and_does_not_self_reference():
    """🔴 같은 구간 재실행 → **같은 ID** · `updates_alert_id`는 **None**.

    `alert_id` 가 (window_end, 상품, aspect, 채널)에서 나오므로 같은 구간을 다시 돌리면
    글자까지 같은 ID 가 나온다 — 백엔드가 그 값으로 upsert 하니 그래야 맞다.

    그런데 그때 `filter_suppressed` 는 여전히 갱신 경로를 탄다(`elapsed_days == 0` 인데
    `+5%p` 는 넘음). 가드가 없으면 `updates_alert_id` 에 **자기 자신의 ID** 가 들어가서,
    백엔드가 한 행을 갱신하면서 그 행이 자기 자신의 갱신 대상이라는 상태가 된다.
    """
    prior = _alert(day=7, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.18)  # 같은 window_end, +5%p

    assert current.alert_id == prior.alert_id  # 결정론적 — 같은 입력 같은 ID

    published, suppressed = filter_suppressed([current], [prior])

    assert suppressed == []  # 수치가 뛰었으니 갱신으로는 나간다
    assert published[0].updates_alert_id is None  # 자기 자신을 가리키지 않는다


def test_later_window_update_points_at_the_previous_id():
    """이후 구간 갱신 → **새 ID** · `updates_alert_id`에 **이전 ID**.

    위 테스트의 반대편이다. `window_end` 가 움직였으니 ID 가 달라지고, 그때는 갱신 체인이
    정상적으로 이어져야 한다 — 가드를 "항상 None" 으로 넓히면 이쪽이 끊긴다.
    """
    prior = _alert(day=5, cur_rate=0.13)
    current = _alert(day=7, cur_rate=0.18)  # 억제 기간 안(2일) + 5%p

    assert current.alert_id != prior.alert_id
    assert current.alert_id == "ALT-20260707-P001-COLOR-COUPANG"
    assert prior.alert_id == "ALT-20260705-P001-COLOR-COUPANG"

    published, suppressed = filter_suppressed([current], [prior])

    assert suppressed == []
    assert published[0].updates_alert_id == prior.alert_id


def test_legacy_id_in_prior_cache_still_links_the_update_chain():
    """구형 ID 과도기 — `prior_alerts` 캐시에 옛 형식이 남아 있어도 갱신이 이어진다.

    억제 매칭은 `_key`(상품, aspect, 채널)로 하고 ID 를 대조하지 않으므로, 캐시에
    `ALT-20260705-0001` 같은 옛 형식이 남아 있어도 **매칭 자체는 된다.** 그때
    `updates_alert_id` 에는 **옛 형식 ID 가 그대로** 들어간다 — 백엔드가 가리키는 행이
    실제로 그 ID 로 저장돼 있으므로 새 형식으로 고쳐 쓰면 오히려 링크가 깨진다.

    캐시가 새 형식으로 자연 교체될 때까지(최대 `STATE_RETENTION_DAYS`) 이 동작이 유지된다.
    """
    prior = _alert(day=5, cur_rate=0.13).model_copy(
        update={"alert_id": "ALT-20260705-0001"}
    )
    current = _alert(day=7, cur_rate=0.18)

    published, suppressed = filter_suppressed([current], [prior])

    assert suppressed == []
    assert published[0].alert_id == "ALT-20260707-P001-COLOR-COUPANG"  # 새 형식
    assert published[0].updates_alert_id == "ALT-20260705-0001"  # 옛 형식 그대로


def test_alert_id_axes_all_change_the_id():
    """네 축이 **전부** ID 에 들어간다 — 하나라도 빠지면 다른 알림이 서로를 덮는다.

    aspect 축이 특히 그렇다. 알림의 논리 키가 원래 (상품, main_aspect, 채널)인데
    aspect 를 빼면 색상 알림과 사이즈 알림이 같은 ID 를 받고, 백엔드 멱등 upsert 가
    나중 것으로 앞엣것을 덮어 **원인·근거·권장조치·개선안이 통째로 바뀐다**
    (PR #22 `guideline_id` 사고와 같은 모양).
    """
    base = _alert()
    assert base.alert_id == "ALT-20260707-P001-COLOR-COUPANG"

    assert _alert(day=8).alert_id != base.alert_id  # window_end
    assert _alert(product="P002").alert_id != base.alert_id  # 상품
    assert _alert(aspect="사이즈").alert_id != base.alert_id  # aspect
    assert _alert(channel="NAVER").alert_id != base.alert_id  # 채널


def test_global_verdict_id_uses_the_folded_all_channel():
    """전역형은 channel 이 ALL 로 접히고 **ID 도 그 값을 쓴다.**

    ID 를 `judgement["channel"]`(발화 채널)로 만들면 `...-COUPANG` 인데 필드는 ALL 인
    알림이 나온다 — 상품당 1건이어야 할 전역형이 발화 채널마다 다른 ID 를 받아, 백엔드가
    같은 이상을 채널 수만큼 별개 행으로 저장한다.
    """
    alert = build_alert(
        _judgement(verdict=Verdict.GLOBAL, channel="COUPANG", root_cause=None),
        detected_at=datetime(2026, 7, 7, tzinfo=KST),
        window_start=date(2026, 7, 1),
        window_end=date(2026, 7, 7),
    )

    assert alert.channel == Channel.ALL
    assert alert.alert_id == "ALT-20260707-P001-COLOR-ALL"


def test_alert_id_length_formula_holds_for_the_worst_case():
    """길이 = `33 + len(product_group_id)`. 백엔드 컬럼이 `varchar(72)` 라 상한을 못박는다.

    33 = `ALT-`(4) + 날짜(8) + 구분자 3 + 최장 aspect `MISDELIVERY`(11) +
    최장 channel `COUPANG`(7). 축을 늘리면 이 테스트가 먼저 터진다 — 그때 백엔드
    컬럼 길이를 같이 확인해야 한다.
    """
    longest = make_alert_id(
        window_end=date(2026, 8, 28),
        product_group_id="P001",
        aspect="오배송",  # MISDELIVERY — 멤버명이 가장 길다
        channel=Channel.COUPANG,
    )

    assert longest == "ALT-20260828-P001-MISDELIVERY-COUPANG"
    assert len(longest) == 33 + len("P001")
    assert len(longest) <= 72  # 백엔드 alert_code varchar(72)


def test_make_alert_id_accepts_plain_str_aspect():
    """🔴 호출부는 평범한 `str`(`'색상'`)을 넘긴다 — `Aspect` 멤버가 아니다.

    `build_alert:aspect` 는 `judgement["aspect"]` 를 그대로 받고, `Aspect` 로 바뀌는 건
    `DetectionAlert` 생성 시 Pydantic 이 변환하는 시점이라 **ID 만드는 자리는 그 이전**
    이다. `aspect.name` 을 바로 쓰면 `AttributeError` 로 죽는다.

    enum 멤버를 넘겨도 같은 값이 나오는 것까지 고정한다 — 그래야 호출부가 어느 쪽을
    넘기든 ID 가 안 갈린다.
    """
    kwargs = {
        "window_end": date(2026, 8, 28),
        "product_group_id": "P001",
        "channel": Channel.COUPANG,
    }
    from_str = make_alert_id(aspect="색상", **kwargs)
    from_enum = make_alert_id(aspect=Aspect.COLOR, **kwargs)

    assert from_str == "ALT-20260828-P001-COLOR-COUPANG"
    assert from_str == from_enum


def test_aspect_codes_match_the_backend_mapping_table():
    """aspect 코드 = `Aspect` **멤버명**이고 백엔드 대조표(노션 §6)와 정확히 일치한다.

    새 매핑 테이블을 만들지 말라는 근거를 못박는다 — 두 벌이 되면 한쪽만 바뀌었을 때
    ID 가 조용히 갈린다. 여기가 터지면 enum 멤버명이 바뀐 것이고, 그건 백엔드와 다시
    합의할 일이다(이미 저장된 alert_id 가 전부 어긋난다).
    """
    assert [a.name for a in Aspect] == [
        "COLOR",
        "SIZE",
        "MATERIAL",
        "DAMAGE",
        "MISDELIVERY",
        "ETC",
    ]


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
    assert [row.channel for row in size["channel_rates"]] == [
        Channel.COUPANG,
        Channel.NAVER,
        Channel.ZIGZAG,
    ]
    assert size["channel_rates"][0].rate == pytest.approx(26 / 200)
    assert size["channel_rates"][2].rate is None
    assert size["channel_rates"][2].excluded is True


def test_channel_rates_carry_the_denominator():
    """🔴 비율 옆에 분모가 같이 실린다 — 관측 0건은 `total=0` 이지 `None` 이 아니다.

    `None` 은 이 필드가 생기기 전(2026-08-11) 발행분의 값이다. 신규 발행에서 나오면
    백엔드가 "구버전인가 관측 0건인가" 를 못 가린다.

    채널마다 분모를 다르게 둔 건 `stats.cur_total`(대표 채널 1개분) 을 그대로 복사하는
    구현을 걸러내기 위해서다.
    """
    batch = [
        _test_result("P1", "색상", "COUPANG", "cs", True),
        _test_result("P1", "색상", "NAVER", "cs", False),
    ]
    counts = {
        ("P1", "색상", "COUPANG", "cs"): (26, 200, 40, 800),
        ("P1", "색상", "NAVER", "cs"): (8, 160, 30, 640),
        # ZIGZAG 는 키 자체가 없다 = 그 채널 관측 0건.
    }
    tests = {t["key"]: t for t in batch}

    candidates = _build_candidates(run_verdict(batch, []), "cs", tests, counts)
    rates = candidates[("P1", "COUPANG")]["channel_rates"]

    assert [row.total for row in rates] == [200, 160, 0]
    assert rates[2].total is not None, "구버전 알림(None)과 구분이 안 된다"
    assert rates[2].rate is None and rates[2].excluded is True


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
    assert [row.rate for row in alert.channel_rates] == pytest.approx([0.30, 0.05, 0.05])
    assert not any(row.excluded for row in alert.channel_rates)
    # 발행되는 알림엔 `None`(= 구버전 마커)이 섞이지 않는다. 세 채널이 같은 값인 건
    # 분모가 aspect 무관이라 그렇고, 채널별로 달라지는 경우는 단위 테스트가 잡는다
    # (test_channel_rates_carry_the_denominator).
    assert [row.total for row in alert.channel_rates] == [40, 40, 40]
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
            input_data = json.loads(prompt.rsplit("입력:", 1)[1].split("\n출력:", 1)[0])
            return {
                "results": [
                    {
                        "cs_id": item["cs_id"],
                        "cause": causes[i % 4],
                        "confidence": 0.9,
                        "evidence": item["raw_text"],
                        "aspect_match": True,
                    }
                    for i, item in enumerate(input_data["items"])
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
async def test_cause_validation_failure_does_not_stop_detection(caplog):
    """후보 하나의 잘못된 LLM 응답은 탐지를 중단하지 않고 자동 개선안만 막는다."""

    class _InvalidClient:
        async def complete_json(self, prompt, *, trace_key="-", temperature=0.0):
            return {"results": []}  # 입력 ID 전체 누락

    diagnostics = DetectionDiagnostics()
    with caplog.at_level(logging.ERROR, logger="app.detection.service"):
        alerts, _ = await detect_anomaly(
            _scenario_items(),
            detected_at=datetime(2026, 7, 7, 9, 0),
            window_end=date(2026, 7, 7),
            client=_InvalidClient(),
            diagnostics=diagnostics,
        )

    assert len(alerts) == 1
    assert alerts[0].root_cause is None
    assert alerts[0].detection_confidence == DetectionConfidence.LOW
    assert alerts[0].recommended_action == RecommendedAction.CHANNEL_OPERATION_CHECK
    assert len(diagnostics.cause_failures) == 1
    assert diagnostics.cause_failures[0]["error"].startswith("CauseValidationError:")
    assert any("원인 분류 후보 실패" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_pipeline_empty_input_returns_nothing():
    assert await detect_anomaly([]) == ([], [])


@pytest.mark.asyncio
async def test_detected_at_default_is_kst_not_host_local(monkeypatch):
    """🔴 `detected_at` 기본값은 **KST 벽시계**다 — 호스트 시간대를 보지 않는다.

    이 값의 날짜 부분이 CS 가이드라인 기간(`%Y-%m`, `reporting/cs_reply_service`)이
    된다. naive `datetime.now()` 는 로컬 시각이라, 배치를 **UTC 컨테이너**로 올리면
    KST 오전 9시 이전에 도는 배치가 **하루 전 날짜**로 찍힌다. 개발 머신이 KST 라
    로컬에서는 영원히 안 보이는 종류다(`_to_kst` 와 같은 모양).

    시계를 UTC 호스트로 고정해 재현한다 — UTC 8/11 23:30 은 KST 로 **8/12 08:30** 이라
    두 시간대의 날짜가 갈리는 순간이다. 옛 코드면 8/11 이 나온다.

    ⚠️ **`alert_id` 로는 이 회귀를 못 잡는다** — 그쪽은 `window_end`(데이터 시각)를 쓰기
       때문이다. 예전엔 ID 에 `detected_at` 의 날짜가 들어가서 그걸 프록시로 볼 수 있었고,
       이 테스트도 그렇게 검사했다. 지금은 **`detected_at` 자체를 직접 봐야 한다.**
       (아래 assert 가 그렇게 바뀐 이유다 — 프록시가 사라졌는데 assert 를 안 옮기면
       시간대 회귀가 조용해진다.)
    """

    class _UtcHostClock(datetime):
        """UTC 호스트의 시계. `tz` 를 안 주면 UTC 벽시계를 naive 로 돌려준다."""

        @classmethod
        def now(cls, tz=None):
            moment = datetime(2026, 8, 11, 23, 30, tzinfo=timezone.utc)
            return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(service, "datetime", _UtcHostClock)

    alerts, _ = await detect_anomaly(
        _scenario_items(),
        window_end=date(2026, 7, 7),  # detected_at 은 일부러 안 준다 — 기본값을 잰다
        client=_FakeClient(),
    )

    # KST 벽시계 8/12 08:30 + `+09:00` 라벨. 백엔드가 OffsetDateTime 으로 받는다.
    assert alerts[0].detected_at == datetime(2026, 8, 12, 8, 30, tzinfo=KST)
    assert alerts[0].detected_at.utcoffset() == timedelta(hours=9)

    # ID 는 `detected_at`(8/12)이 아니라 `window_end`(7/7)를 쓴다 — 같은 구간을 다시
    # 돌려도 같은 ID 여야 하므로 실행 시각이 섞이면 안 된다.
    assert alerts[0].alert_id.startswith("ALT-20260707-")


@pytest.mark.asyncio
async def test_aware_detected_at_argument_is_converted_not_relabeled():
    """오프셋이 **있는** 인자는 라벨을 갈아치우지 않고 **변환**한다.

    🔴 **인자도 정규화한다** — 기본값만 KST 로 두면 호출부가 넘긴 값이 그대로 나가서
       naive·aware·비KST 가 한 배치 안에서 섞이고, 백엔드 파싱이 호출 경로에 따라 갈린다.

    ⚠️ **`==` 로만 재면 이 회귀를 못 잡는다.** aware datetime 의 `==` 는 **같은 순간인지**
       를 보므로 UTC 00:30 과 KST 09:30 이 같다고 나온다 — 정규화를 통째로 지워도 통과한다.
       그래서 `isoformat()` 문자열로 잰다(발행 payload 에 실제로 실리는 형태다).
    """
    alerts, _ = await detect_anomaly(
        _scenario_items(),
        detected_at=datetime(2026, 7, 7, 0, 30, tzinfo=timezone.utc),
        window_end=date(2026, 7, 7),
        client=_FakeClient(),
    )

    assert alerts[0].detected_at.isoformat() == "2026-07-07T09:30:00+09:00"
    assert alerts[0].detected_at.utcoffset() == timedelta(hours=9)


@pytest.mark.asyncio
async def test_naive_detected_at_argument_is_treated_as_kst():
    """오프셋이 **없는** 인자는 KST 로 **간주**한다 — 벽시계 값을 옮기지 않는다.

    위 테스트의 반대편 갈래다. 둘 중 하나만 두면 `_to_kst_aware` 를 한 줄로 뭉개는
    변경(`replace` 만 / `astimezone` 만)이 조용히 통과한다.

    ⚠️ **이 테스트만으로는 `astimezone` 단독 구현을 못 잡는다** — 개발 머신이 KST 라
       naive 를 호스트 로컬로 읽어도 같은 값이 나온다. 그쪽은 아래 `TZ=UTC` 서브프로세스
       테스트가 잡는다. **한 쌍이므로 하나만 지우지 말 것.**
    """
    alerts, _ = await detect_anomaly(
        _scenario_items(),
        # 오프셋을 일부러 안 붙인다 — naive 인 것이 **이 테스트의 대상**이다.
        # (DTZ001 억제: 오프셋을 붙이면 검사할 것이 사라진다.)
        detected_at=datetime(2026, 7, 7, 8, 30),  # noqa: DTZ001
        window_end=date(2026, 7, 7),
        client=_FakeClient(),
    )

    assert alerts[0].detected_at.isoformat() == "2026-07-07T08:30:00+09:00"


def test_naive_detected_at_is_kst_even_on_a_utc_host():
    """🔴 위 계약을 **UTC 호스트에서** 확인한다 — 개발 머신이 KST 라 여기서만 잡힌다.

    같은 프로세스에서 재면 호스트가 마침 KST 라 `.astimezone()` 단독 구현도 통과한다.
    `TZ=UTC` 서브프로세스로 띄워서 잰다 — 배치를 컨테이너(UTC)로 올렸을 때 실제로 도는
    조건이다. `test_load_inputs_from_db.test_naive_timestamp_is_kst_even_on_a_utc_host`
    와 같은 레시피이고, 그쪽(`daily._to_kst`, 읽는 쪽)과 **이쪽(탐지 시각을 찍는 쪽)은
    같은 규칙을 공유하는 한 쌍**이다.
    """
    code = (
        "from datetime import datetime;"
        "from app.detection import service;"
        "print(service._to_kst_aware(datetime(2026, 7, 7, 8, 30)).isoformat())"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        # 인코딩을 양쪽 다 못박는다 — 한글 traceback 을 부모가 cp949 로 디코드하면
        # 깨지면서 `stderr` 가 통째로 `None` 이 되고, 아래 assert 메시지가 사라진다.
        env={**os.environ, "TZ": "UTC", "PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=180,
        check=False,  # 종료코드를 직접 본다 — stderr 를 assert 메시지에 실으려고
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "2026-07-07T08:30:00+09:00", (
        "UTC 호스트에서 시각이 밀렸습니다 — naive 값을 호스트 로컬로 해석하고 있습니다"
    )


# ── documents 경유(로더) — 리뷰 분모 (탐지 분모 산출 방식 §1) ──────
def _review_scenario():
    """**부정률은 그대로인데 무관 리뷰만 늘어난** 리뷰 시나리오.

    무관 리뷰(aspect 0개)는 classified_item 부모 행은 있고 자식 행만 없다. 운영 로더는
    부모를 `ClassifiedItem(aspects=[])`로 복원하므로 정상 빈 배열도 분모에 남는다.

        과거 28일  리뷰 200건 = 색상부정 40 + 색상긍정 160 + 무관 0
        현재 7일   리뷰 100건 = 색상부정 20 + 색상긍정 30  + 무관 50

        원본으로 세면(정답)   과거 40/200=20%  →  현재 20/100=20%   변화 없음
        부모 items로 세면     과거 40/200=20%  →  현재 20/100=20%   변화 없음

    documents는 같은 분모를 보존하는 동시에 미분류 부모가 있는지 검사하는 정본이다.
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
        items.append(
            ClassifiedItem(
                item_id=doc_id,
                source="review",
                channel=channel,
                product_group_id="P001",
                raw_text="리뷰 원문",
                # 무관 리뷰도 워커가 classified_item 부모 행을 남긴다.
                aspects=aspects or [],
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
async def test_empty_review_parent_rows_keep_denominator_end_to_end():
    """정상 빈 배열 리뷰의 부모 행이 있으면 items 경로도 분모를 보존한다."""
    documents, items = _review_scenario()
    kwargs = {
        "detected_at": datetime(2026, 7, 7, 9, 0),
        "window_end": date(2026, 7, 7),
        "client": _FakeClient(),
    }

    without_docs, _ = await detect_anomaly(items, **kwargs)
    assert without_docs == []

    # 운영 경로는 documents로 원문 대비 부모 레코드 coverage까지 함께 확인한다.
    with_docs, _ = await detect_anomaly(items, documents=documents, **kwargs)
    assert with_docs == []


def test_documents_row_count_is_document_count():
    """분모의 출처가 원본이라는 것 자체를 숫자로 못박는다."""
    documents, items = _review_scenario()
    assert len(build_rows(documents, items)) == len(documents) == 900
    assert len(normalize(items)) == len(items) == 900


@pytest.mark.asyncio
async def test_documents_auto_coverage_accepts_empty_review_parents():
    """documents 기준 검사에서 빈 배열 리뷰의 부모 행을 완료로 인정한다."""
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
            "label": "사진_색감_오차",
            "consistent": True,
            "count": len(items),
            "total": len(items),
            "freq": {},
            "cs_ids": [],
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
    """documents를 실으면 누락된 리뷰 부모를 찾아 오염 슬롯을 제외한다."""
    docs, items = _review_payload()
    # 정상 빈 배열 리뷰의 부모 레코드를 일부러 누락시켜 실제 분류 실패를 만든다.
    incomplete = [item for item in items if item["aspects"]]
    base = {"items": incomplete, "window_end": "2026-07-07"}

    # documents 없이는 누락 사실을 몰라 줄어든 items 분모로 오탐이 난다.
    without = _detect_via_http(base, monkeypatch)
    assert without["alerts"], (
        "이 시나리오는 documents 가 없으면 오탐이 나야 한다(테스트 전제)"
    )

    # documents를 실으면 review coverage gap을 잡아 해당 슬롯을 검정 전에 제외한다.
    with_docs = _detect_via_http({**base, "documents": docs}, monkeypatch)
    assert with_docs["alerts"] == []
    assert with_docs["suppressed"] == []


def test_detect_endpoint_warns_when_review_without_documents(monkeypatch, caplog):
    """documents 없이 리뷰가 섞이면 경고를 남긴다 (조용히 틀리면 안 된다)."""
    _docs, items = _review_payload()

    with caplog.at_level(logging.WARNING, logger="app.detection.router"):
        _detect_via_http({"items": items, "window_end": "2026-07-07"}, monkeypatch)
    assert any("documents" in r.message for r in caplog.records)


# ── 억제 기간 불변식 (지인님 지적 2026-08-05) ─────────────────────
#
# 억제된 날은 알림이 안 나가 prior_alerts 에 안 남는데도 기준선에서 빠진다.
# 억제가 풀린 뒤 나가는 알림의 윈도우가 그 구간을 덮어주기 때문이다.
# block > CURRENT_WINDOW_DAYS 가 되면 그 덮개가 끊기고, 틈에 남은 이상 구간이
# 과거 기준선에 섞여 '새로운 평소'로 굳는다 → 알림이 스스로 꺼진다.


def test_renotify_block_days_within_current_window():
    """불변식: 억제 기간 <= 현재 윈도우. 지금 여유가 0 이다."""
    assert RENOTIFY_BLOCK_DAYS <= CURRENT_WINDOW_DAYS, (
        f"억제 {RENOTIFY_BLOCK_DAYS}일 > 윈도우 {CURRENT_WINDOW_DAYS}일 — "
        "두 알림 윈도우 사이에 기준선이 덮이지 않는 틈이 생긴다"
    )


def _uncovered_days(block_days: int, cycles: int = 4) -> list[date]:
    """연속 발행 시 어느 알림 윈도우에도 안 들어가는 날짜를 센다."""
    first = date(2026, 1, 20)
    fires = [first + timedelta(days=block_days * i) for i in range(cycles)]
    covered: set[date] = set()
    for fire in fires:
        start = fire - timedelta(days=CURRENT_WINDOW_DAYS - 1)
        covered |= {start + timedelta(days=k) for k in range((fire - start).days + 1)}
    span_start = first - timedelta(days=CURRENT_WINDOW_DAYS - 1)
    span = [
        span_start + timedelta(days=k) for k in range((fires[-1] - span_start).days + 1)
    ]
    return [d for d in span if d not in covered]


def test_current_block_days_leaves_no_gap():
    """현재 설정에서는 빈틈이 없다 — 위 불변식이 실제로 성립하는지 숫자로 확인."""
    assert _uncovered_days(RENOTIFY_BLOCK_DAYS) == []


def test_longer_block_days_would_break_baseline():
    """⚠️ 늘리면 실제로 깨진다 — 불변식이 형식적 assert 가 아님을 보인다."""
    assert len(_uncovered_days(CURRENT_WINDOW_DAYS + 1)) > 0
    assert len(_uncovered_days(14)) >= 21
