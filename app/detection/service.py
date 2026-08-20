"""담당: 서영 (Agent2) — 이상탐지 파이프라인 조립 [0]~[8].

역할 분리 — 이 파일은 조립만 한다. 판정 규칙은 전부 단계별 모듈에 있다:
    aggregate.py   [0][1] 집계·과거기준     statistics.py  [2] 검정 3관문
    verdict.py     [3] 편중/전역 판정       scope.py       [4][5] 주 aspect·스코프
    cause.py       [6] 원인분류 (LLM)       confidence.py  [7] 확신도·권장조치
    combine.py     [8] CS·리뷰 종합         alert.py       발행 규칙
    suppression.py 재알림 억제·갱신
그래서 이 파일에는 임계값도 판정 분기도 없다 — 있으면 그건 모듈로 내려가야 하는 것이다.

입력 의존성(백엔드와의 계약, 미확정): 상품 매핑은 백엔드 담당이고 detection 은 그 산출물인
상품 식별자를 받는다. 이 식별자는 옵션(SKU) 이 아니라 '상품 그룹' 레벨이어야 한다 — 문의·리뷰
원본에 옵션 정보가 없어 옵션 레벨 ID 를 받으면 집계가 성립하지 않는다.

BH 배치 범위 — 두 소스를 한 family 로 묶는다. build_combinations 가 cs·review 조합을 한
리스트로 내므로 BH-FDR 이 두 소스에 걸쳐 한 번에 적용된다. 스펙의 "source별 독립 수행"은
[0][1] 집계와 [3]~[7] 판정을 가리키며 BH family 는 그 예외다 — family 표가 상품당 검정 수를
36검정(6 aspect x 3 채널 x 2 source)으로 세기 때문이고, eval/run_detection_eval.py 가 그
합계를 회귀로 감시한다.

원인 분류(프롬프트3) 결정 둘:
  - confidence 는 쓰지 않고 aspect_match 만 본다. few-shot 의 confidence 가 두 덩어리
    (구체 후보 0.82~0.94 / 기타 0.3~0.4)로 갈려 사실상 `cause == 기타` 의 재표현이라
    판정에 못 쓴다. 다만 `CauseResult.confidence` 필드 자체는 지우면 안 된다 —
    `extra="forbid"` 라 응답 전건이 검증 실패로 떨어진다.
  - evidence 는 유지한다. 원인 라벨 채점에도 알림 계약에도 안 쓰이지만 프롬프트 규칙 5 가
    원문 인용을 요구하는 grounding 제약이고 `_validate_response` 의 검증 입력이다. 토큰
    절약 목적의 제거는 v2 A/B 이후에만 판단한다. (알림의 `evidence.inquiry_ids` 는 다른
    것이다 — 알림 레벨 인용 경계.)

root_cause.label 이 Agent3 의 도구를 정하므로, 구체 후보와 스코프 한계 라벨의 혼동은 처방을
정반대로 뒤집는다.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from app.core.constants import CURRENT_WINDOW_DAYS, KST, PAST_WINDOW_DAYS
from app.core.schemas import (
    Aspect,
    Channel,
    ChannelRate,
    ClassifiedItem,
    DetectionAlert,
    DetectionStats,
    Sentiment,
    Source,
    SubAspectAction,
    Verdict,
)
from app.detection.aggregate import build_combinations
from app.detection.alert import PRODUCT_LEVEL_VERDICTS, build_alert, build_root_cause
from app.detection.cause import diagnose_cause
from app.detection.combine import combine_sources, pick_primary_source, source_signal
from app.detection.confidence import decide_confidence, decide_recommended_action
from app.detection.loader import build_rows, check_coverage, unreliable_slots
from app.detection.scope import is_in_scope, pick_main_aspect
from app.detection.statistics import run_detection
from app.detection.suppression import filter_suppressed
from app.detection.verdict import run_verdict

logger = logging.getLogger(__name__)


@dataclass
class DetectionDiagnostics:
    """알림 반환 계약과 분리해 운영 배치가 수집하는 탐지 보강 단계 진단."""

    cause_failures: list[dict[str, str]] = field(default_factory=list)

ALL_ASPECTS: list[str] = [a.value for a in Aspect]
"""[0] 그리드에 방출할 aspect 택소노미. 탐지는 전 aspect, 원인분류만 스코프 제한."""

_NO_ALERT_VERDICTS = frozenset({Verdict.NORMAL})

_SNAPSHOT_CHANNELS = (Channel.COUPANG, Channel.NAVER, Channel.ZIGZAG)
"""payload 채널별 비율의 고정 순서. 집계용 가상 채널 ALL 은 포함하지 않는다."""


# ── 요청/응답 모델 ───────────────────────────────────────────────
# core/schemas.py 는 전원 회의 확정 사항이라 건드리지 않는다. 모듈 전용 I/O 모델은
# service.py 에 두는 게 팀 선례다 (classification 의 ClassifyRequestItem).


class RawDocument(BaseModel):
    """분모의 출처가 되는 원본 문서 1건. `loader.build_rows()` 가 요구하는 키만 받는다.

    dict 로 받으면 키가 빠졌을 때 `build_rows` 안에서 KeyError 가 나 500 이 된다.
    재현·디버깅 창구에서는 어느 필드가 빠졌는지 422 로 알려주는 편이 쓸모 있다.
    """

    id: str
    product: str
    channel: str
    source: str
    created_at: datetime
    text: str = ""


class DetectRequest(BaseModel):
    """POST /detect 요청. items 는 분류(Agent1) 산출물 그대로.

    운영 진입점은 배치(`app/batch/daily.py`)이고 이 API 는 재현·디버깅용이다 — 메인에서
    AI 로 가는 큐에 '탐지 요청'이 없고, items 를 손에 든 건 분류를 수행하는 AI 노드
    자신뿐이기 때문이다.

    그래서 `documents` 를 반드시 같이 보낼 것. 없으면 분모를 items 기준으로 세어
    (`normalize`) 운영과 결과가 달라지고, 그러면 결과가 이상할 때 로직 문제인지 경로
    차이인지 구분할 수 없다.
    """

    items: list[ClassifiedItem]
    documents: list[RawDocument] | None = Field(
        default=None,
        description=(
            "원본 문서(문의·리뷰). **분모의 출처.** "
            "없으면 분모를 items 에서 세므로 운영 경로와 결과가 달라진다."
        ),
    )
    window_end: date | None = Field(
        default=None,
        description="현재 윈도우 마지막 날. 없으면 items 의 최신 날짜를 쓴다.",
    )
    prior_alerts: list[DetectionAlert] = Field(
        default_factory=list,
        description="과거 발행된 알림. 재알림 억제·기준선 오염 방지에 쓴다.",
    )
    resolved_alert_ids: list[str] = Field(
        default_factory=list,
        description="승인/반려 처리가 끝난 alert_id. 재알림 억제가 풀린다 (로직 §6).",
    )


class DetectResponse(BaseModel):
    """발행할 알림 + 억제된 알림(운영 가시성용)."""

    alerts: list[DetectionAlert]
    suppressed: list[DetectionAlert] = Field(default_factory=list)


# ── [0] 입력 정규화 ──────────────────────────────────────────────


def normalize(items: list[ClassifiedItem]) -> list[dict]:
    """ClassifiedItem → aggregate.py 가 먹는 정규화 행. **문의 1건 = 행 1개.**

    한 문의가 색상·사이즈에 동시에 부정이면 neg_aspects 에 둘 다 담는다. 행으로 쪼개면
    분모(총문의)가 부풀어 부정률이 반토막 나므로 절대 쪼개지 않는다.

    day 는 날짜의 ordinal 을 그대로 쓴다 — 기준일 오프셋을 두면 배치마다 값이 달라져
    alert_days 같은 (상품,채널,day) 키가 배치 간에 안 맞는다.
    """
    rows: list[dict] = []
    for item in items:
        neg_aspects = [
            a.aspect.value for a in item.aspects if a.sentiment == Sentiment.NEGATIVE
        ]
        rows.append(
            {
                "product": item.product_group_id,
                "channel": item.channel.value,
                "source": item.source.value,
                "neg_aspects": neg_aspects,
                "day": item.created_at.date().toordinal(),
                "id": item.item_id,
                "text": item.raw_text,
            }
        )
    return rows


def _to_kst_aware(value: datetime) -> datetime:
    """탐지 시각을 KST aware 로 못박는다. `daily._to_kst` 와 같은 규칙이다.

    백엔드가 OffsetDateTime 으로 수신하므로 오프셋이 없으면 연동하는 순간 파싱이 전건
    실패한다. 벽시계 값은 안 바뀌고 표기만 붙는다.

    기본값만이 아니라 인자로 받은 값도 정규화한다 — 기본값만 KST 로 두면 호출부가 넘긴
    값이 그대로 나가 naive·aware 가 한 배치 안에서 섞이고, 백엔드 파싱이 호출 경로에
    따라 갈린다.

    naive 는 KST 로 간주해 라벨만 붙이고(`replace`), aware 는 KST 로 변환한다
    (`astimezone`). naive 를 `.astimezone()` 에 넘기면 파이썬이 실행 호스트의 로컬 시각
    으로 읽어, 같은 입력이 KST 노트북과 UTC 컨테이너에서 다른 순간이 된다 — 개발 머신이
    KST 라 로컬 테스트로는 영원히 안 잡힌다.

    이 값의 소비처는 CS 가이드라인 기간(`%Y-%m`, `reporting/cs_reply_service`)이다.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def _window_bounds(
    rows: list[dict], window_end: date | None
) -> tuple[int, int, date, date]:
    """현재 윈도우 [cur_start, cur_end] 를 day ordinal 과 date 양쪽으로 낸다."""
    cur_end = window_end.toordinal() if window_end else max(r["day"] for r in rows)
    cur_start = cur_end - CURRENT_WINDOW_DAYS + 1
    return cur_start, cur_end, date.fromordinal(cur_start), date.fromordinal(cur_end)


def _alert_days(prior_alerts: list[DetectionAlert] | None) -> set:
    """기준선 오염 방지용 제외 날짜.

    (상품, aspect, 채널) 단위다 — 색상 알림이 나갔던 날은 색상 과거 집계에서만 빠지고,
    같은 날의 사이즈 집계는 그대로 남는다.

    '알림 구간' = 그 알림이 판정에 쓴 현재 윈도우(window_start~window_end). 전역형은
    channel=ALL 이라 특정 채널로 환원되지 않으므로 제외 대상에서 뺀다.
    """
    excluded: set = set()
    for alert in prior_alerts or []:
        if alert.channel == Channel.ALL:
            continue
        for ordinal in range(
            alert.window_start.toordinal(), alert.window_end.toordinal() + 1
        ):
            excluded.add(
                (
                    alert.product_group_id,
                    alert.main_aspect.value,
                    alert.channel.value,
                    ordinal,
                )
            )
    return excluded


# ── [3]~[5] 소스별 후보 만들기 ───────────────────────────────────


def _build_candidates(
    verdict_results: list[dict],
    source: str,
    tests: dict,
    counts: dict,
) -> dict[tuple, dict]:
    """한 소스의 [3] 판정들을 발행 단위 (상품, main_aspect, 채널) 후보로 접는다.

    [4] 를 여기서 적용한다 — 같은 채널에서 여러 aspect 가 동시 발화하면 delta 최대가
    main 이고 나머지는 sub_aspects 로 병기된다.

    Returns:
        {(product, channel): 후보 dict} — (상품, 채널)당 정확히 1건.
        channel 은 전역·잠정전역이면 "ALL" (alert.resolve_channel 이 최종 변환).

    키에 main_aspect 를 넣지 않는 이유: [8] 종합이 CS·리뷰를 짝지을 때 두 소스의
    main_aspect 가 다르면(CS=색상, 리뷰=사이즈) 키가 어긋나 종합이 안 되고 알림이 2건
    나간다. 스펙이 이 경우를 안 다루므로 "종합의 단일 진실은 CS" 기조를 따라
    (상품, 채널)로 짝짓고 CS 값을 채택한다 — verdict 불일치 처리와 같은 규칙.
    """
    buckets: dict[tuple, dict] = defaultdict(dict)

    for result in verdict_results:
        if result["source"] != source or result["verdict"] in _NO_ALERT_VERDICTS:
            continue
        product, aspect = result["product"], result["aspect"]

        if result["verdict"] in PRODUCT_LEVEL_VERDICTS:
            buckets[(product, Channel.ALL.value)][aspect] = result
        else:
            # 편중형·구분불가 → 발화 채널마다 alert 1건.
            for channel in result["channels"]:
                buckets[(product, channel)][aspect] = result

    candidates: dict[tuple, dict] = {}
    for (product, channel), by_aspect in buckets.items():
        deltas = {
            aspect: _representative_delta(
                tests, product, aspect, channel, source, result
            )
            for aspect, result in by_aspect.items()
        }
        main, subs = pick_main_aspect(deltas)
        main_result = by_aspect[main]
        stats_channel = _stats_channel(
            tests, product, main, channel, source, main_result
        )

        candidates[(product, channel)] = {
            "product": product,
            "aspect": main,
            "channel": channel,
            "verdict": main_result["verdict"],
            "significant_channels": main_result["channels"],
            # 보류 채널도 **이 alert 의 main_aspect 판정에서 나온 것**만 붙인다.
            # 상품 단위로 모아두면, 같은 상품의 다른 aspect 가 보류시킨 채널이
            # 이 alert 에도 "표본 부족"으로 병기돼 셀러에게 거짓 정보가 나간다.
            # (past_total==0 폴백은 aspect 슬롯별로 걸려서 실제로 aspect 마다 다를 수 있다.)
            "excluded_channels": main_result["held"],
            "channel_rates": _build_channel_rates(
                counts,
                product,
                main,
                source,
                excluded_channels=main_result["held"],
            ),
            "stats": _build_stats(tests, counts, product, main, stats_channel, source),
            "sub_aspects": [
                SubAspectAction(
                    aspect=aspect,
                    delta=deltas[aspect],
                    # sub 는 [6] 을 돌리지 않으므로 원인 상태는 미상(None).
                    recommended_action=decide_recommended_action(
                        by_aspect[aspect]["verdict"], aspect, None
                    ),
                )
                for aspect in subs
            ],
            # 아래 3개는 [6]·[7] 에서 채운다.
            "diagnosis": None,
            "confidence": None,
            "fired": True,
        }
    return candidates


def _build_channel_rates(
    counts: dict,
    product: str,
    aspect: str,
    source: str,
    *,
    excluded_channels: list[str],
) -> list[ChannelRate]:
    """이미 집계한 현재 윈도우 counts를 채널별 비율 스냅샷으로 보존한다.

    알림의 ``stats.source``와 같은 source, ``main_aspect``와 같은 aspect 기준이다.
    관측 문서가 전혀 없는 채널은 비율을 만들 수 없으므로 ``rate=None``이며 판정에서도
    제외된 것으로 표시한다. rate는 퍼센트가 아닌 기존 ``stats.cur_rate``와 같은 0~1 값이다.
    ``total``은 그 비율의 분모다.
    """
    excluded = set(excluded_channels)
    snapshots: list[ChannelRate] = []

    for channel in _SNAPSHOT_CHANNELS:
        channel_counts = counts.get((product, aspect, channel.value, source))
        if channel_counts is None:
            rate = None
            # None(= 구버전 알림)이 아니라 0 이다. build_combinations 가 문서를 센
            # Counter 를 돌므로 키가 없다 = 그 채널 관측 0건이고, 0 이 사실이다.
            # None 을 실으면 백엔드가 구버전과 구분하지 못한다.
            total = 0
            is_excluded = True
        else:
            cur_neg, cur_total, _past_neg, _past_total = channel_counts
            rate = cur_neg / cur_total if cur_total else None
            total = cur_total
            # ``cur_total == 0``은 실질 도달 불가다(Counter 라 키가 있으면 최소 1).
            # 방어용으로 남긴다.
            is_excluded = channel.value in excluded or cur_total == 0

        snapshots.append(
            ChannelRate(channel=channel, rate=rate, excluded=is_excluded, total=total)
        )

    return snapshots


def _representative_delta(
    tests: dict, product: str, aspect: str, channel: str, source: str, result: dict
) -> float:
    """[4] 비교용 delta. 전역형(ALL)은 발화 채널 중 최대값을 대표로 쓴다."""
    if channel != Channel.ALL.value:
        test = tests.get((product, aspect, channel, source))
        return test["delta"] if test else 0.0
    deltas = [
        tests[(product, aspect, ch, source)]["delta"]
        for ch in result["channels"]
        if (product, aspect, ch, source) in tests
    ]
    return max(deltas) if deltas else 0.0


def _stats_channel(
    tests: dict, product: str, aspect: str, channel: str, source: str, result: dict
) -> str:
    """stats 를 어느 채널 값으로 채울지. 전역형은 delta 최대 채널 대표값."""
    if channel != Channel.ALL.value:
        return channel
    return max(
        result["channels"],
        key=lambda ch: (
            tests[(product, aspect, ch, source)]["delta"]
            if (product, aspect, ch, source) in tests
            else 0.0
        ),
    )


def _build_stats(
    tests: dict, counts: dict, product: str, aspect: str, channel: str, source: str
) -> DetectionStats:
    """stats 는 가공 전 원자료 — 대시보드가 "5%→13%"를 재계산 없이 렌더링한다."""
    key = (product, aspect, channel, source)
    test = tests[key]
    cur_neg, cur_total, past_neg, past_total = counts[key]
    return DetectionStats(
        source=Source(source),
        cur_rate=cur_neg / cur_total,
        past_rate=past_neg / past_total,
        delta=test["delta"],
        p_value=test["p_value"],
        bh_significant=test["bh_significant"],
        cur_total=cur_total,
    )


# ── [6] 원인 분류 ────────────────────────────────────────────────


async def _diagnose(
    candidates: dict,
    texts: dict,
    client: Any,
    diagnostics: DetectionDiagnostics | None = None,
) -> None:
    """편중형 & 스코프 내 후보에만 [6] 을 돌린다. 결과를 후보에 제자리로 채운다.

    소스별로 독립 수행한다. 전역·구분불가·스코프 밖은 호출 자체를 안 해 비용이 0 이다.
    """
    targets = [
        (key, candidate)
        for key, candidate in candidates.items()
        if candidate["verdict"] == Verdict.BIASED and is_in_scope(candidate["aspect"])
    ]
    if not targets:
        return

    async def one(key: tuple, candidate: dict) -> None:
        product, channel = key
        aspect = candidate["aspect"]
        source = candidate["stats"].source.value
        items = texts.get((product, aspect, channel, source), [])
        trace_key = (
            f"product={product} aspect={aspect} channel={channel} source={source}"
        )
        try:
            candidate["diagnosis"] = await diagnose_cause(
                aspect,
                items,
                client=client,
                trace_key=trace_key,
            )
        except Exception as exc:
            # 원인 분류는 탐지 이후의 보강 단계다. 후보 하나의 LLM/검증 실패가 다른
            # 상품의 탐지까지 중단시키지 않되, 이 후보는 원인 근거 없이 낮은 확신도로
            # 내려가 개선안을 자동 생성하지 않게 한다.
            candidate["diagnosis"] = None
            candidate["inquiry_ids"] = []
            if diagnostics is not None:
                diagnostics.cause_failures.append(
                    {
                        "product": product,
                        "aspect": aspect,
                        "channel": channel,
                        "source": source,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            logger.exception("원인 분류 후보 실패 — 탐지는 계속합니다 [%s]", trace_key)
            return
        # 인용 경계 = 원인 집계에 실제로 쓴 문의(aspect_match 통과분).
        # 개수가 root_cause.total 과 맞아야 하는 이유는 cause.diagnose_cause 참고.
        candidate["inquiry_ids"] = candidate["diagnosis"]["cs_ids"]

    await asyncio.gather(*(one(key, candidate) for key, candidate in targets))


# ── 진입점 ───────────────────────────────────────────────────────


async def detect_anomaly(
    items: list[ClassifiedItem],
    *,
    documents: list[dict] | None = None,
    detected_at: datetime | None = None,
    window_end: date | None = None,
    prior_alerts: list[DetectionAlert] | None = None,
    resolved_alert_ids: set | None = None,
    past_rate_fallback: dict | None = None,
    change_log: dict | None = None,
    unreliable_denominators: set | None = None,
    client: Any = None,
    diagnostics: DetectionDiagnostics | None = None,
) -> tuple[list[DetectionAlert], list[DetectionAlert]]:
    """[0]~[8] 전체 파이프라인. ClassifiedItem 집합 → 발행할 DetectionAlert.

    Args:
        items: 분류(Agent1) 산출물. 분자의 출처.
        documents: 원본 문서(문의·리뷰). 분모의 출처 — 주면 loader.build_rows() 로 조인하고
            안 주면 items 에서 분모를 센다. 운영에서는 반드시 줘야 한다: 분류 자체가 실패한
            원문은 items 에 없어서, documents 가 있어야 커버리지를 계산하고 누락 슬롯을
            탐지에서 제외할 수 있다(loader 모듈 docstring).
        detected_at: 탐지 시각. 없으면 현재 시각.
        window_end: 현재 윈도우 마지막 날. 없으면 items 의 최신 날짜.
        prior_alerts: 과거 알림. 재알림 억제 + 기준선 오염 방지에 쓴다.
        resolved_alert_ids: 승인/반려 처리가 끝난 alert_id — 억제가 풀린다.
        past_rate_fallback: {(channel, aspect): 과거 부정률} 설정값 테이블. 과거 표본이 알림
            구간 제외로 절반 이하로 줄었을 때 baseline 으로 쓴다. 값을 코드에 박지 않고
            주입받는 이유는 aggregate._apply_baseline_fallback 참고.
        change_log: {(product, aspect): change_id} — 상세페이지 수정 이력이 이상 시점과
            일치하는 건. 확신도 보강용이지 판정 권한이 없어, 없으면 확신도가 '높음'까지 못
            갈 뿐 기각하지 않는다.
        unreliable_denominators: 분류 커버리지 미달로 분모를 믿을 수 없는 (product, channel,
            source) 집합. 검정 전에 family 에서 빠진다(사유는 loader.unreliable_slots).
            documents 를 줬는데 이 값을 안 주면 여기서 직접 계산한다 — 분모의 출처를 넘겨놓고
            커버리지를 안 보면 로더를 쓰는 의미가 절반이라 안전한 쪽을 기본값으로 둔다.
        client: LlmClient (테스트 목킹용 주입).
        diagnostics: 운영 배치가 원인 분류 실패를 수집할 선택적 진단 객체. 알림 반환 계약에는
            영향을 주지 않는다.

    Returns:
        (발행할 알림, 억제된 알림)
    """
    if documents is not None:
        rows = build_rows(documents, items)
        if unreliable_denominators is None:
            unreliable_denominators = unreliable_slots(check_coverage(documents, items))
    else:
        rows = normalize(items)
    if not rows:
        return [], []

    detected_at = _to_kst_aware(detected_at or datetime.now(KST))
    cur_start, cur_end, window_start_date, window_end_date = _window_bounds(
        rows, window_end
    )

    # [0][1] 집계 · 과거 기준
    combinations, texts = build_combinations(
        rows,
        cur_start,
        cur_end,
        aspects=ALL_ASPECTS,
        past_days=PAST_WINDOW_DAYS,
        alert_days=_alert_days(prior_alerts),
        past_rate_fallback=past_rate_fallback,
    )

    # [2] 검정 3관문 → [3] 편중/전역 판정
    batch, held = run_detection(
        combinations, unreliable_denominators=unreliable_denominators
    )
    verdict_results = run_verdict(batch, held)

    tests = {test["key"]: test for test in batch}
    counts = {(p, a, c, s): cnt for p, a, c, s, cnt in combinations}

    # [3]~[5] 를 소스별로 접어 발행 후보를 만든다 (source 독립 수행).
    by_source = {
        source: _build_candidates(verdict_results, source.value, tests, counts)
        for source in (Source.CS, Source.REVIEW)
    }

    # [6] 원인 분류 — 편중형 & 스코프 내만, 소스별 독립
    await asyncio.gather(
        *(_diagnose(c, texts, client, diagnostics) for c in by_source.values())
    )

    # [7] 소스별 확신도
    for candidates in by_source.values():
        for candidate in candidates.values():
            diagnosis = candidate["diagnosis"]
            change_key = (candidate["product"], candidate["aspect"])
            candidate["confidence"] = decide_confidence(
                candidate["verdict"],
                diagnosis["consistent"] if diagnosis else None,
                timestamp_matched=bool(change_log and change_key in change_log),
            )

    # [8] 종합 → 알림 1건씩 발행
    alerts: list[DetectionAlert] = []
    cs_candidates, review_candidates = by_source[Source.CS], by_source[Source.REVIEW]

    for key in sorted(set(cs_candidates) | set(review_candidates)):
        cs, review = cs_candidates.get(key), review_candidates.get(key)
        confidence, interpretation = combine_sources(cs, review)
        if confidence is None:
            continue

        primary_source = pick_primary_source(cs, review)
        primary = cs if primary_source == Source.CS else review
        diagnosis = primary["diagnosis"]

        alerts.append(
            build_alert(
                {
                    **primary,
                    "confidence": confidence,
                    "interpretation": interpretation,
                    "cs_signal": source_signal(cs),
                    "review_signal": source_signal(review),
                    "root_cause": build_root_cause(diagnosis),
                    "inquiry_ids": primary.get("inquiry_ids", []),
                    "linked_change_id": (change_log or {}).get(
                        (primary["product"], primary["aspect"])
                    ),
                },
                detected_at=detected_at,
                window_start=window_start_date,
                window_end=window_end_date,
            )
        )

    published, suppressed = filter_suppressed(
        alerts, prior_alerts, resolved_alert_ids=resolved_alert_ids
    )
    logger.info(
        "detection 완료 window=%s~%s 검정=%d 발화=%d 발행=%d 억제=%d",
        window_start_date,
        window_end_date,
        len(batch),
        sum(1 for t in batch if t["fired"]),
        len(published),
        len(suppressed),
    )
    return published, suppressed
