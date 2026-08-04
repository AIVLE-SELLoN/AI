"""담당: 서영 (Agent2) — DetectionAlert 조립 (탐지 결과 스키마 §3.1·§5).

순수 함수만. 통계도 LLM 도 여기서 안 돈다 — [2]~[8] 이 낸 결론을 **발행 규칙대로 알림
객체로 옮기는 것**만 한다. 그래야 "어떤 채널로 어떤 조치가 나가는가"를 숫자 없이 테스트한다.

발행 단위 = (상품, main_aspect, 채널) 1건. (§5.1·§5.2)
    편중형        발화 채널마다 1건        channel=해당 채널, root_cause 있음
    전역형        상품당 1건              channel=ALL, root_cause=null, stats=delta 최대 채널
    잠정 전역형    상품당 1건              위와 동일 + excluded_channels 병기
    구분불가       1건                    channel=유의 채널, root_cause=null
    정상          미발행                  (애초에 여기 오지 않는다)

한 채널에서 여러 aspect 가 동시 발화해도 alert 은 1건이다 — [4] 가 뽑은 main_aspect 가
대표가 되고 나머지는 sub_aspects 배열에 각자의 조치와 함께 병기된다 (§3.2 SC-029).

⚠️ root_cause 2상태 (§5.2): ① [6] 미수행(전역·잠정전역·구분불가·스코프밖) → None.
   ② [6] 수행했으나 원인 분산 → {label:"미특정", consistent:False}. 둘을 섞지 말 것 —
   "원인을 안 봤다"와 "봤는데 흩어졌다"는 다른 정보다.
"""

from collections.abc import Iterator
from datetime import date, datetime

from app.core.schemas import (
    Channel,
    DetectionAlert,
    Evidence,
    RootCause,
    SourceSignals,
    Verdict,
)
from app.detection.confidence import decide_recommended_action
from app.detection.scope import is_in_scope

# 상품 단위로 1건만 발행하는 판정들 — 채널은 ALL 로 접힌다. (§5.1)
PRODUCT_LEVEL_VERDICTS = frozenset({Verdict.GLOBAL, Verdict.TENTATIVE_GLOBAL})

UNSPECIFIED_CAUSE = "미특정"
"""[6] 을 수행했으나 원인이 흩어졌을 때의 root_cause.label. (§5.2)"""


def make_alert_id(detected_at: datetime, seq: int) -> str:
    """ALT-날짜-일련번호. (§3 alert_id)"""
    return f"ALT-{detected_at:%Y%m%d}-{seq:04d}"


def resolve_channel(verdict: str, channel: str) -> Channel:
    """전역·잠정 전역은 상품 단위 판정이라 channel 이 ALL 로 접힌다. (§5.1)"""
    if verdict in PRODUCT_LEVEL_VERDICTS:
        return Channel.ALL
    return Channel(channel)


def build_root_cause(diagnosis: dict | None) -> RootCause | None:
    """[6] 진단 결과를 root_cause 필드로 옮긴다. (§5.2 root_cause 2상태)

    Args:
        diagnosis: cause.diagnose_cause() 반환값. [6] 미수행이면 None.

    Returns:
        None            → [6] 자체를 안 돌렸다
        RootCause(...)  → 돌렸다. 분산됐으면 label="미특정", consistent=False
    """
    if diagnosis is None:
        return None

    consistent = bool(diagnosis["consistent"])
    return RootCause(
        label=diagnosis["label"] if consistent else UNSPECIFIED_CAUSE,
        count=diagnosis["count"],
        total=diagnosis["total"],
        consistent=consistent,
    )


def build_alert(
    judgement: dict,
    *,
    detected_at: datetime,
    window_start: date,
    window_end: date,
    seq: Iterator[int],
) -> DetectionAlert:
    """종합 판정 1건 → DetectionAlert 1건. (§3·§5)

    Args:
        judgement: service 가 [3]~[8] 을 엮어 만든 (상품, main_aspect, 채널) 단위 결과.
            {
              "product":        상품 그룹 ID,
              "aspect":         main_aspect ([4] 결과),
              "channel":        발화 채널 (전역형이면 무시되고 ALL 로 접힘),
              "verdict":        채택 소스의 verdict,
              "significant_channels": 유의 판정된 채널 목록,
              "excluded_channels":    표본 부족으로 제외된 채널 목록,
              "stats":          DetectionStats (채택 소스 기준),
              "confidence":     [8] 종합 확신도,
              "interpretation": [8] 해석 라벨,
              "cs_signal" / "review_signal": bool|None (null=보류),
              "root_cause":     RootCause | None,
              "inquiry_ids":    [6] 원인 집계에 쓴 문의 ID (= root_cause.total 건),
              "sub_aspects":    [SubAspectAction],
              "linked_change_id": str | None,
            }
        seq: alert_id 일련번호 이터레이터 (윈도우 전체에서 공유).
    """
    verdict = judgement["verdict"]
    aspect = judgement["aspect"]
    root_cause = judgement["root_cause"]

    return DetectionAlert(
        alert_id=make_alert_id(detected_at, next(seq)),
        detected_at=detected_at,
        updates_alert_id=None,  # 갱신 여부는 suppression 이 나중에 채운다
        product_group_id=judgement["product"],
        channel=resolve_channel(verdict, judgement["channel"]),
        window_start=window_start,
        window_end=window_end,
        verdict=verdict,
        significant_channels=list(judgement["significant_channels"]),
        excluded_channels=list(judgement["excluded_channels"]),
        main_aspect=aspect,
        sub_aspects=list(judgement["sub_aspects"]),
        stats=judgement["stats"],
        source_signals=SourceSignals(
            cs=judgement["cs_signal"],
            review=judgement["review_signal"],
            interpretation=judgement["interpretation"],
        ),
        root_cause=root_cause,
        detection_confidence=judgement["confidence"],
        # scope_in 은 순수 aspect 속성 — verdict 를 섞지 않는다 (scope.is_in_scope 주석).
        scope_in=is_in_scope(aspect),
        recommended_action=decide_recommended_action(
            verdict, aspect, root_cause.consistent if root_cause else None
        ),
        evidence=Evidence(
            inquiry_ids=judgement["inquiry_ids"],
            linked_change_id=judgement["linked_change_id"],
        ),
    )
