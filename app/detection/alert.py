"""담당: 서영 (Agent2) — DetectionAlert 조립.

순수 함수만. 통계도 LLM 도 여기서 안 돈다 — [2]~[8] 이 낸 결론을 발행 규칙대로 알림
객체로 옮기는 것만 한다.

발행 단위 = (상품, main_aspect, 채널) 1건:
    편중형        발화 채널마다 1건        channel=해당 채널, root_cause 있음
    전역형        상품당 1건              channel=ALL, root_cause=null, stats=delta 최대 채널
    잠정 전역형    상품당 1건              위와 동일 + excluded_channels 병기
    구분불가       1건                    channel=유의 채널, root_cause=null
    정상          미발행                  (애초에 여기 오지 않는다)

한 채널에서 여러 aspect 가 동시 발화해도 alert 은 1건이다 — [4] 가 뽑은 main_aspect 가
대표가 되고 나머지는 sub_aspects 에 각자의 조치와 함께 병기된다.

root_cause 는 2상태다. [6] 미수행(전역·잠정전역·구분불가·스코프밖)이면 None, [6] 을
수행했으나 원인이 분산됐으면 {label:"미특정", consistent:False}. 둘을 섞지 말 것 —
"원인을 안 봤다"와 "봤는데 흩어졌다"는 다른 정보다.
"""

from datetime import date, datetime

from app.core.ids import ALERT_ID_PREFIX
from app.core.schemas import (
    Aspect,
    Channel,
    DetectionAlert,
    Evidence,
    RootCause,
    SourceSignals,
    Verdict,
)
from app.detection.confidence import decide_recommended_action
from app.detection.scope import is_in_scope

# 상품 단위로 1건만 발행하는 판정들 — 채널은 ALL 로 접힌다.
PRODUCT_LEVEL_VERDICTS = frozenset({Verdict.GLOBAL, Verdict.TENTATIVE_GLOBAL})

UNSPECIFIED_CAUSE = "미특정"
"""[6] 을 수행했으나 원인이 흩어졌을 때의 root_cause.label."""


def make_alert_id(
    *, window_end: date, product_group_id: str, aspect: str, channel: Channel
) -> str:
    """알림의 논리 키를 그대로 옮긴 결정론적 ID.

    예: ALT-20260828-P001-COLOR-COUPANG · ALT-20260828-P001-DAMAGE-ALL(전역형)

    백엔드가 이 값을 멱등 키로 두고 upsert 하므로 같은 입력이면 같은 ID 여야 한다. 예전
    형식(실행일 + 4자리 일련번호)은 일련번호가 `sorted(후보집합)` 순서라 후보가 하나만
    달라져도 뒤쪽이 통째로 밀렸고, 밀린 것이 백엔드에서 새 행이 되며 옛 행은 고아로 남았다.

    축이 네 개인 이유:
      - `window_end`: 데이터 시각이라 같은 구간을 다시 돌려도 같은 ID 다. 억제도 경과일을
        window_end 로 세므로 시계가 일치한다.
      - `aspect`: 논리 키가 (상품, main_aspect, 채널)이고 aspect 가 달라지면 원인·근거·
        권장조치·개선안이 전부 달라진다. 빼면 색상 알림이 사이즈 알림으로 덮여 이력이 사라진다.
      - `channel`: 편중형은 발화 채널, 전역형은 `ALL`.
      - 회사 축은 넣지 않는다 — `product_group_id` 가 회사별 시퀀스라 A사·B사에 똑같이
        `P001` 이 있지만 백엔드가 `(companyId, alert_id)` 복합 유니크로 흡수한다.

    `Aspect(aspect).name` 이지 `aspect.name` 이 아니다 — 호출부가 넘기는 값은 평범한 `str`
    이고 `Aspect` 로 바뀌는 건 `DetectionAlert` 생성 시점이라 여기는 그 이전이다. 바로 쓰면
    `AttributeError` 로 죽는다. 멤버명이 백엔드 대조표와 일치하므로 매핑 테이블을 만들지 말 것.

    길이 = `33 + len(product_group_id)`. 백엔드 컬럼은 `varchar(72)`.
    """
    return (
        f"{ALERT_ID_PREFIX}{window_end:%Y%m%d}-{product_group_id}"
        f"-{Aspect(aspect).name}-{Channel(channel).value}"
    )


def resolve_channel(verdict: str, channel: str) -> Channel:
    """전역·잠정 전역은 상품 단위 판정이라 channel 이 ALL 로 접힌다."""
    if verdict in PRODUCT_LEVEL_VERDICTS:
        return Channel.ALL
    return Channel(channel)


def build_root_cause(diagnosis: dict | None) -> RootCause | None:
    """[6] 진단 결과를 root_cause 필드로 옮긴다.

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
) -> DetectionAlert:
    """종합 판정 1건 → DetectionAlert 1건.

    Args:
        judgement: service 가 [3]~[8] 을 엮어 만든 (상품, main_aspect, 채널) 단위 결과.
            {
              "product":        상품 그룹 ID,
              "aspect":         main_aspect ([4] 결과),
              "channel":        발화 채널 (전역형이면 무시되고 ALL 로 접힘),
              "verdict":        채택 소스의 verdict,
              "significant_channels": 유의 판정된 채널 목록,
              "excluded_channels":    표본 부족으로 제외된 채널 목록,
              "channel_rates":        탐지 당시 채널별 현재 부정률 스냅샷,
              "stats":          DetectionStats (채택 소스 기준),
              "confidence":     [8] 종합 확신도,
              "interpretation": [8] 해석 라벨,
              "cs_signal" / "review_signal": bool|None (null=보류),
              "root_cause":     RootCause | None,
              "inquiry_ids":    [6] 원인 집계에 쓴 문의 ID (= root_cause.total 건),
              "sub_aspects":    [SubAspectAction],
              "linked_change_id": str | None,
            }
    """
    verdict = judgement["verdict"]
    aspect = judgement["aspect"]
    root_cause = judgement["root_cause"]
    product_group_id = judgement["product"]

    # 접힌 채널을 ID 와 필드가 같이 쓴다. 전역형은 channel 이 ALL 로 접히는데 ID 를
    # judgement["channel"] 로 만들면 `...-COUPANG` 인데 필드는 ALL 인 알림이 나온다 —
    # 상품당 1건이어야 할 전역형이 발화 채널마다 다른 ID 를 받는다.
    channel = resolve_channel(verdict, judgement["channel"])

    return DetectionAlert(
        alert_id=make_alert_id(
            window_end=window_end,
            product_group_id=product_group_id,
            aspect=aspect,
            channel=channel,
        ),
        detected_at=detected_at,
        updates_alert_id=None,  # 갱신 여부는 suppression 이 나중에 채운다
        product_group_id=product_group_id,
        channel=channel,
        window_start=window_start,
        window_end=window_end,
        verdict=verdict,
        significant_channels=list(judgement["significant_channels"]),
        excluded_channels=list(judgement["excluded_channels"]),
        channel_rates=list(judgement.get("channel_rates", [])),
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
        # scope_in 은 순수 aspect 속성 — verdict 를 섞지 않는다 (scope.is_in_scope).
        scope_in=is_in_scope(aspect),
        recommended_action=decide_recommended_action(
            verdict, aspect, root_cause.consistent if root_cause else None
        ),
        evidence=Evidence(
            inquiry_ids=judgement["inquiry_ids"],
            linked_change_id=judgement["linked_change_id"],
        ),
    )
