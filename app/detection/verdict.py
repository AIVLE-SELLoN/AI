"""담당: 서영 (Agent2) — [3] 편중·전역 판정.

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.

[2]가 '각 채널 vs 자기 과거'였다면 [3]은 '한 (상품, aspect)를 채널끼리 비교'해 발화 채널
수로 편중/전역을 가른다.

verdict 5종:
    정상         발화 채널 0개                                → 알림 없음
    구분불가     판정 가능 채널이 1개뿐(나머지 보류)           → [6] 생략, 확신도 '중간'
    전역형       판정 가능 채널 전부 발화 · 보류 없음          → 상품 자체 점검
    잠정 전역형   판정 가능 채널 전부 발화 · 보류 있음          → 위 + "확정 시 재판정"
    편중형       일부만 발화 (1~2개)                          → [6] 원인 진단 진행

verdict 값은 Verdict enum 경유로 낸다 — 문자열을 다시 적으면 schemas.py 가 바뀔 때
이 파일만 옛 값으로 남는다.

판정 순서: fired==0 → testable==1 → 전부발화 → 편중. testable==1 검사가 '전부 발화'보다
먼저다 — 1채널은 발화해도 비교 대상이 없어 구분불가다.
"""

from app.core.schemas import Verdict


def classify_pattern(channel_status: dict) -> dict:
    """한 (상품, aspect)의 채널별 상태로 편중/전역 verdict 를 판정한다.

    판정 단위는 aspect별이다 ("쿠팡이 편중"이 아니라 "쿠팡의 색상이 편중").

    Args:
        channel_status: {channel: {"testable", "fired"}}. testable=False 는 표본 부족으로
            보류된 채널이고, fired 는 testable 채널에만 유효하다.

    Returns:
        {"verdict", "channels", "held"} — 구분불가면 "note" 가 붙는다. held 는 알림에
        "표본 부족"을 병기하는 데 쓴다.
    """
    testable = [ch for ch, s in channel_status.items() if s["testable"]]
    held = [ch for ch, s in channel_status.items() if not s["testable"]]
    fired = [ch for ch in testable if channel_status[ch]["fired"]]

    if not fired:
        return {"verdict": Verdict.NORMAL, "channels": [], "held": held}

    # 비교 대상이 없는데 "편중"이라 단정하면 과잉 주장이다. '전부 발화'보다 먼저 검사한다.
    if len(testable) == 1:
        return {
            "verdict": Verdict.INDETERMINATE,
            "channels": fired,
            "held": held,
            "note": "타 채널 표본 부족 — 편중/전역 구분 불가",
        }

    # 보류 채널이 있으면 '잠정' — 그 채널은 멀쩡할 수도 있다.
    if len(fired) == len(testable):
        return {
            "verdict": Verdict.TENTATIVE_GLOBAL if held else Verdict.GLOBAL,
            "channels": fired,
            "held": held,
        }

    return {"verdict": Verdict.BIASED, "channels": fired, "held": held}


def run_verdict(fired_batch: list, held: list) -> list:
    """윈도우 전체를 (상품, aspect, source) 로 묶어 classify_pattern 을 돌린다.

    [3]은 source 별로 독립 수행한다. 소스 종합([8] combine_sources)은 이 뒤의 별도 단계다.

    Args:
        fired_batch: [2] decide_fires 결과. 각 dict 에 "key"·"fired" 가 있다.
        held: [2] build_batch 의 보류 (상품, 채널, source) 리스트.

    Returns:
        classify_pattern 반환값 + {"product", "aspect", "source"} 리스트.

    배치에 있는 채널은 testable=True, held 채널 중 이 그룹 배치에 없는 것만 testable=False
    로 넣는다. held 는 source·채널 단위라 다른 source 그룹엔 끼지 않고, 같은 채널이 이 그룹
    배치에 있으면 testable 이라 중복되지 않는다.
    """
    held_channels: dict = {}
    for product, channel, source in held:
        held_channels.setdefault((product, source), set()).add(channel)

    groups: dict = {}
    for test in fired_batch:
        product, aspect, channel, source = test["key"]
        groups.setdefault((product, aspect, source), {})[channel] = test["fired"]

    results: list = []
    for (product, aspect, source), channel_fired in groups.items():
        channel_status = {
            ch: {"testable": True, "fired": fired}
            for ch, fired in channel_fired.items()
        }
        for ch in held_channels.get((product, source), ()):
            if ch not in channel_status:
                channel_status[ch] = {"testable": False, "fired": False}

        result = classify_pattern(channel_status)
        result["product"] = product
        result["aspect"] = aspect
        result["source"] = source
        results.append(result)

    return results
