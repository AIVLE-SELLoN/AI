"""담당: 서영 (Agent2) — [3] 편중·전역 판정 (이상탐지 로직 V3 §[3]).

순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다 ([2] statistics.py 와 동일 원칙).

[2]가 '각 채널 vs 자기 과거'였다면, [3]은 '한 (상품, aspect)를 채널끼리 비교'해서
verdict 를 정한다. 채널간 baseline 차이는 [2]에서 이미 자기-과거 비교로 제거됐고,
여기서는 '어느 채널이 울렸나'의 분포만 본다.

verdict 4종 (탐지 결과 스키마 §5 / 로직 §[3]):
    전역형       관측 채널이 전부 발화 · 보류 없음            → 상품 자체 문제
    편중형       일부 채널만 발화 · 나머지 정상 · 보류 없음    → 채널 운영 문제
    잠정 전역형   관측 채널은 전부 발화했으나 일부 채널 보류    → "확정 시 재판정"
    구분불가     판정 가능 채널이 1개뿐(나머지 보류)          → 편중/전역 구분 불가

보류(held)는 [2] build_batch 의 표본 가드에서 나온 (상품,채널) 목록을 그대로 쓴다.

⚠️ 열린 항목 (월요일 구현 시 정리 — 탐지 스키마 §269):
    combine_sources() 가 현재 (확신도, 라벨)만 반환. [3] verdict 를 정하려면
    cs/review 를 CS 우선으로 합친 '채널별 발화 여부'가 선행돼야 하므로,
    combine_sources 가 verdict·stats·source 채택까지 반환하도록 확장 필요.

TODO(월): 아래 함수 본문 + 수제 숫자 유닛테스트(tests/test_detection.py).
          판정 경계는 로직 §[3]·시나리오(SC-030~038 편중/전역/구분불가) 확정본 대조 후.
"""


def decide_verdict(per_channel: dict) -> dict:
    """한 (상품, aspect)에 대해 채널 분포를 보고 verdict 를 정한다.

    Args:
        per_channel: {channel: status}
            status ∈ {"fired", "normal", "held"}
              - fired:  [2]에서 발화(BH 유의 AND min_delta)
              - normal: 판정됐으나 미발화
              - held:   표본 가드로 보류(판정 불가)

    Returns:
        {"verdict", "significant_channels", "excluded_channels"}
          - verdict:              전역형 / 편중형 / 잠정 전역형 / 구분불가
          - significant_channels: 발화한 채널 리스트
          - excluded_channels:    보류(held) 채널 리스트 ("표본 부족" 병기용)

    판정 규칙(요지, 확정본 대조 예정):
        관측 채널 = held 아닌 채널.
        - held 없음 & 관측 전부 fired            → 전역형
        - held 없음 & 일부만 fired               → 편중형
        - held 있음 & 관측 전부 fired            → 잠정 전역형
        - 판정 가능(fired/normal) 채널이 1개뿐   → 구분불가
    """
    raise NotImplementedError("월요일 구현 — 로직 §[3] 확정본 대조 후")


def run_verdict(fired_batch: list, held: list) -> list:
    """윈도우 전체를 (상품, aspect) 로 묶어 decide_verdict 를 돌린다.

    Args:
        fired_batch: [2] decide_fires 결과 (각 dict 에 "key"=(product,aspect,channel,source),
                     "fired" 포함)
        held:        [2] build_batch 의 보류 (상품, 채널) 리스트

    Returns:
        (상품, aspect) 단위 판정 결과 리스트 — 각 dict 에 verdict 등 부착.

    NOTE: cs/review 합산(combine_sources) 후의 '채널별 발화 여부'를 만들어
          decide_verdict 에 넘겨야 한다 (위 열린 항목 참조).
    """
    raise NotImplementedError("월요일 구현 — combine_sources 확장과 함께")
