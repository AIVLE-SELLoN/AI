"""담당: 서영 (Agent2) — 통계 검정 로직.

여기는 순수 함수만. LLM·DB·FastAPI 를 import 하지 않는다.
그래야 fixture 없이도 숫자만 넣어 단위테스트할 수 있다.

핵심(이상탐지 로직 V2.1): **채널간 비교가 아니라 채널별 "현재 vs 자기 과거"** 를
Fisher 단측으로 검정한다. 각 채널이 자기 평소와만 싸우므로 채널간 baseline 차이가
판정에 끼어들지 않는다. 채널 비교는 [3] 편중/전역 판정에서만 한다.

TODO(서영): 구현.
  - detect_anomaly_per_channel(cur_neg, cur_total, past_neg, past_total) -> bool
      scipy.stats.fisher_exact(..., alternative='greater') 단측
      반환 = (p < ALPHA) and (delta >= MIN_DELTA)   ← 이중 잠금
  - classify_channels(counts_by_channel) -> list[str]
      cur_total >= MIN_SAMPLE_SIZE 인 채널만 판정, 미만은 보류(이월)
  - (선택) 하루 검정 결과 전체에 BH-FDR 보정 — statsmodels.stats.multitest.multipletests

주의:
  - **카이제곱·z-test 는 쓰지 않는다.** V2.1 에서 Fisher 단측으로 단일화했다.
    Fisher 는 표본 크기와 무관하게 정확해서 분기가 필요 없다.
    (`min(cur_neg, past_neg) < 5` 분기도 삭제됨 — 정석인 기대빈도 기준과도 달랐음)
  - 임계값은 전부 core/constants.py 사용. 함수 기본인자에 0.05/0.03 박지 말 것.
    MIN_DELTA 는 평가셋 스윕으로 캘리브레이션할 값이라 특히 중요.
  - p < ALPHA 만으로 판정하면 안 된다. 조합마다 매일 검정해서 다중검정 문제가
    생기므로 MIN_DELTA 와의 이중 잠금이 필수.
"""