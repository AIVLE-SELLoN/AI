"""담당: 서영 (Agent2) — 이상탐지 파이프라인 [0]~[7].

성격: 통계 + LLM 워크플로우 → scipy/statsmodels + 순수 Python (프레임워크 없음).

역할 분리:
  - statistics.py : 순수 통계 계산 (scipy 로직). LLM·DB 모름 → 단위테스트 쉬움.
  - service.py    : 파이프라인 조립. 통계 결과 + LLM 원인분류를 엮는다.

TODO(서영): schemas.py 확정 후 구현.
  - detect_anomaly(items) -> AnomalyResult
  - _is_biased_channel(...) -> bool     (편중형 판정, bool 이므로 is_ 접두어)
  - 원인 분류는 load_prompt("detection", "classify_cause_v1") + get_llm_client()
  - 유의수준·표본크기는 constants.ALPHA / MIN_SAMPLE_SIZE 사용 (매직넘버 금지)
"""
