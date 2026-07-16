"""담당: 지인 (Agent3) — 진입점.

이 파일은 얇게 유지. 실제 흐름은 graph.py 의 LangGraph 가 들고 있다.
router 가 service 를 부르고, service 가 그래프를 실행하는 구조.

TODO(지인): schemas.py 확정 후 구현.
  - generate_recommendation(anomaly) -> Recommendation
    (그래프 초기 상태 구성 → graph.ainvoke() → 결과를 Recommendation 으로 변환)
"""
