"""담당: 지인 (Agent3) — LangGraph 정의.

4개 모듈 중 유일하게 프레임워크를 쓰는 곳.
이유(문서 5장): 루프·상태·HITL 이 실제로 있는 컴포넌트에만 프레임워크 적용.

흐름:
    RAG 조회 → 개선안 생성 → 인용 검증 → (실패 시) 재시도 루프
                                       → (MAX_RETRY 초과) 근거없음 경로
             → interrupt (B4/B5 승인·반려 대기)

TODO(지인): schemas.py 확정 후 구현.
  - 노드: _retrieve() / _generate() / _verify() / _fallback()
  - 조건부 엣지: 인용 검증 결과에 따라 재시도 vs 통과 vs fallback
  - 재시도 횟수는 constants.MAX_RETRY 사용
  - 컬렉션2(반려사유)를 RAG 조회에 포함 → 같은 이유로 또 반려당하지 않도록

HITL 메모: 승인·반려 상태의 소유자는 Spring Boot 입니다 (문서 1장).
  LangGraph interrupt 로 멈춘 뒤 재개하려면 그래프 상태를 어딘가 영속화해야 하는데,
  그 저장소를 checkpointer 로 할지 Spring Boot DB 로 할지는 5주차 연동 때 정해야 합니다.
  → 지금 정하지 말고, 스키마 회의 때 안건으로만 올려두세요.
"""
