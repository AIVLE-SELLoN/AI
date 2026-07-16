"""담당: 지인 (Agent3) — 인용 검증.

LLM 이 만들어낸 개선안이 "상세페이지에 실제로 있는 내용"을 근거로 삼았는지 확인.
환각을 여기서 거른다.

TODO(지인): 구현.
  - _verify_quote(quote, source_text) -> bool
    (반환이 bool 이므로 _is_/_has_ 접두어 고려: _has_evidence 등)
  - 근거 없으면 EvidenceNotFoundError → graph 가 fallback 경로로 분기

검증 방식 메모: 완전일치로 하면 LLM 이 조사 하나만 바꿔도 실패하고,
너무 느슨하면 환각을 통과시킵니다. 정규화 후 부분일치 → 유사도 임계값 순으로
느슨하게 가되, 임계값은 constants.py 에 상수로 빼서 정량 실험 때 조정하세요.
"""
