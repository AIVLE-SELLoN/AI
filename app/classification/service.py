"""담당: 현진 (Agent1) — 분류 로직.

성격: LLM 워크플로우 (단일 패스, 분기 없음) → 프레임워크 불필요, 순수 Python.

TODO(현진): schemas.py 확정 후 구현.
  - classify_aspect(items) -> list[ClassifiedItem]
  - _parse_llm_response(raw) -> ...
  - 프롬프트는 load_prompt("classification", "classify_aspect_v1") 로 로딩
  - LLM 호출은 반드시 get_llm_client() 경유 (직접 openai import 금지)
"""
