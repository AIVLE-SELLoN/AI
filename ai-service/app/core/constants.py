"""전역 상수.

매직넘버 금지. 정량 실험 때 바꿔가며 돌려야 하는 값들이라 전부 여기 모아둔다.
값을 바꾸면 실험 결과가 통째로 달라지므로 변경 전 팀 합의 필수.
"""

# --- 통계 (detection / 서영) ---
ALPHA = 0.05
"""유의수준. 카이제곱·Fisher·비율검정 공통."""

MIN_SAMPLE_SIZE = 10
"""이 미만이면 검정 자체를 건너뛴다 (표본 부족 → 위양성 급증)."""

# --- LLM 공통 ---
MAX_RETRY = 2
"""LLM 호출/파싱 실패 시 재시도 횟수. 총 시도 = 1 + MAX_RETRY."""

# --- 벡터DB 컬렉션 이름 ---
COLLECTION_DETAIL_PAGES = "detail_pages"
"""컬렉션1 — 상세페이지. 개선안 생성의 인용 근거."""

COLLECTION_REJECTION_REASONS = "rejection_reasons"
"""컬렉션2 — 반려 사유. 다음 생성 시 참고 (B5 반려 → 적재)."""
