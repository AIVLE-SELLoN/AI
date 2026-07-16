"""단계 간 입출력 Pydantic 모델 = 팀 계약서.

┌─────────────────────────────────────────────────────────────────┐
│  ⚠️  아직 비어 있습니다. 의도된 상태입니다.                       │
│                                                                 │
│  기술 정리 문서 9장(0주차 체크리스트):                            │
│    "전원 회의: schemas.py 확정 — 완료 전 개발 시작 금지"          │
│                                                                 │
│  스키마는 4명이 주고받는 계약이라 한 명이 정하면 안 됩니다.        │
│  회의에서 확정한 뒤 이 파일을 채우고, 그 다음에 모듈 개발을        │
│  시작하세요.                                                     │
└─────────────────────────────────────────────────────────────────┘

회의에서 정해야 할 것 (문서 2장 파이프라인 기준):

  1. ClassifiedItem      — ① classification 출력 → ② detection 입력
                           aspect, sentiment 부착. 원문 추적 키(cs_id) 포함.

  2. AnomalyResult       — ② detection 출력 → ③ recommendation 입력
                           편중형/전역형 구분 + 원인라벨 + 통계 근거.

  3. Recommendation      — ③ recommendation 출력 → Spring Boot 저장
                           개선안 JSON + 확신도 + 인용 근거.

  4. API 요청/응답 모델  — ClassifyRequest/Response, DetectRequest/Response,
                           GenerateRecommendationRequest/Response

네이밍 규칙 (문서 네이밍 컨벤션):
  - 클래스: PascalCase
  - API 입출력:        `~Request` / `~Response`
  - 단계 간 데이터:     `~Result` / `~Item`
  - 필드: snake_case, 로그 추적 키(cs_id, sku)는 가능한 한 모든 모델에 포함

확정 후 이 docstring 은 지우고 모델 정의로 교체하세요.
"""
