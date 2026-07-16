# 프롬프트3 — 원인 분류 (v1)

TODO(서영): 작성. 통계로 "이상하다"를 잡은 뒤, 그 원인 라벨을 붙이는 단계입니다.

구버전 삭제 금지 — 개선 시 `classify_cause_v2.md` 신규 생성.

---

## 지시

(여기에 원인 분류 지시문)

## 입력

- 이상징후: {anomaly_summary}
- 관련 CS/리뷰: {related_items}
- 상세페이지 변경 이력: {detail_changes}

## 출력 형식

(JSON 스키마 — 원인 라벨 + 근거)
