# 개선안 출력 스키마

***Agent3 → HITL·피드백 인터페이스 계약***

---

> 🟢 **문서 상태: [미확정]**
> 

## 1. 개요

Agent3(개선안 생성)가 내보내는 JSON의 정의. 

**생산자:** Agent3 (+ Evaluator 자기검증). 

**소비자 4곳:** 

① HITL 승인 화면 (B4/B5) 

② 피드백 저장 (승인→RAG 정예시 / 반려→부예시) 

③ 대시보드 

④ 품질 평가 (루브릭 기반 사람 평가 대상).

```
탐지 결과 스키마(입력) ──▶ Agent3 + Evaluator ──(이 스키마의 JSON)──▶ HITL 화면 / 피드백 / 대시보드
```

**개선안(recommendation) 1건의 단위 = alert 1건당 1건.** 

## 2. JSON 전체 예시

```json
{
  "recommendation_id": "REC-20260528-0001",
  "alert_id": "ALT-20260528-0001",
  "created_at": "2026-05-28T10:31:40",

  "proposal": {
    "type": "image_guide",
    "target_field": "상세 이미지 - 색상 컷",
    "current_text": "실내 조명 기준 착용 컷 (색감 왜곡)",
    "proposed_text": "자연광·보정 최소화로 색상 컷 재촬영 권장. 실물 색감이 드러나는 각도 포함",
    "rationale": "문의 20건 중 14건이 화면과 실물 색감 차이 지적 — 문구가 아닌 재촬영으로 실물 색 전달",
    "detailpage_grounded": false
  },

  "citations": [
    { "inquiry_id": "INQ-000412", "quote": "사진이랑 색이 너무 달라요" },
    { "inquiry_id": "INQ-000415", "quote": "화면보다 어두워요" }
  ],

  "evaluator": {
    "passed": true,
    "attempts": 1,
    "checks": {
      "grounding": true,
      "consistency": true,
      "actionability": true
    },
    "failure_reason": null
  },

  "similar_case": "지난 4월 미디원피스A, 동일 원인 재촬영 후 2주 뒤 부정률 정상화",

  "recommendation_confidence": "중간",
  "confidence_reason": "상세페이지 텍스트 근거는 없으나 유사사례 있음 — 중간",
  "capped_by_detection": false,

  "hitl_status": "반려",
  "hitl_feedback": {
    "processed_at": "2026-05-28T11:00:00",
    "processed_by": "user_id_123",
    "rejection_reason": {
      "reason_code": "근거부족",
      "reason_text": "유사사례 1건만으로는 문구 교체 근거가 부족함. 추가 검증 필요."
    },
    "edited_text": null
  }
}
```

<aside>
🚧

`evaluator` 설명

- `passed=true, attempts=1` → 첫 시도에 자기검증 통과(재시도 0).
- `checks` → **`grounding`**=인용이 근거(inquiry_ids·상세페이지) 안에 실재 / **`consistency`**=제안이 원인·aspect와 일치 / **`actionability`**=바로 실행 가능
- `recommendation_confidence="중간"` → 개선안 확신도(탐지와 별개). 상세페이지 근거·유사사례 중 하나만 있어 중간
- `capped_by_detection=false` → 탐지 확신도로 눌린 적 없음(이 alert는 탐지 높음이라 상한 없음)
- `hitl_status="반려"` → 이 예시는 이미 처리된 건. **생성 직후엔 항상 이 값으로 옴**
- `hitl_status="대기"` + `hitl_feedback=null`.
- 반려 처리 시 `hitl_feedback.rejection_reason.reason_code` **필수**(근거부족/이미조치함/원인다름/기타) — 사유 없는 반려는 컬렉션2 학습에 못 씀
</aside>

## 3. 필드 정의

| 필드 | 타입 | 값 범위 | 설명 | 주 소비자 |
| --- | --- | --- | --- | --- |
| recommendation_id | string | REC-날짜-일련번호 | 개선안 고유 ID | 전체 |
| alert_id | string |  | 원본 탐지 알림 ID — **탐지 결과 스키마와 1:1 join 키** | 전체 |
| created_at | datetime |  | 생성 시각 | 대시보드 |
| proposal.type | enum | copy_draft / image_guide | 문구 초안 도구 / 이미지 가이드 도구 중 무엇의 산출물인지 | HITL 화면 |
| proposal.target_field | string |  | 수정 대상 위치 (예: 상세설명 색상 안내) | HITL 화면 |
| proposal.current_text | text |  | 현재 문구 (image_guide면 CS 문의 기반 현재 이미지 문제 요약 — 이미지·비전 미참조) | HITL 화면 (before/after 렌더링) |
| proposal.proposed_text | text |  | 제안 문구 또는 촬영·보정 지침 | HITL 화면 |
| proposal.rationale | text |  | 왜 이 개선안인지 — 근거 요약 | HITL 화면 |
| proposal.detailpage_grounded | bool | true/false | 상세페이지 텍스트 근거 유무. **도구 선택(§6)·확신도 입력**. false면 image_guide 강제 + 확신도 하향 (스코프 한계 케이스는 예외 — §6) | Agent3·평가 |
| citations | array |  | 인용 문의 목록 (inquiry_id + 발췌) — **탐지 결과의 evidence.inquiry_ids 부분집합만 허용** | HITL 화면·Evaluator |
| similar_case | string/null |  | 유사 사례 요약(RAG 컬렉션2). 0건이면 null + 확신도 한 단계 하향 | HITL 화면 |
| evaluator.passed | bool |  | 자기검증 최종 통과 여부 | 전체 |
| evaluator.attempts | int | 1~3 | 시도 횟수 (재시도 최대 2회) | 평가 |
| evaluator.checks.* | bool | 3개 기준 | 기준별 통과 여부(셋 다 true여야 passed=true). **grounding**=인용이 근거 안에 실재 / **consistency**=제안이 원인·aspect와 일치 / **actionability**=바로 실행 가능 | 평가 |
| evaluator.failure_reason | string/null |  | 최종 실패 시 사유 | 대시보드 |
| recommendation_confidence | enum | 높음/중간/낮음/null | **개선안 확신도**(탐지 확신도와 별개, 셀러 화면 표시). **높음**=상세페이지 근거+유사사례 둘 다 / **중간**=하나만 / **낮음**=둘 다 없음 / **null**=검증 실패(§5) | HITL 화면 |
| confidence_reason | string |  | 확신도 판단 이유 한 줄(셀러 화면 근거 표시용) | HITL 화면 |
| capped_by_detection | bool |  | 탐지 확신도 때문에 개선안 확신도가 눌렸으면 true. 탐지 중간→개선안 상한도 중간(높음 금지), 탐지 높음→상한 없음 | 평가 |
| hitl_status | enum | 대기/승인/반려/수정후승인 | 생성 시 항상 "대기" — 이후 갱신은 Agent 아닌 HITL 처리 담당 | HITL 화면·피드백 |
| hitl_feedback | object/null |  | HITL 처리 결과. **생성 시 null** — 승인/반려 처리 후 백엔드가 채움 | 피드백 |
| hitl_feedback.processed_at / .processed_by | datetime / string |  | 처리 시각·처리자 | 대시보드 |
| hitl_feedback.rejection_reason.reason_code | enum/null | 근거부족/이미조치함/원인다름/기타/null | **반려 시 필수**, 승인 시 null. 팀 코멘트 반영 — 사유 없는 반려는 컬렉션2(반려 사례 RAG) 학습에 못 쓰임 | 피드백·컬렉션2 |
| hitl_feedback.rejection_reason.reason_text | string/null |  | 자유 입력 보충 사유. 컬렉션2 검색용 텍스트에 포함(로직 §4-2) | 피드백 |
| hitl_feedback.edited_text | string/null |  | 수정후승인 시 셀러가 고친 최종 문구. 대기·승인·반려 시 null. 컬렉션2 정예시 저장의 정본 소스(원안 proposed_text 대신 이 값 우선) | 피드백·컬렉션2 |

## 4. 설계 원칙

1. **alert_id 사슬** — 문의(inquiry_id) → 알림(alert_id) → 개선안(recommendation_id)이 ID로 연결돼 "이 개선안이 어떤 문의에서 왔나"를 끝까지 추적 가능 (데모에서 클릭 내비게이션의 근간)
2. **citations는 evidence의 부분집합** — 탐지 결과가 준 inquiry_ids 밖의 인용은 Evaluator가 grounding 실패로 기각 (환각 방지의 구조적 장치)
3. **두 확신도의 분리** — detection_confidence(입력)와 recommendation_confidence(출력)는 다른 값. 캡핑 규칙: 탐지 확신도가 낮으면 개선안 확신도가 그 이상으로 표시될 수 없음 (캡핑 상세는 §3 capped_by_detection 참조)
4. **hitl_status의 소유권 분리** — Agent는 "대기"로만 생성(hitl_feedback=null), 이후 상태 변경은 백엔드(HITL 처리)가 담당. **반려 시 hitl_feedback.rejection_reason.reason_code 필수** — 사유 없는 반려는 컬렉션2 학습에 못 쓰임(팀 코멘트 반영). 승인→RAG 정예시, 반려→부예시(사유 포함) 저장으로 이어짐. **수정후승인 시** hitl_feedback.edited_text에 최종 문구를 담고, 컬렉션2 정예시는 proposed_text가 아닌 이 값으로 저장(편집 여부 로깅 = edited_text≠null)

## 5. 경계 케이스 규칙

| 상황 | 처리 |
| --- | --- |
| Evaluator 최종 실패 (3회 시도 소진) | proposal=null로 발행, evaluator.passed=false + failure_reason 기록, recommendation_confidence=null. HITL 화면엔 "개선안 생성 실패 — 원인·알림만 표시". **기각 없음 원칙 유지** |
| recommended_action ≠ "개선안 생성" (트리거 미충족) | Agent3 미호출 — JSON 미생성. **트리거는 recommended_action=="개선안 생성" 단일 조건**(scope_in=false·전역/잠정전역·구분불가·원인 미특정 모두 제외) |
| 갱신 알림 (updates_alert_id 있음) | 새 개선안 생성하되 이전 recommendation의 hitl_status를 참조 — 이전 건이 "대기"면 백엔드가 만료 처리(soft-delete — hitl_status 값 아님) 후 신규 발행 |
| detection_confidence=낮음 (원인 미특정) | **해소(트리거로 자동 제외)**: 원인 미특정 → recommended_action="채널 운영 요소 점검 권장"이라 개선안 생성 트리거에 미해당 → Agent3 미호출. 별도 분기 불필요 |
| 이미지 가이드 (image_guide) | proposed_text에 촬영·보정 지침 서술 (예: "자연광, 보정 최소화 재촬영 권장"). current_text는 CS 문의에서 드러난 현재 이미지 문제 요약(이미지 미참조) |

## 6. 도구 선택 규칙 & 스코프 한계

**proposal.type 선택** — root_cause.label 기준(로직 §5-2 라벨표). Agent3는 색상·사이즈·소재 3개 aspect만 처리하며, 원인이 "텍스트로 고쳐지는 문제냐, 사진 문제냐"로 도구를 정한다.

| proposal.type | 선택되는 원인 라벨(root_cause.label) | 이유 |
| --- | --- | --- |
| **copy_draft** (문구 초안) | 표기_오타 / 실측_표기_편차 / 채널_사이즈_표준차이 / 소재_정보_누락 | 상세설명 텍스트를 고치면 해결되는 원인 |
| **image_guide** (촬영·보정 가이드) | 사진_색감_오차 / 조명_보정_차이 / 이미지_질감표현_부족 | 문구로는 못 고치고 사진을 다시 찍어야 하는 원인 |
- **detailpage_grounded=false**(상세페이지 텍스트 근거 없음) → **image_guide 강제** + 확신도 하향
- **root_cause.label="기타"(consistent=true)** → 일반 가이드(도구는 해당 aspect 성격을 따름) + 확신도 상한 중간
- **스코프 한계 케이스(실물_염색_편차·실제_원단_문제)** → image_guide 강제 예외: detailpage_grounded 값과 무관하게 proposal.type=copy_draft로 "상품/공급 확인 권장" 일반 가이드 문구 출력 + recommendation_confidence=낮음 고정

**스코프 한계 (의도적 스코프 밖)** — 편중형인데 원인이 실제 상품 문제(실물_염색_편차·실제_원단_문제)로 잡히는 드문 경우, 문구·사진 어느 도구로도 해결되지 않는다. 이때는 recommendation_confidence를 "낮음"으로 두고 일반 가이드 + "상품/공급 확인 권장" 성격 문구로 처리하며, 상세페이지 자동 개선 범위 밖으로 명시한다. (향후 과제: 상품 품질 이슈를 탐지 단계에서 별도 라우팅 분리 — 현 스코프에선 전역형만 "상품 자체 점검 권장"으로 걸러짐.)

## 7. 변경 내역

| 날짜 | 변경 내용 | 합의자 |
| --- | --- | --- |
| 2026-07-16 | 최초 초안 (데이터 트랙 제안) | 유지인 (Agent3 담당 공동 확정 대기) |
| 2026-07-22 | 탐지 스키마(확정) 정합 개정 — 트리거 단일화(recommended_action)·캡핑 규칙·Evaluator 3기준을 §3·§5 본문에 반영, §5 경계표 2행 해소, JSON 값 빠른이해 추가, 확신도 null 허용 | 유지인 (지인 검토 대기) |
| 2026-07-22 | 도구 선택 규칙(§6) 확정 + 필드 3개 추가(detailpage_grounded·similar_case·confidence_reason) + JSON 예시 반영, 스코프 한계 명시. 팀 컨펌 대기 | 유지인 |
| 2026-07-22 | 확정 이상탐지 대조 검수 반영 — hitl_feedback.rejection_reason 필드 신설(반려 사유 필수, 팀 코멘트 반영), JSON 구문오류 수정 | 유지인 |
| 2026-07-23 | 로직 문서 정합 확정 — §6 copy_draft에 채널_사이즈_표준차이 추가, 스코프 한계를 image_guide 강제 예외로 명시, hitl_feedback.edited_text 필드 신설(수정후승인 정본·컬렉션2 정예시), 로직 상호참조 갱신(§5-2·§4-1). [미확정]→[확정] | 유지인 |
| 2026-07-23 | 로직 §4 번호 밀림 반영 — 컬렉션2 스키마 참조 §4-1→§4-2(reason_text) | 유지인 |
| 2026-07-23 | 냉철 검수 반영 — §2 JSON 예시를 규칙 정합본으로 교체(색감→image_guide, detailpage_grounded=false, 유사사례 있음→중간), §5 갱신 만료 처리 명확화(soft-delete), §3 detailpage_grounded 스코프 예외 주석, §6 "기타" 도구규칙 aspect 반영, §5 Evaluator 실패 시 recommendation_confidence=null 명시, §2 예시에 edited_text=null 필드 보강 | 유지인 |
| 2026-07-23 | image_guide current_text 소스 확정(CS 문의 기반, 이미지·비전 미참조) — §3·§5 정정. grounding 소스 도구별 분리는 개선안 로직 §4-3 참조 | 유지인 |

---