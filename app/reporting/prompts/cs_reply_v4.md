# CS 대응 가이드라인 생성

패션 커머스 CS 오퍼레이션 리드로서, 아래 이상 탐지 결과와 고객 문의를 상담원용 대응 가이드로 만드세요.

## 규칙
1. `inquiry_specific_guides[].item_id`는 [문의] 표에 있는 ID만 쓴다. 없는 ID를 만들면 반려.
2. `key_metric_text`·`root_cause_summary`의 수치는 [지표]·[원인]에 있는 값만 쓴다.
3. 금지어: p-value, p값, FDR, 유의확률, `p=0.0x`.
4. `root_cause_summary`에 [원인] 문구를 그대로 포함한다.
5. `draft_reply`는 존댓말로 사과 → 원인 설명 → 즉시 조치(무상 교환·반품) 순. 고객 과실을 암시하지 않는다.
6. `key_talking_points`는 2~3개, `inquiry_specific_guides`는 문의별 1건씩.
7. JSON만 출력한다.
$validation_feedback

## 데이터
가이드라인 ID $guideline_id · 알림 $alert_id · $product_group_id / $product_name · $channel · $main_aspect · $verdict · 확신도 $detection_confidence · 권장조치 $recommended_action
[지표] $stats_summary
[원인] $root_cause_summary

[문의] 문의ID|원문
$inquiry_table

## 출력 형식
```json
{"guideline_id":"$guideline_id","alert_id":"$alert_id",
"summary":{"issue_title":"쿠팡 색상 불만 급증 대응","risk_level":"WARNING","key_metric_text":"색상 부정 비율이 5%에서 13%로 8%p 올랐습니다 (문의 200건)."},
"root_cause_summary":"사진_색감_오차 18건 / 전체 26건 (69%)",
"standard_guideline":{"core_message":"촬영 조명 차이로 색상이 다르게 보일 수 있음을 안내하고 무상 교환을 접수합니다.","draft_reply":"안녕하세요 고객님, 색상 차이로 불편을 드려 죄송합니다. 확인 후 무상 반품·교환을 도와드리겠습니다.","key_talking_points":["조명 차이 정중히 안내","고객 과실 암시 표현 금지"]},
"ops_action_guide":"쿠팡 대표 이미지의 색보정 상태를 점검하고 원본 기준으로 재등록하세요.",
"inquiry_specific_guides":[{"item_id":"INQ-000001","recommended_point":"사과 후 무상 회수 접수를 안내하세요."}]}
```
`guideline_id` 와 `alert_id` 는 위에 주어진 값을 **그대로** 옮겨 적으세요(직접 만들지 마세요).
`risk_level` 은 CRITICAL / WARNING / NORMAL 중 하나입니다.
