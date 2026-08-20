# CS 대응 가이드라인 생성

패션 커머스 CS 오퍼레이션 리드로서, 아래 이상 탐지 결과와 고객 원문을 상담원용 대응 가이드로 만드세요.

## 원문 출처 — 문의와 리뷰는 응대 방식이 다릅니다
[원문] 표의 `출처`가 `문의`인지 `리뷰`인지에 따라 갈립니다. 섞여 있으면 각각 그 규칙을 적용하세요.

| | 문의 | 리뷰 |
| --- | --- | --- |
| 성격 | 고객이 답변을 요청한 1:1 문의 | 고객이 남긴 공개 후기. 셀러가 **답글**을 답니다 |
| 읽는 사람 | 그 고객 한 명 | 그 고객 + **구매를 검토 중인 다른 고객들** |
| 조치 | 교환·반품을 **그 자리에서 접수** | 답글로는 접수가 안 됩니다. **고객센터·1:1 문의로 안내** |

## 규칙
1. `inquiry_specific_guides[].item_id`는 [원문] 표에 있는 ID만 쓴다. 없는 ID를 만들면 반려.
2. `key_metric_text`·`root_cause_summary`의 수치는 [지표]·[원인]에 있는 값만 쓴다.
3. 금지어: p-value, p값, FDR, 유의확률, `p=0.0x`.
4. `root_cause_summary`에 [원인] 문구를 그대로 포함한다.
5. `draft_reply`는 존댓말로 사과 → 원인 설명 → 조치 순. 고객 과실을 암시하지 않는다.
   - 문의가 하나라도 있으면 **문의 답변**으로 쓴다(즉시 조치: 무상 교환·반품 접수).
   - **리뷰만 있으면 리뷰 답글**로 쓴다. 지키지 못할 약속을 하지 않는다 —
     답글로는 반품·교환을 접수할 수 없으므로 "고객센터로 연락 주시면 도와드리겠습니다" 처럼 안내한다.
     다른 구매 검토 고객도 읽으므로 개선 조치를 함께 밝힌다.
6. `key_talking_points`는 2~3개, `inquiry_specific_guides`는 원문별 1건씩.
   - 리뷰 항목의 `recommended_point`는 **답글에서 할 일**을 쓴다(공개 답글 톤·고객센터 유도).
   - 문의 항목은 **응대에서 할 일**을 쓴다(접수·회수 등 즉시 조치).
7. 아래 「출력 형식」의 **`<…>` 는 자리표시자다.** 그 자리에 [지표]·[원인]·[원문]의 값을 넣어라 —
   **꺾쇠 안의 말을 그대로 옮겨 적거나, 예시처럼 보이는 숫자·ID 를 지어내면 반려된다.**
   특히 `item_id` 는 반드시 [원문] 표에서 골라야 한다. 지어낸 ID 가 우연히 다른 고객의
   실제 문의와 겹치면 **엉뚱한 고객의 문의에 이 가이드가 붙는다.**
8. JSON만 출력한다.
$validation_feedback

## 데이터
가이드라인 ID $guideline_id · 알림 $alert_id · $product_group_id / $product_name · $channel · $main_aspect · $verdict · 확신도 $detection_confidence · 권장조치 $recommended_action
[지표] $stats_summary
[원인] $root_cause_summary

[원문] ID|출처|내용
$inquiry_table

## 출력 형식
```json
{"guideline_id":"$guideline_id","alert_id":"$alert_id",
"summary":{"issue_title":"<채널> <속성> 불만 급증 대응","risk_level":"WARNING","key_metric_text":"<속성> 부정 비율이 <지표의 과거 비율>에서 <지표의 현재 비율>로 <지표의 변동폭> 올랐습니다 (문의 <지표의 표본 수>건)."},
"root_cause_summary":"<원인 문구를 그대로>",
"standard_guideline":{"core_message":"촬영 조명 차이로 색상이 다르게 보일 수 있음을 안내하고 무상 교환을 접수합니다.","draft_reply":"안녕하세요 고객님, 색상 차이로 불편을 드려 죄송합니다. 확인 후 무상 반품·교환을 도와드리겠습니다.","key_talking_points":["조명 차이 정중히 안내","고객 과실 암시 표현 금지"]},
"ops_action_guide":"<채널> 대표 이미지의 색보정 상태를 점검하고 원본 기준으로 재등록하세요.",
"inquiry_specific_guides":[{"item_id":"<원문 표의 문의 ID>","recommended_point":"사과 후 무상 회수 접수를 안내하세요."},{"item_id":"<원문 표의 리뷰 ID>","recommended_point":"공개 답글로 조명 차이를 정중히 설명하고, 교환은 고객센터로 안내하세요."}]}
```
`guideline_id` 와 `alert_id` 는 위에 주어진 값을 **그대로** 옮겨 적으세요(직접 만들지 마세요).
`risk_level` 은 CRITICAL / WARNING / NORMAL 중 하나입니다.
