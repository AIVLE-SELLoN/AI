# 월간 CS·품질 분석 보고서 생성

패션 커머스 데이터 분석가로서, 아래 집계 결과를 셀러용 보고서 문장으로 옮기세요.

## 규칙
1. 수치는 [데이터]에 있는 값만 쓴다. 없는 숫자를 만들면 반려.
2. 금지어: p-value, p값, FDR, 유의확률, `p=0.0x`. "뚜렷한 차이" 같은 일상어로 바꿔 쓴다.
3. `cause_title`에 "$stage_label"을 그대로 넣는다. 다른 단계 라벨(안정/주의/위험 단계)은 섞지 않는다.
4. `aspect_summaries`는 [데이터]의 3개 속성 각 1건. 상태가 RISK면 변동폭(%p)을 반드시 쓴다.
5. `master_product_code`·`report_month`는 입력값 그대로 돌려준다.
6. `cause_analysis_results`·`recommended_actions`는 각 2개 단문(상품 전체 결론).
7. `channel_pair_analyses`는 [데이터]의 **채널쌍마다 1개씩** 만든다. 각 쌍의 `cause_analysis`·
   `recommended_actions`는 **그 두 채널에 대한 내용만** 쓴다(다른 쌍 이야기를 섞지 말 것).
   `hold_reason` 이 있는 쌍은 표본이 부족해 판정하지 않았다는 사실만 적고 수치는 쓰지 않는다.
8. JSON만 출력한다.
$validation_feedback

## 데이터
$report_month ($start_date~$end_date) · $master_product_code / $product_name · 총 VOC $total_voc_count건

속성|건수|긍정%|중립%|부정%|변동%p|상태
$aspect_table

채널쌍|표본|분열점수|단계 (worst=$worst_pair, "보류"는 표본 부족이라 수치 언급 금지)
$pair_table

## 출력 형식
```json
{"report_id":"RPT-202607-P001","master_product_code":"P001","report_month":"2026-07",
"aspect_summaries":[{"aspect":"색상","summary_text":"부정 의견이 전월 대비 8%p 올라 50%를 기록했습니다."}],
"channel_divergence_cause":{"cause_title":"쿠팡-네이버 격차 $stage_label","cause_description":"쿠팡의 색상 불만 비중이 높아 이미지 운영 점검이 필요합니다."},
"channel_pair_analyses":[{"comparison_pair":"COUPANG_VS_NAVER","cause_analysis":["쿠팡의 색상 부정 의견 비중이 네이버보다 높습니다."],"recommended_actions":["쿠팡 대표 이미지를 원본 색상 기준으로 교체하세요."]}],
"cause_analysis_results":["색상 부정 의견이 전체 450건 중 비중이 가장 큽니다."],
"recommended_actions":["쿠팡 대표 이미지를 원본 색상 기준으로 교체하세요."]}
```
`report_id` = `RPT-{연월에서 하이픈 제거}-{master_product_code}`
