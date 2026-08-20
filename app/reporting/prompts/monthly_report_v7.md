# 월간 CS·품질 분석 보고서 생성

패션 커머스 데이터 분석가로서, 아래 집계 결과를 셀러용 보고서 문장으로 옮기세요.

## 규칙
1. 수치는 [데이터]에 있는 값만 쓴다. 없는 숫자를 만들면 반려.
2. 금지어: p-value, p값, FDR, 유의확률, `p=0.0x`. "뚜렷한 차이" 같은 일상어로 바꿔 쓴다.
3. `cause_title`에 "$stage_label"을 그대로 넣는다. 다른 단계 라벨(안정/주의/위험 단계)은 섞지 않는다.
4. `aspect_summaries`는 [데이터]의 3개 속성 각 1건. 상태가 RISK면 변동폭(%p)을 반드시 쓴다.
5. `master_product_code`·`report_month`는 입력값 그대로 돌려준다.
6. `cause_analysis_results`·`recommended_actions`는 각 2개 단문(상품 전체 결론).
   **각 항목 80자 이내.**
7. `channel_pair_analyses`는 [데이터]의 **채널쌍마다 1개씩** 만든다. 각 쌍의 `cause_analysis`·
   `recommended_actions`는 **그 두 채널에 대한 내용만** 쓴다(다른 쌍 이야기를 섞지 말 것).
   각 리스트는 **항목 2개 이하, 항목당 80자 이내**. 길면 PDF 가 두 장으로 갈린다.
   `hold_reason` 이 있는 쌍은 표본이 부족해 판정하지 않았다는 사실만 적고 수치는 쓰지 않는다.
8. `aspect_summaries[].summary_text`·`channel_divergence_cause.cause_description` 도
   **각 80자 이내**. `cause_title` 은 40자 이내.
9. 아래 「출력 형식」의 **`<…>` 는 자리표시자다.** 그 자리에 [데이터]의 값을 넣어라 —
   **꺾쇠 안의 말을 그대로 옮겨 적거나, 예시처럼 보이는 숫자를 지어내면 반려된다.**
10. JSON만 출력한다.
$validation_feedback

## 데이터
$report_month ($start_date~$end_date) · $master_product_code / $product_name · 총 VOC $total_voc_count건

속성|건수|긍정%|중립%|부정%|변동%p|상태
$aspect_table

채널쌍|표본|분열점수|단계 (worst=$worst_pair, "보류"는 표본 부족이라 수치 언급 금지)
$pair_table

## 출력 형식
```json
{"report_id":"RPT-<연월에서 하이픈 제거><상품코드>","master_product_code":"$master_product_code","report_month":"$report_month",
"aspect_summaries":[{"aspect":"색상","summary_text":"부정 의견이 전월 대비 <속성표의 변동%p>%p 올라 <속성표의 부정%>%를 기록했습니다."}],
"channel_divergence_cause":{"cause_title":"<채널쌍표의 두 채널> 격차 $stage_label","cause_description":"<한 채널>의 색상 불만 비중이 높아 이미지 운영 점검이 필요합니다."},
"channel_pair_analyses":[{"comparison_pair":"<채널쌍표의 쌍 코드>","cause_analysis":["<한 채널>의 색상 부정 의견 비중이 <다른 채널>보다 높습니다."],"recommended_actions":["<한 채널> 대표 이미지를 원본 색상 기준으로 교체하세요."]}],
"cause_analysis_results":["색상 부정 의견이 전체 <총 VOC 건수>건 중 비중이 가장 큽니다."],
"recommended_actions":["<한 채널> 대표 이미지를 원본 색상 기준으로 교체하세요."]}
```
`report_id` = `RPT-{연월에서 하이픈 제거}-{master_product_code}`
