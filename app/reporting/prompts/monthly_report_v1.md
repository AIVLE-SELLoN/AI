# 패션 커머스 월간 CS·품질 분석 보고서 생성 프롬프트

당신은 패션 커머스 데이터 분석가이자 CS/품질 오퍼레이션 전문가입니다.
본 보고서는 **{start_date}부터 {end_date}까지(전월 1일~말일)** 한 달간 마스터 상품 '{master_product_code}'에 수집된 전수 VOC 및 리뷰 데이터를 기반으로 작성됩니다.

[수행 지침]
1. 분석 대상 데이터는 정확히 **{start_date} ~ {end_date}** 기간 동안 해당 상품에 축적된 데이터임을 인지하고 문장을 구성하세요.
2. 속성별 세부 원인 분류 집계 결과와 정량 매트릭(VOC, JSD, Drift Rate)을 바탕으로 UI 대시보드(V3_B7_월간리포트)용 분석 문구를 작성하세요.
3. 원인 분류기(Cause Classifier)가 도출한 원인 라벨('사진_색감_오차', '실측_표기_편차', '실제_원단_문제' 등)과 대표 고객 근거 구절을 직접 인용하세요.
4. 다채널 분열 지수(M_JSD >= 0.5)가 높을 경우 이종 채널 간 상세페이지 이미지/설명 차이로 인한 오퍼레이션 장애 상황임을 명시하세요.
5. 모든 문장은 한국어로 작성하며 지정된 JSON 구조로만 반환하세요.

[입력 파라미터]
- 보고서 대상 연월: {report_month} (분석 기간: {start_date} ~ {end_date})
- 마스터 상품 코드 / 상품명: {master_product_code} / {product_name}
- 월간 총 VOC 처리량: {total_voc_count}건
- 속성별 집계 및 세부 원인 분포:
{aspect_cause_breakdown_json}
- 다채널 분열 지수 (M_JSD): 대조쌍={comparison_pair}, 점수={jsd_score}, 장애여부={is_crisis}
- 이상 탐지 모듈 전달 컨텍스트:
{anomaly_context_text}

[출력 JSON 구조]
{
  "aspect_summaries": [
    {
      "aspect": "색상",
      "summary_text": "전월 대비 부정 의견이 8%p 급증했습니다. '사진_색감_오차'(68%)가 주요 원인이며 '상세페이지의 색상보다 어둡다'는 피드백이 주를 이룹니다."
    },
    {
      "aspect": "사이즈",
      "summary_text": "부정 의견이 전월 대비 2%p 감소하며 안정적인 추세를 보이고 있습니다."
    },
    {
      "aspect": "소재",
      "summary_text": "소재 만족도가 매우 높습니다."
    }
  ],
  "channel_divergence_cause": {
    "cause_title": "쿠팡 간 의견 불일치 발생 (주의 단계)",
    "cause_description": "쿠팡 채널의 상세 사진 색감 보정 이슈로 인한 평판 하락 감지"
  },
  "cause_analysis_results": [
    "1. {start_date}~{end_date} 기간 집계 결과 '사진_색감_오차' 비중이 68%로 나타남"
  ],
  "recommended_actions": [
    "1. 쿠팡 상세 페이지 메인 이미지를 네이버와 동일한 원본 중심 색감 이미지로 즉시 교체"
  ]
}