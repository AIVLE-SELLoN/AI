# 남은 헛알림 분해, 2026-08-07

범위: 채점 제외 알림을 `ignored`로 분리한 뒤, `product x source` family + real classification 기준으로 남은 false 알림.

- real classification 발행 알림 수: 75
- oracle/golden 발행 알림 수: 80
- 분석 대상 남은 false 알림 수: 42

## 요약

- 추정 원인 구분: {'detector_or_data_definition': 41, 'real_classifier_or_real_only_path': 1}
- TRUE 관계 구분: {'early_same_channel': 30, 'no_true_same_product_aspect_source': 12}
- oracle 매칭 여부: {'oracle_match': 41, 'real_only': 1}
- false가 많이 나온 날짜: {29: 16, 32: 7, 36: 4, 31: 3, 37: 3, 30: 2, 39: 2, 53: 2, 43: 1, 44: 1}
- false가 많이 나온 상품: {'P001': 6, 'P017': 6, 'P032': 4, 'P035': 4, 'P033': 3, 'P034': 3, 'P036': 3, 'P011': 3, 'P030': 2, 'P031': 2}
- `early_same_channel` 알림은 TRUE 시작일보다 평균 18.60일 먼저 발생

## 해석

- `detector_or_data_definition`: oracle/golden 실행에서도 같은 알림이 발생했다. real classification만의 문제라고 보기 어렵다.
- `real_classifier_or_real_only_path`: 같은 날 또는 인접일에 oracle 매칭 알림이 없다. 분류기/cache 영향 또는 real 경로의 억제 차이가 원인일 수 있다.
- `early_same_channel`: 같은 상품/aspect/source/channel에 미래 TRUE window가 있다. 조기 신호일 수도 있고, golden window가 늦게 잡혔거나 detector가 민감한 것일 수도 있다.
- 이 audit은 threshold를 바꾸지 않았고, mock data도 재생성하지 않았다.

## 결론

남은 false 42개 중 41개가 oracle/golden에서도 발생한다.

따라서 현재 병목은 프롬프트/분류기보다 mock 생성기의 baseline 해석과 그로 인해 생긴 case-past 경계 단차 쪽에 있다.

## 2026-08-07 추가 분해

`no_true_same_product_aspect_source` 12건도 추가 감사했다.

- 10/12건은 관련 config의 case-past 구간과 현재 window가 겹쳤다.
- 6건은 미래 채점 제외 same PAS case-past 영향이었다.
- 2건은 미래 scored FALSE same PAS case-past 영향이었다.
- 2건은 같은 상품의 다른 aspect case-past 영향이었다.
- 2건은 단순 config overlap으로는 아직 설명되지 않았다.

상세 내용은 `eval/results/no_true_false_breakdown_20260807.md`를 기준으로 한다.

CSV: `eval\results\remaining_false_breakdown_20260807.csv`
