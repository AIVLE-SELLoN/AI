# 데모 헛알림 감사, 2026-08-06

범위: `product x source` family + real classification + raw demo publishing.

- 전체 문서 수: 128,228
- classification cache row: 15,228
- 실제 분류 결과로 교체한 row: 15,228
- false/ignored로 보이는 발행 알림 감사 대상: 48

## 요약

- kind별: {'false': 42, 'ignored': 6}
- reason hint별: {'early_same_case': 30, 'no_golden_case_same_product_aspect_source': 12, 'scoring_excluded_window': 6}
- source별: {'cs': 44, 'review': 4}
- aspect별: {'색상': 41, '사이즈': 3, '파손': 2, '소재': 2}
- channel별: {'COUPANG': 19, 'NAVER': 11, 'ZIGZAG': 9, 'ALL': 9}
- 상위 product: {'P001': 6, 'P017': 6, 'P032': 4, 'P035': 4, 'P042': 4, 'P033': 3, 'P034': 3, 'P036': 3, 'P011': 3, 'P030': 2}
- false가 많이 나온 날짜: {29: 16, 32: 7, 36: 4, 31: 3, 37: 3, 30: 2, 39: 2, 51: 2, 53: 2, 43: 1}

## CS 색상 집중 현상

- CS 색상 false-like alert: 39/48
- 평균 current rate: 0.1051
- 평균 past rate: 0.0167
- 평균 configured baseline rate: 0.0456

## 해석 메모

- `ignored`는 `scoring_included=N`이거나 `intended_answer`가 빈칸인 시나리오 window와 겹치는 알림이다. 공식 헛알림 분자/분모에서 제외하는 것이 맞다.
- `past_rate`가 `baseline_rate`보다 훨씬 낮다는 사실만으로 헛알림 원인을 단정하면 안 된다. 생성기 희석 때문에 순수 배경 대부분이 이 조건을 만족할 수 있다.
- `cur_rate`가 baseline에 가깝고 `delta`만 큰 경우에는 mock background 또는 시간축 설계를 추가로 의심해야 한다.
- `no_golden_case_same_product_aspect_source`는 현재 golden anomaly 정의 밖에 있는 알림이다. 진짜 헛알림일 수도 있고, golden case 누락일 수도 있으므로 별도 검토가 필요하다.

## 2026-08-07 이후 정정

이 문서의 48개 중 6개는 `ignored`로 분리되었다. 따라서 채점 정정 후 공식 false는 42개다.

자세한 분해는 아래 문서를 기준으로 한다.

- `eval/results/scoring_audit_update_20260807.md`
- `eval/results/remaining_false_breakdown_20260807.md`
- `eval/results/early_same_channel_window_audit_20260807.md`

CSV: `eval\results\demo_false_alert_audit_20260806.csv`

