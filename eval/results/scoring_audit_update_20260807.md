# 채점 및 baseline 감사 업데이트, 2026-08-07

## 범위

mock data는 재생성하지 않았다.

이번 업데이트는 사전에 채점 제외로 정의된 시나리오 window를 헛알림으로 세지 않도록 평가·감사 스크립트만 수정한 것이다.

수정한 스크립트:

- `scripts/detection_experiments/demo_sim.py`
- `scripts/detection_experiments/audit_demo_false_alerts.py`
- `scripts/detection_experiments/audit_mock_baselines.py`

## 데모 채점 정정

대상: `product x source` family + real classification + demo publishing flow.

이전 raw 결과:

| 지표 | 이전 값 |
|---|---:|
| 발행 알림 | 75 |
| true 알림 | 27 |
| false 알림 | 48 |
| 헛알림률 | 64.0% |
| 케이스 도달률 | 20/26 |

채점 제외 window를 분리한 뒤:

| 지표 | 정정 후 |
|---|---:|
| 발행 알림 | 75 |
| 채점 대상 알림 | 69 |
| true 알림 | 27 |
| ignored 알림 | 6 |
| false 알림 | 42 |
| 헛알림률 | 42 / 69 = 60.9% |
| 케이스 도달률 | 20/26 |

해석:

- 이는 채점 정정이지 모델 성능 개선이 아니다.
- 현재 demo simulation에서 셀러가 받는 알림은 여전히 75개다.
- 공식 헛알림률을 계산할 때는 사전에 채점 제외로 정의된 6개 알림을 false 분자/분모에서 제외하는 것이 맞다.

ignored 알림:

| Day | Product | Source | Aspect | Channel |
|---:|---:|---:|---:|---:|
| 51 | P027 | cs | 색상 | COUPANG |
| 51 | P028 | cs | 사이즈 | ALL |
| 52 | P029 | cs | 소재 | ZIGZAG |
| 55 | P041 | cs | 색상 | COUPANG |
| 56 | P042 | cs | 색상 | COUPANG |
| 57 | P042 | cs | 색상 | ALL |

## 정정 후 false-like 알림 분해

기존에 false처럼 집계되던 48개 알림 중:

| 구분 | 개수 |
|---|---:|
| false | 42 |
| ignored | 6 |

사유 힌트:

| 사유 | 개수 |
|---|---:|
| early_same_case | 30 |
| no_golden_case_same_product_aspect_source | 12 |
| scoring_excluded_window | 6 |

남은 false 42개는 추가 분석이 필요하다. 가장 큰 미해결 덩어리는 채점 제외 window가 아니라 `early_same_case` 30개다.

## baseline 감사 정정

baseline audit을 slice와 label mode 기준으로 다시 분리했다.

중요한 정정:

- 이전 2-3% baseline 수치는 분류기 결과가 audit에 섞여서 나온 것이 아니었다.
- 기존 audit도 golden label을 읽고 있었다.
- 차이는 slice 정의 때문이었다. 기존 slice는 순수 배경만 본 것이 아니라 다른 구간도 섞고 있었다.

순수 배경 상품만, golden label 기준:

| Channel | Aspect | 관측값 | Config |
|---|---:|---:|---:|
| COUPANG | 색상 | 0.903% | 5.000% |
| NAVER | 색상 | 1.042% | 6.000% |
| ZIGZAG | 색상 | 0.972% | 7.000% |
| NAVER | 사이즈 | 1.667% | 9.000% |
| ZIGZAG | 소재 | 0.903% | 6.000% |

순수 배경 CS 색상/사이즈/소재 중 config가 95% CI 밖인 행: 9/9.

이는 생성기 희석 문제를 확인한다. 순수 배경은 전체 분모 기준 config baseline이 아니라 `config / aspect_count`에 가깝다.

순수 배경에서 classifier overlay:

- 순수 배경 구간의 cache coverage는 0.000%다.
- 따라서 pure-background overlay delta가 0인 이유는 real-classification cache가 해당 row들을 덮지 않았기 때문이다.
- 이 결과만으로 분류기 영향이 없다고 결론내리면 안 된다.

## 이 결과가 증명하지 않는 것

이 업데이트는 mock data 재생성이 헛알림을 낮춘다는 것을 증명하지 않는다.

오히려 baseline 희석을 고치면 헛알림이 늘 수도 있다. `MIN_DELTA`가 절대값 3%p 관문이기 때문에, 정상 baseline이 올라가면 상대적으로 더 작은 배율 상승만으로도 발화할 수 있다.

따라서:

- 생성기 정정을 성능 개선으로 발표하면 안 된다.
- 아직 threshold, persistence, gating을 튜닝하면 안 된다.
- 팀 합의 없이 `data/input`, `data/golden`을 덮어쓰면 안 된다.

## 다음 작업

남은 false 42개를 추가로 조사한다.

- `early_same_case` 30개
- `no_golden_case_same_product_aspect_source` 12개

다음 audit에서는 이 알림들이 아래 중 무엇인지 구분해야 한다.

- 실제로 관련 있는 조기 신호
- 누락되었거나 너무 좁게 잡힌 golden window
- cache가 덮인 구간에서 분류기가 만든 추가 부정
- 탐지기 자체의 과민 반응
