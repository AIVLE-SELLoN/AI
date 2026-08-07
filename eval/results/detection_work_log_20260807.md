# 이상탐지 작업 로그, 2026-08-07

## 이번 작업에서 바꾼 것

공식 mock data는 재생성하지 않았다.

탐지 임계값, 프롬프트, 알림 발행 정책도 튜닝하지 않았다.

지금까지 한 작업의 성격은 성능 개선이 아니라 평가·감사 기준 정정이다.

- 채점 제외로 설계된 시나리오 window는 `false`가 아니라 `ignored`로 분리했다.
- 남은 헛알림을 oracle/golden 기준과 real classification 기준으로 비교했다.
- 조기 헛알림이 미래 TRUE 케이스의 `case-past` 구간과 겹치는지 확인했다.
- baseline audit을 순수 배경 / 케이스 window / 채점 제외 window / 분류 cache 적용 범위로 분해했다.

## 현재 정직하게 말할 수 있는 데모 지표

범위: `product x source` family + real classification + demo publishing.

| 지표 | 값 |
|---|---:|
| 발행 알림 | 75 |
| 채점 대상 알림 | 69 |
| true 알림 | 27 |
| ignored 알림 | 6 |
| false 알림 | 42 |
| 헛알림률 | 60.9% |
| 케이스 도달률 | 20/26 |

해석: `60.9%`는 채점 기준을 바로잡은 값이지, 모델 성능을 개선해서 얻은 값이 아니다.

## 주요 발견

### 1. 생성기의 baseline 희석은 확정

순수 배경 CS 부정률이 config의 전체 분모 기준 baseline이 아니라 `config / aspect_count`에 가깝게 생성되고 있다.

예시:

- COUPANG 색상 순수 배경: 0.903%
- config baseline: 5.000%

이는 mock data 생성 정합성 문제다. 추가 감사 결과 case-past는 config baseline대로 생성되어 있고, 순수 배경이 약 1/6로 희석되어 경계 단차가 생긴 것으로 확인됐다. 따라서 baseline 보정 후 헛알림 순감은 유력하지만, 재생성·재측정 전에는 성능 개선 수치로 말하면 안 된다.

### 2. 채점 제외 window가 6개 알림을 설명

기존 false-like 알림 48개 중:

- 42개는 여전히 false
- 6개는 `ignored`

이 6개는 `scoring_included=N`이거나 `intended_answer`가 빈칸인 시나리오 window와 겹친다.

### 3. 남은 false 대부분은 분류기 실패가 아님

남은 false 42개 중:

- 41개는 oracle/golden 실행에서도 같은 날 발생
- 1개만 real classification에서만 발생

따라서 현재 가장 큰 문제는 프롬프트/분류기 오류가 아니라, 탐지 로직이 읽는 데이터 구조와 golden/scoring 정의 쪽이다.

### 4. 가장 큰 원인은 baseline 희석으로 생긴 case-past 경계 단차

`early_same_channel` false 30개는 모두 미래 TRUE 케이스의 28일짜리 `case-past` 구간과 겹친다.

요약:

- 감사 대상 `early_same_channel`: 30개
- 현재 7일 window가 generated case-past와 겹침: 30/30
- 평균 overlap: 6.60일
- 평균 detector current rate: 7.93%
- 평균 pre-case-past rate: 0.62%
- 평균 generated case-past full rate: 5.32%
- 평균 TRUE-window rate: 12.31%

해석: 탐지기가 미래 TRUE 케이스를 만들기 위해 심어둔 과거 28일 구간을 “오늘의 현재 window”로 보고, 그보다 더 이전의 낮은 배경 구간과 비교해서 공식 TRUE window보다 먼저 발화한다.

### 5. TRUE 없는 false 12개도 대부분 config case-past 영향

`no_true_same_product_aspect_source` false 12개를 추가로 분해했다.

요약:

- 분석 대상: 12개
- oracle/golden에서도 같은 날 발생: 11개
- real classification에서만 발생: 1개
- 현재 window가 관련 config의 case-past와 겹침: 10/12

category별:

- 미래 채점 제외 same product/aspect/source case-past: 6개
- 미래 scored FALSE same product/aspect/source case-past: 2개
- 같은 상품의 다른 aspect case-past: 2개
- 단순 config overlap으로 설명 안 됨: 2개

해석: TRUE가 없는 것처럼 보인 12개도 대부분 완전한 랜덤 헛알림이 아니라, configured FALSE / 채점 제외 / 다른 aspect 시나리오의 case-past 구간과 daily current window가 겹치며 생긴다.

남은 2개도 row-level로 확인했다.

- P012 / review / 사이즈 / NAVER / day53: current 8/68 = 11.76%, past28 6/276 = 2.17%
- P022 / cs / 사이즈 / NAVER / day53: current 4/43 = 9.30%, past28 0/197 = 0.00%

두 건 모두 oracle/golden에서도 발생하므로 프롬프트 문제는 아니다. 단순 case-past overlap은 없지만, config window 밖에서 current 7일에 부정 표본이 몰린 배경 변동 또는 hot/gap 생성 영향으로 보인다.

## 산출물 목록

핵심 결과 문서:

- `eval/results/scoring_audit_update_20260807.md`
- `eval/results/remaining_false_breakdown_20260807.md`
- `eval/results/early_same_channel_window_audit_20260807.md`
- `eval/results/no_true_false_breakdown_20260807.md`
- `eval/results/unexplained_false_rows_20260807.md`
- `eval/results/mock_baseline_audit_20260806.md`
- `eval/results/demo_false_alert_audit_20260806.md`
- `eval/results/detection_performance_20260806.md`

상세 CSV:

- `eval/results/remaining_false_breakdown_20260807.csv`
- `eval/results/early_same_channel_window_audit_20260807.csv`
- `eval/results/no_true_false_breakdown_20260807.csv`
- `eval/results/unexplained_false_rows_20260807.csv`
- `eval/results/demo_false_alert_audit_20260806.csv`
- `eval/results/mock_baseline_audit_20260806.csv`

감사 스크립트:

- `scripts/detection_experiments/audit_demo_false_alerts.py`
- `scripts/detection_experiments/audit_mock_baselines.py`
- `scripts/detection_experiments/audit_remaining_false_breakdown.py`
- `scripts/detection_experiments/audit_early_same_channel_windows.py`
- `scripts/detection_experiments/audit_no_true_false_breakdown.py`
- `scripts/detection_experiments/audit_unexplained_false_rows.py`

수정된 데모 채점 스크립트:

- `scripts/detection_experiments/demo_sim.py`

## 다음 의사결정

아직 threshold를 튜닝하면 안 된다.

먼저 `BASELINE_RATE`의 의미를 정해야 한다.

1. `BASELINE_RATE`가 전체 문의 분모인지 aspect 내부 비율인지 확정한다.
2. 전체 문의 분모가 맞다면 generator의 배경 baseline 생성 방식을 보정한다.
3. 보정된 mock data에서 실험 2를 다시 측정한다.
4. 재측정 후에도 남는 헛알림에 대해서만 threshold, persistence, gating, 분류기 bias를 검토한다.

이건 모델 튜닝 문제가 아니라 데이터·채점 정의 결정이다.
