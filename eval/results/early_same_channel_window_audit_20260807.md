# 조기 same-channel 헛알림 window 감사, 2026-08-07

범위: 남은 false 중 `early_same_channel`으로 분류된 30개 알림.

## 요약

- 감사 대상 row: 30
- 탐지기의 현재 window가 generated case-past 구간과 겹친 row: 30/30
- 현재 window 안 평균 overlap: 6.60일
- 평균 detector current rate: 7.93%
- 평균 pre-case-past rate: 0.62%
- 평균 generated case-past full rate: 5.32%
- 평균 TRUE-window rate: 12.31%

## 해석

조기 알림의 주원인은 분류기 오류가 아니다. 미래 TRUE 케이스의 공식 TRUE window 시작 전 28일짜리 `past_neg` 구간이 daily simulation에서 탐지기의 현재 7일 window 안으로 들어온다.

추가 감사 결과 TRUE case-past는 config `past_rate` 및 `BASELINE_RATE`와 33/33 일치했다. 즉 case-past 자체가 비정상적으로 높은 것이 아니라, 더 이전의 순수 배경이 `BASELINE_RATE`보다 약 1/6로 희석되어 있다.

그 결과 탐지기는 `희석된 배경 -> config baseline 수준 case-past` 경계 단차를 이상 상승으로 보고 발화한다. 이는 mock 생성기의 baseline 해석 문제다. threshold 튜닝으로 먼저 해결하면 안 된다.

## 예시 row

| Day | Product | Aspect | Channel | TRUE | Case-past | Current | Overlap | Current | Detector past | Pre-case-past |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 29 | P001 | 색상 | COUPANG | 54-60 | 26-53 | 23-29 | 4d | 4.04% | 0.65% | 0.57% |
| 29 | P017 | 색상 | COUPANG | 54-60 | 26-53 | 23-29 | 4d | 3.96% | 0.34% | 0.29% |
| 29 | P017 | 색상 | NAVER | 54-60 | 26-53 | 23-29 | 4d | 4.00% | 0.64% | 0.57% |
| 29 | P030 | 색상 | COUPANG | 47-53 | 19-46 | 23-29 | 7d | 6.90% | 2.42% | 1.39% |
| 29 | P030 | 색상 | ZIGZAG | 47-53 | 19-46 | 23-29 | 7d | 16.67% | 3.03% | 0.00% |
| 29 | P031 | 색상 | COUPANG | 48-54 | 20-47 | 23-29 | 7d | 6.90% | 1.63% | 0.76% |
| 29 | P032 | 색상 | COUPANG | 49-55 | 21-48 | 23-29 | 7d | 6.90% | 1.77% | 1.24% |
| 29 | P033 | 색상 | COUPANG | 49-55 | 21-48 | 23-29 | 7d | 6.90% | 1.46% | 0.89% |
| 29 | P034 | 색상 | COUPANG | 50-56 | 22-49 | 23-29 | 7d | 6.90% | 0.65% | 0.34% |
| 29 | P035 | 색상 | COUPANG | 51-57 | 23-50 | 23-29 | 7d | 10.00% | 1.33% | 1.33% |
| 30 | P001 | 색상 | ZIGZAG | 54-60 | 26-53 | 24-30 | 5d | 11.91% | 0.73% | 0.67% |
| 30 | P031 | 색상 | COUPANG | 48-54 | 20-47 | 24-30 | 7d | 6.90% | 1.87% | 0.76% |

CSV: `eval\results\early_same_channel_window_audit_20260807.csv`
