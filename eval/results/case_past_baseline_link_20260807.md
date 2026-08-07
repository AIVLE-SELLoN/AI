# Case-past와 baseline 희석 연결 감사, 2026-08-07

## 요약

- TRUE config 행: 33
- case-past 관측률이 config `past_neg/past_total`과 일치: 33/33
- config `past_rate`가 `BASELINE_RATE`와 일치: 33/33
- 평균 case-past 관측률: 5.03%
- 평균 config past_rate: 5.03%
- 평균 순수 배경률: 0.85%
- 평균 observed/config 비율: 1.00x
- 평균 config/순수배경 비율: 6.04x

## 해석

TRUE case-past 구간은 config 명세대로 생성되어 있다. 단차의 직접 원인은 case-past를 특별 이상 구간으로 잘못 심은 것이 아니라, 일반 배경이 `BASELINE_RATE`보다 낮게 희석되어 있다는 점이다.

따라서 기존 보고서의 가설 4(순수 배경 baseline 희석)와 가설 6(case-past 조기 발화)은 별도 원인이라기보다 같은 생성기 baseline 해석 문제의 두 현상으로 보는 것이 더 정확하다.

## TRUE case-past 샘플

| Case | Product | Source | Channel | Aspect | Case-past | Config | Pure background | Config/Pure |
|---|---|---|---|---|---:|---:|---:|---:|
| SC-001 | P019 | cs | COUPANG | 색상 | 5.00% | 5.00% | 0.90% | 5.54x |
| SC-002 | P009 | cs | NAVER | 색상 | 6.00% | 6.00% | 1.04% | 5.76x |
| SC-003 | P003 | cs | ZIGZAG | 색상 | 7.00% | 7.00% | 0.97% | 7.2x |
| SC-004 | P004 | cs | NAVER | 사이즈 | 9.00% | 9.00% | 1.67% | 5.4x |

## Case-past 시작일 분포

- 전체 config 행: 123
- ws-28 in 23/24/26: 24/123
- 시작일별: {23: 12, 24: 3, 26: 9}
- 해당 행 source 구성: {'cs': 21, 'review': 3}
- 해당 행 aspect 구성: {'파손': 3, '색상': 21}
- 해당 행 intended 구성: {'TRUE': 6, 'FALSE': 11, 'blank': 7}

## scoring 제외 config 행

- 로컬 config 기준 scoring 제외 또는 intended blank 행: 21
- 그중 config 설계 상승폭이 3%p 이상인 행: 12

## ignored 알림 6건

| Day | Product | Source | Aspect | Channel | Current | Past | Delta | Matched config |
|---:|---|---|---|---|---:|---:|---:|---|
| 51 | P027 | cs | 색상 | COUPANG | 11.50% | 5.00% | 6.50% | SC-026 COUPANG 45-51 config_delta=6.50% intended=blank |
| 51 | P028 | cs | 사이즈 | ALL | 16.00% | 9.00% | 7.00% | SC-027 NAVER 46-52 config_delta=9.00% intended=blank / SC-027 ZIGZAG 46-52 config_delta=7.50% intended=blank |
| 52 | P029 | cs | 소재 | ZIGZAG | 13.00% | 6.00% | 7.00% | SC-028 ZIGZAG 46-52 config_delta=7.00% intended=blank |
| 55 | P041 | cs | 색상 | COUPANG | 11.00% | 4.52% | 6.48% | SC-037 COUPANG 51-57 config_delta=8.00% intended=blank |
| 56 | P042 | cs | 색상 | COUPANG | 11.50% | 5.01% | 6.49% | SC-038 COUPANG 51-57 config_delta=7.50% intended=blank |
| 57 | P042 | cs | 색상 | ALL | 12.50% | 5.05% | 7.45% | SC-038 COUPANG 51-57 config_delta=7.50% intended=blank / SC-038 NAVER 51-57 config_delta=8.00% intended=blank |

CSV: `eval\results\case_past_baseline_link_20260807.csv`
