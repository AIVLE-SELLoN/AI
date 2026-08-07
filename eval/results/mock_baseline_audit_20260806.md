# Mock baseline 감사, 2026-08-06

이 감사는 순수 생성 배경, 케이스 상품의 configured window 밖 구간, 채점 제외 window, classifier overlay 영향을 분리해서 본다.

탐지 false-alert 결과를 기준으로 baseline을 고르거나 조정하지 않았다.

## Slice 정의

- `pure_background_products`: `config_anomaly.csv`에 한 번도 등장하지 않는 상품.
- `case_products_outside_windows`: 케이스 상품이지만 해당 product/channel/source에 configured window가 없는 날짜.
- `scoring_excluded_windows`: `scoring_included=N`이거나 `intended_answer`가 빈칸이라 채점에서 제외된 configured window.
- `scored_true_windows` / `scored_false_windows`: 비교 진단용 scored window.

## 순수 배경, golden label 기준

| Channel | Aspect | Neg/Total | 관측값 | 95% CI | Config | 차이 | Config in CI |
|---|---:|---:|---:|---:|---:|---:|---:|
| COUPANG | 사이즈 | 17/1440 | 1.181% | 0.738%-1.883% | 8.000% | -6.819% | False |
| COUPANG | 색상 | 13/1440 | 0.903% | 0.528%-1.538% | 5.000% | -4.097% | False |
| COUPANG | 소재 | 7/1440 | 0.486% | 0.236%-1.000% | 4.000% | -3.514% | False |
| NAVER | 사이즈 | 24/1440 | 1.667% | 1.123%-2.468% | 9.000% | -7.333% | False |
| NAVER | 색상 | 15/1440 | 1.042% | 0.632%-1.712% | 6.000% | -4.958% | False |
| NAVER | 소재 | 9/1440 | 0.625% | 0.329%-1.184% | 5.000% | -4.375% | False |
| ZIGZAG | 사이즈 | 21/1440 | 1.458% | 0.956%-2.219% | 7.000% | -5.542% | False |
| ZIGZAG | 색상 | 14/1440 | 0.972% | 0.580%-1.625% | 7.000% | -6.028% | False |
| ZIGZAG | 소재 | 13/1440 | 0.903% | 0.528%-1.538% | 6.000% | -5.097% | False |

순수 배경 CS 색상/사이즈/소재 중 config가 95% CI 밖인 행: 9/9

## 순수 배경 classifier overlay delta

| Channel | Aspect | Golden | Overlay | Delta | Cache coverage |
|---|---:|---:|---:|---:|---:|
| COUPANG | 사이즈 | 1.181% | 1.181% | +0.000% | 0.000% |
| COUPANG | 색상 | 0.903% | 0.903% | +0.000% | 0.000% |
| COUPANG | 소재 | 0.486% | 0.486% | +0.000% | 0.000% |
| NAVER | 사이즈 | 1.667% | 1.667% | +0.000% | 0.000% |
| NAVER | 색상 | 1.042% | 1.042% | +0.000% | 0.000% |
| NAVER | 소재 | 0.625% | 0.625% | +0.000% | 0.000% |
| ZIGZAG | 사이즈 | 1.458% | 1.458% | +0.000% | 0.000% |
| ZIGZAG | 색상 | 0.972% | 0.972% | +0.000% | 0.000% |
| ZIGZAG | 소재 | 0.903% | 0.903% | +0.000% | 0.000% |

주의: 순수 배경의 overlay delta가 0인 것은 classifier 영향이 없다는 뜻이 아니다. 해당 slice의 cache coverage가 0%라서 golden label이 fallback으로 그대로 쓰인 것이다.

## 채점 제외 window, golden label 기준

| Channel | Aspect | Neg/Total | 관측값 | 95% CI | Config | 차이 | Config in CI |
|---|---:|---:|---:|---:|---:|---:|---:|
| COUPANG | 기타 | 7/1400 | 0.500% | 0.242%-1.029% | 3.000% | -2.500% | False |
| COUPANG | 사이즈 | 44/1400 | 3.143% | 2.349%-4.193% | 8.000% | -4.857% | False |
| COUPANG | 색상 | 94/1400 | 6.714% | 5.518%-8.147% | 5.000% | 1.714% | False |
| COUPANG | 소재 | 14/1400 | 1.000% | 0.597%-1.672% | 4.000% | -3.000% | False |
| COUPANG | 오배송 | 1/1400 | 0.071% | 0.013%-0.404% | 1.000% | -0.929% | False |
| COUPANG | 파손 | 4/1400 | 0.286% | 0.111%-0.732% | 2.000% | -1.714% | False |
| NAVER | 기타 | 3/575 | 0.522% | 0.178%-1.523% | 3.000% | -2.478% | False |
| NAVER | 사이즈 | 43/575 | 7.478% | 5.599%-9.922% | 9.000% | -1.522% | True |
| NAVER | 색상 | 40/575 | 6.957% | 5.150%-9.334% | 6.000% | 0.957% | True |
| NAVER | 소재 | 5/575 | 0.870% | 0.372%-2.019% | 5.000% | -4.130% | False |
| NAVER | 오배송 | 0/575 | 0.000% | 0.000%-0.664% | 1.000% | -1.000% | False |
| NAVER | 파손 | 2/575 | 0.348% | 0.095%-1.259% | 2.000% | -1.652% | False |
| ZIGZAG | 기타 | 0/540 | 0.000% | 0.000%-0.706% | 3.000% | -3.000% | False |
| ZIGZAG | 사이즈 | 32/540 | 5.926% | 4.229%-8.246% | 7.000% | -1.074% | True |
| ZIGZAG | 색상 | 17/540 | 3.148% | 1.975%-4.984% | 7.000% | -3.852% | False |
| ZIGZAG | 소재 | 29/540 | 5.370% | 3.765%-7.606% | 6.000% | -0.630% | True |
| ZIGZAG | 오배송 | 0/540 | 0.000% | 0.000%-0.706% | 1.000% | -1.000% | False |
| ZIGZAG | 파손 | 1/540 | 0.185% | 0.033%-1.041% | 2.000% | -1.815% | False |

## 해석

- 순수 배경이 `config/aspect_count`에 가깝다면, generator가 baseline table을 전체 분모 기준이 아니라 aspect 내부 비율처럼 적용하고 있다는 뜻이다.
- classifier overlay가 golden보다 크게 높다면 prompt/classifier bias가 탐지 분자를 늘릴 수 있다. 다만 순수 배경 slice는 cache coverage가 0%라 이 판단에 쓸 수 없다.
- 채점 제외 window는 사전에 scoring에서 제외된 구간이므로 demo 성능에서 false alert로 세면 안 된다.
- 이 audit에서는 공식 `data/input`, `data/golden`을 재생성하지 않았다.

CSV: `eval\results\mock_baseline_audit_20260806.csv`

