# 데모 탐지 튜닝 탐색, 2026-08-06

## 2026-08-07 정정 메모

이 문서는 당시 “정책 후보를 탐색”한 기록이다.

현재 결론상 이 결과를 성능 개선으로 주장하면 안 된다. 특히 `warmup shadow d29-32`는 현재 평가셋의 날짜 분포를 본 뒤 나온 후보이므로, 별도 holdout 없이 최종 성능 개선으로 말하면 오버피팅 위험이 있다.

또한 2026-08-07 감사에서 헛알림의 큰 원인이 mock 생성기의 baseline 해석 문제로 확인되었기 때문에, threshold나 gating을 튜닝하기 전에 데이터 생성 정의를 먼저 확정해야 한다.

중요 정정: 이 문서의 원래 sweep 표는 `ignored` 채점 제외 알림을 분리하기 전 기준이었다. 2026-08-07에 `tune_demo_policies.py`를 정정해 `classify_alert(alert, truth, day_n, ignored)` 기준으로 핵심 조합을 재실행했다.

## 목적

통계 임계값을 먼저 바꾸지 않고, 실험 2 / demo-mode detection에서 가능한 실무적 튜닝 방향을 탐색했다.

이 sweep은 `BH_FDR_Q`와 `MIN_DELTA`를 그대로 두고, real-classification cache 결과 위에서 탐지 후 발행 정책만 비교했다.

## 실행 명령

`uv run python scripts/detection_experiments/tune_demo_policies.py`

LLM 호출: 0회. 기존 classification cache와 `CountingClient`를 사용했다.

## 당시 결과, 정정 전 채점 기준

| Family | Policy | 발행 | True | False | 헛알림률 | 알림/일 | 케이스 도달 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Product | baseline | 59 | 18 | 41 | 69.5% | 1.84 | 15/26 |
| Product | warmup skip d29-32 | 36 | 18 | 18 | 50.0% | 1.12 | 15/26 |
| Product | warmup shadow d29-32 | 36 | 18 | 18 | 50.0% | 1.12 | 15/26 |
| Product | CS color 2-day | 41 | 15 | 26 | 63.4% | 1.28 | 14/26 |
| Product | warmup skip + CS color 2-day | 26 | 15 | 11 | 42.3% | 0.81 | 14/26 |
| Product x source | baseline | 75 | 27 | 48 | 64.0% | 2.34 | 20/26 |
| Product x source | warmup skip d29-32 | 47 | 24 | 23 | 48.9% | 1.47 | 20/26 |
| Product x source | warmup shadow d29-32 | 47 | 27 | 20 | 42.6% | 1.47 | 20/26 |
| Product x source | CS color 2-day | 58 | 23 | 35 | 60.3% | 1.81 | 18/26 |
| Product x source | warmup skip + CS color 2-day | 37 | 20 | 17 | 45.9% | 1.16 | 17/26 |

## 2026-08-07 정정 후 핵심 재실행

명령:

`uv run python scripts\detection_experiments\tune_demo_policies.py --families "상품x source" --policies baseline --skip-warmup-sweep`

`uv run python scripts\detection_experiments\tune_demo_policies.py --families "상품x source" --policies warmup_shadow_d29_32 --skip-warmup-sweep`

`uv run python scripts\detection_experiments\tune_demo_policies.py --families "상품별" --policies baseline --skip-warmup-sweep`

| Family | Policy | 발행 | 채점 | True | Ignored | False | 헛알림률 | 알림/일 | 케이스 도달 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Product | baseline | 59 | 53 | 18 | 6 | 35 | 66.0% | 1.84 | 15/26 |
| Product x source | baseline | 75 | 69 | 27 | 6 | 42 | 60.9% | 2.34 | 20/26 |
| Product x source | warmup shadow d29-32 | 47 | 41 | 27 | 6 | 14 | 34.1% | 1.47 | 20/26 |

## 당시 관찰

당시 숫자만 보면 가장 좋아 보였던 후보는 다음이었다.

`Product x source` family + 초기 4일 shadow warmup.

기준 `Product x source` demo run과 비교:

| 지표 | Baseline | Shadow warmup |
|---|---:|---:|
| 케이스 도달 | 20/26 | 20/26 |
| True alerts | 27 | 27 |
| Ignored alerts | 6 | 6 |
| False alerts | 42 | 14 |
| 헛알림률 | 60.9% | 34.1% |
| 알림/일 | 2.34 | 1.47 |

하지만 이 결과는 현재 기준에서 “성능 개선”으로 주장하지 않는다. day 29-32에 false가 몰린 것을 본 뒤 만든 정책이므로, 같은 데이터에서 낮아진 헛알림률을 최종 성능처럼 말하면 평가셋 오버피팅이 된다.

## 잘 맞지 않았던 후보

`CS color 2-day` gating은 알림량을 줄였지만 true alert와 케이스 도달률도 함께 줄였다.

- `Product x source` baseline: 20/26 cases hit
- `Product x source` + CS color 2-day: 18/26 cases hit

따라서 색상 전용 persistence를 첫 production tuning lever로 삼는 것은 위험하다.

## 현재 주의사항

warmup은 “실제 문제를 무시한다”는 뜻은 아니다. 운영상으로는 초기 며칠을 shadow mode로 돌려 내부 기록만 남기고 셀러에게는 발행하지 않는 방식일 수 있다.

그러나 `4일`이라는 기간은 현재 mock/demo sequence에서 day 29-32에 false가 집중된 것을 보고 나온 경험값이다. 보편적 production 상수로 주장하면 안 된다.

현재는 정책 튜닝보다 `BASELINE_RATE` 의미와 generator 보정 여부를 먼저 확정해야 한다.
