# 이상탐지 성능 점검, 2026-08-06

## 2026-08-07 정정 메모

이 문서는 2026-08-06에 산출한 최초 성능 점검 결과다.

이후 2026-08-07에 `scoring_included=N` 또는 `intended_answer` 빈칸인 시나리오 window를 `ignored`로 분리했다. 따라서 `product x source + real`의 데모 헛알림률은 원본 기준 64.0%였고, 채점 정정 후 공식 보고용 값은 60.9%다.

자세한 정정 내용은 `eval/results/scoring_audit_update_20260807.md`를 기준으로 한다.

## 실행 명령

- `uv run python eval/run_detection_eval.py`
- `uv run python scripts/detection_experiments/daily_exp2.py`
- `uv run python scripts/detection_experiments/demo_sim.py`

`demo_sim.py`는 현재 `app.batch.daily` API에 맞게 `CountingClient`를 import하도록 수정했다. 예전 이름인 `_CountingClient`는 더 이상 존재하지 않는다.

## 1. Oracle 탐지 로직

`eval/run_detection_eval.py`

| 지표 | 결과 |
|---|---:|
| 탐지 recall | 100.0% (25/25) |
| FPR | 0.0% (0/8) |
| verdict 정확도 | 100.0% (33/33) |
| is_biased 정확도 | 100.0% (25/25) |
| main_aspect 정확도 | 100.0% (25/25) |
| biased channel 정확도 | 100.0% (66/66) |
| confidence 정확도 | 100.0% (6/6) |

해석: oracle count 기준으로는 탐지 구현이 golden 설계와 일치한다. 이는 구현 검증이지, 실서비스 모델 성능 주장으로 쓰면 안 된다.

## 2. 일별 sliding 실험

`scripts/detection_experiments/daily_exp2.py`

연속 1일, 2일, 3일 gate 모두 classification cache coverage는 충분했다.

### 케이스 종료일 기준 recall

| Family | Label | 연속 1일 | 연속 2일 | 연속 3일 |
|---|---:|---:|---:|---:|
| 현재 global | oracle | 15.2% | 0.0% | 0.0% |
| 현재 global | real classification | 6.1% | 0.0% | 0.0% |
| Product | oracle | 100.0% | 90.9% | 21.2% |
| Product | real classification | 66.7% | 54.5% | 9.1% |
| Product x source | oracle | 100.0% | 100.0% | 69.7% |
| Product x source | real classification | 78.8% | 60.6% | 36.4% |

### 알림 품질, slot x day 기준

| Family | Gate | Label | Recall | Window 안 | Window 밖 | 헛알림률 | 알림/일 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 현재 global | 1d | oracle | 15.2% | 5 | 21 | 80.8% | 0.81 |
| 현재 global | 1d | real | 6.1% | 2 | 21 | 91.3% | 0.72 |
| Product | 1d | oracle | 100.0% | 70 | 110 | 61.1% | 5.62 |
| Product | 1d | real | 66.7% | 43 | 106 | 71.1% | 4.66 |
| Product x source | 1d | oracle | 100.0% | 89 | 149 | 62.6% | 7.44 |
| Product x source | 1d | real | 78.8% | 58 | 144 | 71.3% | 6.31 |
| Product x source | 2d | real | 60.6% | 32 | 105 | 76.6% | 4.28 |
| Product x source | 3d | real | 36.4% | 12 | 79 | 86.8% | 2.84 |

해석: global BH는 recall 관점에서 너무 보수적이다. Product와 product-source family는 recall을 회복하지만, production suppression/combine flow를 적용하기 전 slot-day 헛알림률은 여전히 높다.

## 3. 데모 simulation

`scripts/detection_experiments/demo_sim.py`

이 결과는 `detect_anomaly`, `combine_sources`, prior-alert suppression을 포함한다. 발행 알림 1건은 셀러가 실제로 받는 알림 1건이다.

| Family | Label | 발행 | True | Echo | False | 헛알림률 | 알림/일 | 케이스 도달 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 현재 global | oracle | 23 | 13 | 0 | 10 | 43.5% | 0.72 | 12/26 |
| 현재 global | real | 15 | 5 | 0 | 10 | 66.7% | 0.47 | 5/26 |
| Product | oracle | 69 | 28 | 0 | 41 | 59.4% | 2.16 | 25/26 |
| Product | real | 59 | 18 | 0 | 41 | 69.5% | 1.84 | 15/26 |
| Product x source | oracle | 80 | 32 | 0 | 48 | 60.0% | 2.50 | 25/26 |
| Product x source | real | 75 | 27 | 0 | 48 | 64.0% | 2.34 | 20/26 |

해석: 데모식 발행 알림 기준에서는 `product x source + real classification`이 테스트한 후보 중 케이스 도달률이 가장 높다(20/26). 다만 원본 기준 헛알림률은 64.0%로 높았다. 이후 채점 제외 window 6건을 `ignored`로 분리하면 공식 보고용 헛알림률은 60.9%다.

## 당시 권고

현재 global family는 성능 목표로 제시하지 않는다. daily/demo 조건에서 너무 보수적이고 케이스를 많이 놓친다.

recall 후보로는 `product x source`가 가장 강하지만, 헛알림률이 높으므로 production-ready라고 주장하면 안 된다.

다음 목표는 threshold 하나를 바꾸는 것이 아니라, false alert가 왜 생기는지 분해하는 것이었다. 이후 2026-08-07 감사에서 주요 원인이 mock 생성기의 baseline 해석 문제와 case-past 경계 단차로 좁혀졌다.
