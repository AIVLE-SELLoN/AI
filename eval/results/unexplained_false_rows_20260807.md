# 미설명 false row 상세 확인, 2026-08-07

범위: `no_true_false_breakdown`에서 `unexplained_by_config_overlap`으로 남은 2건.

## 요약

- 대상 row: 2
- 두 row 모두 oracle/golden에서도 발생했으므로 real classification만의 문제는 아니다.
- 단순한 미래 case-past overlap으로는 설명되지 않는다.
- 현재 7일 window의 부정률이 직전/과거 window보다 높게 샘플링된 배경 변동 또는 hot/gap 생성 영향으로 보인다.

## 상세

| Day | Product | Source | Aspect | Channel | Current | Past28 | Prev7 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 53 | P012 | review | 사이즈 | NAVER | 8/68 (11.76%) | 6/276 (2.17%) | 3/71 (4.23%) |
| 53 | P022 | cs | 사이즈 | NAVER | 4/43 (9.30%) | 0/197 (0.00%) | 0/50 (0.00%) |

## 해석

이 2건은 현재 감사 기준으로 마지막 미해결 row다. 둘 다 oracle에서도 발생하므로 프롬프트 수정으로 해결할 대상은 아니다.

가능성이 높은 원인은 config window 밖에서 발생한 mock background/hot-gap 샘플링 변동이다. 다만 case-past와 달리 명확한 시나리오 overlap이 없으므로, 공식 개선 전에 팀이 row-level로 라벨/시나리오 의도를 확인해야 한다.

CSV: `eval\results\unexplained_false_rows_20260807.csv`
