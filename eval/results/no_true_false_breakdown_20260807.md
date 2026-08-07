# TRUE 없는 false 알림 분해, 2026-08-07

범위: 남은 false 중 `no_true_same_product_aspect_source` 12건.

## 요약

- 분석 대상: 12
- oracle/golden에서도 같은 날 발생: 11
- real classification에서만 발생: 1
- 현재 window가 관련 config의 case-past와 겹친 row: 10/12
- category별: {'future_scoring_excluded_same_pas_case_past': 6, 'future_scored_FALSE_same_pas_case_past': 2, 'other_aspect_same_product_case_past': 2, 'unexplained_by_config_overlap': 2}

## 해석

- `future_scored_FALSE_same_pas_case_past`: 같은 product/aspect/source/channel에 미래 configured FALSE window가 있고, 그 case-past가 현재 window와 겹친다.
- `future_scoring_excluded_same_pas_case_past`: 같은 product/aspect/source/channel에 미래 채점 제외 window가 있고, 그 case-past가 현재 window와 겹친다.
- `other_aspect_same_product_case_past`: 같은 상품의 다른 aspect 시나리오 case-past가 현재 window와 겹친다.
- `real_classifier_only_no_config_overlap`: oracle에서는 안 뜨고 real classification에서만 뜬다.

이 결과도 threshold 튜닝 근거가 아니라 mock 생성기의 baseline 해석과 case-past 경계 단차를 확인한 감사 결과다.

## 상세

| Day | Product | Source | Aspect | Channel | Category | Evidence |
|---:|---:|---:|---:|---:|---:|---|
| 29 | P035 | cs | 색상 | NAVER | future_scored_FALSE_same_pas_case_past | SC-035 NAVER cs 색상 51-57 scoring=Y intended=FALSE case_past=23-50 overlap=7d note= |
| 29 | P035 | cs | 색상 | ZIGZAG | future_scored_FALSE_same_pas_case_past | SC-035 ZIGZAG cs 색상 51-57 scoring=Y intended=FALSE case_past=23-50 overlap=7d note= |
| 29 | P036 | cs | 색상 | NAVER | future_scoring_excluded_same_pas_case_past | SC-036 NAVER cs 색상 52-58 scoring=N intended=FALSE case_past=24-51 overlap=6d note= |
| 29 | P036 | cs | 색상 | ZIGZAG | future_scoring_excluded_same_pas_case_past | SC-036 ZIGZAG cs 색상 52-58 scoring=N intended=FALSE case_past=24-51 overlap=6d note= |
| 29 | P041 | cs | 색상 | COUPANG | future_scoring_excluded_same_pas_case_past | SC-037 COUPANG cs 색상 51-57 scoring=N intended=blank case_past=23-50 overlap=7d note=구분불가 관찰 — 발화 p=0.00013 |
| 29 | P042 | cs | 색상 | NAVER | future_scoring_excluded_same_pas_case_past | SC-038 NAVER cs 색상 51-57 scoring=N intended=blank case_past=23-50 overlap=7d note=잠정전역 관찰 — 발화 p=0.00029 |
| 32 | P036 | cs | 색상 | ALL | future_scoring_excluded_same_pas_case_past | SC-036 NAVER cs 색상 52-58 scoring=N intended=FALSE case_past=24-51 overlap=7d note= |
| 36 | P013 | review | 소재 | ZIGZAG | other_aspect_same_product_case_past | SC-012 ZIGZAG cs 파손 36-42 scoring=Y intended=TRUE case_past=8-35 overlap=6d note= |
| 36 | P042 | cs | 색상 | NAVER | future_scoring_excluded_same_pas_case_past | SC-038 NAVER cs 색상 51-57 scoring=N intended=blank case_past=23-50 overlap=7d note=잠정전역 관찰 — 발화 p=0.00029 |
| 53 | P012 | review | 사이즈 | NAVER | unexplained_by_config_overlap | SC-011 COUPANG cs 파손 35-41 scoring=Y intended=FALSE case_past=7-34 overlap=0d note= |
| 53 | P022 | cs | 사이즈 | NAVER | unexplained_by_config_overlap | SC-021 COUPANG cs 소재 41-47 scoring=Y intended=FALSE case_past=13-40 overlap=0d note= |
| 54 | P011 | cs | 색상 | COUPANG | other_aspect_same_product_case_past | SC-010 COUPANG cs 파손 54-60 scoring=Y intended=TRUE case_past=26-53 overlap=6d note= |

## 미설명 2건 후속 확인

`unexplained_by_config_overlap` 2건은 별도로 row-level count를 확인했다.

- P012 / review / 사이즈 / NAVER / day53: current 8/68 = 11.76%, past28 6/276 = 2.17%
- P022 / cs / 사이즈 / NAVER / day53: current 4/43 = 9.30%, past28 0/197 = 0.00%

두 건 모두 oracle/golden에서도 발생했으므로 real classification만의 문제는 아니다. 단순한 미래 case-past overlap은 없지만, config window 밖에서 current 7일에 부정 표본이 몰린 배경 변동 또는 hot/gap 생성 영향으로 보인다.

상세 문서: `eval/results/unexplained_false_rows_20260807.md`

CSV: `eval\results\no_true_false_breakdown_20260807.csv`
