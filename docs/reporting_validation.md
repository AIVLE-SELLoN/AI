# 보고서 생성 Validation + Evaluation

> **범위**: 월간 리포트 · CS 가이드라인 생성물의 검증 판정 로직과 정량 실험.
> **정본 관계**: 스키마 정의는 노션 「문서 생성 스키마 (확정)」이 정본이고, 이 문서는
> 그 §4-4(출력 검증 규칙)·§4-2(판정식)를 **코드에서 실제로 어떻게 판정하는지**와
> 그 판정기 자체의 실측 성능을 적는다.
> **구현 위치**: `app/reporting/{monthly_report_validator,cs_reply_validator,grounding,metrics_calculator}.py`,
> `app/core/schemas.py`, 실험은 `eval/run_reporting_eval.py`.

---

## 1. 왜 "검증"이 필요한가

리포팅은 **정답 문장이 없는** 산출물이다. 요약문이 좋은지 나쁜지는 골든으로 채점할 수
없다. 대신 **틀렸다고 단정할 수 있는 것**은 명확하다 — 입력에 없는 수치를 쓰거나,
존재하지 않는 문의를 인용하거나, 셀러 문서에 통계 용어가 새는 경우다.

그래서 검증기는 "좋은 글인가"를 재지 않고 **"거짓인가"만 판정**한다. 이 판정을 통과하지
못한 문서는 자동 발송하지 않는다(§3 상태 코드 `FAILED_VALIDATION`).

---

## 2. 검증 계층 — 무엇을 어디서 막는가

같은 오류를 두 곳에서 막으면 규칙이 갈라진다. 계층을 나누고 **각 계층이 한 번씩만**
책임진다.

| 계층 | 담당 | 대상 | 위반 시 |
| --- | --- | --- | --- |
| **구조** | Pydantic 스키마 (`app/core/schemas.py`) | 미정의 키, 배열 길이, 문자열 길이, enum 도메인 | `ValidationError` → 재시도 |
| **도메인** | 스키마 `model_validator` | 비율 합, status 경계, 게이트-null 정합, `s3_full_key` 조합 | `ValidationError` → 재시도 |
| **그라운딩** | 검증기 (`*_validator.py`) | 수치 팩트체크, 단계 라벨 대조, cs_id 포함관계, 금지 표현 | `(False, 사유목록)` → 사유를 프롬프트에 되먹여 재시도 |

### 2-1. 산출물 적재 정책 (2026-08-03 확정) — 검증 규칙이 갈리는 지점

| | 월간 리포트 | CS 가이드라인 |
| --- | --- | --- |
| 산출물 | **전 상품 합본 PDF 1개/월** (첫 페이지만 화면, 전체는 presigned URL 다운로드) | 출력 데이터(DB) + PDF |
| DB 적재 | 하지 않음 | 함 |
| `source_payload` | **미전송** | **필수** (재컴파일 원본) |
| S3 위치 | 버킷 **1개**, prefix `monthly-report/` | 같은 버킷, prefix `cs-guideline/` |
| 자동 삭제(S3 Lifecycle) | **180일** — 원본이 없어 만료 = 영구 소실(경과분 조회 불필요로 확정) | **7일** — 원본으로 재컴파일 (`GUIDELINE_RETENTION_HOURS`) |
| presigned 링크 | 7일 (만료 시 재발급) | 7일 — SigV4 상한 (`PRESIGNED_URL_TTL_HOURS`) |
| PDF 생성 실패 시 | **산출물 없음** → 재시도 필수 | 데이터는 남음, PDF만 재컴파일 |

월간은 **UI가 이 PDF를 뷰어로 그대로 띄우므로**(2026-08-03 확정) 문서 안에 수치 표·차트까지
모두 들어가야 한다(`pdf_compiler`의 월간 템플릿이 자립 문서). 표를 빼면 그 수치는 어디에도
남지 않는다.

### 2-2. 구조·도메인 계층이 막는 것 (스키마 단독 판정)

| 규칙 | 위치 |
| --- | --- |
| `positive + neutral + negative = 1.00 (±0.005)` | `MonthlyAspectDistribution` |
| `status = RISK` ⟺ `drift_rate ≥ 0.03` | `MonthlySentimentDrift` |
| `hold_reason` 설정 시 판정 6개 값 전부 `null`, 아니면 전부 `non-null` | `ChannelDivergencePair` |
| `worst_pair` = **`(severity, excess)` 사전식 최댓값** 쌍, `is_crisis` = pairs 롤업 | `MonthlyChannelDivergenceInput` (`metrics_calculator._worst_rank`) |
| `start_date` = 해당 월 1일, `end_date` = 말일 | `MonthlyReportInput` |
| aspect 3종 고정, `aspect_distributions`와 `sentiment_drifts`의 aspect 집합 동일 | `MonthlyReportInput` |
| `delta = cur_rate − past_rate` | `CSGuidelineStatsInput` |
| `root_cause.count ≤ total` | `CSGuidelineRootCause` |
| `verdict = 정상`은 생성 대상 아님 (422) | `CSGuidelineInput` |
| `s3_full_key = s3_file_path + new_file_name`, `file_size_bytes ≤ 10MB` | `PdfS3Meta` |
| `presigned_expires_at ≤ object_expires_at` (링크가 객체보다 오래 못 산다) | `PdfS3Meta` |
| 파일 메타 4종 필수(`original_file_name`·`new_file_name`·`created_at`·`file_size_bytes`) | `PdfS3Meta` |
| `report_id`/`guideline_id` 정확히 하나, `SUCCESS`면 `pdf_s3_meta` 필수 | `GenerationCallback` |
| `source_payload`는 **CS 가이드라인 SUCCESS 시에만 필수** (월간은 미전송 — §2-1) | `GenerationCallback` |

이 계층은 **LLM 출력뿐 아니라 입력도 검사**한다. 잘못된 집계가 들어오면 생성 자체를
막는 게 맞다 — 틀린 입력으로 만든 문서는 검증기가 통과시켜도 틀린 문서다.

---

## 3. 그라운딩 판정 규칙 (§4-4)

### 3-1. 수치 팩트체크

LLM이 쓴 문장에서 수치를 뽑아 **입력에 실제로 있는 값인지** 대조한다.

**추출 규칙** (`grounding.extract_metric_numbers`)

- 정규식 `(\d+(?:\.\d+)?)\s*(%p|%|건|)` 으로 `13%` / `8%p` / `450건` / `0.54` 를 잡는다.
- **단위 없는 정수는 버린다.** `3개`·`2번`·순번 `1.` 같은 표기가 전부 걸려 오탐만 늘린다.
- 단위 없는 **소수**는 JSD 점수로 본다.

**허용 집합** (입력에서 생성)

| 단위 | 허용값 |
| --- | --- |
| `%` | 각 aspect의 긍정·중립·부정 비율 ×100, CS 현재/직전 부정률, 최다 원인 지분율 |
| `%p` | `drift_rate` ×100, `delta` ×100 (부호 없이 쓰는 표기가 많아 **절댓값도 허용**) |
| `건` | `total_voc_count`, aspect별 `total_count`, `cur_total`, `root_cause.count`/`total`, `sample_size` |
| (없음) | `jsd_score`, `jsd_baseline` |

반올림 표기(`12.5% → 13%`)를 허용하려고 원본값과 반올림값을 함께 넣는다.

**허용 오차**

| 단위 | 오차 | 근거 |
| --- | --- | --- |
| `%`, `%p`, `건` | ±0.5 | 반올림 표기 흡수 |
| 단위 없는 소수 | ±0.005 | ±0.5로 재면 `0.54`와 `0.18`이 같은 값으로 통과해 무의미해진다 |

**대상 필드 / 제외 필드**

| 구분 | 필드 |
| --- | --- |
| 대상 | `aspect_summaries[].summary_text`, `channel_divergence_cause.cause_title`·`cause_description`, `cause_analysis_results[]`, `summary.key_metric_text`, `root_cause_summary` |
| 제외 | `standard_guideline.*`, `ops_action_guide`, `inquiry_specific_guides[].recommended_point` |

제외 필드에는 **정책 상수**(무상 교환 7일, 재촬영 14일 등)가 정당하게 들어간다. 여기까지
팩트체크하면 입력에 없는 숫자라는 이유로 정상 문서가 전부 반려된다.

### 3-2. 금지 표현

`p-value`, `p값`, `p 값`, `FDR`, `유의확률` + 정규식 `p\s*[=<>≤≥]\s*0?\.\d+`.

**제외 필드에도 적용된다** — 수치는 정책 상수가 들어올 수 있지만 통계 용어는 어느
필드에도 나오면 안 된다.

> 애초에 프롬프트에 `p_value`·`bh_significant`를 **넣지 않는다.** 보여주면 모델이 옮겨
> 적고 검증에서 반려되는 낭비가 생긴다. 이 검증은 2차 방어선이다.

### 3-3. 월간 리포트 전용

| 규칙 | 반려 사유 |
| --- | --- |
| `master_product_code`·`report_month`가 입력값과 일치 | 식별자 불일치 |
| `aspect_summaries`의 aspect 집합 = 입력 집합 | 속성 요약 누락 / 입력에 없는 속성 생성 |
| `cause_title`에 worst_pair의 severity 단계 라벨이 **문자열 그대로** 포함 | 단계 라벨 누락 |
| 다른 단계 라벨이 섞이지 않음 | 단계 라벨 혼입 |

단계 라벨 매핑: `SAFE → 안정 단계`, `CAUTION → 주의 단계`, `CRISIS → 위험 단계`.
전 쌍이 보류(severity=null)면 단계 자체가 없으므로 이 검사는 건너뛴다.

### 3-4. CS 가이드라인 전용

| 규칙 | 반려 사유 |
| --- | --- |
| `alert_id`가 입력값과 일치 | 식별자 불일치 |
| `inquiry_specific_guides[].item_id` ⊆ `linked_inquiries[].item_id` | Grounding 오류 |
| `root_cause`가 있으면 그 라벨이 `root_cause_summary`에 포함 | 원인 라벨 누락 |
| `root_cause`가 `null`이면 대체 문구(`원인 미특정`) 포함 | 대체 문구 누락 |

없는 문의를 가리키는 맞춤 가이드는 상담원이 헛짚게 만든다. 이게 CS 쪽에서 가장 중요한
판정이다.

---

## 4. 파이프라인에서의 위치

```
[보류 게이트] total_voc_count < 10 → LLM 미호출, HOLD_INSUFFICIENT_DATA
     ↓
[생성] LLM → 스키마 파싱(구조·도메인 계층)
     ↓
[검증] 그라운딩 계층 → 실패 시 사유를 프롬프트에 되먹여 재시도 (총 1 + MAX_RETRY = 3회)
     ↓ 3회 실패                    ↓ 통과
  FAILED_VALIDATION            [PDF·S3] → SUCCESS
  (자동 발송 중단)              용량 초과 → FAILED_SIZE_EXCEEDED (업로드 전 차단)
                                그 외 예외 → FAILED_ERROR
```

**fallback 생성물을 만들지 않는다.** 구버전은 검증 실패 시 하드코딩 문장으로 문서를 만들어
성공처럼 반환했는데, 스키마 §4-3이 `FAILED_VALIDATION`을 "운영자 알림, 자동 발송 중단"으로
규정한다. 검증을 통과 못 한 문서가 메일로 나가면 틀린 수치가 셀러에게 그대로 간다.

**재시도는 피드백 되먹임이다.** 실패 사유 목록을 그대로 다음 프롬프트의
`$validation_feedback`에 넣는다. 같은 온도로 다시 뽑는 게 아니라 "무엇이 틀렸는지"를
알려주고 고쳐 쓰게 한다.

| 콜백 상태 | 발생 조건 | 동작 |
| --- | --- | --- |
| `SUCCESS` | 검증 통과 + PDF/S3 완료 | PDF 첨부 메일 자동 발송 |
| `HOLD_INSUFFICIENT_DATA` | `total_voc_count < 10` | 고정 안내 문구 출력, **LLM 추론 미수행** |
| `FAILED_VALIDATION` | 검증 3회 연속 실패 | 운영자 알림, 자동 발송 중단 |
| `FAILED_SIZE_EXCEEDED` | `file_size_bytes > 10MB` | S3 업로드·메일 트랜잭션 이전 차단 |
| `FAILED_ERROR` | 그 외 | 에러 알림 |

---

## 5. 채널 분열 severity 판정식 (§4-2)

검증기가 아니라 **입력을 만드는** 로직이지만, 단계 라벨 검증의 근거가 되므로 함께 적는다.
구현은 `app/reporting/metrics_calculator.py`.

```
[게이트]  min(n_A, n_B) ≥ 1 AND N ≥ 30
          미충족 → 판정 6개 값 전부 null + hold_reason 세팅 (반쪽 상태 금지)
[판정]    ① 순열검정 B = 10,000 → p값
          ② 전 (상품 × 채널쌍) p값에 BH-FDR(q=0.05)  ← 반드시 먼저
          ③ severity · is_crisis 산출                ← 나중

excess = jsd_score − jsd_baseline        # bits, δ_min = 0.10
  excess < δ_min  또는 미유의  → SAFE   , is_crisis = false
  δ_min ≤ excess < 2δ_min 이고 유의 → CAUTION, is_crisis = true
  excess ≥ 2δ_min        이고 유의 → CRISIS , is_crisis = true
```

**순서가 중요하다.** BH-FDR을 severity 산출보다 먼저 돌려야 한다. 유의성이 확정되기 전에
단계를 매기면 다중검정 보정이 무의미해진다. 유의하지 않은 쌍은 excess가 아무리 커도
SAFE로 떨어진다.

**baseline을 빼는 이유**: 표본이 작으면 같은 분포에서 나눠도 JSD가 0이 아니다. 순열 분포의
평균(= 귀무 기댓값)을 빼지 않으면 소표본 채널이 항상 위기로 보인다.

**JSD 단위는 bits(log₂)** — 스키마가 `0.00–1.00 (log₂, bits)`로 못박아서다. 제곱근을 씌운
JS distance가 아니라 divergence 그대로 쓴다.

---

## 6. 임계값 상수

전부 `app/core/constants.py`에 있다(매직넘버 금지 컨벤션). **값을 바꾸면 판정이 통째로
달라지므로 변경 전 합의 필수.**

| 상수 | 값 | 용도 |
| --- | --- | --- |
| `MIN_VOC_COUNT_FOR_REPORT` | 10 | 미만이면 LLM 미호출·보류 |
| `MONTHLY_ASPECT_COUNT` | 3 | 색상·사이즈·소재 고정 길이 |
| `DRIFT_RISK_THRESHOLD` | 0.03 | `RISK` 판정 하한(ΔP_neg) |
| `RATIO_SUM_TOLERANCE` | 0.005 | 비율 합·delta 검증 허용 오차 |
| `FACTCHECK_NUMBER_TOLERANCE` | 0.5 | %·%p·건 팩트체크 오차 |
| `FACTCHECK_SCORE_TOLERANCE` | 0.005 | 단위 없는 소수(JSD) 팩트체크 오차 |
| `MAX_PDF_SIZE_BYTES` | 10,485,760 | PDF 용량 상한(10MB) |
| `MONTHLY_RETENTION_DAYS` | 180 | 월간 PDF 자동 삭제(6개월). S3 Lifecycle 과 동일해야 함 |
| `GUIDELINE_RETENTION_HOURS` | 24 | CS 가이드라인 PDF 자동 삭제 |
| `JSD_DELTA_MIN` | 0.10 | δ_min (bits) |
| `JSD_GATE_MIN_TOTAL` | 30 | 게이트 N 하한 |
| `PERMUTATION_B` | 10,000 | 순열검정 반복 |
| `BH_FDR_Q` | 0.05 | 다중검정 목표 FDR |
| `MAX_RETRY` | 2 | 총 시도 = 1 + 2 |

> `DRIFT_RISK_THRESHOLD`(0.03)는 detection의 `MIN_DELTA`(0.03)와 수치가 같지만 **다른
> 계약의 값이라 별도 상수**다. 한쪽을 캘리브레이션할 때 다른 쪽이 딸려가면 안 된다.

---

## 7. Evaluation 설계

`eval/run_reporting_eval.py`. 정답 문장을 만들 수 없으므로 **검증 가능한 것만** 잰다.

| 실험 | 무엇을 재나 | LLM 비용 | 상태 |
| --- | --- | --- | --- |
| **(A) 검증기 민감도** | 정상 출력에 오염을 한 군데씩 주입해 검증기가 잡는 비율(재현율) + 정상 변형을 반려하지 않는지(오탐률) | **$0 (호출 0회)** | 🟢 실측 완료 |
| **(B) 1차 통과율** | 재시도 없이 통과하는 비율, 반려 사유 유형 분포 | 과금 | 🔴 미실행 |
| **(C) 프롬프트 버전 비교** | 같은 입력에 버전을 바꿔 (B) 지표 대조 | 과금 | 🔴 미실행 |
| **(D) 적재 정책 점검** | 코드가 계산한 "다운로드 기한"이 S3 Lifecycle 설정과 같은가 | **$0** | 🟢 8/8 통과 |

**(A)를 먼저 하는 이유**: 검증기가 LLM 생성물의 최후 방어선이다. 방어선 자체가 새는지
모르는 채로 통과율을 재면 그 숫자가 무의미하다.

**(B)에서 재시도를 태우지 않는 이유**: 재시도까지 포함하면 "프롬프트가 좋은가"와 "재시도
로직이 좋은가"가 섞여서 프롬프트 개선 효과를 볼 수 없다.

---

## 8. 실측 결과 — (A) 검증기 민감도

| 항목 | 값 |
| --- | --- |
| 실행일 | 2026-08-03 11:07 (KST) |
| 모델 | 해당 없음 (**LLM 호출 0회**) |
| 프롬프트 버전 | `monthly_report_v4` / `cs_reply_v3` (판정에 미사용) |
| 시드 | 해당 없음 (합성 케이스 고정) |
| 표본 | 월간 3케이스 × 8변형 − 스킵 2 = **22**, CS 2케이스 × 7변형 − 스킵 1 = **13** (총 35) |

| 지표 | 결과 |
| --- | --- |
| **오염 탐지율(재현율)** | **30/30 (100%)** |
| **정상 통과(오탐률)** | **5/5 (0%)** |

### 오염 유형별 결과

| 대상 | 오염 유형 | 기대 | 결과 |
| --- | --- | --- | --- |
| 월간 | 수치 환각 (`33%p`) | 반려 | ✅ |
| 월간 | p값 노출 (`p = 0.002`) | 반려 | ✅ |
| 월간 | FDR 노출 | 반려 | ✅ |
| 월간 | 단계 라벨 누락 | 반려 | ✅ |
| 월간 | 단계 라벨 혼입 (위험+안정) | 반려 | ✅ |
| 월간 | 식별자 불일치 (`P999`) | 반려 | ✅ |
| 월간 | 속성 누락 (소재 빠짐) | 반려 | ✅ |
| 월간 | **반올림 표기** (`20%`, `1%p`) | **통과** | ✅ |
| CS | 미존재 문의 ID (`INQ-999999`) | 반려 | ✅ |
| CS | 수치 환각 (`27%`) | 반려 | ✅ |
| CS | p값 노출 | 반려 | ✅ |
| CS | 통계 용어 노출 (유의확률) | 반려 | ✅ |
| CS | `alert_id` 불일치 | 반려 | ✅ |
| CS | 원인 라벨 누락 | 반려 | ✅ |
| CS | **제외 필드의 정책 수치** (`7일`, `30%`) | **통과** | ✅ |

케이스 구성: 월간은 `MR-CRISIS`/`MR-CAUTION`/`MR-HOLD`(전 쌍 보류), CS는
`CS-ROOTCAUSE`/`CS-NO-ROOTCAUSE`. 단계 라벨이 없는 `MR-HOLD`는 라벨 오염 2종을,
원인이 없는 `CS-NO-ROOTCAUSE`는 라벨 오염 1종을 제외한다(성립하지 않는 조합).

### 한계 (인용 시 반드시 함께 말할 것)

- **100%는 "완벽"이 아니라 "우리가 상상한 오염을 전부 잡았다"이다.** 오염 시나리오를
  우리가 만들었으므로, 상상 못 한 오염 유형은 측정 밖이다.
- 입력 케이스가 **합성 5건**이다. 실서비스 분포가 아니다.
- 이 실험은 **검증기**의 성능이지 **생성물**의 품질이 아니다. 생성 품질은 (B) 필요.

---

## 9. 미실행 — (B)/(C)

과금 구간이라 아직 돌리지 않았다. 실행 시 예상 호출량:

- (B) 5케이스 × `--repeat 3` = **15콜**
- (C) 버전 2개 비교 시 = **30콜** (gpt-4o-mini 기준 소액)

LLM은 temperature=0에서도 실행마다 흔들리므로 **`--repeat 3` 이상 평균**을 쓴다
(`eval/README.md` 원칙).

특히 (C)는 이번에 프롬프트를 토큰 절감판으로 교체(`monthly_report_v3→v4`,
`cs_reply_v2→v3`, 조립 프롬프트 기준 **39~57% 감소**)했으므로, **"토큰을 절반으로 줄이고도
1차 통과율이 유지되는가"** 를 확인하는 게 첫 실험이 되어야 한다.

```bash
python eval/run_reporting_eval.py                                          # (A)만, $0
python eval/run_reporting_eval.py --live --repeat 3                        # + (B)
python eval/run_reporting_eval.py --compare monthly_report_v3,monthly_report_v4 --repeat 3
```

---

## 10. 재현 방법

```bash
# (A) 검증기 민감도 — LLM 호출 0회
python eval/run_reporting_eval.py
# 결과: eval/results/reporting_eval_{YYYYMMDD_HHMMSS}.json

# 검증 로직 단위 테스트 (28개, 비용 0)
pytest tests/test_report.py -q
```

`tests/test_report.py`는 검증기 반려 케이스 15종 + 판정식 5종 + 파이프라인 6종 +
프롬프트 압축 가드 4종을 덮는다. **(A)가 "규칙이 실제 데이터에서 작동하는가"라면,
테스트는 "규칙이 코드로 살아있는가"다.**

---

## 11. 미확정 사항

| 항목 | 현재 | 필요한 결정 |
| --- | --- | --- |
| `ROOT_CAUSE_UNSPECIFIED_TEXT` | 임시로 `"원인 미특정"` | 스키마 §2-2의 확정 대체 문구 확인 (원본 캡처 판독 불가) |
| `MonthlyReportInput.recommended_id` | 문서 표기 그대로 | `recommendation_id` 오타 여부 |
| `CSGuidelineInput.stats` alias `status` | 문서 표기 그대로(양쪽 수용) | 통계 객체에 `status` alias가 맞는지 |
| 콜백 아웃바운드 | 응답 본문으로 반환 | Spring Boot 콜백 URL을 `app/config.py`에 추가할지(공유 영역, 합의 대상) |
