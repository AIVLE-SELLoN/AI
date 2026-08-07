# 문서 생성 스키마 — 변경 명세 (2026-08-04, 리포팅/용준)

> 노션 「**문서 생성 스키마 (확정)**」에 대한 **변경분만** 정리한 문서입니다.
> 바뀌지 않은 절(§1-3 대시보드 요약, §2-1 CS 입력, §4-1 Enum 목록, §4-2 판정식)은
> 손대지 않았습니다.
> 정본은 `app/core/schemas.py`이며, 아래 값은 전부 현재 코드에서 확인한 것입니다.

## 변경 요약

| 절 | 조치 | 사유 |
| --- | --- | --- |
| §1-1 월간 입력 | `aspect_distributions[]` 비율 합 규칙에 **0건 예외** 추가 | 없는 관측을 중립 100%로 채우던 문제 |
| §1-1 월간 입력 | `worst_pair` 정의를 **severity → excess** 기준으로 정정 | jsd 최댓값 기준이면 SAFE 쌍이 제목에 박힘 |
| §1-2 월간 출력 | **`channel_pair_analyses` 신설** (채널쌍별 원인·조치) | 리포트가 쌍마다 따로 표시하도록 화면 확정 |
| §1-2 월간 출력 | PDF 렌더링 대상 필드 축소 — 3개 필드가 **문서에서 빠짐** | 화면 구성 확정 (⚠️ 아래 경고 참고) |
| §2-2 CS 출력 | `guideline_id` 생성 규칙 **전면 교체** | 같은 날 같은 상품 알림이 서로를 덮어쓰던 버그 |
| §3-1 S3 메타 | `created_at` **필수 추가** → 파일 필수 4종 확정 · `object_expires_at` 명시 | 파일 산출물 공통 규칙 |
| §4-4 검증 규칙 | 팩트체크·금지표현 대상 확대 + **채널쌍 커버리지 검증 신설** | 신설 필드가 검증 사각지대였음 |
| 전역 | 임계값 **`constants.py` 이관** | 매직넘버 금지 컨벤션 |

---

## §1-1. 월간 보고서 입력 (`MonthlyReportInput`) — 2건 정정

### (1) `aspect_distributions[]` — 관측 0건일 때의 비율 합

기존 문서의 제약은 "**세 비율 합 = 1.00 (±0.005)**" 하나뿐이었습니다. 여기에 예외를
추가합니다.

| 조건 | 제약 |
| --- | --- |
| `total_count > 0` | `positive + neutral + negative = 1.00 (±0.005)` — 기존과 동일 |
| **`total_count = 0`** | **세 비율도 전부 `0.0`** (합 ≤ 0.005) |

> **왜 바꿨나** — 합을 1.00으로 맞추려고 관측 0건인 속성을 `neutral_ratio = 1.0`으로
> 채우면, "관측이 없다"가 "**전부 중립이다**"로 바뀝니다. LLM은 이를 실제 관측으로 읽고
> 문장을 만들며, §4-4 수치 팩트체크는 1.0이 입력에 실재하는 값이라 걸러내지 못합니다.

### (2) `channel_divergence.worst_pair` — 선정 기준 정정

| 구분 | 정의 |
| --- | --- |
| 기존 문서 | `pairs[]` 중 `jsd_score` **최댓값** 쌍 |
| **정정** | `pairs[]` 중 **가장 위험한** 쌍 — `(severity 등급, excess)` 사전식 최댓값<br>`severity` 등급: `CRISIS(3) > CAUTION(2) > SAFE(1)` / `excess = jsd_score − jsd_baseline` |

판정된 쌍이 하나도 없으면(전 쌍 보류) `worst_pair`는 `pairs[]` 안에 존재하기만 하면
됩니다.

> **왜 바꿨나** — `severity`는 `excess`와 유의성으로 정해지고 `jsd_baseline`은 쌍마다
> 다릅니다(표본이 작을수록 큽니다). 그래서 **`jsd_score`가 가장 큰 쌍이 `SAFE`인데 다른
> 쌍이 `CRISIS`인 상태**가 실제로 재현됐고, 그때 리포트 제목에는 "안정 단계"가 박히면서
> `is_crisis = true`로 나갔습니다. 게이지 색과 제목 문구가 반대로 표시됩니다.

---

## §1-2. 월간 보고서 출력 (`MonthlyReportOutput`) — 필드 신설

### (1) 신규 필드 `channel_pair_analyses`

| 필드명 | 데이터타입 | 제약 조건 / 값 범위 | 설명 |
| --- | --- | --- | --- |
| `channel_pair_analyses` | array\<object\> | 길이 **0–3**, `comparison_pair` **중복 불가**<br>**입력 `channel_divergence.pairs[]`와 1:1** | 채널쌍별 원인·조치 |
| `channel_pair_analyses[].comparison_pair` | string | Pattern `^[A-Z]+_VS_[A-Z]+$`<br>**입력 pairs에 있는 값만** | 대상 채널쌍 (입력과 동일 표기) |
| `channel_pair_analyses[].cause_analysis` | array\<string\> | 길이 **1–2**, 단문 / **수치 그라운딩 대상** | 이 채널쌍의 원인 분석 |
| `channel_pair_analyses[].recommended_actions` | array\<string\> | 길이 **1–2**, 단문 / **금지표현 검사 대상** | 이 채널쌍의 권장 조치 |

- **스키마 기본값은 `[]`** 이지만, §4-4 검증기가 **입력 pairs와 1:1이 아니면 반려**하므로
  실질적으로 필수입니다. 화면에서 게이지 바로 아래에 붙는 자리라, 비어 있으면 그 칸이
  통째로 빈칸이 됩니다.
- 보고서 전체 단위인 `cause_analysis_results` / `recommended_actions`와 **다른 필드**입니다.
  전자는 "이 상품 전체"의 결론이고, 이것은 "이 채널쌍" 한정입니다.
- **보류된 채널쌍**(`hold_reason` 있음)도 분석 1건이 필요하며, 이때는 판정 수치를 쓰지
  않고 "표본이 부족해 판단하지 않았다"는 취지로만 씁니다.

프롬프트 버전: `monthly_report_v5` (구버전 `v3`·`v4`는 컨벤션대로 보존).

### (2) ⚠️ PDF 렌더링 대상에서 빠진 필드 — **결정 필요**

2026-08-04 화면 확정으로 상품 페이지 구성이 바뀌면서, 아래 세 필드가 **PDF에
렌더링되지 않습니다.**

| 필드 | 스키마 | PDF 렌더링 | MQ payload |
| --- | --- | --- | --- |
| `aspect_summaries[].summary_text` | 유지 (필수, 3건) | ❌ 미렌더 (속성 카드 내 AI 요약 문장 삭제) | ❌ 미전송 |
| `cause_analysis_results[]` | 유지 (필수, 1–5) | ❌ 미렌더 (하단 종합 카드 삭제) | ❌ 미전송 |
| `recommended_actions[]` | 유지 (필수, 1–5) | ❌ 미렌더 (하단 종합 카드 삭제) | ❌ 미전송 |
| `channel_divergence_cause` | 유지 | ✅ 문서 상단 요약 배너 | ❌ 미전송 |
| `channel_pair_analyses[]` | **신설** | ✅ 게이지 아래 카드 2장 | ❌ 미전송 |

> **⚠️ 월간 리포트는 PDF가 유일한 산출물이고 DB에 데이터를 적재하지 않습니다.**
> 따라서 **PDF에 렌더링되지 않는 필드는 어디에도 남지 않습니다.** 위 세 필드는 현재
> LLM 생성 비용만 쓰고 버려지는 상태입니다. 다음 중 하나를 정해야 합니다.
>
> 1. **스키마에서 제거** — 생성하지 않는다 (토큰·비용 절감)
> 2. **PDF에 되살린다** — 예: 문서 말미 부록 페이지
> 3. **현행 유지** — 향후 화면 확장을 위해 계약만 남겨둔다
>
> 결정 전까지는 스키마·검증 규칙을 그대로 두었습니다(제거는 팀 계약 변경이라 합의
> 대상입니다).

---

## §2-2. CS 가이드라인 출력 (`CSGuidelineOutput`) — `guideline_id` 규칙 교체

| 구분 | 규칙 | 예시 |
| --- | --- | --- |
| 기존 문서 | `GD-{탐지일}-{상품ID}` | `GD-20260528-P001` |
| **정정** | **`alert_id`의 `ALT-` 접두어만 `GD-`로 치환** (alert_id와 **1:1**)<br>`ALT-`로 시작하지 않으면 `GD-` + alert_id 전체 | `ALT-20260528-P001-COUPANG`<br>→ `GD-20260528-P001-COUPANG` |

> **왜 바꿨나** — 이상 탐지는 **(상품, 속성, 채널)** 단위로 발화합니다. 기존 규칙은 같은
> 날 같은 상품의 서로 다른 알림이 **전부 같은 ID**가 되고, 백엔드가 멱등 upsert를 하므로
> **나중에 도착한 가이드라인이 앞의 것을 조용히 덮어썼습니다**(쿠팡 색상 가이드가
> 네이버 사이즈 가이드로 바뀌는 식). 재현 확인 후 수정했습니다.

- 구현은 `app/reporting/ids.py::build_guideline_id()` **한 곳**입니다. 서비스와 검증기가
  같은 함수를 봅니다(규칙이 갈라지면 백엔드가 엉뚱한 문서를 덮어씁니다).
- §4-4에 **`guideline_id` 일치 검증**이 추가됐습니다 (LLM이 다른 ID를 만들면 반려).
- 프롬프트 버전: `cs_reply_v4` (서버가 계산한 `guideline_id`를 프롬프트에 주입).

---

## §3-1. S3 PDF 메타데이터 (`PdfS3Meta`) — 필수 4종 확정

**📌 파일 산출물은 종류를 불문하고 아래 4종을 반드시 실어 보냅니다** (2026-08-03 확정).
앞으로 PDF 외 형식(엑셀·CSV 등)이 늘어도 같은 4종을 유지합니다.

| 필드명 | 데이터타입 | 제약 조건 / 값 범위 | 설명 |
| --- | --- | --- | --- |
| `original_file_name` | string | **필수** | [필수 4종] 원본·표시용 파일명 |
| `new_file_name` | string | **필수** | [필수 4종] 버킷에 저장한 파일명 |
| **`created_at`** | DateTime(ISO8601) | **필수 — 신규** | [필수 4종] 파일 생성(업로드) 일자 |
| `file_size_bytes` | int | **필수**, `ge=0`, `≤ 10485760` (10MB) | [필수 4종] 파일 크기 |
| `object_expires_at` | DateTime \| null | 명시 추가 | S3 Lifecycle 자동 삭제 시각 = **다운로드 가능 기한** |
| `presigned_expires_at` | DateTime \| null | **`≤ object_expires_at`** | 링크 만료. 만료 후 `s3_full_key`로 재발급 |
| **`company_id`** | string | **필수 — 신규(2026-08-06)** | 경로에 쓰인 고객사 식별자 |
| **`company_name`** | string \| null | 신규(2026-08-06) | 표시용 고객사명. **경로에는 쓰지 않음** |

모델 검증(둘 다 위반 시 반려):

1. `s3_full_key == s3_file_path + new_file_name`
2. `company_id`가 `s3_file_path`의 `companies/…/` 구간과 일치 — 어긋나면 메인이 둘 중
   무엇을 믿어야 할지 알 수 없고, 조용히 남의 회사 것으로 분류된다
3. `presigned_expires_at ≤ object_expires_at` — 객체가 사라진 뒤에도 살아있는 링크는
   "받을 수 있다"는 잘못된 안내가 됩니다.

보존 정책 — 인프라 「S3 파일 구조 규칙 정의」(2026-08-05) 반영. **버킷은 하나**이고
문서 종류는 프리픽스로 가릅니다. Lifecycle 규칙이 프리픽스 단위로 걸리기 때문입니다.

| 문서 | 프리픽스 | 객체 보존 | presigned TTL | 만료 후 재생성 |
| --- | --- | --- | --- | --- |
| 월간 리포트 | `monthly-report/` | **180일(6개월)** | 7일 | ❌ 불가 — **만료 = 영구 소실** |
| CS 가이드라인 | `cs-guideline/` | **7일** | 7일 | ✅ `source_payload`로 재컴파일 |

객체 경로·파일명:

```
reports/{report_type}/{company_id}/{yyyy}/{mm}/{report_type}_{yyyyMM}_{uuid4}.pdf
```

- `{yyyy}/{mm}`·`{yyyyMM}`은 **보고 대상 기간**입니다(업로드 시각이 아닙니다). 업로드
  시각을 쓰면 8/1 새벽에 올린 7월 리포트가 `2026/08` 폴더에 `…_202607_….pdf`로 들어가
  폴더와 파일명의 연월이 어긋납니다.
- presigned TTL은 **문서 종류 무관 7일 고정**입니다(인프라 §5, SigV4 상한). AI 노드가
  업로드 시점에 최초 발급하고, 백엔드는 유효한 동안 재사용하다 만료 시에만 재발급합니다.
- 등록되지 않은 `report_type`은 **짧은 보존(1일)** 쪽으로 보냅니다 — 6개월 프리픽스에
  정체 불명의 객체가 쌓이는 것보다 하루 뒤 사라지는 쪽이 안전합니다.

재생성 플로우 (2026-08-05 확정):

- **월간 리포트는 재생성하지 않습니다.** 업로드 후 6개월 Lifecycle로 자동 삭제될 때까지
  두는 것이 전부입니다. 데이터를 DB에 적재하지 않아 PDF가 유일한 산출물이므로
  **만료 = 영구 소실**입니다(`recompilable=False`).
  → 인프라 문서 §6의 "필요 시 원본 데이터를 DB 기준으로 재생성"은 월간에 해당하지
    않습니다. 재생성할 원본이 없습니다.
- **CS 가이드라인만 재생성합니다.** 출력 JSON을 DB(JSONB)에 영구 보관해 두었다가,
  객체가 만료된 뒤 재생성 요청이 오면 그 원본으로 PDF를 다시 만들어 올립니다
  (`recompilable=True`).

---

## §4-4. 출력 검증 규칙 — 대상 확대 + 신규 계층

### (1) 채널쌍 커버리지 검증 (신설)

| 조건 | 반려 사유 |
| --- | --- |
| 입력 `pairs[]`에 있는 쌍이 `channel_pair_analyses`에 없음 | `채널쌍 분석 누락: [...]` |
| `channel_pair_analyses`에 입력에 없는 쌍이 있음 | `입력에 없는 채널쌍 분석: [...]` |

쌍이 어긋나면 **다른 채널 이야기가 그 게이지 자리에 인쇄**되므로 구조 검증만으로는
부족합니다.

### (2) 수치 팩트체크 대상 필드 — 추가

기존: `aspect_summaries[].summary_text` · `channel_divergence_cause.cause_title` ·
`channel_divergence_cause.cause_description` · `cause_analysis_results[]` ·
`summary.key_metric_text` · `root_cause_summary`

**추가:** `channel_pair_analyses[].cause_analysis[]`

### (3) 금지 표현 스캔 대상 — 추가

금지 표현(`p-value`, `p값`, `p 값`, `FDR`, `유의확률`, 정규식 `p\s*[=<>≤≥]\s*0?\.\d+`)은
**어느 필드에도** 나오면 안 됩니다. 수치 팩트체크 대상이 아닌 아래 필드도 금지 표현은
검사합니다.

- `recommended_actions[]` (보고서 전체)
- `channel_pair_analyses[].recommended_actions[]`

### (4) 교차검증 함수 제거

`schemas.py`에 있던 `validate_monthly_output_grounded()` /
`validate_guideline_output_grounded()`를 **삭제**했습니다. 같은 규칙이 두 벌로 존재하면서
아무도 호출하지 않는 쪽만 낡아가는 상태였습니다.

**유일한 구현**은 아래 두 모듈입니다.

- `app/reporting/monthly_report_validator.py`
- `app/reporting/cs_reply_validator.py`

검증기는 반려 사유 목록을 돌려주므로 재시도 프롬프트에 그대로 되먹일 수 있습니다.

### (5) 팩트체크 허용 오차 (수치 명시)

| 상수 | 값 | 용도 |
| --- | --- | --- |
| `FACTCHECK_NUMBER_TOLERANCE` | `0.5` | 건수·퍼센트 표기의 반올림 허용 폭 |
| `FACTCHECK_SCORE_TOLERANCE` | `0.005` | 점수(0–1 스케일) 허용 폭 |

천단위 구분 쉼표(`1,200건`)는 파싱 전에 제거합니다 — 제거하지 않으면 `1,200`이
`200.0`으로 읽혀 정상 문장이 반려됐습니다(재현 확인).

---

## 전역 — 임계값 `constants.py` 이관 (매직넘버 금지)

`schemas.py`에 하드코딩돼 있던 값을 전부 `app/core/constants.py`로 옮겼습니다.
**값을 바꾸면 정량 실험 결과가 통째로 달라지므로 변경 전 합의가 필요합니다.**

| 상수 | 값 | 쓰이는 곳 |
| --- | --- | --- |
| `MONTHLY_ASPECT_COUNT` | `3` | 월간 aspect 개수 (색상·사이즈·소재) |
| `DRIFT_RISK_THRESHOLD` | `0.03` | `DriftStatus.RISK` 판정 |
| `RATIO_SUM_TOLERANCE` | `0.005` | 비율 합·`delta` 검증 |
| `MAX_PDF_SIZE_BYTES` | `10485760` | PDF 상한 (10MB) |
| `MIN_VOC_COUNT_FOR_REPORT` | `10` | 월간 보류 게이트 |
| `JSD_DELTA_MIN` | `0.10` | §4-2 δ_min |
| `JSD_GATE_MIN_TOTAL` | `30` | §4-2 게이트 N |
| `PERMUTATION_B` | `10000` | 순열검정 B |
| `FACTCHECK_NUMBER_TOLERANCE` | `0.5` | §4-4 팩트체크 |
| `FACTCHECK_SCORE_TOLERANCE` | `0.005` | §4-4 팩트체크 |
| `MONTHLY_RETENTION_DAYS` | `180` | S3 Lifecycle (월간) |
| `GUIDELINE_RETENTION_HOURS` | `24` | S3 Lifecycle (CS) |
| `PRESIGNED_URL_TTL_HOURS` | `24` | presigned URL 만료 (문서 종류 무관 고정) |
| `SEVERITY_STAGE_LABEL` | `{SAFE: 안정 단계, CAUTION: 주의 단계, CRISIS: 위험 단계}` | 단계 라벨 대조 |
| `HOLD_INSUFFICIENT_DATA_NOTICE` | 고정 문구 | 보류 안내 |
| `FORBIDDEN_METRIC_EXPRESSIONS` / `FORBIDDEN_P_VALUE_PATTERN` | — | 금지 표현 |

> `MONTHLY_ASPECTS`와 `GUIDELINE_EXCLUDED_VERDICTS`는 `schemas.py`의 Enum에서 파생되는
> 값이라 **그대로 두었습니다** — `constants.py`로 옮기면 `constants → schemas` 역방향
> import가 생겨 순환합니다.

---

## 미결 사항

| 항목 | 내용 |
| --- | --- |
| 렌더링되지 않는 3개 필드 | `aspect_summaries` · `cause_analysis_results` · `recommended_actions` — 제거 / 부록 렌더 / 현행 유지 중 택1 (§1-2 참고) |
| `ROOT_CAUSE_UNSPECIFIED_TEXT` | 현재 `"원인 미특정"` 임시값. 노션 원문 확인 후 확정 필요 |