# 메시지 큐 컨벤션 — 보고서 생성 관련 변경 명세 (2026-08-04, 리포팅/용준)

> 노션 「**메시지 큐 컨벤션 정의**」 및 그 08-03 수정안에 대한 **변경분만** 정리한
> 문서입니다. 08-03 수정안에서 정한 내용 중 아래에 없는 것은 **그대로 유효**합니다.
> 정본은 `app/core/schemas.py`이며, 아래 값은 전부 현재 코드에서 확인한 것입니다.

## 변경 요약

| 절 | 조치 | 사유 |
| --- | --- | --- |
| §4.2 `ai.report.generated` | **이벤트 단위를 상품별 → 월 1건으로 변경** (`report_id` 형식 변경, `product_group_id` 제거) | PDF가 월 1개 합본으로 확정 |
| §4.2 `notice_message` | **`SUCCESS`일 때도 전송** (보류·실패 상품 안내) | 합본 표지 페이지 제거 |
| §4.2 `HOLD_INSUFFICIENT_DATA` | **월간에서는 발생하지 않음** — 상품별 보류는 안내 문구로 흡수 | 상동 |
| §4.2 `pdf_s3_meta` | `created_at` **필수 추가** (파일 필수 4종) | 파일 산출물 공통 규칙 |
| §4.6 `ai.guideline.generated` | `guideline_id` 멱등 키 **형식 교체** | 같은 날 같은 상품 알림이 서로를 덮어씀 |
| §5 소비 측 처리 규칙 | 월간 보관 필드에서 `product_group_id` 제거 | §4.2 변경에 연동 |
| §4.2·§4.6·§7 S3 적재 | **버킷 2개 → 1개 + 프리픽스**, 경로·파일명 규칙 교체 | 인프라 「S3 파일 구조 규칙 정의」(2026-08-05) |
| §4.2·§4.6 `pdf_s3_meta` | **`file_extension` 제거**, presigned TTL **7일**·재사용 정책 | 인프라 §4·§5 (2026-08-06) |

---

## §4.2 `ai.report.generated` — 이벤트 단위 변경

> **⚠️ 이번 결정으로 이벤트 발행 단위가 바뀌었습니다 (2026-08-04 확정).**
> 월간 리포트 PDF는 상품별로 생성되지 않고 **전 상품을 합친 월 1개 합본**입니다.
> 화면은 그중 **첫 페이지만 미리보기로 띄우고** 전체는 presigned URL로 내려받습니다.
> 따라서 **이벤트도 월 1건**이며, payload에서 상품 구분이 사라집니다.

### 필드 변경

| 필드 | 08-03 수정안 | **08-04 정정** |
| --- | --- | --- |
| `report_id` | `RPT-{YYYYMM}-{상품코드}`<br>예: `RPT-202607-P001` | **`RPT-{YYYYMM}`**<br>예: `RPT-202607` — 상품 코드가 들어가지 않습니다 |
| `product_group_id` | 필수 (상품별 생성) | **제거** — 합본이라 상품 구분이 없습니다 |
| `notice_message` | 비 `SUCCESS`일 때 안내 문구 | **`SUCCESS`일 때도 올 수 있음** (아래 참고) |
| `pdf_s3_meta.created_at` | (없음) | **신규 필수** |

### 최종 payload 명세

| **필드** | **타입** | **설명** |
| --- | --- | --- |
| `report_id` | string | 보고서 고유 ID. **멱등 키**. 형식 `RPT-{YYYYMM}` — 예: `RPT-202607`<br>월 1건이므로 **같은 달을 다시 돌려도 같은 ID**입니다 |
| `report_month` | string | 보고서 연월 `YYYY-MM` |
| `status` | enum | **`SUCCESS` \| `FAILED_SIZE_EXCEEDED` \| `FAILED_ERROR` 3종만**<br>⚠️ `HOLD_INSUFFICIENT_DATA` · `FAILED_VALIDATION`은 **상품 단위** 상태라 월 1건 이벤트에서는 발생하지 않습니다 |
| `pdf_s3_meta` | object \| null | `SUCCESS`일 때만 non-null |
| `notice_message` | string \| null | 사용자 안내 문구. **`SUCCESS`일 때도 채워질 수 있습니다** |
| `validation_report` | object \| null | `FAILED_VALIDATION`일 때 실패 사유 목록(운영자 확인용) |

> `product_group_id`는 payload에 없습니다. 상품별 추적용 ID(`RPT-202607-P001`)는
> **AI 쪽 배치 로그에만** 남으며 이벤트로 나가지 않습니다.

### `status` — 월간 발생 조건 (정정)

| 값 | 발생 조건 | 메인이 할 일 |
| --- | --- | --- |
| `SUCCESS` | 상품 **1개 이상** 수록 + PDF/S3 완료 | 링크 저장, PDF 첨부 메일 발송. `notice_message`가 있으면 화면에 함께 표시 |
| `FAILED_SIZE_EXCEEDED` | 합본 PDF > 10MB | 발송 중단 (S3 업로드 **이전에** 차단) |
| `FAILED_ERROR` | **수록할 상품이 하나도 없음**, S3 미구성, 그 외 오류 | 에러 알림 |
| ~~`HOLD_INSUFFICIENT_DATA`~~ | **월간 이벤트에서는 발생하지 않음** | — |
| ~~`FAILED_VALIDATION`~~ | **월간 이벤트에서는 발생하지 않음** | — |

> **두 상태가 월간 이벤트에서 사라진 이유** — 보류(VOC < 10건)와 검증 실패는 둘 다
> **상품 단위** 판정인데, 이벤트는 이제 **월 단위**입니다. 해당 상품은 합본에서 빠지고
> 그 사실이 `notice_message`에 실립니다. 상품 하나가 보류·실패해도 나머지 상품의
> 리포트를 막을 이유가 없으므로 합본은 그대로 나갑니다.
>
> 두 상태는 **`POST /api/v1/reports` (상품 1건 REST 호출)** 에서는 그대로 살아 있습니다 —
> 보류는 `409 Conflict` + 고정 안내 문구, 검증 3회 실패는 `422`로 응답합니다.
> `HOLD_INSUFFICIENT_DATA`는 CS 가이드라인(§4.6)에서도 발생하지 않으므로, **어느
> 이벤트에도 나가지 않는 상태**가 됐습니다.

### `notice_message` — 의미 확장

| 상태 | 08-03 수정안 | **08-04 정정** |
| --- | --- | --- |
| 비 `SUCCESS` | **필수** | 변경 없음 (**필수**) |
| `SUCCESS` | (없음) | **선택** — 합본에서 빠진 상품이 있을 때 채워짐 |

`SUCCESS` + `notice_message` 예시:

```
표본 부족으로 보류된 상품 2개: 플리츠 스커트(P004), 린넨 셔츠(P011) — VOC 10건 미만이라
분석하지 않았습니다. 생성에 실패해 이번 호에서 빠진 상품 1개: 트렌치 코트(P005) —
데이터는 정상이며 운영자가 확인 중입니다.
```

> **왜 필요한가** — 합본 PDF의 **표지(총합 요약) 페이지를 없앴습니다**(2026-08-04 확정).
> 첫 페이지가 곧 첫 상품의 리포트입니다. 표지에 인쇄하던 "보류·실패 상품 안내"를
> 이벤트로 옮기지 않으면 그 정보가 어디에도 남지 않습니다.
>
> **보류와 실패는 반드시 구분해서 표기합니다.** 합치면 VOC 500건인 상품이
> "VOC 10건 미만이라 분석하지 않았다"고 잘못 안내됩니다.

### `pdf_s3_meta` — `created_at` 추가

파일 산출물은 종류를 불문하고 **아래 4종을 반드시** 실어 보냅니다.

| 필드 | 설명 |
| --- | --- |
| `original_file_name` | 원본·표시용 파일명 |
| `new_file_name` | 버킷에 저장한 파일명 |
| **`created_at`** | **신규** — 파일 생성(업로드) 일자 (ISO8601) |
| `file_size_bytes` | 파일 크기 (상한 10MB) |

**회사 구분도 함께 실립니다** (2026-08-06 확정).

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| **`company_id`** | string | **신규·필수** — 경로(`reports/{report_type}/{company_id}/`)에 쓰인 고객사 식별자 |
| **`company_name`** | string \| null | **신규·선택** — 표시용 고객사명 |

S3 경로가 회사 단위로 갈리는데 그 값이 어느 입력 스키마에도 없어, 산출물만 보고는 어느
회사 것인지 알 수 없었습니다. `company_id`를 실어 **메인이 S3 키를 파싱하지 않아도** 되게
합니다. 스키마가 `company_id`와 경로의 회사 구간이 어긋나면 거부합니다.

> ⚠️ **경로에는 `company_id`만 씁니다.** `company_name`은 표시용입니다 — 회사명이 바뀌면
> 경로가 갈라져 이전 산출물을 못 찾습니다.

나머지 필드(`s3_bucket_name` / `s3_file_path` / `s3_full_key` / `presigned_url` /
`presigned_expires_at` / `object_expires_at`)는 08-03 수정안과 동일합니다.

> ⚠️ **`file_extension` 은 제거됐습니다** (인프라 §4, 2026-08-06). "확장자는 파일명에
> `.pdf` 로 고정 포함(이미지와 다르게 DB 별도 컬럼에 저장하지 않음)"이 규칙입니다.

> ⚠️ **`presigned_expires_at` 이 24시간 → 7일로 바뀌었습니다** (인프라 §5).
> AI 노드가 업로드 시점에 최초 1회 발급하고, 메인 서버는 **유효한 동안 재사용**하다가
> 만료됐을 때만 `s3_full_key` 로 재발급합니다(기존 "발송마다 재발급"에서 변경).

### 예시 payload (갱신)

```json
{
  "eventId": "f2b5...",
  "eventType": "ai.report.generated",
  "occurredAt": "2026-08-01T05:10:00.000Z",
  "source": "ai-server",
  "traceId": "trace-002",
  "payload": {
    "report_id": "RPT-202607",
    "report_month": "2026-07",
    "status": "SUCCESS",
    "notice_message": "표본 부족으로 보류된 상품 1개: 플리츠 스커트(P004) — VOC 10건 미만이라 분석하지 않았습니다.",
    "validation_report": null,
    "pdf_s3_meta": {
      "company_id": "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
      "company_name": "주식회사 셀론",
      "s3_bucket_name": "sellon-reports-dev-337658133748-ap-northeast-2-an",
      "s3_file_path": "reports/monthly-report/{company_id}/2026/07/",
      "original_file_name": "monthly-report_202607.pdf",
      "new_file_name": "monthly-report_202607_a3f4c9e2-b7d1-4f2a-9e6c-2a5f8b3d1c7a.pdf",
      "s3_full_key": "reports/monthly-report/{company_id}/2026/07/monthly-report_202607_a3f4c9e2-b7d1-4f2a-9e6c-2a5f8b3d1c7a.pdf",
      "created_at": "2026-08-01T05:10:00.000Z",
      "file_size_bytes": 497142,
      "presigned_url": "https://sellon-reports-dev-337658133748-ap-northeast-2-an.s3.ap-northeast-2.amazonaws.com/...",
      "presigned_expires_at": "2026-08-08T05:10:00.000Z",
      "object_expires_at": "2027-01-28T05:10:00.000Z"
    }
  }
}
```

> **경로 순서가 바뀌었습니다** (2026-08-06) — `report_type` 이 `company_id` 보다 **위**입니다.
> S3 Lifecycle 은 리터럴 prefix 완전 일치만 지원하고 와일드카드를 못 씁니다. 회사가 위에
> 있으면 공통 prefix 가 `reports/` 까지밖에 안 잡혀 월간(180일)과 CS(7일)를 분리할 수
> 없습니다. 지금 순서면 규칙이 **회사 수와 무관하게 2개**로 고정됩니다.
>
> 경로·파일명은 인프라 「S3 파일 구조 규칙 정의」를 따릅니다.
> `{yyyy}/{mm}`·`{yyyyMM}`은 **보고 대상 월**이고, 버킷은 하나이며 문서 종류는
> 프리픽스(`monthly-report/` · `cs-guideline/`)로 가릅니다. presigned TTL은 문서 종류
> 무관 **7일 고정**입니다(SigV4 상한).

---

## §4.6 `ai.guideline.generated` — 멱등 키 형식 교체

| 구분 | `guideline_id` 형식 | 예시 |
| --- | --- | --- |
| 08-03 수정안 | `GD-{탐지일}-{상품ID}` | `GD-20260528-P001` |
| **08-04 정정** | **`alert_id`의 `ALT-` 접두어만 `GD-`로 치환** (alert_id와 **1:1**) | `ALT-20260528-P001-COUPANG`<br>→ `GD-20260528-P001-COUPANG` |

> **왜 바꿨나** — 이상 탐지는 **(상품, 속성, 채널)** 단위로 발화합니다. 기존 형식은 같은
> 날 같은 상품의 서로 다른 알림이 **전부 같은 멱등 키**가 되고, 메인이 upsert를 하므로
> **나중에 도착한 가이드라인이 앞의 것을 조용히 덮어썼습니다**. 재현 확인 후 수정했습니다.

- "생성 시각이 아니라 탐지 시각 기준이라 **재생성해도 같은 ID**"라는 성질은 그대로
  유지됩니다(`alert_id`가 탐지 시각 기준이므로).
- `alert_id` ↔ `guideline_id`가 1:1이므로, 메인은 둘 중 어느 키로도 조인할 수 있습니다.
- `source_payload` JSONB 영구 보관은 변경 없습니다.
- `object_expires_at` = 업로드 **+7일**(기존 24시간에서 연장 — 메일이 운영 MD 승인 뒤에
  나가므로 하루로는 승인 대기 중에 객체가 사라집니다). `presigned_expires_at` 도 7일이라
  두 시각이 같습니다.
- **적재 위치만 바뀝니다** — 전용 버킷 `sellon-temp-reports` 가 아니라 공용 버킷의
  `cs-guideline/` 프리픽스입니다 (인프라 2026-08-05):
  `reports/cs-guideline/{company_id}/{yyyy}/{mm}/cs-guideline_{yyyyMM}_{uuid4}.pdf`
  `{yyyy}/{mm}` 은 **탐지 연월**(`detected_at`)입니다 — 생성 시각이 아니라 탐지 시각이라
  재생성해도 같은 폴더에 떨어집니다.

---

## §5 소비 측 처리 규칙 — 월간 항목 정정

기존 "월간 리포트: 본문 데이터를 저장하지 않습니다" 항목의 **보관 필드 목록**을 아래로
교체합니다.

- **월간 리포트**: 본문 데이터를 저장하지 않습니다.
  ~~`report_id` · `product_group_id` · `report_month` · `s3_full_key` ·
  `object_expires_at`~~ → **`report_id` · `report_month` · `s3_full_key` ·
  `object_expires_at`** 만 보관하면 됩니다. 화면은 presigned URL로 PDF를 띄웁니다.
- **`SUCCESS`여도 `notice_message`를 확인**합니다. 값이 있으면 합본에서 빠진 상품이
  있다는 뜻이므로 리포트 화면에 함께 노출합니다.
- 나머지 항목(멱등 키 upsert, `status` 우선 분기, CS `source_payload` 영구 보관, 링크
  재발급, 재생성 시 고아 객체 처리)은 변경 없습니다.

---

## §6 enum 대조표 — 변경 없음 (주석만 보강)

`CallbackStatus` · `RiskLevel` · `DriftStatus` · `Severity` · `HoldReason` 5종은 그대로입니다.
다만 실제 발생 범위를 아래와 같이 좁혀 적습니다.

| enum | 값 | 실제 발생 |
| --- | --- | --- |
| `CallbackStatus` | `SUCCESS` `HOLD_INSUFFICIENT_DATA` `FAILED_VALIDATION` `FAILED_SIZE_EXCEEDED` `FAILED_ERROR` | 월간 이벤트는 **3종만**(`SUCCESS`/`FAILED_SIZE_EXCEEDED`/`FAILED_ERROR`), CS 이벤트는 `HOLD_INSUFFICIENT_DATA` 제외 4종. 나머지는 REST 응답 전용 (§4.2 참고) |
| `HoldReason` | `INSUFFICIENT_SAMPLE` `EMPTY_CHANNEL` | 월간 PDF 내부(채널쌍 보류)에서만 쓰임 — 이벤트 미노출 |
| `DriftStatus` · `Severity` | — | 월간 PDF 안에서만 쓰임 — 이벤트 미노출 (변경 없음) |

---

## §7 체크리스트 — 리포팅 항목 갱신

- [ ] **S3 버킷 1개 + 프리픽스별 Lifecycle 규칙** (인프라) — **08-03 수정안에서 변경**
  - [ ] 버킷 **1개** (`sellon-reports-dev-337658133748-ap-northeast-2-an`) — 예전의 `sellon-temp-reports` 분리는 폐기
  - [ ] `…/monthly-report/` 프리픽스 — 만료 **180일(6개월)**
  - [ ] `…/cs-guideline/` 프리픽스 — 만료 **1일**
  - [ ] 객체 경로: `reports/{report_type}/{company_id}/{yyyy}/{mm}/` — **report_type 이 위**
  - [ ] ⚠️ Lifecycle 값을 바꾸면 AI 쪽 상수(`MONTHLY_RETENTION_DAYS` /
        `GUIDELINE_RETENTION_HOURS`)도 같이 고쳐야 합니다
- [ ] **AI: `S3_COMPANY_ID` 주입 경로 확정** (**신규**) — 경로가 회사 단위로 갈리는데
      `company_id` 가 어떤 입력 스키마에도 없습니다. 현재는 환경변수 단일 테넌트 가정이며,
      멀티테넌트가 되면 입력 스키마에 넣어야 합니다 (팀 합의 대상)
- [ ] **메인 컨슈머: `ai.report.generated`를 월 1건으로 처리** — `product_group_id` 기준
      분기 코드가 있다면 제거 (**신규**)
- [ ] **메인 컨슈머: `SUCCESS` + `notice_message` 동시 처리** (**신규**)
- [ ] **메인: `guideline_id` 멱등 키 형식 변경 반영** — `alert_id`와 1:1 (**신규**)
- [ ] 메인 컨슈머: `ai.guideline.generated` 수신 + `source_payload` JSONB 저장
- [ ] 메인: presigned URL 재발급 경로 (`s3_full_key` 기준)
- [ ] 메인: `object_expires_at`을 화면 "다운로드 기한"으로 표시
- [ ] AI: `ai.report.generated` · `ai.guideline.generated` 발행부 구현
      (현재 REST 응답으로만 반환)

---

## 운영 참고 — 월간 배치 타임라인

이벤트 1건이 언제 나가는지에 대한 전제입니다.

| 단계 | 시각 | 내용 |
| --- | --- | --- |
| 집계 대상 구간 | 전월 **1일 00:00:00 ~ 말일 23:59:59.99** | — |
| 집계 시작 | 당월 **1일 00:00** | 원본 DB → `MonthlyReportInput` |
| 생성·S3 적재 완료 | 당월 **1일 08:00**까지 | 이 시점에 **`ai.report.generated` 1건** 발행 |

- 동시성 상한을 건 워커 큐로 처리하며, **상품 단위로 실패를 격리**합니다.
- 보류 게이트(VOC < 10건)는 LLM 슬롯을 점유하지 않아 **지연 감속기** 역할을 합니다.