# 메시지 큐 컨벤션 — 보고서 생성 관련 수정안 (2026-08-03, 리포팅/용준)

> 노션 「메시지 큐 컨벤션 정의」의 **리포팅 관련 절만** 고쳐 쓴 것입니다. 다른 절
> (§0·§1·§2.1·§4.1·§4.3·§4.5)은 손대지 않았습니다.
> 정본은 `app/core/schemas.py`이고, 아래 값은 전부 현재 코드에서 확인한 것입니다.

수정 요약

| 절 | 조치 |
| --- | --- |
| §2 이벤트 종류 | `ai.guideline.generated` 행 추가 |
| §3 payload 설계 원칙 | 멱등 키에 `guideline_id` 추가 |
| §4.2 `ai.report.generated` | **payload 축소** + `status` 5종 추가 + `pdf_s3_meta` 갱신 |
| §4.6 (신설) `ai.guideline.generated` | CS 가이드라인 이벤트 정의 |
| §5 소비 측 처리 규칙 | 월간/CS 저장 방식 분리, 상태별 분기 |
| §6 enum 대조표 | 리포팅 전용 enum 5종 추가 |
| §7 체크리스트 | S3 버킷 2개 + Lifecycle 항목 추가 |

---

## §2 이벤트 종류 & 라우팅 키 — 행 추가

기존 표에 아래 한 줄을 추가합니다. 바인딩이 `ai.#`라 **CRD는 그대로**입니다.

| **이벤트** | **방향** | **라우팅 키** | **소비 큐** | **소비자** |
| --- | --- | --- | --- | --- |
| CS 가이드라인 생성 완료 | AI → 메인 | `ai.guideline.generated` | `main.inbound` | 메인 서버 |

---

## §3 Payload 설계 원칙 — 멱등 키 한 줄 수정

- 멱등성 키: `alert_id` / `report_id` / **`guideline_id`** / `recommendation_id` 기준으로 upsert.

---

## §4.2 `ai.report.generated` — 수정

> **⚠️ 이번 결정으로 payload가 줄었습니다 (2026-08-03 확정).**
> 월간 리포트는 **데이터를 DB에 적재하지 않고 PDF만 S3에 올린 뒤 링크로 조회**합니다.
> **UI는 PDF 뷰어로 확정**되었으므로, 저장하지도 렌더링하지도 않을 본문 데이터를 큐에
> 흘릴 이유가 없어 `aspect_summaries` · `channel_divergence_cause` ·
> `cause_analysis_results` · `recommended_actions`를 **payload에서 뺐습니다.**
> 이 내용은 전부 PDF 안에 표·차트로 들어갑니다.

| **필드** | **타입** | **설명** |
| --- | --- | --- |
| report_id | string | 보고서 고유 ID. **멱등 키**. 형식 `RPT-{YYYYMM}-{상품코드}` — 예: `RPT-202607-P001` ⚠️ 기존 문서 예시(`RPT-P001-202607`)와 순서가 다릅니다 |
| product_group_id | string | 마스터 상품 그룹 ID. 리포트는 **상품별로** 생성됩니다 |
| report_month | string | 보고서 연월 `YYYY-MM` |
| status | enum | `SUCCESS` \| `HOLD_INSUFFICIENT_DATA` \| `FAILED_VALIDATION` \| `FAILED_SIZE_EXCEEDED` \| `FAILED_ERROR` — **신규**. 아래 표 참고 |
| pdf_s3_meta | object \| null | `SUCCESS`일 때만 non-null. 상세는 아래 |
| notice_message | string \| null | 비 `SUCCESS`일 때 사용자 안내 문구. 화면에 그대로 노출 |
| validation_report | object \| null | `FAILED_VALIDATION`일 때 실패 사유 목록(운영자 확인용) |

**`status` — 성공만 오는 게 아닙니다 (신규)**

| 값 | 발생 조건 | 메인이 할 일 |
| --- | --- | --- |
| `SUCCESS` | 검증 통과 + PDF/S3 완료 | 링크 저장, PDF 첨부 메일 발송 |
| `HOLD_INSUFFICIENT_DATA` | 월간 VOC < 10건 | **실패가 아닙니다.** 고정 안내 문구를 화면에 표시. LLM 추론을 아예 하지 않음 |
| `FAILED_VALIDATION` | 생성 결과가 검증 3회 연속 실패 | 자동 발송 중단 + 운영자 알림 |
| `FAILED_SIZE_EXCEEDED` | PDF > 10MB | 발송 중단. S3 업로드 이전에 차단됨 |
| `FAILED_ERROR` | 그 외 | 에러 알림 |

> `HOLD`가 이벤트로 오지 않으면 메인은 **"아직 안 왔다"와 "표본 부족으로 보류됐다"를
> 구분할 수 없습니다.** 화면에 안내를 띄우려면 이 필드가 필요합니다.

**`pdf_s3_meta` — 필드 2개 추가·값 정정**

| **필드** | **설명** |
| --- | --- |
| s3_bucket_name | **`sellon-reports`** (월간 전용, 6개월 보존) |
| s3_file_path / new_file_name / s3_full_key | `s3_full_key = s3_file_path + new_file_name` (스키마가 강제) |
| file_extension / file_size_bytes | 상한 10MB |
| presigned_url | 다운로드·미리보기 URL |
| **presigned_expires_at** | **신규** — 링크 만료(발급 +**7일**). ⚠️ 기존 문서의 "1시간 유효"는 정정입니다. 만료 후에는 `s3_full_key`로 **백엔드가 재발급**합니다(SigV4 상한이 7일이라 영구 링크는 불가) |
| **object_expires_at** | **신규** — S3 Lifecycle 자동 삭제 시각 = **다운로드 가능 기한**. 월간은 생성 +**6개월** |

> `presigned_expires_at ≤ object_expires_at`은 스키마가 검증합니다. 객체가 사라진 뒤에도
> 살아있는 링크는 "받을 수 있다"는 잘못된 안내가 되기 때문입니다.

> **⚠️ 월간 리포트는 6개월 뒤 영구 소실됩니다.** 원본 데이터를 어디에도 보관하지 않는
> 구조라(=PDF가 유일 산출물) 재생성 경로가 없습니다.
> **6개월 경과 리포트는 조회하지 않기로 확정**(2026-08-03)했으므로 아카이빙은 두지
> 않습니다. 다만 메인은 `object_expires_at`이 지난 리포트를 **목록에서 내리거나
> "보관 기간 만료"로 표시**해야 합니다 — 링크만 남으면 깨진 다운로드가 됩니다.

```json
{
  "eventId": "f2b3...",
  "eventType": "ai.report.generated",
  "occurredAt": "2026-08-01T05:10:00.000Z",
  "source": "ai-server",
  "traceId": "trace-002",
  "payload": {
    "report_id": "RPT-202607-P001",
    "product_group_id": "P001",
    "report_month": "2026-07",
    "status": "SUCCESS",
    "notice_message": null,
    "validation_report": null,
    "pdf_s3_meta": {
      "s3_bucket_name": "sellon-reports",
      "s3_file_path": "reports/monthly/2026/08/",
      "original_file_name": "monthly_2026-07.pdf",
      "new_file_name": "monthly_P001_2026-07_20260801_a1b2c3d4.pdf",
      "s3_full_key": "reports/monthly/2026/08/monthly_P001_2026-07_20260801_a1b2c3d4.pdf",
      "file_extension": "pdf",
      "file_size_bytes": 43622,
      "presigned_url": "https://sellon-reports.s3.amazonaws.com/...",
      "presigned_expires_at": "2026-08-08T05:10:00Z",
      "object_expires_at": "2027-01-28T05:10:00Z"
    }
  }
}
```

보류 예시:

```json
{
  "payload": {
    "report_id": "RPT-202607-P042",
    "product_group_id": "P042",
    "report_month": "2026-07",
    "status": "HOLD_INSUFFICIENT_DATA",
    "notice_message": "해당 상품의 월간 CS 표본 수는 부족으로 인하여 보고서 생성이 보류되었습니다. 데이터가 누적되면 분석이 재개됩니다.",
    "pdf_s3_meta": null
  }
}
```

---

## §4.6 `ai.guideline.generated` (신설) — CS 가이드라인 생성 완료

> 기존 문서의 "CS 가이드라인도 AI 산출물로 존재합니다. 별도 이벤트가 필요한지는 리포팅
> 담당 확인 후 추가하겠습니다"에 대한 답입니다 — **필요합니다.** 월간 리포트와 생명주기가
> 다릅니다.

| | 월간 리포트 | CS 가이드라인 |
| --- | --- | --- |
| 트리거 | 월 1회 배치(1일) | **알림 1건마다** |
| 멱등 키 | `report_id` | `guideline_id` |
| 소비자 동작 | 링크 저장 → 화면 표시 | **CS팀 알림·메일 발송** |
| 데이터 적재 | 하지 않음 | **DB에 저장(JSONB)** |
| 보류 상태 | 있음 | 없음 |

| **필드** | **타입** | **설명** |
| --- | --- | --- |
| guideline_id | string | **멱등 키**. 형식 `GD-{탐지일 YYYYMMDD}-{상품그룹}` — 예: `GD-20260528-P001`. 생성 시각이 아니라 **탐지 시각** 기준이라 재생성해도 같은 ID |
| alert_id | string | 원본 알림 ID (`ai.anomaly.analyzed`의 `alert_id`와 동일) |
| status | enum | §4.2와 같은 5종. 단 `HOLD_INSUFFICIENT_DATA`는 발생하지 않음 |
| pdf_s3_meta | object \| null | `SUCCESS`일 때만. 버킷 **`sellon-temp-reports`**, `object_expires_at` = 업로드 +**24시간**, `presigned_expires_at` = +24시간(객체 수명과 동일) |
| **source_payload** | object | **필수.** 입력 JSON + 출력 JSON 원본. 메인이 **PostgreSQL JSONB에 영구 보관**하며, PDF가 24시간 뒤 사라져도 이 원본으로 재컴파일합니다 |
| notice_message | string \| null | 비 `SUCCESS`일 때 안내 문구 |
| validation_report | object \| null | `FAILED_VALIDATION` 사유 |

`source_payload.output` 구조(= `CSGuidelineOutput`)

| **필드** | **설명** |
| --- | --- |
| summary | `{issue_title, risk_level, key_metric_text}` — `risk_level` = `CRITICAL` \| `WARNING` \| `NORMAL` |
| root_cause_summary | 최다 원인 지분율 요약 |
| standard_guideline | `{core_message, draft_reply, key_talking_points[]}` — 상담원이 그대로 쓰는 표준 응대 세트 |
| ops_action_guide | 운영팀 조치 지침 |
| inquiry_specific_guides | `[{item_id, recommended_point}]` — `item_id`는 입력 `linked_inquiries` 안의 문의만 (검증됨) |

실측 payload 크기: **약 2.3KB**(문의 2건 기준). Claim-Check 없이 그대로 실어 보냅니다.

```json
{
  "eventId": "9d10...",
  "eventType": "ai.guideline.generated",
  "occurredAt": "2026-05-28T10:31:00.000Z",
  "source": "ai-server",
  "traceId": "trace-001",
  "payload": {
    "guideline_id": "GD-20260528-P001",
    "alert_id": "ALT-20260528-0001",
    "status": "SUCCESS",
    "pdf_s3_meta": {
      "s3_bucket_name": "sellon-temp-reports",
      "s3_file_path": "reports/cs_guidelines/2026/05/",
      "original_file_name": "cs_guidelines_ALT-20260528-0001.pdf",
      "new_file_name": "cs_guidelines_P001_ALT-20260528-0001_20260528_9f8e7d6c.pdf",
      "s3_full_key": "reports/cs_guidelines/2026/05/cs_guidelines_P001_ALT-20260528-0001_20260528_9f8e7d6c.pdf",
      "file_extension": "pdf",
      "file_size_bytes": 28114,
      "presigned_url": "https://sellon-temp-reports.s3.amazonaws.com/...",
      "presigned_expires_at": "2026-05-29T10:31:00Z",
      "object_expires_at": "2026-05-29T10:31:00Z"
    },
    "source_payload": {
      "input": { "alert_id": "ALT-20260528-0001", "...": "CSGuidelineInput 원본" },
      "output": {
        "guideline_id": "GD-20260528-P001",
        "alert_id": "ALT-20260528-0001",
        "summary": {
          "issue_title": "쿠팡 색상 불만 급증 대응 가이드",
          "risk_level": "WARNING",
          "key_metric_text": "색상 부정 비율이 5%에서 13%로 8%p 상승했습니다 (문의 200건 기준)."
        },
        "root_cause_summary": "사진_색감_오차 18건 / 전체 26건 (69%)",
        "standard_guideline": {
          "core_message": "촬영 조명 차이로 실물 색상이 다르게 보일 수 있음을 안내하고 무상 교환을 접수합니다.",
          "draft_reply": "안녕하세요 고객님, 색상 차이로 불편을 드려 죄송합니다 ...",
          "key_talking_points": ["조명 차이 정중히 안내", "고객 과실 암시 표현 금지"]
        },
        "ops_action_guide": "쿠팡 대표 이미지의 색보정 상태를 점검하고 원본 기준으로 재등록하세요.",
        "inquiry_specific_guides": [
          { "item_id": "INQ-000412", "recommended_point": "사과 후 무상 회수 접수를 우선 안내하세요." }
        ]
      }
    },
    "notice_message": null,
    "validation_report": null
  }
}
```

---

## §5 소비 측 처리 규칙 — 리포팅 행 보강

기존 "메인 서버(`main.inbound`)" 칸에 아래를 덧붙입니다.

- 멱등 키에 **`guideline_id` 추가** (`alert_id` / `report_id` / `guideline_id` 기준 upsert).
- **`status`로 먼저 분기합니다.** `SUCCESS`가 아니면 `pdf_s3_meta`가 `null`이므로
  링크 저장·메일 발송을 시도하면 안 됩니다. `HOLD_INSUFFICIENT_DATA`는 실패가 아니라
  정상 상태이며 `notice_message`를 화면에 그대로 표시합니다.
- **월간 리포트**: 본문 데이터를 저장하지 않습니다. `report_id` · `product_group_id` ·
  `report_month` · `s3_full_key` · `object_expires_at`만 보관하면 됩니다. 화면은
  presigned URL로 PDF를 띄웁니다.
- **CS 가이드라인**: `source_payload`를 **JSONB 컬럼에 영구 보관**합니다. PDF는 24시간 뒤
  사라지므로, 이후 다운로드 요청은 이 원본으로 재컴파일해야 합니다.
- **링크 재발급**: `presigned_expires_at`이 지난 요청은 `s3_full_key`로 새 presigned URL을
  발급합니다. 단 `object_expires_at`이 지났으면 객체가 이미 삭제된 상태입니다 —
  월간은 재생성 불가, CS는 `source_payload`로 재컴파일.
- **재생성 시 고아 객체**: `report_id`가 결정적이라 같은 ID로 upsert되지만 PDF 키에는
  uuid suffix가 붙어 이전 객체가 남습니다. **항상 최신 이벤트의 `s3_full_key`만 신뢰**하면
  되고, 남은 객체는 Lifecycle이 정리합니다.

---

## §6 enum 대조표 — 리포팅 전용 5종 추가

아래는 **값 자체가 영문**이라 한글 매핑이 필요 없습니다(`Channel`·`Source`와 같은 부류).

| **enum** | **값** | **쓰이는 곳** |
| --- | --- | --- |
| CallbackStatus | `SUCCESS` `HOLD_INSUFFICIENT_DATA` `FAILED_VALIDATION` `FAILED_SIZE_EXCEEDED` `FAILED_ERROR` | §4.2 · §4.6 `status` |
| RiskLevel | `CRITICAL` `WARNING` `NORMAL` | CS 가이드라인 `summary.risk_level` |
| DriftStatus | `NORMAL` `RISK` | 월간 내부 판정(이벤트 미노출, PDF에만 표기) |
| Severity | `SAFE` `CAUTION` `CRISIS` | 채널 분열 단계(이벤트 미노출, PDF에만 표기) |
| HoldReason | `INSUFFICIENT_SAMPLE` `EMPTY_CHANNEL` | 채널쌍 판정 보류 사유(이벤트 미노출) |

> `DriftStatus` · `Severity` · `HoldReason`은 월간 PDF 안에서만 쓰이고 이벤트 payload에는
> 나가지 않습니다. 메인에서 구현할 필요는 없고, 나중에 화면에서 단계를 표시하게 되면
> 그때 payload에 올리면 됩니다.

---

## §7 체크리스트 — 리포팅 항목 추가

- [ ] **S3 버킷 2개 + Lifecycle 규칙** (인프라)
  - [ ] `sellon-reports` — 월간 리포트, **만료 180일(6개월)**
  - [ ] `sellon-temp-reports` — CS 가이드라인, **만료 24시간**
  - [ ] ⚠️ Lifecycle 값을 바꾸면 AI 쪽 상수(`MONTHLY_RETENTION_DAYS` / `GUIDELINE_RETENTION_HOURS`)도 같이 고쳐야 합니다. 어긋나면 이미 지워진 파일을 "받을 수 있다"고 안내하게 됩니다
- [ ] 메인 컨슈머: `ai.report.generated` **status 5종 분기** 처리
- [ ] 메인 컨슈머: `ai.guideline.generated` 수신 + `source_payload` JSONB 저장
- [ ] 메인: presigned URL 재발급 경로 (`s3_full_key` 기준)
- [ ] 메인: `object_expires_at`을 화면 "다운로드 기한"으로 표시
- [ ] AI: `ai.report.generated` · `ai.guideline.generated` 발행부 구현 (현재 REST 응답으로만 반환)

---

## 확정된 사항 (2026-08-03)

- **월간 화면 = PDF 뷰어.** §4.2의 축소 payload로 확정합니다. 본문 데이터는 이벤트에
  싣지 않고 PDF 안에만 둡니다.
- **6개월 경과 리포트 조회 불필요.** 아카이빙·원본 보관을 두지 않습니다. 만료 = 영구
  소실이며, 메인은 `object_expires_at` 경과분을 목록에서 내리거나 만료 표시합니다.

## 남은 확인 사항

1. **`feedback.report.created`의 AI측 소비**는 아직 미구현입니다. 피드백을 무엇에 쓸지
   (프롬프트 개선 데이터셋 / 재생성 트리거 / 단순 저장)가 정해지면 붙이겠습니다.
   `rating`·`feedbackType`은 현재 합성 케이스로만 재고 있는 **생성 품질의 유일한 실사용
   지표**가 될 수 있어, 축적만 먼저 시작해도 좋습니다.
2. **AI측 이벤트 발행부가 아직 없습니다.** 현재는 REST 응답으로 `GenerationCallback`을
   돌려주기만 합니다(§7 체크리스트 마지막 항목). 발행 시점·재시도 정책이 정해지면
   `app/reporting/callback.py`에 전송 함수를 붙이겠습니다.
