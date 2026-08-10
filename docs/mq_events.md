# 메시지 큐 이벤트 계약

***AI 노드 ↔ 메인 노드(Spring Boot) RabbitMQ 인터페이스***

> 🟢 **문서 상태: [확정 — 구현 완료, 접속 정보 대기]** 발행(`app/core/mq.py`)·수신
> (`app/core/mq_consumer.py`)·실행 진입점(`app/consumer.py`)이 들어와 있고 로컬 브로커로
> 양방향 검증까지 마쳤다. 다만 **운영 접속 정보(C1)와 `MQ_COMPANY_ID` 가 아직 없어
> `MQ_ENABLED=false` 로 꺼둔 상태**다. 정본 스키마는 `app/core/schemas.py`.
> 남은 항목은 §12 구현 현황 참고.

---

## 1. 개요

- **전송 수단:** RabbitMQ. 단일 Topic Exchange `app.events` 위에서 라우팅 키로 분기.
- **Kafka가 아닌 이유:** 유입 구간(Mock Producer → 메인 서버)만 Kafka다. AI 산출물은
  생산자·소비자가 같은 프로세스이고 하루 1회 배치라 fan-out 소비자가 없다.
- **통신은 비대칭이다.** AI가 결과를 밀고(`ai.#`), 메인은 **사용자 피드백 2종만** 되돌린다
  (`feedback.#`). 메인 → AI 방향의 "연산해달라" 요청 이벤트는 존재하지 않는다.

```
AI 노드 ──ai.#──▶ app.events ──▶ main.inbound ──▶ 메인 서버
메인 서버 ──feedback.#──▶ app.events ──▶ ai.inbound ──▶ AI 노드
```

---

## 2. 이벤트 목록

| 이벤트 | 방향 | 라우팅 키 | 멱등 키 | AI측 담당 |
|---|---|---|---|---|
| 이상 탐지 + 개선안 생성 완료 | AI → 메인 | `ai.anomaly.analyzed` | `alert_id` | 서영(탐지) + 지인(개선안) |
| 보고서 생성 완료 | AI → 메인 | `ai.report.generated` | `report_id` | 용준 |
| CS 가이드라인 생성 완료 | AI → 메인 | `ai.guideline.generated` | `guideline_id` | 용준 |
| 대시보드 연산 완료 | AI → 메인 | `ai.dashboard.computed` | — | **해당 없음** (§7) |
| 보고서 피드백 등록 | 메인 → AI | `feedback.report.created` | `feedbackId` | 용준 |
| 개선안 승인/반려 (HITL) | 메인 → AI | `feedback.recommendation.reviewed` | `recommendation_id` | 지인 |

**라우팅 키 개명 이력:** `ai.anomaly.detected` → `ai.anomaly.analyzed` (2026-08-03).
탐지만이 아니라 원인분석·개선안까지 끝난 상태라 이름을 내용에 맞췄다. 바인딩이 `ai.#`라
큐 CRD는 안 고쳐도 된다.

### 2.1 큐·바인딩

`main.inbound`는 신규 생성(quorum, durable, DLX `app.events.dlx`, delivery-limit 5, TTL 24h),
바인딩 키 `ai.#`. `ai.inbound`는 **기존 큐에 `feedback.#` 바인딩만 추가**한다.
CRD YAML 원문은 백엔드 「메시지 큐 컨벤션 정의」 §2.1.

🔴 **큐·바인딩·exchange 는 전부 인프라(Messaging Topology Operator CRD)가 만든다 —
AI 는 선언하지 않는다** (2026-08-06 §2.1 확인). 우리가 다른 인자로 `declare` 하면
`PRECONDITION_FAILED` 로 거부당해 발행도 수신도 못 뜬다. 그래서 `app/core/mq.py`
`resolve_exchange()` / `mq_consumer.py` `resolve_queue()` 는 기본이 **존재 확인만**
(passive)이고, `MQ_DECLARE_TOPOLOGY=true` 일 때만 우리가 만든다 — 로컬 브로커 전용이다.
바인딩(`queue.bind`)도 같은 이유로 로컬에서만 건다.

⚠️ **vhost 는 `/app` 이다** (CRD 세 군데 모두). 로컬 docker-compose 는 `/` 를 쓴다.

---

## 3. Envelope 공통 규약

```json
{
  "eventId": "b3e1c9a0-1234-4a11-9c22-abcdef123456",
  "eventType": "ai.anomaly.analyzed",
  "occurredAt": "2026-08-01T05:10:00.000Z",
  "source": "ai-server",
  "traceId": "trace-001",
  "companyId": "SLN-xxxxxxxxxx",
  "payload": { }
}
```

- **`companyId` 는 배포마다 고정값이다** (백엔드 §3, 2026-08-06 추가). 회사 구분용이라
  "하드코딩으로 박아두라"는 요청이었고, 우리는 `MQ_COMPANY_ID` 설정으로 받는다.
  **비어 있으면 발행 자체를 막는다**(`MqConfigError`) — 빈 값으로 나가면 백엔드 DB 에
  회사 미상 행이 쌓이는데 나중에 어느 회사 것인지 복구할 단서가 없다.
  ⚠️ **실제 값은 아직 못 받았다.**
- **Envelope은 camelCase, payload 안은 snake_case.** 우리 Pydantic 정본·평가 스크립트가
  필드명으로 join하기 때문. 변환은 메인 쪽 자유.
- **발행 단위 = 알림 1건당 메시지 1개.** 배치가 알림 20건을 만들면 메시지 20개.
  배열로 묶지 않는다 — 멱등 키가 건별이고, 묶으면 1건 실패로 전체가 재처리된다.
  개선안 생성이 건당 5~20초라 **알림+개선안 한 쌍이 완성될 때마다** 발행한다.
- **`traceId` = 배치 1회 단위.** 한 배치가 만든 메시지 20개는 같은 `traceId`를 공유한다.
  장애 추적 단위가 "이 알림 하나"보다 "오늘 배치 전체"인 경우가 많아서. 건별 추적은 `alert_id`.
- **⚠️ `occurredAt` ≠ `payload.detected_at`.** `detected_at`은 탐지가 알림을 만든 시각,
  `occurredAt`은 개선안까지 만들어 발행한 시각. 둘이 몇 분씩 벌어진다.
  **화면의 "탐지 시각"은 `payload.detected_at`을 쓸 것.**
- **멱등성:** 같은 이벤트가 재전달돼도 결과가 같아야 한다. 소비 측이 멱등 키로 upsert한다
  → AI가 `recommendation_id`를 매번 새로 만들어도 중복 저장이 안 생긴다.
- **Claim-Check(`contentRef`) 안 쓴다.** AI측 NoSQL이 없고(Chroma는 벡터DB) payload도
  크지 않다. 리포트 본문만 예외로 S3 PDF(`pdf_s3_meta`).

---

## 4. `ai.anomaly.analyzed`

`DetectionAlert` 전 필드 + 개선안 1건. 필드 정의 정본은 `docs/detection_schema.md`와
`app/core/schemas.py`.

### 4.1 payload 최상위

| 필드 | 타입 | 설명 |
|---|---|---|
| `alert_id` | string | **멱등 키.** 예: `ALT-20260528-0001` |
| `detected_at` | string (ISO 8601) | 탐지 시각 |
| `updates_alert_id` | string \| null | 갱신 알림일 때 원본 alert_id. 신규면 null |
| `product_group_id` | string | 상품 그룹 ID (메인 서버 상품 매핑 산출물) |
| `channel` | enum | `COUPANG` \| `NAVER` \| `ZIGZAG` \| `ALL` (전역형·잠정전역형은 ALL) |
| `window_start` / `window_end` | string (date) | 판정에 쓴 현재 윈도우 (최근 7일) |
| `verdict` | enum | 정상 \| 편중형 \| 전역형 \| 잠정 전역형 \| 구분불가 |
| `significant_channels` | string[] | 유의 판정된 채널 |
| `excluded_channels` | string[] | 표본 부족(<10)으로 판정 제외된 채널 |
| `channel_rates` | object[] | `[{channel, rate, excluded}]`. 탐지 당시 `stats.source`·`main_aspect`·현재 7일 기준 채널별 부정률(0~1). 관측 표본이 없으면 rate는 null |
| `main_aspect` | enum | 색상 \| 사이즈 \| 소재 \| 파손 \| 오배송 \| 기타 |
| `sub_aspects` | object[] | `[{aspect, delta, recommended_action}]`. 없으면 `[]` |
| `stats` | object | `{source, cur_rate, past_rate, delta, p_value, bh_significant, cur_total}` |
| `source_signals` | object | `{cs, review, interpretation}`. cs/review는 `true`\|`false`\|`null` |
| `root_cause` | object \| null | `{label, count, total, consistent}` |
| `detection_confidence` | enum | 높음 \| 중간 \| 낮음 \| 해당없음 |
| `scope_in` | bool | 개선안 생성 가능한 aspect인지 (색상·사이즈·소재=true) |
| `recommended_action` | enum | §6 대조표 참고. 대시보드가 이 값을 그대로 셀러에게 노출 |
| `evidence` | object | `{inquiry_ids, linked_change_id}` — 원인분류에 투입된 문의 ID 전체 |
| `recommendation` | object \| null | **개선안 1건.** §4.2 |

**주의 3건**

- **`p_value`는 셀러 화면 노출 금지.** 대시보드는 rate 변화 + `bh_significant` 배지만 표시.
- **`source_signals`의 `null` ≠ `false`.** `null` = 표본 부족 보류(판정 안 함),
  `false` = 판정했으나 미발화. 화면에서 합치면 안 된다.
- **`root_cause`가 null인 판정이 있다.** 전역형·구분불가·스코프밖은 원인 분류를 안 한다.

### 4.2 `recommendation` 객체 (Agent3 산출물)

`recommended_action == "개선안 생성"`일 때만 non-null. 그 외 6종 조치는 항상 null.
정본은 `docs/recommenation_schema.md`.

| 필드 | 타입 | 설명 |
|---|---|---|
| `recommendation_id` | string | 예: `REC-a1b2c3d4e5f6` |
| `alert_id` | string | 상위 알림 ID (payload 최상위와 동일 값) |
| `created_at` | string (ISO 8601) | 생성 시각 |
| `proposal` | object \| null | `{type, target_field, current_text, proposed_text, rationale, detailpage_grounded}`<br>`type` = `copy_draft`(상세페이지 문구) \| `image_guide`(촬영 가이드) |
| `citations` | object[] | `[{inquiry_id, quote}]`. `evidence.inquiry_ids` 밖은 들어갈 수 없음 |
| `evaluator` | object | `{passed, attempts, checks{grounding, consistency, actionability}, failure_reason}` |
| `similar_case` | string \| null | 참고한 과거 사례 |
| `recommendation_confidence` | enum \| null | 높음 \| 중간 \| 낮음. 탐지 확신도로 상한 캐핑됨 |
| `confidence_reason` | string \| null | 확신도 산출 근거 문구 |
| `capped_by_detection` | bool | 탐지 확신도로 상한이 깎였는지 |
| `hitl_status` | enum | **발행 시점엔 항상 `대기`** |
| `hitl_feedback` | object \| null | 발행 시점엔 null. §8 이벤트로 채워짐 |

**⚠️ `citations`는 현재 항상 빈 배열이다.** 인용 원문을 채우려면 원본 DB의 `cs`·`reviews`를
`evidence.inquiry_ids`로 재조회해야 하는데, `ClassifiedItem.item_id`와 두 테이블 PK의 연결이
아직 확인 안 됐다. 가짜로 채우지 않고 비워 둔다.

### 4.3 예시

```json
{
  "eventId": "e6a1...",
  "eventType": "ai.anomaly.analyzed",
  "occurredAt": "2026-08-01T05:00:00.000Z",
  "source": "ai-server",
  "traceId": "trace-001",
  "payload": {
    "alert_id": "ALT-20260528-0001",
    "detected_at": "2026-05-28T10:30:00",
    "updates_alert_id": null,
    "product_group_id": "P001",
    "channel": "COUPANG",
    "window_start": "2026-05-22",
    "window_end": "2026-05-28",
    "verdict": "편중형",
    "significant_channels": ["COUPANG"],
    "excluded_channels": [],
    "channel_rates": [
      { "channel": "COUPANG", "rate": 0.13, "excluded": false },
      { "channel": "NAVER", "rate": 0.05, "excluded": false },
      { "channel": "ZIGZAG", "rate": null, "excluded": true }
    ],
    "main_aspect": "색상",
    "sub_aspects": [
      { "aspect": "파손", "delta": 0.07, "recommended_action": "물류 점검 권장" }
    ],
    "stats": {
      "source": "cs",
      "cur_rate": 0.13,
      "past_rate": 0.05,
      "delta": 0.08,
      "p_value": 0.00013,
      "bh_significant": true,
      "cur_total": 200
    },
    "source_signals": {
      "cs": true,
      "review": false,
      "interpretation": "CS 선행 신호 — 리뷰는 시차로 미반영 가능"
    },
    "root_cause": {
      "label": "사진_색감_오차",
      "count": 14,
      "total": 20,
      "consistent": true
    },
    "detection_confidence": "높음",
    "scope_in": true,
    "recommended_action": "개선안 생성",
    "evidence": {
      "inquiry_ids": ["INQ-000412", "INQ-000415"],
      "linked_change_id": "CHG-0009"
    },
    "recommendation": {
      "recommendation_id": "REC-a1b2c3d4e5f6",
      "alert_id": "ALT-20260528-0001",
      "created_at": "2026-05-28T10:30:12Z",
      "proposal": {
        "type": "copy_draft",
        "target_field": "상세설명",
        "current_text": "상세페이지 원문에서 그대로 인용한 현재 문구",
        "proposed_text": "모니터 환경에 따라 실제 색상과 차이가 있을 수 있습니다 ...",
        "rationale": "사진 색감이 실물과 다르다는 문의가 20건 중 14건 ...",
        "detailpage_grounded": true
      },
      "citations": [],
      "evaluator": {
        "passed": true,
        "attempts": 1,
        "checks": { "grounding": true, "consistency": true, "actionability": true },
        "failure_reason": null
      },
      "similar_case": null,
      "recommendation_confidence": "높음",
      "confidence_reason": "상세페이지 근거 있음 + 유사사례 없음",
      "capped_by_detection": false,
      "hitl_status": "대기",
      "hitl_feedback": null
    }
  }
}
```

---

## 5. `ai.report.generated`

**월 1건 발행.** 상품별이 아니다.

| 필드 | 타입 | 설명 |
|---|---|---|
| `report_id` | string | **멱등 키.** 형식 `RPT-{YYYYMM}` — 예: `RPT-202607`. 같은 달을 다시 돌려도 같은 ID |
| `report_month` | string | `YYYY-MM` |
| `status` | enum | `SUCCESS` \| `FAILED_SIZE_EXCEEDED` \| `FAILED_ERROR` **3종만** |
| `pdf_s3_meta` | object \| null | `SUCCESS`일 때만 non-null |
| `notice_message` | string \| null | 안내 문구. **`SUCCESS`일 때도 채워질 수 있다** |
| `validation_report` | object \| null | 실패 사유 목록(운영자 확인용) |

> ⚠️ **`product_group_id`는 payload에 없다.** 상품별 추적 ID(`RPT-202607-P001`)는 AI 배치
> 로그에만 남고 이벤트로 나가지 않는다. 백엔드 문서 §4.2의 JSON 예시에는 아직 구버전
> (`RPT-202607-P001` + `product_group_id` + `HOLD_INSUFFICIENT_DATA`)이 남아 있는데,
> **본문 표가 맞고 예시가 뒤처진 것**이다. 이 문서 기준으로 구현할 것.

### `status` 발생 조건

| 값 | 조건 | 메인이 할 일 |
|---|---|---|
| `SUCCESS` | 상품 1개 이상 수록 + PDF/S3 완료 | 링크 저장, PDF 첨부 메일 발송. `notice_message` 있으면 함께 표시 |
| `FAILED_SIZE_EXCEEDED` | 합본 PDF > 10MB | 발송 중단. S3 업로드 이전에 차단 |
| `FAILED_ERROR` | 수록할 상품이 하나도 없음, S3 미구성, 그 외 오류 | 에러 알림 |

**`HOLD_INSUFFICIENT_DATA`·`FAILED_VALIDATION`은 월간 이벤트에 나오지 않는다.** 둘 다
**상품 단위** 판정인데 이벤트는 월 단위이기 때문. 해당 상품은 합본에서 빠지고 그 사실이
`notice_message`에 실린다. 두 상태는 `POST /api/v1/reports`(상품 1건 REST)에서는 살아 있다
— 보류는 `409`, 검증 3회 실패는 `422`.

**보류와 실패는 반드시 구분해 표기한다.** 합치면 VOC 500건인 상품이 "VOC 10건 미만이라
분석하지 않았다"고 잘못 안내된다.

> 🚨 **`SUCCESS`인데 `notice_message`가 채워져 나가는 경우가 있다 — 무시하면 안 된다.**
> 합본 PDF의 **표지(총합 요약) 페이지를 없앴기 때문이다**(2026-08-04 확정). 첫 페이지가 곧
> 첫 상품의 리포트라, 표지에 인쇄하던 "보류·실패로 이번 호에서 빠진 상품 안내"를 이
> 필드로 옮겼다. **소비 측이 "`SUCCESS`면 `notice_message` 무시"로 구현하면 셀러는 자기
> 상품이 왜 리포트에 없는지 알 방법이 어디에도 없다.**
> `app/core/schemas.py`의 필드 description이 아직 "비 `SUCCESS`일 때 필수"로만 되어 있어
> 오해 소지가 있다 — 읽을 때 이 문서를 기준으로 할 것.
>
> 예시:
> ```
> 표본 부족으로 보류된 상품 2개: 플리츠 스커트(P004), 린넨 셔츠(P011) — VOC 10건 미만이라
> 분석하지 않았습니다. 생성에 실패해 이번 호에서 빠진 상품 1개: 트렌치 코트(P005) —
> 데이터는 정상이며 운영자가 확인 중입니다.
> ```

### `pdf_s3_meta`

| 필드 | 설명 |
|---|---|
| `s3_bucket_name` | `sellon-reports` (월간 전용, 6개월 보존) |
| `s3_file_path` / `new_file_name` / `s3_full_key` | `s3_full_key = s3_file_path + new_file_name` (스키마가 강제) |
| `file_extension` / `file_size_bytes` | 상한 10MB |
| `original_file_name` | 원본·표시용 파일명 |
| `presigned_url` | 다운로드·미리보기 URL |
| `presigned_expires_at` | 링크 만료 = 발급 +**7일**. 만료 후 `s3_full_key`로 백엔드가 재발급 (SigV4 상한이 7일이라 영구 링크 불가) |
| `object_expires_at` | S3 Lifecycle 자동 삭제 시각 = 다운로드 가능 기한. 월간은 생성 +**6개월** |

`presigned_expires_at ≤ object_expires_at`은 스키마가 검증한다 — 객체가 사라진 뒤에도
살아있는 링크는 "받을 수 있다"는 잘못된 안내가 된다.

> ⚠️ **월간 리포트는 6개월 뒤 영구 소실된다.** 원본 데이터를 어디에도 보관하지 않아
> (=PDF가 유일 산출물) 재생성 경로가 없다.

---

## 6. `ai.guideline.generated`

월간 리포트와 **생명주기가 다르다.**

| | 월간 리포트 | CS 가이드라인 |
|---|---|---|
| 트리거 | 월 1회 배치 | **알림 1건마다** |
| 멱등 키 | `report_id` | `guideline_id` |
| 소비자 동작 | 링크 저장 → 화면 표시 | **CS팀 알림·메일 발송** |
| 데이터 적재 | 하지 않음 | **DB에 저장(JSONB)** |
| 보류 상태 | 있음 | 없음 |

| 필드 | 타입 | 설명 |
|---|---|---|
| `guideline_id` | string | **멱등 키.** `alert_id`의 `ALT-` 접두어를 `GD-`로 바꾼 값 — 예: `ALT-20260528-P001-COUPANG` → `GD-20260528-P001-COUPANG`. 알림과 **1:1**이라 재생성해도 같은 ID<br>⚠️ 백엔드 문서의 `GD-{탐지일}-{상품그룹}`은 **폐기된 규칙**이다. 탐지가 (상품, aspect, 채널) 단위로 발화하므로 같은 날 같은 상품의 다른 알림이 전부 같은 ID가 됐고, 멱등 upsert 때문에 나중 가이드라인이 앞의 것을 조용히 덮어썼다 (PR #22에서 수정, `app/reporting/ids.py`) |
| `alert_id` | string | 원본 알림 ID |
| `status` | enum | §5와 동일. `HOLD_INSUFFICIENT_DATA`는 발생하지 않음 |
| `pdf_s3_meta` | object \| null | 버킷 `sellon-temp-reports`, `object_expires_at` = 업로드 +**24시간**, `presigned_expires_at`도 동일 |
| `source_payload` | object | **필수.** 입력 JSON + 출력 JSON 원본. 메인이 PostgreSQL JSONB에 영구 보관하며, PDF가 24시간 뒤 사라져도 이 원본으로 재컴파일한다 |
| `notice_message` | string \| null | 비 `SUCCESS`일 때 안내 문구 |
| `validation_report` | object \| null | `FAILED_VALIDATION` 사유 |

`source_payload.output` = `CSGuidelineOutput`:
`summary{issue_title, risk_level, key_metric_text}` · `root_cause_summary` ·
`standard_guideline{core_message, draft_reply, key_talking_points[]}` · `ops_action_guide` ·
`inquiry_specific_guides[{item_id, recommended_point}]`.
`risk_level` = `CRITICAL` \| `WARNING` \| `NORMAL`. `item_id`는 입력 `linked_inquiries`
안의 문의만 (검증됨).

실측 payload 크기 약 **2.3KB**(문의 2건 기준) — Claim-Check 없이 그대로 싣는다.

---

## 7. `ai.dashboard.computed` — AI팀 해당 없음

**우리 파이프라인에 대응 산출물이 없다 (2026-08-03 결정).** 화면에 들어갈 숫자(알림 건수 ·
채널별 분포 · 상품별 추이)는 `ai.anomaly.analyzed`로 쌓인 **서비스 DB를 백엔드가 집계**하면
나온다. 백엔드 문서에 남아 있는 필드(`dashboardKey`, `avgCpuUsage`)는 서버 모니터링 예시이지
우리 스키마가 아니므로 **구현 기준으로 삼지 말 것.**

이 결정의 파급: 대시보드 집계 규칙(배너를 `(product_group_id, main_aspect, channel)` 최신
1건으로 세기 등)은 **백엔드 구현 사항**이지 우리 것이 아니다. 우리는 알림을 정확히 발행하고
집계에 걸리는 코드 사실을 전달하는 데까지만 책임진다.

---

## 8. `feedback.recommendation.reviewed` (메인 → AI)

셀러의 개선안 승인·반려 결과. **이게 돌아와야 과거·반려 사례 벡터DB(컬렉션2)가 쌓인다.**

| 필드 | 타입 | 설명 |
|---|---|---|
| `recommendation_id` | string | **멱등 키** |
| `alert_id` | string | 원본 알림 ID |
| `hitl_status` | enum | `승인` \| `반려` \| `수정후승인` (`대기`는 발행 대상 아님) |
| `hitl_feedback` | object | `{processed_at, processed_by, rejection_reason{reason_code, reason_text}, edited_text}` |

```json
{
  "eventId": "c1d2...",
  "eventType": "feedback.recommendation.reviewed",
  "occurredAt": "2026-05-29T09:12:00.000Z",
  "source": "main-server",
  "traceId": "trace-005",
  "payload": {
    "recommendation_id": "REC-a1b2c3d4e5f6",
    "alert_id": "ALT-20260528-0001",
    "hitl_status": "반려",
    "hitl_feedback": {
      "processed_at": "2026-05-29T09:11:50Z",
      "processed_by": "seller_001",
      "rejection_reason": {
        "reason_code": "이미조치함",
        "reason_text": "지난주에 상세페이지 이미 수정했습니다"
      },
      "edited_text": null
    }
  }
}
```

**REST와의 관계:** 운영 경로는 이 이벤트, `POST /recommendations/hitl` REST는
**로컬 개발·테스트용으로만** 남긴다. 입력 스키마가 같아서 AI측 처리 함수
(`pipeline.record_hitl_outcome()`)는 하나를 공유한다.

**재알림 억제와의 연결:** 승인·반려가 처리되면 그 알림의 재알림 억제(7일)가 풀린다.
백엔드가 `alert_status = 해결됨`인 `alert_id` 배열을 다음 배치 전에 넘겨주고, 우리는
`DetectRequest.resolved_alert_ids` / `detect_anomaly(resolved_alert_ids=...)`로 받는다.
**열람(`is_read`)과 처리 완료(`alert_status`)는 별도 필드다** — 억제 해제에 쓰는 건 후자.

### `feedback.report.created` (메인 → AI)

보고서 피드백. 필드는 camelCase다(`feedbackId`, `reportId`, `userId`, `feedbackType`,
`rating`, `comment`, `submittedAt`). `feedbackType` = `POSITIVE` \| `NEGATIVE` \| `NEUTRAL`.
멱등 키는 `feedbackId`. 담당은 리포팅(용준).

---

## 9. enum 상수명 · 값 대조표

**JSON에 실리는 값은 한글, 상수명은 영문.** 파이썬도 같은 구조(`Verdict.BIASED = "편중형"`)라
메인도 아래 영문 상수명을 그대로 쓰면 된다. 정본은 `app/core/schemas.py`.

| enum | 영문 상수명 | 한글 값 (JSON 실제 값) |
|---|---|---|
| `Verdict` | `NORMAL` `BIASED` `GLOBAL` `TENTATIVE_GLOBAL` `INDETERMINATE` | 정상 · 편중형 · 전역형 · 잠정 전역형 · 구분불가 |
| `DetectionConfidence` | `HIGH` `MEDIUM` `LOW` `NOT_APPLICABLE` | 높음 · 중간 · 낮음 · 해당없음 |
| `Aspect` | `COLOR` `SIZE` `MATERIAL` `DAMAGE` `MISDELIVERY` `ETC` | 색상 · 사이즈 · 소재 · 파손 · 오배송 · 기타 |
| `RecommendedAction` | `GENERATE_RECOMMENDATION` `CHANNEL_OPERATION_CHECK` `LOGISTICS_CHECK` `OPERATION_CHECK` `PRODUCT_CHECK` `SCOPE_UNDETERMINED` `OTHER_TYPE_CHECK` | 개선안 생성 · 채널 운영 요소 점검 권장 · 물류 점검 권장 · 운영 점검 권장 · 상품 자체 점검 권장 · 편중·전역 구분 불가(채널 표본 부족) · 기타 유형 |
| `HitlStatus` | `PENDING` `APPROVED` `REJECTED` `EDITED_APPROVED` | 대기 · 승인 · 반려 · 수정후승인 |
| `RecommendationConfidence` | `HIGH` `MEDIUM` `LOW` | 높음 · 중간 · 낮음 |
| `RejectionReasonCode` | `INSUFFICIENT_GROUNDS` `ALREADY_HANDLED` `DIFFERENT_CAUSE` `OTHER` | 근거부족 · 이미조치함 · 원인다름 · 기타 |

**매핑이 필요 없는 enum** — `Channel`(COUPANG/NAVER/ZIGZAG/ALL) · `Source`(cs/review) ·
`ProposalType`(copy_draft/image_guide). 값 자체가 영문이다.

**한글 값 자체는 바꿀 수 없다** — 스키마 정본이고 평가용 golden 정답 CSV가 이 값으로
채점되어, 바꾸면 채점이 깨진다. DB에 한글 값을 넣을지 영문 상수명으로 변환해 넣을지는
메인 내부 판단이며 우리쪽 제약은 없다.

---

## 10. 소비 측 처리 규칙

| 컨슈머 | 규칙 |
|---|---|
| 메인 서버 (`main.inbound`) | Manual ACK, prefetch 10~50. 멱등 키 기준 upsert. 저장 후 웹 대시보드는 메인 DB만 조회하며 AI를 동기 호출하지 않는다. **`contentRef` 기반 AI 내부 API 호출은 없다** |
| AI 서버 (`ai.inbound`, `feedback.#`) | Manual ACK. **멱등 키가 이벤트별로 다르다** — `feedback.report.created`는 `feedbackId`, `feedback.recommendation.reviewed`는 `recommendation_id` |

장애 처리(DLX · 재시도 5회 · TTL 24시간)는 인프라 공통 정책을 그대로 따르며 예외를 두지 않는다.

---

## 11. 스키마 변경 규칙

- 필드 **추가**는 옵셔널로만 (하위 호환 유지, 컨슈머는 모르는 필드를 무시).
- 필드 **제거·타입 변경**은 라우팅 키에 버전을 병기하고(`ai.report.generated.v2`)
  기존 컨슈머 전환 전까지 v1/v2를 병행 발행한다.
- enum 값은 양쪽 코드에서 문자열 상수로 수동 동기화 (프로젝트 규모상 스키마 레지스트리는 과함).

---

## 12. 구현 현황

| 항목 | 상태 |
|---|---|
| `main.inbound` 큐·바인딩 생성 | 백엔드 미완 |
| `ai.inbound`에 `feedback.#` 바인딩 추가 | 백엔드 미완 |
| 메인 서버 컨슈머 (upsert + 멱등) | 백엔드 미완 |
| **AI 발행자(Publisher)** | ✅ **완료** — `app/core/mq.py` (`aio-pika==9.5.7`). 로컬 브로커로 발행→수신 검증 (`scripts/smoke_mq.py`) |
| **AI 컨슈머 (`feedback.#` 수신)** | ✅ **코드 완료** — `app/core/mq_consumer.py`. ⚠️ 실행 진입점 미구현(아무도 `consume()` 을 안 부른다) · `feedback.recommendation.reviewed` 적재는 §8 확장 대기 |
| enum 동기화 확인 | §9 대조표 전달 완료, 백엔드 반영 확인 대기 |
| Publisher Confirm(AI) · Manual ACK(양쪽) | ✅ AI 쪽 완료 (`publisher_confirms=True` · 컨슈머 Manual ACK) |
| `MQ_COMPANY_ID` 실제 값 | **백엔드 대기** — 없으면 발행이 막힌다 |

`ai.anomaly.analyzed` · `ai.guideline.generated` 두 발행 경로는 배치(`app/batch/daily.py`)에
붙어 있다. REST 6개는 그대로 남아 재현·디버깅용으로 쓴다.

### 백엔드 문서와 어긋나는 곳 (우리 문서가 맞음)

백엔드가 아직 반영하지 않은 것들이다. 구현은 이 문서를 따를 것.

1. 「데이터 플로우 총정리」 개선안 도메인 표에 `ai.anomaly.detected`가 남아 있다
   → `ai.anomaly.analyzed`가 맞다 (§2 개명 이력).
2. 「메시지 큐 컨벤션 정의」 §4.2의 JSON 예시에 `RPT-202607-P001` · `product_group_id` ·
   `HOLD_INSUFFICIENT_DATA`가 남아 있다 → 같은 절 본문 표대로 **월 1건 `RPT-{YYYYMM}`,
   `product_group_id` 없음, status 3종**이 맞다 (§5).
3. 「메시지 큐 컨벤션 정의」 §4.6의 `guideline_id` 형식이 `GD-{탐지일}-{상품그룹}`으로
   남아 있다 → **`alert_id` 파생**이 맞다 (§6). 구 규칙은 같은 날 같은 상품의 다른 채널·
   aspect 알림이 전부 같은 ID가 되어 멱등 upsert가 서로를 덮어쓴다.
