# schemas.py

> *데이터 계약 — 컴포넌트 간 교환 데이터의 Pydantic 정의*
> 

---

## 1. 개요

`app/core/schemas.py`는 SELLoN 파이프라인의 컴포넌트가 주고받는 데이터 구조를 Pydantic 모델로 정의한 **단일 계약 파일**이다.

분류·이상탐지·개선안·인사이트가 이 파일을 공통 import하며, 컴포넌트 간 데이터는 이 모델을 통과·검증한다. 

데이터 구조만 정의하고, 처리 로직·인프라(Kafka·DB)는 포함하지 않는다.

- **위치**: `app/core/schemas.py`
- **소비자**: 분류 워커 / 이상탐지 / Agent3(개선안) / 인사이트
- **의존 원칙**: 각 컴포넌트는 core에서만 import하며, 서로의 모듈에 의존하지 않는다.

## 2. 정의 대상

| 모델 | 생산자 | 주 소비자 | 정본 |
| --- | --- | --- | --- |
| ClassifiedItem | 분류 워커 | 이상탐지 집계 | 본 문서 §4 |
| DetectionAlert | 이상탐지 | Agent3·인사이트·대시보드 | [탐지 결과 스키마 [확정]](https://app.notion.com/p/39fe39c614978038bb10d19a6a487517?pvs=21) |
| Recommendation | Agent3 | HITL·피드백·대시보드 | [개선안 출력 스키마 [확정]](https://app.notion.com/p/39fe39c614978017b36befb4730e520c?pvs=21) |

## 3. 공통 Enum

| 이름 | 값 |
| --- | --- |
| Channel | COUPANG / NAVER / ZIGZAG / ALL |
| Aspect | 색상 / 사이즈 / 소재 / 파손 / 오배송 / 기타 |
| Sentiment | -1 / 0 / 1 (int) |
| Verdict | 정상 / 편중형 / 전역형 / 잠정 전역형 / 구분불가 |

## 4. ClassifiedItem

분류 워커가 문의·리뷰 1건을 분류한 결과. `aspects`는 리스트로 담는다 

| 필드 | 타입 | 값 범위 | 설명 |
| --- | --- | --- | --- |
| item_id | str |  | inquiry_id 또는 review_id |
| source | enum | cs / review |  |
| channel | Channel |  |  |
| product_group_id | str |  |  |
| raw_text | str |  | 원문 텍스트 — [원인 분류([6]단계)](https://app.notion.com/p/3a3e39c614978081a40ddd7475e4d596?pvs=21)용 |
| aspects | array | list[AspectSentiment] | aspects = 그 문의/리뷰에서 언급된 aspect를 전부 담은 리스트 (각각 sentiment 포함) |
| created_at | datetime |  |  |

**AspectSentiment** (aspects 원소)

| 필드 | 타입 | 값 범위 | 설명 |
| --- | --- | --- | --- |
| aspect | Aspect |  |  |
| sentiment | int | -1 / 0 / 1 |  |
| mixed_signal | bool / null |  | 리뷰만 사용, CS는 null |

## 5. DetectionAlert

[탐지 결과 스키마 [확정]](https://app.notion.com/p/39fe39c614978038bb10d19a6a487517?pvs=21) (이상탐지 → Agent3·인사이트 입력 계약)

## 6. Recommendation

[개선안 출력 스키마 [확정]](https://app.notion.com/p/39fe39c614978017b36befb4730e520c?pvs=21) (Agent3 → HITL·피드백·대시보드)

## 7. 검증 규칙 (validator)

- `sentiment` ∈ {-1, 0, 1}
- `recommendation_confidence` ∈ {높음, 중간, 낮음, null}
- `citations[].inquiry_id` ⊆ `DetectionAlert.evidence.inquiry_ids` (모델 간 교차검증 함수)
- `source == "review"`이면 모든 `aspects[].aspect` ∈ {색상, 사이즈, 소재} (아니면 검증 에러)

## 9. 변경 내역

| 날짜 | 내용 | 합의자 |
| --- | --- | --- |
| 2026-07-24 | 최초 초안 | 유지인 |