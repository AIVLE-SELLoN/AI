# 탐지 결과 스키마 [확정]

***이상탐지 → Agent3 인터페이스 계약***

> 🟡 **문서 상태: [확정 후보]** — 상품 그룹 ID 필드명(§7-1) 하나만 매핑팀 확정 대기. 그 외 전 항목 로직 V3·시나리오 V3.2 기준 확정. 확정 후 변경은 양측 합의 + 변경 내역 기록. 서영 리뷰 15건 전수 반영 통합본(원본은 맨 끝 부록).
> 

---

## 1. 개요

이상탐지 파이프라인이 한 윈도우를 처리한 뒤 내보내는 JSON의 정의.

- **생산자:** 이상탐지 파이프라인 (로직 V3의 [0]~[7])
- **소비자 4곳:** ① Agent3(개선안 생성) ② 인사이트 리포트 ③ 대시보드 알림(SSE) ④ 평가 스크립트(golden_anomaly 대조)

```
이상탐지 ──(이 스키마의 JSON)──▶ Agent3 / 인사이트 / 대시보드 / 평가
```

**알림 1건의 단위 = (상품, 채널, main_aspect).**

- 편중형 1채널 → 알림 1건
- 2채널 편중 → 채널별 알림 2건
- 전역형·잠정 전역형 → 상품 1건 (channel="ALL")
- 정상 → 알림 없음

**CS·리뷰 종합 (중요):** 두 소스는 [0]~[6]과 [7]의 확신도 산출까지 각각 독립 수행하지만, **알림은 종합 후 1건만 발행**한다. 종합 규칙은 §3.1.

## 2. JSON 전체 예시

```json
{
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
    "inquiry_ids": ["INQ-000412", "INQ-000415", "…(원인분류 투입 문의 전체 20개)"],
    "linked_change_id": "CHG-0009"
  }
}
```

## 3. 필드 정의

| 필드 | 타입 | 값 범위 | 설명 | 주 소비자 |
| --- | --- | --- | --- | --- |
| alert_id | string | ALT-날짜-일련번호 | 알림 고유 ID | 전체 |
| detected_at | datetime |  | 탐지 시각 | 대시보드 |
| updates_alert_id | string/null |  | 갱신 알림일 때 원본 ID (신규면 null) | 대시보드 |
| product_group_id | string | P001~ | 매핑 산출 상품 그룹 ID.  | 전체 |
| channel | enum | COUPANG / NAVER / ZIGZAG / ALL | 전역형·잠정전역형은 ALL | 전체 |
| window_start / window_end | date |  | 판정에 쓴 현재 윈도우 구간 | 평가 |
| verdict | enum | 정상 / 편중형 / 전역형 / 잠정 전역형 / 구분불가 (5종) | 보류는 verdict 아님(채널 상태, §5.2). golden과 동일 enum | 전체 |
| significant_channels | array |  | 유의 판정된 채널 목록 | 인사이트 |
| excluded_channels | array |  | 표본<10으로 판정 제외된 채널 ("표본 부족" 병기용) | 대시보드 |
| main_aspect | enum | aspect 6종 | delta 최대 aspect (로직 [4]) | Agent3 |
| sub_aspects | array of object | [{aspect, delta, recommended_action}] | 동시 유의한 부가 aspect + 각자의 조치. 로직 [4] "병기" 원칙. 없으면 [] | 인사이트·대시보드 |
| stats.source | enum | cs / review | 이 통계가 어느 소스 기준인지. 리뷰만 발화 시 review | 평가·인사이트 |
| stats.cur_rate / past_rate | float | 0~1 | 현재/과거 윈도우 부정률 | 대시보드 |
| stats.delta | float |  | 상승폭(cur−past). 전역형(ALL)은 delta 최대 채널 대표값 | 대시보드 |
| stats.p_value | float |  | raw Fisher 단측 p값. 발화는 이 값이 아니라 배치 BH-FDR 결과로 결정(로직 [2-B]). 대시보드 노출 금지(§3.3) | 평가 |
| stats.bh_significant | bool | true/false | BH-FDR 보정 후에도 유의했는지 = 실제 발화 근거. 대시보드 표시 규칙은 §3.3 | 대시보드·평가 |
| stats.cur_total | int |  | 현재 윈도우 총문의(stats.source 기준 분모) | 평가 |
| source_signals.cs / .review | bool/null | true / false / null | 소스별 유의 여부(독립 판정). null = 해당 소스 보류(표본<10) — 미발화(false)와 구분 | 인사이트 |
| source_signals.interpretation | enum | 강한 신호(양 소스) / CS 선행 신호 — 리뷰는 시차로 미반영 가능 / 리뷰 지연 반영 또는 CS 미표출 | combine_sources 반환값과 문자열 완전일치(로직 §5) | 인사이트 |
| root_cause | object/null |  | [6] 미수행 시 null. 수행 시 아래 4필드 | Agent3 |
| root_cause.label | string | 원인 enum 또는 "미특정" | consistent=false면 "미특정" | Agent3 |
| root_cause.count / total | int |  | 최다 원인 문의 수 / 투입 문의 수 ("20건 중 14건") | 인사이트 |
| root_cause.consistent | bool |  | 최다 원인 ≥50% AND ≥5건 충족 여부 | Agent3 |
| detection_confidence | enum | 높음 / 중간 / 낮음 / 해당없음 | combine_sources 종합값(§3.1). Agent3 캡핑 입력. 전역/잠정전역=해당없음 | Agent3 |
| scope_in | bool |  | 순수 aspect 속성. 색상·사이즈·소재=true, 파손·오배송·기타=false. 개선안 생성 여부와 별개 | Agent3 |
| recommended_action | enum | (§3.2 표) | 결과 발행 경로와 1:1 | 대시보드 |
| evidence.inquiry_ids | array |  | 원인분류 투입 문의 전체(= root_cause.total 건). Agent 인용 가능 경계 — 이 밖 인용은 Evaluator 기각 | Agent3 |
| evidence.linked_change_id | string/null |  | 시점 일치한 상세페이지 변경 ID | Agent3·인사이트 |

### 3.1. CS·리뷰 종합 규칙 (combine_sources)

두 소스를 [0]~[6]+[7]확신도까지 독립 판정한 뒤 아래로 종합해 **알림 1건**을 만든다.

| 상황 | 채택 소스 | verdict·main_aspect·root_cause·stats | detection_confidence | interpretation |
| --- | --- | --- | --- | --- |
| CS·리뷰 둘 다 발화 | CS 우선 | CS 값 채택 (stats.source=cs) | CS 확신도 1단계 상향(상한 높음). ⚠ ***편중형일 때만** — 전역/잠정전역은 확신도가 "해당없음"이라 상향 미적용(로직 combine_sources 가드)* | 강한 신호(양 소스) |
| CS만 발화 | CS | CS 값 (stats.source=cs) | CS 확신도 | CS 선행 신호 — 리뷰는 시차로 미반영 가능 |
| 리뷰만 발화 | 리뷰 | 리뷰 값 (stats.source=review) | 리뷰 확신도 | 리뷰 지연 반영 또는 CS 미표출 |
- **verdict가 소스마다 다를 때**(예: CS=편중형, 리뷰=전역형): 둘 다 발화면 CS의 verdict를 채택(CS가 빠른 신호이자 주 소스). 리뷰의 다른 판정은 source_signals에 발화 사실만 기록되고 alert verdict에는 반영하지 않는다 — 종합의 단일 진실은 CS.
- 한쪽만 발화면 그 소스 판정이 그대로 alert verdict.

### 3.2. recommended_action 값 (7종)

값은 **verdict + main_aspect + 원인 상태**의 조합으로 자동 결정된다. 대시보드는 그대로 셀러 화면에 노출하고, ***Agent3는 "개선안 생성"일 때만 작동***한다.

| recommended_action 값 | 발생하는 판정 상황 | scope_in |
| --- | --- | --- |
| ***개선안 생성*** | 편중형 + 색상/사이즈/소재 + 원인 명확 | true |
| 채널 운영 요소 점검 권장 | 편중형이나 원인 분산(미특정) | true |
| 물류 점검 권장 | 편중형 + 파손 | false |
| 운영 점검 권장 | 편중형 + 오배송 | false |
| 상품 자체 점검 권장 | 전역형 / 잠정 전역형 | false |
| 편중·전역 구분 불가(채널 표본 부족) | 구분불가 | false |
| 기타 유형 — 확인 필요 | 편중형 + 기타(잔여 버킷). Mock 미생성이라 실제 미출현, enum 완전성용 | false |
- alert 1건의 recommended_action은 main_aspect 기준 1개만. SC-029처럼 색상(개선안 생성)+파손(물류 점검)이 동시 발화하면, main인 색상 값이 이 칸에 들어가고 파손 조치는 sub_aspects 배열에 담긴다.

### 3.3. stats.bh_significant 대시보드 표시 규칙

bh_significant는 *"이 알림이 왜 떴나"의 **정식 발화 근거***다. 대시보드는 다음 원칙으로 표시한다.

- **보여주는 것:** rate 변화(예: "색상 불만 5% → 13%")를 크게 + 그 옆에 "이번 주 기준 통계적으로 유의 ✓" 배지(bh_significant=true일 때). 셀러에겐 "평소보다 확실히 늘었습니다"라는 신뢰 표시로만 쓴다.
- **보여주지 않는 것:** *p_value 숫자 자체는 노출 금지.* BH 컷오프가 배치마다 달라 셀러가 "0.0001인데 왜?"로 오해하기 쉽다. *p값·컷오프는 내부 로그·평가용.*
- 정리: 셀러 화면 = rate 변화 + 유의 배지 / 내부 = p_value + bh_significant + cur_total.

## 4. 설계 원칙

1. *golden_anomaly와 필드명·enum 완전 통일* — 평가 스크립트가 예측 JSON ↔ 정답 CSV를 칸 이름(필드명)으로 짝지어 대조. 이름이 어긋나면 채점 자체가 불가. 대조표는 §6.2.
2. *stats는 가공 전 원자료*(source, rate, delta, p, bh_significant, N) — 대시보드가 "5%→13%"를 재계산 없이 렌더링. p값 노출 규칙은 §3.3.
3. *evidence가 Agent 그라운딩 경계* — Agent3는 inquiry_ids(원인분류 투입 전체) 안의 문의만 인용 가능. 밖이면 Evaluator 기각.

## 5. verdict / 발행 상황 규칙

### 5.1. verdict 값 5종 — 판정 결과

verdict 필드에 들어갈 수 있는 값은 아래 **5개뿐**이다.

| verdict | 발행 | 채널 | 핵심 필드 규칙 |
| --- | --- | --- | --- |
| 정상 | 미발행 | — | 평가 시 "해당 윈도우 alert 부재"가 곧 예측값 |
| 편중형 | 채널 1건 | 유의 채널 | root_cause 있음, scope_in=aspect속성, action=개선안생성/점검권장 |
| 전역형 | 상품 1건 | ALL | root_cause=null, scope_in=false, action="상품 자체 점검 권장", stats=delta 최대 채널 대표값 |
| 잠정 전역형 | 상품 1건 | ALL | 전역형과 동일 + excluded_channels에 보류 채널 + "○○ 채널 표본 부족, 확정 시 재판정" 병기 |
| 구분불가 | 1건 | 유의 채널 | root_cause=null, action="편중·전역 구분 불가(채널 표본 부족)", detection_confidence="중간", excluded_channels에 보류 채널 |

### 5.2. 발행 상황 규칙 — verdict 값이 아닌 것

아래는 verdict 값이 아니라, 위 판정을 **어떻게 발행하는가**에 대한 세부 규칙이다.

| 상황 | verdict | 처리 |
| --- | --- | --- |
| 2채널 편중 | 편중형 | 채널별 alert 2건 각각 발행 (alert 단위 원칙). 새 값 아님 |
| 원인 미특정 ([6] 수행·분산) | 편중형 | root_cause={label:"미특정", consistent:false}, confidence="낮음", action="채널 운영 요소 점검 권장" |
| 채널 보류 (표본<10) | — | verdict 아니라 채널 단위 상태. 다른 채널이 유의하면 그 alert의 excluded_channels에 기록. 전 채널 보류 시 미발행(정상과 동일, 평가 영향 없음) |
| 재알림 억제 (7일) | — | 같은 (상품,aspect,채널) 7일 미발행. 단 +5%p 추가 상승 시 갱신 — 새 alert_id + updates_alert_id에 원본 |

**root_cause 2상태:** ① [6] 미수행(전역/잠정전역/구분불가/스코프밖) → root_cause=null. ② [6] 수행·분산 → {label:"미특정", consistent:false}. 평가 join은 null 허용.

## 6. 평가(채점) join 규칙

### 6.1. verdict별 join 단위

평가 스크립트는 예측 alert와 golden_anomaly를 **verdict에 따라 다른 단위로** 대조한다.

| verdict | join 단위 | 대조 필드 |
| --- | --- | --- |
| 편중형 / 2채널 편중 | (case_id × channel) — 발행 채널별 | is_anomaly, verdict, is_biased, main_aspect, root_cause. detection_confidence는 SC-030/031·3-3 한정(시나리오 §4 채점 범위) |
| 전역형 | case_id (케이스 수준) | is_anomaly, verdict, is_biased=false. golden 3채널 행 모두 Y인 것이 전역 정의이므로 채널별 대조 면제 |
| 구분불가 / 잠정 전역형 | 채점 제외 (관찰 케이스 SC-037/038, §7) | — |
| 정상 | (case_id × channel) alert 부재 확인 | is_anomaly=false |
- 편중형은 "다른 채널이 안 울리는 것"까지가 정답이라, golden의 channel_significant=N 행(alert 없음)과 예측의 alert 부재를 대조.
- sub_aspects는 채점 대상 아님(시나리오 §4 지표에 sub 없음) — SC-029는 main_aspect=색상 일치로 채점.

### 6.2. 필드명 대조표 (스키마 ↔ golden_anomaly)

**원칙:** 스키마와 golden_anomaly는 **칸 이름(필드명)과 enum 값 목록이 반드시 같아야 한다.** 채점 스크립트가 같은 이름끼리 자동으로 짝지어(join) 값을 비교하기 때문 — 이름이 다르면 짝을 못 찾아 채점이 통째로 멈춘다. (값 자체는 예측≠정답이어야 정상 — 그 차이를 세는 게 채점이다.)

| 스키마 | golden_anomaly | 이름 일치? | 조치 |
| --- | --- | --- | --- |
| detection_confidence | expected_confidence | ✗ | golden을 detection_confidence로 개명(값 목록 이미 동일) |
| verdict | verdict | △ | golden에서 보류 값 제거(§7) 후 값 목록 동일 |
| main_aspect / root_cause | main_aspect / root_cause | ✓ | OK |
| stats.source | source | ✓ | OK |
| **(파생) is_anomaly** | is_anomaly | 파생 | 예측 JSON엔 없는 값 → 평가 스크립트가 **is_anomaly = (alert 발행됨 = verdict≠정상)** 로 파생 |
| **(파생) is_biased** | is_biased | 파생 | 예측 JSON엔 없는 값 → **is_biased = (verdict=="편중형")** 로 파생. 전역/잠정전역=false |

## 7. 남은 미결

1. 🔴 **상품 그룹 ID 필드명** — 매핑팀이 실제 명칭 확정 후 스키마·로직 V3·golden_anomaly 3문서 일괄 통일 (로직 영향 0, 이름만 교체).
2. 🔴 **golden_anomaly 수정** — 스키마 확정 후 golden 담당이: ① verdict 보류 값 제거(SC-036은 verdict를 비우고 scoring_included=N 컴럼 값으로만 채점 제외 표기 — 새 컴럼 추가 아니며 기존 scoring_included=N 사용) ② expected_confidence → detection_confidence 개명 ③ 전역형 케이스 수준 대조 반영.
3. ✅ **stats.bh_significant** — 본 버전에 필드 추가 완료(§3, §3.3). 이상탐지 담당은 BH 결과를 이 필드에 채우고, 대시보드는 §3.3 규칙대로 표시.
4. 🟡 **구분불가·잠정전역형 검증** — C안 확정: 시나리오 정의서에 관찰 케이스 2개(SC-037 구분불가·SC-038 잠정전역형) 추가(채점 제외). G 케이스(SC-026~028)와 동일 취급 — 데이터로 경로 작동만 확인, 고정 정답 없으니 채점 31개에서 제외. 반영은 시나리오·Mock 담당 작업(시나리오 §6 미검증 경로 → 관찰 케이스 승격, Mock에 상품 1개: 쿠팡 발화 + 2채널 보류 추가).

## 8. 변경 내역

| 날짜 | 변경 내용 | 합의자 |
| --- | --- | --- |
| 2026-07-16 | 최초 제안 | 유지인 |
| 2026-07-21 | 로직 V3 정합성 반영(verdict·확신도·BH·interpretation) | 유지인 |
| 2026-07-21 | 서영 리뷰 15건 전수 반영 통합본 | 유지인·서영 |
| 2026-07-21 | 가독성 개정 — recommended_action 표화(§3.2), verdict/발행상황 분리(§5), 필드명 일치 원칙 명시(§6.2), stats.bh_significant 정식 추가(§3.3), 구분불가 관찰 케이스 C안 확정(§7) | 유지인 |
| 2026-07-21 | 완결 개정 — 잠정전역형 채점 제외로 분류 정정(§6.1), 관찰 케이스 2개 반영(§7-4), 기타 aspect action 추가(§3.2), 편중형 join에 is_biased 명시(§6.1) | 유지인 |
| 2026-07-21 | 팀원(이상탐지 담당) 피드백 반영 — §7-4 상품 2개 정정, §6.2에 is_anomaly·is_biased 파생 필드 명시, §6.1 detection_confidence 채점 범위 한정, §7-2 hold 표기 명확화, §3.1 상향 "편중형 한정" 명시. (로직 §5 verdict 채택규칙·family 재계산은 별도 문서 작업) | 유지인 |

## 부록. 서영 리뷰 원본 (2026-07-21, traceability)

| # | 지적 | 반영 위치 |
| --- | --- | --- |
| 1 | [7] source별 독립 → 알림 2건 충돌 | §3.1 (독립 범위 [0]~[6]+확신도, 발행 1건) |
| 2 | stats가 어느 source인지 불분명(SC-035) | stats.source 신설 |
| 3 | 전 채널 보류 시 정상/보류 구분 불가 | §5.2 채널 보류 행 (평가 영향 없음, 대시보드는 향후) |
| 4 | scope_in 의미 충돌 | §3 scope_in = 순수 aspect 속성 |
| 5 | recommended_action enum에 신규 없음 | §3.2 표(7종) |
| 6 | root_cause 없음 처리 모순 | §5.2 root_cause 2상태(null vs 미특정) |
| 7 | p값 렌더링 지침 정반대 | §3.3 (p 노출 금지, bh_significant로 표시) |
| 8 | source_signals bool → 미발화/보류 구분 못함 | 3값(true/false/null) |
| 9 | 미검증 verdict를 평가가 만나면? | §6.1 채점 제외 / §7 관찰 케이스 |
| 10 | SC-029 sub_aspect 조치 자리 없음 | sub_aspects 객체 배열 (※ "채점 불가"는 정정: main만 채점) |
| 11 | 전역형 1건 ALL vs golden 3행 join 불일치 | §6.1 케이스 수준 join |
| 12 | verdict enum 3문서 다름 | §5.1 5종 통일 + §7 golden 수정 |
| 13 | interpretation 문자열 불일치 | §3 완전일치 문자열 |
| 14 | inquiry_ids ↔ total 관계 불명 | §3 evidence = 투입 전체 |
| 15 | expected_confidence ↔ detection_confidence 이름 불일치 | §6.2 대조표 |

## 새로 발견한 것 5건

### ① 🔴 §7-4 "상품 1개" — 실제로는 2개 필요합니다

> §7-4: "Mock에 **상품 1개**: 쿠팡 발화 + 2채널 보류 추가"
> 

그런데 시나리오 I절의 두 케이스는 **채널 구성이 다릅니다:**

|  | 발화 | 보류 |
| --- | --- | --- |
| SC-037 (구분불가) | 쿠팡 | 네이버·지그재그 |
| SC-038 (잠정전역) | 쿠팡·**네이버** | 지그재그 |

*"케이스 1개 = 상품 1개 전용"* 원칙상 **한 상품에 못 넣습니다.** §7-4를 "상품 2개"로 고쳐야 합니다.

### ② 🔴 로직 V3가 아직 안 따라왔습니다 — 제일 중요

§3.1이 *"둘 다 발화면 verdict·main_aspect·root_cause·stats를 CS 값 채택"* 으로 확장됐는데, **로직 V3 §5의 `combine_sources()`는 `(확신도, 라벨)` 두 개만 반환합니다.** verdict 채택 규칙이 로직에 없습니다.

제 1번 지적의 해결책이 **스키마에만 적히고 로직에는 반영이 안 된** 상태입니다. 구현자가 로직 문서를 보면 그 규칙을 모릅니다. 로직 §5를 갱신하셔야 합니다.

### ③ 🟡 상품 수 → BH family 재계산

케이스가 36 → 38개가 됐는데, `config_anomaly.csv`는 **P001~~P036 케이스 + P037~~P040 배경**입니다. 상품 2개가 더 필요하니 **배경이 4 → 2개로 줄거나 상품을 42개로 늘려야** 합니다.

보류 채널은 family에서 빠지므로 `m ≈ 1,440`이 달라지고, **제가 검증한 컷오프 `0.001224`도 재계산 대상**입니다. (방향은 참양성에 유리하지만 수치는 다시 뽑아야 합니다.)

### ④ 🟡 §6.1 `detection_confidence` 대조 범위

§6.1은 편중형 **전체**에 `detection_confidence`를 대조한다고 되어 있는데, 시나리오 §4는 탐지 확신도 채점 대상을 **SC-030/031·3-3으로 한정**합니다. 전 케이스에 요구하면 범위가 안 맞습니다.

### ⑤ 🟡 사소한 것 2개

- §3.2 제목은 **"7종"**, 부록 #5는 **"6종"** — 표를 세면 7개입니다. 부록이 낡았습니다.
- §7-2의 **"hold 표기"** 가 뭘 뜻하는지 불명확합니다. `golden_anomaly`에 컬럼을 새로 만드는 건지, 기존 컬럼에 값을 넣는 건지 — golden 담당이 헷갈릴 소지가 있습니다.

### 추가로 발견한 것 2건

#### 🔴 §6.1이 **스키마에 없는 필드**로 대조하려 해 — 채점 불가

§6.1 대조 필드 목록에 **`is_anomaly`, `is_biased`** 가 있는데, **§3 필드 정의에 이 둘이 없어.** golden_anomaly에는 있지만 **예측 JSON에는 없는 필드**라 짝을 지을 수가 없어.

§6.2가 세운 원칙(*"칸 이름이 어긋나면 채점이 통째로 멈춘다"*)에 **정작 §6.1이 걸리는** 구조야.

→ **파생 규칙을 명시**하면 해결돼:

```
is_anomaly = (alert 발행됨)          ← verdict ≠ 정상is_biased  = (verdict == "편중형")
```

이걸 §6.2 대조표에 "파생 필드" 행으로 추가하면 돼.

#### 🟡 §3.1 "1단계 상향"에 편중형 한정 조건이 빠짐

§3.1 표는 *"CS·리뷰 둘 다 발화 → CS 확신도 1단계 상향"* 이라고만 적혀 있는데, 로직 V3 코드에는 **`if base_verdict != "편중형": return base_conf`** 로 **편중형에만 상향**하는 가드가 있어. 전역형은 확신도가 `"해당없음"`이라 상향 자체가 성립 안 하거든(`LEVELS.index("해당없음")` → ValueError).

§3 필드 정의에 "전역/잠정전역=해당없음"이 있어서 유추는 되지만, **§3.1 표에 "편중형에만 적용" 한 줄** 넣는 게 안전해.