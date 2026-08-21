# 문서 생성 스키마 v2 — 백엔드 인계본

> **대체 대상**: 노션 「문서 생성 스키마 (1)」(Edited Jul 27) — **폐기**
> **기준**: 노션 「문서 생성 스키마」 + Reporting 검증 계획 §A(JSD 재정비) · §1(계약 불일치 6건)
> **작성 목적**: Reporting 노드(월간 리포트 · CS 가이드라인) 입출력 계약을 백엔드에 인계
>
> **표기 규칙**
> - 🟢 **확정** — 오타·정합 수정. 논쟁 여지 없음, 바로 반영 가능
> - 🟡 **제안** — 기본값 제시. 팀 확인 필요하나 착수 가능
> - 🔴 **확정 필요** — 백엔드 착수 전 결정 필수. 미확정 상태로 구현 시 재작업 발생
>
> **nullable 필드는 Java에서 래퍼 타입**(`Integer`, `Double`, `Boolean`)으로 받아야 합니다.
> 원시 타입이면 null이 0/false로 뭉개집니다.

---

## §0. 변경 요약

| # | 변경 | 구분 | 영향 |
| --- | --- | --- | --- |
| 1 | `totla_voc_count` → **`total_voc_count`** | 🟢 | 오타 |
| 2 | `linked_inquiries[].cs_id` → **`item_id`** | 🟢 | `ClassifiedItem.item_id` 와 정합 |
| 3 | `inquiry_specific_guides[].cs_id` → **`item_id`** | 🟢 | 동상 |
| 4 | `s3_full_key` 의 `Alias: s3_full_kdy` **제거** | 🟢 | 오타 별칭 |
| 5 | `aspect_stats[]` 비율 3종의 **분모 통일** (합 = 1.0) | 🟡 | **DDL·집계 쿼리 영향** |
| 6 | `aspect_stats[].scope_in` **신규** | 🟡 | 대시보드 강조 대상 구분 |
| 7 | `aspect_stats[].detection_negative_rate` **신규** | 🟡 | 알림 화면과 숫자 일치용 |
| 8 | `channel_divergence` 구조 **전면 개편** (단일 → 3쌍) | 🔴 | **응답 형태 변경** |
| 9 | `jsd_score` / `is_crisis` **nullable** | 🔴 | 표본 부족 시 null |
| 10 | `is_crisis` 판정: `≥0.5` → **BH-FDR + 효과크기** | 🔴 | 게이지 UI 변경 |
| 11 | 보류 임계값 `≤10` → **`<10`** | 🟡 | 탐지·CS가이드와 통일 |
| 12 | 대시보드 상단 3지표용 **별도 API 신설** | 🔴 | 신규 엔드포인트 |
| 13 | `presigned_url` 만료 정책 | 🔴 | 메일 첨부 방식 결정 |

> ✅ **위 13건은 모두 반영이 끝났고 아래 본문이 그 결과다.** 이 표는 v1 대비 무엇이 바뀌었는지를
> 보여주는 기록이지 남은 할 일 목록이 아니다. `#8~#10`(`channel_divergence` 개편)은 $M_{JSD}$
> 수식이 함께 바뀐 건이라 §1-1 에 근거까지 적혀 있다.

---

## §1. 월간 리포트 — 입력 (`MonthlyReportInput`)

**생성 주체**: FastAPI(reporting 노드)가 DB 조회·계산 후 자체 구성.
백엔드는 **트리거와 콜백 수신**만 담당합니다.

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `report_month` | String | 구간0 · 상단 타이틀 | `YYYY-MM`. Pattern `^\d{4}-\d{2}$` |
| `start_date` | String (date) | — | `YYYY-MM-01` |
| `end_date` | String (date) | — | `YYYY-MM-DD` (말일) |
| `product_group_id` | String | 구간0 · 상품 선택 드롭다운 값 | 마스터 상품 그룹 고유 ID |
| `product_name` | String | 구간0 · 드롭다운 라벨 | 상품명 |
| 🟢 `total_voc_count` | Integer (`≥0`) | 구간1 · 카드① | 월간 CS 문의 + 리뷰 총합 |
| `aspect_stats` | Array\<Object\> | 구간2 · 속성별 스택 바 | 아래 세부 |
| `aspect_stats[].aspect` | String (enum) | 구간2 · 각 바 제목 | `색상`·`사이즈`·`소재`·`파손`·`오배송`·`기타` (**6종 유지**) |
| 🟡 `aspect_stats[].scope_in` | Boolean | 구간2 · 강조/비강조 | `true` = 색상·사이즈·소재. **대시보드는 true만 상단 강조, false는 접힘** |
| `aspect_stats[].total_count` | Integer (`≥0`) | 구간2 · 바 옆 "(비월 총 N건)" | **해당 속성 언급 건수** (분모) |
| 🟡 `aspect_stats[].positive_ratio` | Number (0.00~1.00) | 구간2 · 바 좌측 | **분모 = `total_count`** |
| 🟡 `aspect_stats[].neutral_ratio` | Number (0.00~1.00) | 구간2 · 바 중앙 | **분모 = `total_count`** |
| 🟡 `aspect_stats[].negative_ratio` | Number (0.00~1.00) | 구간2 · 바 우측 | **분모 = `total_count`** |
| 🟡 `aspect_stats[].detection_negative_rate` | Number (0.00~1.00) | 구간4 · 그리드 "당월 부정률" | **분모 = (상품×채널) 총 문의수** — 이상탐지 알림 화면과 같은 숫자 |
| `aspect_stats[].drift_rate` | Number | 구간2 · "드리프트 율 +8%p ▲" | ΔP_neg(a) = `negative_ratio(t) − negative_ratio(t−1)` |
| 🟡 `aspect_stats[].status` | String (enum) | 구간2 · `[RISK]`/`[NORMAL]` 배지 | `RISK` if `drift_rate ≥ 0.03`, else `STABLE` |
| 🔴 `channel_divergence` | Object | 구간3 · Divergence Gauge | **§1-1 로 전면 개편** |

### 🟡 #5·#7 근거 — 분모를 통일해야 하는 이유

노션 「월 간 보고서 필요 사항 정리」 현재 원문:

> ㄴ 긍정 비율 | `positive_ratio` | 0.00~1.00 (**리뷰 데이터 소스 기반** 연산)
> ㄴ 부정 비율 | `negative_ratio` | 0.00~1.00 (**이상 탐지 분모 대비** 비율)

**긍정은 리뷰, 부정은 CS 탐지 분모** — 모집단이 서로 다릅니다.
그런데 같은 문서의 UI 목업은 이 셋을 **한 스택 바 100%** 로 그립니다.

```
■ 색상 속성 (비월 총 4,500건)
├─────────────────────────────┬──────┬──────┤
│ 긍정 75%                    │중립12│부정13│
```

모집단이 다르면 합이 100%가 되지 않아 바가 깨집니다.
그리고 노션 「이상탐지 로직 [확정]」 §5가 이 합산을 명시적으로 금지합니다.

> **합산하지 않는다(분모가 다르므로).**
> CS는 상품×채널 문의 총건수, 리뷰는 상품×채널 리뷰 총건수.

→ **셋 다 `total_count` 분모로 통일**해 바를 성립시키고,
알림 화면과 맞춰야 하는 숫자는 **`detection_negative_rate` 로 분리**합니다.
이렇게 하면 "알림엔 13%인데 리포트엔 43%"라는 사용자 혼란도 사라집니다(둘 다 제공하되 표시 위치를 분리).

### 🟡 #6 근거 — aspect 6종 유지 + `scope_in` 분리

| 문서 | 값 |
| --- | --- |
| 「문서 생성 스키마 (1)」 | `aspect_stats[].aspect` enum = **색상, 사이즈, 소재, 파손, 오배송, 기타** (6종) |
| 「월 간 보고서 필요 사항 정리」 | *"연산 대상은 '색상', '사이즈', '소재' **3개 지표 속성에 한정**"* |

**둘 다 살립니다.** 월간 리포트는 **현황 보고서**라 6종을 다 세지 않으면
`total_voc_count` 와 속성 합계가 어긋납니다. 반면 개선 가능 영역은 3종뿐이라 강조 대상은 좁혀야 합니다.

근거는 노션 「이상탐지 로직 [확정]」 §[5] 스코프 필터의 사상 그대로입니다.

> **탐지는 전 aspect 수행하고**, 원인분류·개선안 생성만 스코프로 좁힌다.
> **파손·오배송을 aspect 목록에서 빼면 안 된다.** 빼면 그 문의들이 다른 aspect로 오분류되어 신호를 오염시킨다.

---

## §1-1. 🔴 `channel_divergence` — 개편본

### 변경 전

```json
"channel_divergence": {
  "comparison_pair": "COUPANG_VS_NAVER",
  "jsd_score": 0.54,
  "is_crisis": true
}
```

### 변경 후

```json
"channel_divergence": {
  "worst_pair": "COUPANG_VS_NAVER",
  "is_crisis": true,
  "aspect_scope": ["색상", "사이즈", "소재"],
  "pairs": [
    {
      "comparison_pair": "COUPANG_VS_NAVER",
      "jsd_score": 0.54,
      "sample_size": 216,
      "p_value": 0.0002,
      "bh_significant": true,
      "is_crisis": true,
      "hold_reason": null
    },
    {
      "comparison_pair": "COUPANG_VS_ZIGZAG",
      "jsd_score": 0.11,
      "sample_size": 198,
      "p_value": 0.31,
      "bh_significant": false,
      "is_crisis": false,
      "hold_reason": null
    },
    {
      "comparison_pair": "NAVER_VS_ZIGZAG",
      "jsd_score": null,
      "sample_size": 18,
      "p_value": null,
      "bh_significant": null,
      "is_crisis": null,
      "hold_reason": "INSUFFICIENT_SAMPLE"
    }
  ],
  "jsd_trend": [
    { "week_start": "2026-07-01", "jsd_score": 0.48, "sample_size": 52 },
    { "week_start": "2026-07-08", "jsd_score": 0.51, "sample_size": 55 },
    { "week_start": "2026-07-15", "jsd_score": 0.57, "sample_size": 54 },
    { "week_start": "2026-07-22", "jsd_score": 0.59, "sample_size": 55 }
  ]
}
```

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `worst_pair` | String | 구간3 · 게이지 상단 라벨 | **가장 위험한 쌍** — `(severity 등급, excess)` 사전식 최댓값(`CRISIS(3) > CAUTION(2) > SAFE(1)`, `excess = jsd_score − jsd_baseline`). 기존 `comparison_pair` 대체<br>⚠️ 구 정의 "`jsd_score` **최댓값** 쌍"은 폐기다 — `jsd_baseline`이 쌍마다 달라(표본이 작을수록 크다) `jsd_score`가 최대인 쌍이 `SAFE`인데 다른 쌍이 `CRISIS`인 상태가 재현됐고, 그때 제목엔 "안정 단계"가 박힌 채 `is_crisis=true`로 나갔다 (2026-08-04 정정, `app/reporting/metrics_calculator.py`) |
| `is_crisis` | Boolean **\| null** | 구간3 · 하이라이트 마킹 | **3쌍 중 하나라도 true면 true.** 전 쌍 보류면 null |
| `aspect_scope` | Array\<String\> | 구간3 · 각주 "색상·사이즈·소재 기준" | JSD 계산에 쓴 aspect. **`scope_in=true` 3종 고정** |
| `pairs[]` | Array\<Object\> | 구간3 · 게이지 3개 (탭 or 스택) | 채널쌍 전수 |
| `pairs[].comparison_pair` | String | 각 게이지 라벨 | `COUPANG_VS_NAVER` 등 3종 |
| `pairs[].jsd_score` | Number **\| null** | 게이지 바늘 위치 | **범위 [0,1]** (log₂ 발산). null = 표본 부족 |
| `pairs[].sample_size` | Integer | 게이지 하단 "표본 N건" | 두 채널 부정 건수 합 |
| `pairs[].p_value` | Number **\| null** | **노출 금지** | 내부·평가용 |
| `pairs[].bh_significant` | Boolean **\| null** | 게이지 옆 "통계적으로 유의 ✓" 배지 | BH-FDR(q=0.05) 통과 |
| `pairs[].is_crisis` | Boolean **\| null** | 게이지 색상 (적/녹/회) | `bh_significant AND jsd_score ≥ δ_min` |
| `pairs[].hold_reason` | String (enum) **\| null** | 게이지 자리에 "표본 부족" 안내 | `INSUFFICIENT_SAMPLE` / `EMPTY_CHANNEL` / null |
| `jsd_trend[]` | Array\<Object\> | 구간3 · 스파크라인 (**판정 미사용**) | 7일 롤링, 참고용 |

### 🔴 `p_value` 노출 금지 근거

노션 「탐지 결과 스키마 [확정]」 §3.3:

> **보여주지 않는 것:** *p_value 숫자 자체는 노출 금지.*
> BH 컷오프가 배치마다 달라 셀러가 "0.0001인데 왜?"로 오해하기 쉽다.
> *p값·컷오프는 내부 로그·평가용.*
> **정리**: 셀러 화면 = rate 변화 + 유의 배지 / 내부 = p_value + bh_significant + cur_total.

**동일 원칙을 리포트 게이지에 적용합니다.** 응답에는 담되 UI에 그리지 마십시오.

### 🔴 왜 `≥0.5` 단일 임계값을 버려야 하는가 — 정량 근거

JS 발산을 경험분포로 추정하면 표본이 작을수록 **위로 편향**됩니다.
2×k 분할표의 우도비 통계량 `G = 2N·Î` 가 카이제곱(자유도 k−1)을 따르고,
JS 발산은 채널-속성 상호정보량과 같으므로:

```
E[JSD_bits | H0] ≈ (k − 1) / (1.3863 × N),      N = n_A + n_B
```

노션 「생성기 (7/22)」의 볼륨 상수(`NORMAL_VOLUME = {"cs": 6}`)와
배경 부정률(4~9%)을 대입하면:

| 집계 단위 | 채널당 부정 | N | **이상이 없어도 나오는 JSD** | 임계값 0.5 |
| --- | --- | --- | --- | --- |
| **일별 (현행)** | ≈1건 | **2** | **0.721** | 🔴 **귀무 기댓값이 임계값 초과** |
| 주간 | ≈7건 | 14 | 0.103 | 🟡 |
| **월 누적 (제안)** | ≈30건 | **60** | **0.024** | ✅ |

**현행대로면 아무 문제 없는 상품도 기댓값 0.721 > 0.5 → `is_crisis` 가 항상 `true`** 입니다.
임계값을 어떻게 조정해도 고칠 수 없고, **집계 단위를 월 누적으로 바꿔야** 합니다.

근사식 검증 — k=3, N=2, 참분포 균등일 때 손계산:

| 사건 | 확률 | JSD (nats) |
| --- | --- | --- |
| 두 채널이 같은 aspect | 1/3 | 0 |
| 다른 aspect | 2/3 | ln2 = 0.6931 (최대) |

```
E[JSD] = (2/3) × 0.6931 = 0.4621 nats
근사식  = (3−1) / (2×2)  = 0.5    nats     → 일치
```

### 🔴 계산 규칙 (FastAPI 소관이나 응답 해석에 필요)

```
[게이트]  min(n_A, n_B) ≥ 1  AND  N ≥ 30        # k=3 기준
          미충족 → jsd_score/p_value/is_crisis = null, hold_reason 세팅

[점수]    jsd_score = ½Σ P·log₂(P/M) + ½Σ Q·log₂(Q/M)   ∈ [0,1]
          ※ log 밑 2, 발산(거리 아님)
          ※ scipy.spatial.distance.jensenshannon 은 거리(√)이므로 제곱 필요

[판정]    ① 순열검정 B=10,000 → p값
          ② 전 (상품 × 채널쌍) p값에 BH-FDR (q=0.05)     ← 반드시 먼저
          ③ is_crisis = bh_significant AND (jsd_score ≥ δ_min)   ← 나중
```

**② → ③ 순서가 뒤바뀌면 안 됩니다.** 노션 「이상탐지 로직 [확정]」 [2-B]:

> **순서를 바꾸면 안 되는 이유**: min_delta로 먼저 걸러 family를 줄이면,
> **관측된 delta라는 데이터에 근거해 검정 집합을 고르는 것**이 되어 FDR 보장이 깨진다.
> 또 m과 순서가 달라져 컷오프 자체가 바뀐다.

`δ_min` 은 **`0.10` 초기값, 환경변수/설정으로 노출** (R1 실험 후 확정 예정).

**최소 표본 N_min 산출 근거**

```
(k − 1) / (1.3863 × N) ≤ 0.05   →   N ≥ (k − 1) / 0.0693
```

| aspect 수 k | 필요 N_min | 비고 |
| --- | --- | --- |
| **3** (색상·사이즈·소재) | **30** | 월 누적 일반 채널 N≈60 → 통과 |
| 6 (전 aspect) | 73 (→ 80 권장) | 월 누적 일반 채널 N≈60 → **미달, 판정 불가** |

→ `aspect_scope` 를 3종으로 고정하는 또 하나의 이유입니다.

---

## §2. 월간 리포트 — 출력 (`MonthlyReportOutput`)

**필드 구조 유지.** 백엔드는 이 JSON을 콜백으로 수신해 저장합니다.

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `report_id` | String | — (내부 키) | 예 `RPT-202607-P001` — **상품별 추적용**이고 로그·배치 결과에서만 쓴다(`build_report_id`)<br>⚠️ MQ `ai.report.generated`의 멱등 키는 이 값이 아니라 **월 1건 `RPT-202607`**(`build_book_report_id`)이고 payload에 `product_group_id`는 없다 — `docs/mq_events.md` §5 |
| `product_group_id` | String | — | 마스터 상품 그룹 ID |
| `report_month` | String | 구간0 · 타이틀 | `YYYY-MM` |
| `aspect_summaries` | Array\<Object\> | 구간2 · 각 바 하단 요약 문구 | LLM 생성 |
| `aspect_summaries[].aspect` | String (enum) | — (조인 키) | `aspect_stats[].aspect` 와 1:1 |
| `aspect_summaries[].summary_text` | String | **미렌더** (아래 경고) | LLM 생성 |
| `channel_divergence_cause` | Object | 구간3 · [진단 결과] 박스 | LLM 생성 |
| `channel_divergence_cause.cause_title` | String | 진단 박스 제목 | |
| `channel_divergence_cause.cause_description` | String | 진단 박스 본문 | |
| `cause_analysis_results` | Array\<String\> | **미렌더** (아래 경고) | 단문 리스트 |
| `recommended_actions` | Array\<String\> | **미렌더** (아래 경고) | 단문 리스트 |
| `channel_pair_analyses` | Array\<Object\> | 구간3 · 게이지 아래 카드 | **채널쌍별** 원인·조치. 길이 0–3, `comparison_pair` 중복 불가, **입력 `channel_divergence.pairs[]`와 1:1**(§4-4 검증기가 1:1이 아니면 반려하므로 실질 필수). 보고서 전체 단위인 `cause_analysis_results`/`recommended_actions`와 **다른 필드**다 — 전자는 "이 상품 전체", 이쪽은 "이 채널쌍" 한정. 보류된 쌍(`hold_reason`)도 1건이 필요하고 이때는 판정 수치를 쓰지 않는다 (2026-08-04 신설, 프롬프트 `monthly_report_v5`+) |
| `channel_pair_analyses[].comparison_pair` | String | 카드 제목 | Pattern `^[A-Z]+_VS_[A-Z]+$` · 입력 `pairs`에 있는 값만 |
| `channel_pair_analyses[].cause_analysis` | Array\<String\> | 카드 본문 | 길이 1–2 · **수치 그라운딩 대상** |
| `channel_pair_analyses[].recommended_actions` | Array\<String\> | 카드 본문 | 길이 1–2 · **금지표현 검사 대상** |
| 🟡 `pdf_s3_meta` | Object **\| null** | 다운로드 버튼 | §5. **CS 가이드라인에만 있던 필드를 월간에도 추가** |

> 🔴 **미렌더 3필드 — 결정이 아직 열려 있다.**
> `aspect_summaries[].summary_text` · `cause_analysis_results[]` · `recommended_actions[]`은
> 2026-08-04 화면 확정으로 PDF에서 빠졌고(`app/reporting/pdf_compiler.py`가 이 셋을 참조하지
> 않는다) MQ payload에도 안 실린다. 월간 리포트는 **PDF가 유일한 산출물이고 DB에 적재하지
> 않으므로 이 셋은 어디에도 남지 않는다** — 지금은 LLM 생성 비용만 쓰고 버려진다.
> 선택지는 ① 스키마에서 제거 ② PDF 부록으로 되살리기 ③ 현행 유지 셋이고, 스키마·검증 규칙을
> 그대로 둔 지금은 사실상 ③이지만 **합의로 닫힌 적은 없다**(제거는 계약 변경이라 팀 합의 대상).

> 🟡 `pdf_s3_meta` 추가 근거 — 아키텍처 설명 원문:
> *"해당 가이드라인과 원간 리포트는 pdf로 생성하여 s3 버킷에 적재하여
> 사용자가 presigned url을 통해 다운받을 수 있도록 구성"*
> 월간 리포트도 PDF 산출물이 있는데 기존 출력 스키마엔 메타 필드가 없습니다.

---

## §3. CS 가이드라인 — 입력 (`CSGuidelineInput`)

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `alert_id` | String | PDF 헤더 · "발행 ID" | `DetectionAlert.alert_id` (1:1 매핑 키) |
| `detected_at` | String (ISO8601) | PDF 헤더 · "탐지 시각" | |
| `product_group_id` | String | 메타 테이블 · 마스터 코드 | |
| `product_name` | String | 메타 테이블 · 마스터 상품명 | |
| `channel` | String (enum) | 메타 테이블 · 발행 채널 | `COUPANG`·`NAVER`·`ZIGZAG`·`ALL` |
| `main_aspect` | String (enum) | 메타 테이블 · 주요 원인 | Aspect 6종 |
| `verdict` | String (enum) | 메타 테이블 · 판정 | 5종 — 아래 표 |
| `recommended_action` | String (enum) | PDF 헤더 · 권장 조치 배지 | 7종 |
| `detection_confidence` | String (enum) | 메타 테이블 · 탐지 확신도 | `높음`·`중간`·`낮음`·`해당없음` |
| `stats.cur_rate` | Number (0.00~1.00) | 본문 · "현재 부정률" | |
| `stats.past_rate` | Number (0.00~1.00) | 본문 · "과거 기준율" | |
| `stats.delta` | Number | 본문 · "+8.0%p 상승" | `cur_rate − past_rate` |
| `stats.cur_total` | Integer (`≥0`) | 본문 · "200건 중 26건"의 분모 | 현재 윈도우 총 문의 건수 |
| `root_cause` | Object **\| null** | 메타 테이블 · 주요 원인 라벨 | **null 가능** — 아래 주의 |
| `root_cause.label` | String | 원인 분류 명칭 | 또는 `"미특정"` |
| `root_cause.count` | Integer (`≥0`) | "20건 중 14건"의 분자 | |
| `root_cause.total` | Integer (`≥0`) | "20건 중 14건"의 분모 | |
| `linked_inquiries` | Array\<Object\> | 본문 · 대표 고객 문의 원문 인용 블록 | `evidence.inquiry_ids` 기반 DB 조인 |
| 🟢 `linked_inquiries[].item_id` | String | — (조인 키) | **`cs_id` 에서 개명.** `inquiry_id` 또는 `review_id` |
| `linked_inquiries[].raw_text` | String | 인용 블록 본문 | 고객 작성 문의 원문 |
| `linked_inquiries[].created_at` | String (ISO8601) | 인용 블록 하단 | CS 접수 일시 |

### 🟢 #2 개명 근거

노션 「schemas.py」 §4: `item_id | str | inquiry_id 또는 review_id`

노션 「탐지 결과 스키마 [확정]」 §6.2:

> **원칙:** 스키마와 golden_anomaly는 **칸 이름(필드명)과 enum 값 목록이 반드시 같아야 한다.**
> 채점 스크립트가 같은 이름끼리 자동으로 짝지어 값을 비교하기 때문
> → **이름이 다르면 짝을 못 찾아 채점이 통째로 멈춘다.**

「문서 생성 스키마 (1)」에는
`linked_inquiries[].cs_id | String | inquiry_id / review_id | CS 문의 고유 ID (ClassifiedItem.item_id)`
로 되어 있어, **설명란은 이미 `item_id` 를 가리키는데 필드명만 `cs_id`** 인 상태입니다.

### ⚠️ `root_cause` 가 null인 경우

노션 「탐지 결과 스키마 [확정]」 §5.2:

> **root_cause 2상태:**
> ① [6] 미수행(전역/준전역/구분불가/스코프밖) → `root_cause = null`
> ② [6] 수행·분산 → `{label:"미특정", consistent:false}`

**`recommended_action != "개선안 생성"` 인 알림은 CS 가이드라인 생성 대상에서 제외**하는 것이
안전합니다(원인 없이 응대 스크립트를 만들 수 없음). 백엔드 트리거 조건에 반영해 주십시오.

### `verdict` enum 5종

노션 「탐지 결과 스키마 [확정]」 §5.1

| 값 | 의미 |
| --- | --- |
| `정상` | 미발화 (가이드라인 생성 대상 아님) |
| `편중형` | 특정 채널만 이상 |
| `전역형` | 전 채널 공통 |
| `준전역형` | 전역이나 보류 채널 존재 |
| `구분불가` | 판정 가능 채널 1개 |

---

## §4. CS 가이드라인 — 출력 (`CSGuidelineOutput`)

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `guideline_id` | String | — (내부 키) | **`alert_id`의 `ALT-` 접두어만 `GD-`로 치환**한 값(alert_id와 **1:1**) — 예 `ALT-20260828-P001-COLOR-COUPANG` → `GD-20260828-P001-COLOR-COUPANG`. `ALT-`로 시작하지 않으면 `GD-` + alert_id 전체<br>⚠️ 구 규칙 `GD-{탐지일}-{상품ID}`(`GD-20260528-P001`)는 폐기다 — 탐지가 (상품, 속성, 채널) 단위로 발화하므로 같은 날 같은 상품의 다른 알림이 전부 같은 ID가 됐고, 백엔드 멱등 upsert 때문에 나중 가이드라인이 앞의 것을 조용히 덮어썼다 (PR #22에서 수정, 구현은 `app/core/ids.py::build_guideline_id()` 한 곳) |
| `alert_id` | String | PDF 헤더 · 발행 ID | 원본 알림 ID |
| `summary.issue_title` | String | PDF 최상단 메인 타이틀 | LLM 생성 |
| `summary.risk_level` | String (enum) | 헤더 배지 색상 | `CRITICAL`·`WARNING`·`NORMAL` |
| `summary.key_metric_text` | String | 본문 · 지표 요약 한 줄 | 예 `"부정 비율 5% → 13% (Δ+8%p 급증)"` |
| `root_cause_summary` | String | 메타 테이블 · 원인 지분율 | 예 `"사진_색감_오차 비중 70% (20건 중 14건)"` |
| `standard_guideline.core_message` | String | 본문 · 핵심 안내 매뉴얼 | LLM 생성 |
| `standard_guideline.draft_reply` | String | **강조 상자(Box)** · 복사용 스크립트 | 상담원이 그대로 붙여넣는 문구 |
| `standard_guideline.key_talking_points` | Array\<String\> | 본문 · 필수 언급/금지 표현 목록 | |
| `inquiry_specific_guides` | Array\<Object\> | 본문 · 문의별 맞춤 포인트 | |
| 🟢 `inquiry_specific_guides[].item_id` | String | — (조인 키) | **`cs_id` 에서 개명** |
| `inquiry_specific_guides[].recommended_point` | String | 각 문의 옆 응대 가이드 | |
| `pdf_s3_meta` | Object **\| null** | 다운로드 버튼 | §5 |

### 🔴 백엔드 검증 필수 — 그라운딩 경계

```
{ inquiry_specific_guides[].item_id } ⊆ { linked_inquiries[].item_id }
```

**존재하지 않는 문의에 대한 맞춤 가이드가 생성되면 반려**해야 합니다.

노션 「탐지 결과 스키마 [확정]」 §4 설계 원칙 3:

> **evidence가 Agent 그라운딩 경계** — Agent3는 inquiry_ids 안의 문서만 인용 가능.
> 밖이면 Evaluator 기각 (평가 방지의 구조적 장치)

노션 「schemas.py」 §7 에 `citations[].inquiry_id ⊆ DetectionAlert.evidence.inquiry_ids`
교차검증 함수가 이미 존재합니다. **CS 가이드라인에도 같은 규칙을 적용**합니다.

---

## §5. PDF S3 메타데이터 (`PdfS3Meta`)

| JSON Key | 데이터 타입 | UI/UX 표시 영역 | 설명 |
| --- | --- | --- | --- |
| `s3_bucket_name` | String | — | 예 `sellon-temp-reports` |
| `s3_file_path` | String | — | 디렉토리 경로 (trailing slash 포함) |
| `original_file_name` | String | 다운로드 파일명 | 사용자에게 노출되는 표시용 |
| `new_file_name` | String | — | S3 저장용 고유 파일명 |
| 🟢 `s3_full_key` | String | — | `s3_file_path + new_file_name`. **`Alias: s3_full_kdy` 제거** |
| `file_extension` | String | — | 기본값 `"pdf"` |
| `file_size_bytes` | Integer (`≥0`) | — | **10MB 초과 검증용** |
| `presigned_url` | String **\| null** | 다운로드 버튼 링크 | 발급 +7일 (`PRESIGNED_URL_TTL_HOURS`). 만료 시 `s3_full_key` 로 재발급 |

> 🟢 「문서 생성 스키마 (1)」에 `s3_full_key | Alias: s3_full_kdy` 로 오타 별칭이 남아 있고,
> 노션 「CS 대응 가이드라인 및 보고서 생성 설계」 2안 JSON 에도 `"s3_full_kdy"` 가 전파돼 있습니다.
> **두 곳 모두 수정 필요.**

---

## §6. 상태 코드 · 콜백 (백엔드 소관)

### 6-1. 콜백 API

```
POST /api/v1/internal/reports/complete
```

노션 「메일 전송 필요 사항 정리」:

> FastAPI가 S3에 PDF 업로드 완료 이후, 백엔드 콜백 API를 호출하면,
> Spring Boot가 **단일 트랜잭션 내**에 `email_dispatches` 적재, 상태 제어,
> JavaMailSender 비동기 발송 통제

| JSON Key | 데이터 타입 | 설명 |
| --- | --- | --- |
| `report_id` / `guideline_id` | String | 산출물 ID |
| `status` | String (enum) | 아래 4종 |
| `pdf_s3_meta` | Object \| null | 실패 시 null |
| `notice_message` | String \| null | 사용자 안내 문구 |

### 6-2. `status` enum 5종

| 값 | 발생 조건 | UI 동작 |
| --- | --- | --- |
| `SUCCESS` | 정상 | PDF 첨부 메일 발송 |
| `HOLD_INSUFFICIENT_DATA` | 🟡 `total_voc_count < 10` | 보류 안내 문구 출력, **LLM 추론 미수행** |
| `FAILED_VALIDATION` | 그라운딩 검증 3회 연속 실패 | **자동 발송 중단** + 운영자 알림. 틀린 수치가 담긴 문서가 셀러에게 나가는 것을 막는 값이다 |
| `FAILED_SIZE_EXCEEDED` | `file_size_bytes > 10MB` | S3 업로드·메일 발송 **트랜잭션 이전 차단** |
| `FAILED_ERROR` | 그 외 | SSE 에러 알림 |

⚠️ **월간 합본 이벤트(`ai.report.generated`)에는 `HOLD_INSUFFICIENT_DATA`·`FAILED_VALIDATION` 이
나오지 않는다** — 둘 다 **상품 단위** 판정인데 그 이벤트는 월 단위다. 해당 상품은 합본에서
빠지고 그 사실이 `notice_message` 에 실린다. 상품 1건 REST(`POST /api/v1/reports`)에서는
보류가 `409`, 검증 실패가 `422` 로 살아 있다. 상세는 `docs/mq_events.md` §5.

### 🟡 #11 — 보류 임계값 `≤10` → `<10` 통일

| 문서 | 조건 |
| --- | --- |
| 「이상탐지 로직 [확정]」 [2-A] | `cur_total < 10` → 보류 |
| 「CS 대응 가이드라인 및 보고서 생성 설계」 | `cur_total_samples` **10건 미만** → 422 |
| 「월 간 보고서 필요 사항 정리」 | `total_voc_count` **≤ 10** → HOLD |

**정확히 10건일 때 월간 리포트만 다르게 동작합니다.**
탐지·CS가이드와 맞춰 **`< 10`** 으로 통일합니다.

### 6-3. `HOLD_INSUFFICIENT_DATA` 안내 문구 (고정 문자열)

노션 「월 간 보고서 필요 사항 정리」 원문:

> "해당 상품의 월간 CS 표본 수는 부족으로 인하여 보고서 생성이 보류되었습니다.
> 데이터가 누적되면 분석이 재개됩니다."

**LLM을 태우지 말고 하드코딩** — 같은 문서: *"무의미한 통계 연산 및 LLM 추론을 즉시 중단(Bypass)"*
