# `classified_item` 버전 컬럼 추가 명세

- 상태: **적용 완료** (2026-08-12). 확정 문서 갱신까지 끝났다.
  - 반영된 곳 — 노션 「RAW DB 스키마」 DDL 전문(`classified_item` 6컬럼) · 「Raw DB 스키마
    확정 (8/7)」 §2-6 컬럼표(+ fail-closed 근거) · 「메시지 큐 컨벤션 정의」 §4.1 표·📌
    callout·JSON 예시·§7 체크리스트(`classifier_versions` 4키).
  - ⚠️ **이 파일이 명세의 정본이다.** 같은 내용의 노션 명세 페이지(「classified_item 칼럼
    추가건」)는 확정 문서로 흡수된 뒤 정리됐다. 위 두 노션 페이지가 상세 근거를 물을 때
    가리키는 곳이 여기이므로, **경로를 옮기거나 지우면 그 참조가 죽는다.**
  - 코드가 문서보다 먼저 나갔던 기간이 있었다(§4 적용 절차 1번 "문서 먼저" 미준수).
    `raw_schema.py` 모듈 docstring 이 "문서가 바뀌지 않는 한 이 파일도 바뀌지 않아야
    한다"고 못박은 것과 어긋났던 상태이고, 지금은 해소됐다.
- 작성: 2026-08-12
- 대상 테이블: `classified_item` (「Raw DB 스키마 확정 (8/7)」 §2-6)
- 소유권: **AI 노드** (§1). 백엔드는 이 테이블을 읽지도 쓰지도 않음 —
  2026-08-12 백엔드 확인 완료(AI팀 자체 추가 가능)
- 코드 정본: `app/core/raw_schema.py` `CLASSIFIED_ITEM_DDL`

---

## 1. 왜 필요한가

이상탐지는 **35일**(현재 7일 + 과거 28일)을 한 번에 읽어 하나의 검정을 돌린다.
그 사이에 분류기가 바뀌면 **과거 구간과 현재 구간이 서로 다른 라벨러의 결과**가 된다.

부정률이 올라간 원인이 "고객이 달라졌다"인지 "우리가 라벨러를 바꿨다"인지 구분되지 않는데,
Fisher 검정은 그 둘을 가르지 못한다. 결과적으로 **분류기 개선이 그대로 고객 이상 알림으로
발화한다.**

프롬프트 축은 기존 `prompt_version` 컬럼으로 2026-08-12에 막았다
(`app/batch/daily.py` 활성 버전 필터 + 워커 `--reclassify-stale`).
**남은 두 축이 이 문서의 대상이다** — 프롬프트는 한 글자도 안 바뀌었는데 라벨러가
달라지는 경우가 실제로 있다:

| 축 | 바뀌는 예 | 현재 탐지가 아는가 |
|---|---|---|
| prompt | `classify_aspect_v5` → `v6` | **예** (2026-08-12 적용) |
| model | `LLM_MODEL` 을 `gpt-4o-mini` → 다른 모델로 | 아니오 |
| pipeline | 허용 aspect 집합 변경, 후처리·정규화 수정 | 아니오 |

`pipeline` 축이 필요한 근거는 **분자를 직접 바꾸는 두 트리거**다:

- **허용 aspect 집합 변경** — 안 내던 aspect 가 나오기 시작하면 그 aspect 의 분자가 생긴다
- **후처리·정규화 수정** — 같은 계열. 라벨이 다른 값으로 정규화되면 집계가 갈린다

둘 다 프롬프트 파일은 한 글자도 안 바뀌었는데 숫자가 달라지는 경우다.

`_cs_empty_fallback` 도 pipeline 축의 트리거이긴 하다 — **"프롬프트는 그대로인데 파이프라인이
바뀐 사례"** 로는 유효하다. 다만 **분포를 움직이는 예로 쓰면 안 된다.**

> 🔴 **정정 (2026-08-13, 지인님 리뷰).** 이 문단은 원래 두 가지가 틀려 있었다.
>
> **①** "CS 전량 96,524건 중 2.1%(284건 중 6건)" — 한 문장에 분모가 둘 섞였다. `6/284` 는
> 284건 표본 기준이고 `96,524`(= `cs` 테이블 전체 행수)와는 관계가 없다. 전량 기준 비율은
> 측정된 적이 없다. 그 수치는 2026-08-04 서영님 측정이고 원문은
> `service.py` `_cs_empty_fallback` docstring 과 `loader.py` 모듈 docstring 이다
> (커밋 `9a1cc84` · `b67bd04`).
>
> **②** 분모를 내린 자리에 "이 폴백을 끄고 켜는 것만으로 분포가 움직인다"를 세웠는데,
> **재현해 보면 아무것도 안 움직인다.** 문의 4건(색상 부정 1 · 빈 배열 1 · 색상 중립 2)으로
> `build_rows` → `count_window` 를 태운 결과:
>
> ```
> fallback OFF (aspects=[])   totals={('P001','COUPANG','cs'): 4}  negs={('P001','색상',...): 1}
> fallback ON  (기타/중립)     totals={('P001','COUPANG','cs'): 4}  negs={('P001','색상',...): 1}
> check_coverage              양쪽 다 []
> ```
>
> 분모·분자·커버리지가 전부 같다. 이유는 셋이다 — ⑴ `build_rows` 가 문서 1건 = 행 1개라
> 분모가 aspect 와 무관하고, ⑵ `기타/중립` 은 `sentiment=0` 이라 `_neg_aspects()` 가 안
> 집으며, ⑶ 커버리지는 부모 행 존재로 판정한다. 리포팅도 no-op 이다 —
> `monthly_aggregator` 의 두 쿼리가 각각 `JSD_ASPECT_ORDER`(색상·사이즈·소재, `기타` 없음)와
> `sentiment = -1` 로 걸러서 `기타/중립` 행은 양쪽 다 떨어진다.
>
> **`_cs_empty_fallback` docstring 자신이 이미 그렇게 적고 있었다** — *"집계 산식만 보면 이
> 폴백은 no-op 이다(현진 정정, 2026-08-04) … 분모+1·분자+0 으로 완전히 같다"*. 출처로 지목한
> 함수와 정면으로 어긋난 주장을 세운 셈이다.
>
> **왜 위험했나**: 이 문서(와 `CLASSIFIER_PIPELINE_VERSION` docstring)는 "언제 버전을
> 올리는가"의 정본이고, 올리는 순간 탐지가 옛 행을 안 읽는다. 그대로 뒀다면 다음 사람이
> 폴백을 손보면서 버전을 올리고 **결과가 증명 가능하게 하나도 안 바뀌는 변경에 전량
> 재분류(≈$200)를 치렀을 것**이다.
>
> ⚠️ **그 수치는 이제 논거에서 빠졌으므로 재현 명령도 더 이상 필요하지 않다.**

---

## 2. 추가할 컬럼

| 컬럼 | 타입 | Null | 설명 |
|---|---|---|---|
| `model_version` | `TEXT` | 허용 | 분류에 쓴 LLM 모델 식별자. `settings.llm_model`(`LLM_MODEL`) 값을 그대로 기록. 예: `gpt-4o-mini` |
| `pipeline_version` | `TEXT` | 허용 | 프롬프트 **밖** 분류 로직의 버전. 허용 aspect 집합·후처리·정규화를 바꾸면 올린다. 예: `classify_pipeline_v1` |

기존 컬럼은 **그대로 둔다**(변경·삭제 없음):

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `item_id` | `TEXT PRIMARY KEY` | `cs.id` / `reviews.id` 재사용 (§5-1 A안) |
| `source` | `TEXT NOT NULL` | `cs` \| `review` |
| `classified_at` | `TEXT` | 분류 시각 (오프셋 포함 ISO) |
| `prompt_version` | `TEXT` | 프롬프트 버전. **의미 변경 없음** |

### 변경 후 DDL 전문

```sql
CREATE TABLE IF NOT EXISTS classified_item (
    item_id          TEXT PRIMARY KEY,
    source           TEXT NOT NULL,
    classified_at    TEXT,
    prompt_version   TEXT,
    model_version    TEXT,      -- 추가
    pipeline_version TEXT       -- 추가
);
```

### 인덱스 (적용됨)

탐지 조회와 워커의 stale 스캔이 세 컬럼을 **전부 등호로** 거른다.

```sql
CREATE INDEX IF NOT EXISTS idx_classified_item_versions
    ON classified_item (prompt_version, model_version, pipeline_version);
```

처음엔 "성능 근거가 없으니 분류를 돌려 본 뒤 붙여도 늦지 않다"고 적었고 실제로 지금
`classified_item` 이 비어 있어 근거는 여전히 없다. 그럼에도 **DDL 과 함께 넣었다** —
인덱스는 나중에 붙여도 데이터 마이그레이션이 없어 되돌리기 쉽고, 컬럼과 함께 있어야
"이 세 컬럼은 등호 조회용" 이라는 의도가 스키마에 남기 때문이다. 성능이 문제되지 않는
것으로 확인되면 빼도 된다.

---

## 3. 왜 컬럼 3개인가 (합성 문자열이 아니라)

`prompt_version` 한 컬럼에 `classify_aspect_v5|gpt-4o-mini|pipeline_v1` 처럼
합쳐 넣는 안을 검토했다가 **반려**했다. 근거:

1. **문서와 코드가 소리 없이 어긋난다.** 확정 문서에 `prompt_version TEXT` 는
   프롬프트 버전이라고 적혀 있는데 실제로는 셋이 들어간다. DDL diff 가 없어서
   리뷰에서 아무도 못 잡는 형태이고, 이건 `app/core/` 문서 거버넌스 규칙
   (CLAUDE.md 아키텍처 원칙 3)이 막으려던 사고 그 자체다.
2. **축별 조회를 잃는다.** "모델별 분류 정확도 비교" 같은 질의가
   `LIKE '%|gpt-4o-mini|%'` 가 되어 인덱스를 못 탄다.
3. **마이그레이션 비용 논거가 무효였다.** 합성안의 근거는 "기존 행이 stale 이 되어
   재분류 비용이 부당하다"였는데, 확인해 보니 `classified_item` 은 **테이블 자체가
   없다**(08-11 데이터 재생성 이후 워커 미실행). 96,524 는 `cs` 행수였다.
   **지금은 DDL 변경 비용이 0 이다.**

> `eval/run_classify_eval.py` 의 `prompt_fingerprint()`(이름 + 내용 해시)는 선례가
> 아니다. 그쪽은 **한 축(프롬프트 정체성)을 정밀하게** 만드는 것이고, 합성안은 서로
> 다른 세 축을 뭉개는 것이라 방향이 반대다.

---

## 4. 적용 순서 — **표본 재분류 전에**

🔴 지금 `classified_item` 이 비어 있어 **마이그레이션이 필요 없다.** 분류 워커를
대량으로 한 번 돌리는 순간 이 창이 닫히고, 그때부터는 아래 중 하나를 실제로 치러야 한다:

- **backfill**: `--reclassify-stale` — 대상 1건당 LLM 1회. 전량이면 비용 전액 재지불
- **명시적 cutover**: 재분류 없이 컬럼만 채우는 UPDATE. 관측이 아니라 **운영자의 주장**
  이라 주장할 값을 손으로 적게 해야 한다(설계만 기록, 아래 §6)

따라서 **DDL 을 먼저 적용하고 그다음에 분류를 돌린다.** 그 사이에는 워커를 대량 실행하지 않는다.

적용 절차:

1. 「Raw DB 스키마 확정 (8/7)」 §2-6 컬럼표에 위 2줄 추가 (문서 먼저)
2. `app/core/raw_schema.py` `CLASSIFIED_ITEM_DDL` 갱신
3. 기존 로컬 DB 는 AI 소유 테이블만 지우고 재생성
   (`DROP TABLE classified_item; DROP TABLE classified_item_aspect;`)
   — 원문 `cs`·`reviews` 는 건드리지 않는다
4. 워커·탐지 코드를 3축 비교로 확장 (§5)

⚠️ **3번은 이제 사람이 기억할 절차가 아니다 (2026-08-13).** `LEGACY_MARKERS` 의 마커를
`pipeline_version` 으로 옮겨서, 옛 테이블이 남아 있으면 워커·배치가 시작 시점에 안내와
함께 멈춘다. 그 전에는 마커가 `prompt_version` 이라 4컬럼 테이블이 구버전으로 안 잡혔고,
`no such column: model_version` 으로 터졌다 — 가드가 막으려던 바로 그 모양이었다.

🔴 **컬럼을 또 추가하면 마커도 같이 옮길 것.** 판정이 "마커가 없으면 옛것"이라, 마커를
안 옮기면 그 사이 버전의 테이블이 전부 최신으로 통과한다.

안내 문구는 `classified_item_aspect` 를 **항상 함께** 지우게 되어 있다(2026-08-13). 그
테이블에는 마커가 없어 `legacy` 목록에 안 들어오는데, 부모만 지우면 부모 없는 aspect 행이
남고 **월간 집계는 부모를 안 거치므로**(`FROM voc_document r JOIN classified_item_aspect a`)
옛 라벨이 계속 리포트에 잡힌다. 위 3번과 안내가 같은 끝 상태를 만들어야 한다.

### 🔜 후속 — 마커 방식 자체를 걷어내는 안 (미적용)

이 방식은 **"컬럼을 추가하면 마커도 옮긴다"는 사람 규칙에 의존**한다. 실제로 같은 모양의
사고가 이미 두 번 났다(PR #37 → PR #71 → 이 수정). 이번 회귀 테스트는 4컬럼 스냅샷이라
**다음번 누락은 못 잡는다** — 7번째 컬럼을 넣고 마커를 안 옮기면 4컬럼 테스트는 그대로
통과하고 직전 버전인 6컬럼 DB 가 `[]` 로 빠진다.

**DDL 을 정본으로 삼으면 유지 규칙이 사라진다** (지인님 프로토타입, 2026-08-13):

```python
def find_legacy_tables(conn):
    ref = sqlite3.connect(":memory:")
    create_classified_tables(ref)                 # DDL 이 정본
    return [t for t in AI_OWNED_TABLES
            if _cols(conn, t) and not _cols(ref, t) <= _cols(conn, t)]  # 부분집합 검사
```

| 케이스 | 현재(마커) | DDL 기준 |
|---|---|---|
| 4컬럼 / 5컬럼 | 잡음 | 잡음 |
| 최신 · 테이블 없음 | `[]` | `[]` |
| **`classified_item_aspect` 만 구버전** | **못 잡음** | 잡음 |
| 미래에 컬럼이 더 붙은 DB | `[]` | `[]` (부분집합이라 오탐 없음) |

컬럼을 추가해도 **아무것도 안 해도** 옛 DB 가 잡히고 자식 테이블 구멍도 같이 닫힌다.
호출이 시작 시점 1회라 in-memory DB 비용은 무시할 수준이다.

**이 PR 에 넣지 않은 이유**: `app/core/` 계약 파일의 구조 변경이라 "열린 버그를 막는" 이번
수정과 리뷰 단위가 다르다. 지인님도 분리를 권했다. 다만 **컬럼을 다음에 추가하기 전에는
정리하는 것이 맞다** — 그때가 이 방식이 세 번째로 실패할 자리다.

---

## 5. 컬럼이 생긴 뒤 따라오는 코드 변경

| 위치 | 변경 | 상태 |
|---|---|---|
| `app/core/raw_schema.py` | DDL 2컬럼 + 인덱스 + `active_version_predicate()`·`version_params()` | 완료 |
| `app/core/raw_schema.py` | **`LEGACY_MARKERS` 의 `classified_item` 마커를 `pipeline_version` 으로** | 2026-08-13 |
| `app/core/versions.py` | `CLASSIFIER_PIPELINE_VERSION` (신규 파일) | 완료 |
| `scripts/classification_worker.py` | upsert 2컬럼, stale 판정 3축, 버전 분포 로그 | 완료 |
| `app/batch/daily.py` | 탐지 필터 3축, cutover 가드(fail-closed) + 본문 조건 정합 | 완료 |
| `app/batch/daily.py` | `_classifier_versions_for()` 에 `model`·`pipeline` 추가 | 완료 |
| `app/core/mq.py` | 변경 없음 — payload 는 넘겨받은 dict 를 그대로 싣는다 | — |

**술어와 파라미터 순서는 `raw_schema` 가 정본이다.** 적재(`scripts/`)와 조회(`app/`)가
각자 적으면 한쪽만 고쳐졌을 때 조회가 0건이 되고, 그건 미탐이라 조용하다. 자리 수·순서가
어긋나도 SQL 은 에러 없이 **다른 것을 비교**한다(전부 TEXT 라 타입으로도 안 걸린다).

`CLASSIFIER_PIPELINE_VERSION` 은 **`app/core/constants.py` 가 아니라 `app/core/versions.py`**
에 뒀다. constants.py 머리말이 "정량 실험 때 바꿔가며 돌려야 하는 값 + 변경 전 팀 합의 필수"
를 선언하는데 이 값은 성격이 다르다 — 스윕 대상이 아니고, 바꾸면 실험 결과가 달라지는 게
아니라 **이미 적재된 행이 통째로 탐지 대상에서 빠진다.** 같은 파일에 두면 "여기 있는 건 전부
튜닝 손잡이" 라는 신호가 흐려진다.

### `model_version` 을 필터에 넣기로 한 결정

⚠️ **`LLM_MODEL` 오타 하나가 윈도우 전체를 stale 로 만든다.** `_check_version_cutover()`
가 `RuntimeError` 로 세우므로 운영에서 거칠게 느껴질 수 있다. 그럼에도 넣은 이유는,
빼면 "같은 프롬프트 · 다른 모델" 이 통째로 새어 나가고 **그쪽은 조용하기 때문**이다.
세우는 실패는 사람이 보고 고칠 수 있지만, 섞이는 실패는 알림 숫자로만 나타난다.

가르는 법은 에러 메시지가 준다 — 활성 3축을 전부 찍으므로 "설정이 틀렸다" 와
"backfill 이 필요하다" 를 값을 보고 구분할 수 있다.

### 🔴 혼재 윈도우는 경고가 아니라 **중단**이다 (fail-closed, 2026-08-12 결정)

처음 구현은 "활성 0건이면 세우고, 섞여 있으면 경고" 였다. **그게 오탐을 새로 열었다.**

활성 버전 필터는 `_ASPECT_SQL`, 즉 **분자에만** 걸린다. 분모는 원문(`_DOCUMENT_SQL`)이라
필터를 안 타므로, 과거 구간이 stale 이면 `past_neg` 만 0 이 되고 `past_total` 은 그대로다.
기준선이 작아지는 게 아니라 **0 이 된다.**

진짜 부정률을 양쪽 다 5% 로 고정(변화 없음)하고 리뷰 소스로 실측:

| | documents(분모) | items(분자) | 발행 알림 |
|---|---|---|---|
| 대조군(전부 활성) | 700 | 700 | **0건** |
| 섞임(과거=옛 버전) | 700 | 140 | **1건** 🚨 `past=0.0000 cur=0.0500 delta=+0.0500` |

데이터는 한 글자도 다르지 않고 과거 구간의 `prompt_version` 만 다르다. 필터가 없던
시절에 돌리면 0건이므로, **필터가 새로 여는 오탐 경로**였다.

CS 는 우연히 안전하다(과거 aspect 가 0 이 되면 `check_coverage` 가 갭으로 잡는다).
리뷰는 `COVERAGE_CHECKED_SOURCES` 가 CS 전용이라 안 잡히고, 리뷰 커버리지는 그 방법으로
**원리적으로 검증이 안 된다**(`detection/loader.py` docstring) — 방어선이 없다.

**검토한 두 안:**

1. **stale 행의 원문을 분모에서도 뺀다** — 분자·분모를 같은 행 집합에서 센다.
   적용하면 `documents=200 / items=200` 으로 정상화된다.
2. **stale 이 하나라도 있으면 세운다** (채택)

**2안을 택한 이유.** 혼재는 표본이 준 것이 아니라 **검정 전제가 깨진 것**이다. Fisher
검정은 같은 분류기로 완전히 라벨링된 현재 7일과 과거 28일을 비교한다는 전제 위에 선다.
1안은 라벨 누수는 아니지만(결과가 아니라 출처로 거른다) **비무작위 결측**을 들인다 —
`FETCH_STALE_SQL` 이 `ORDER BY r.occurred_at` 이라 `--limit` 으로 나눠 backfill 하면
오래된 것부터 채워져, 남는 분모가 시간순 앞쪽 조각만 된다. 추세가 있으면 교란되고,
무엇보다 **불완전한 윈도우를 정상 검정으로 간주**하게 된다.

지금 `classified_item` 이 비어 있어 fail-closed 비용이 가장 낮은 시점이기도 하다.
가용성보다 통계적 정합성을 택한다 — 재현 가능하고 설명도 명확하다.

⚠️ 한 건 때문에 배치가 서는 비용이 실제로 크다고 **측정으로** 확인되면, 그때 합의를 거쳐
   슬롯 단위 보류를 설계한다. 원문 분모 제외로 조용히 우회하지는 않는다.

회귀 테스트: `tests/test_load_inputs_from_db.py` 의 대조군/혼재군 2개
(`test_control_window_without_stale_raises_no_alert`, `test_mixed_window_stops_before_detection`).
필터가 거르는지까지만 보던 단위 테스트로는 이 오탐을 못 잡았다.

#### 🔴 불변식: 세우는 집합 ⊆ 고칠 수 있는 집합

fail-closed 는 교착을 만들 수 있다. 워커의 stale 조회(`FETCH_STALE_SQL`·`COUNT_STALE_SQL`)는
`TRIM(content) <> ''` 를 요구하는데 탐지의 `_VERSION_COUNT_SQL` 이 같은 조건을 안 걸면,
**본문이 빈 원문의 stale 분류행 1건으로 빠져나갈 길이 없어진다**:

```
워커 count_stale()       = 0     ← --reclassify-stale 대상 없음
워커 fetch_stale_batch() = 0 rows
배치                     = RuntimeError 로 매일 중단
```

에러가 시키는 `--reclassify-stale` 은 "재분류할 문서가 없습니다"로 끝난다. 경고만 하던
때는 무해했고 fail-closed 로 바뀌면서 교착이 됐다.

→ `_VERSION_COUNT_SQL` 에 같은 본문 조건을 건다. **세우는 조건을 바꿀 때는 워커가 고칠 수
있는 집합과 같은지 반드시 확인할 것.** 회귀:
`test_blank_content_stale_row_does_not_deadlock_the_batch`.

고칠 수 없는 행은 `count_orphan_stale()` 로 따로 세어 경고 문구를 가른다 —
"backfill 하세요"와 "backfill 로는 못 없앤다"는 사람이 할 일이 다르다.

---

## 6. 보류 — `--restamp-versions` (설계만 기록)

재분류 없이 버전 컬럼만 채우는 UPDATE 경로. **지금은 대상이 없어 불필요하다.**
나중에 "프롬프트는 그대로인데 장부만 없는 행"이 생기면 이 설계로 간다.

```
python scripts/classification_worker.py --restamp-versions --assume-model gpt-4o-mini
```

`--assume-model` 을 **필수 인자**로 두는 것이 핵심이다. 재도장은 관측이 아니라
운영자의 주장("이 행들은 이 모델로 만들어졌다")이라, `.env` 에서 조용히 주워오게 두면
검증 안 된 값이 검증된 것처럼 굳는다. 손으로 적게 해야 주장이 기록으로 남는다.

---

## 7. 이벤트 payload

`ai.anomaly.analyzed` payload 에 `classifier_versions` 가 실린다(§4.1.2). 컬럼이 생기면서
**3축이 다 들어간다**:

```json
"classifier_versions": {
  "prompt_cs": "classify_aspect_v5",
  "prompt_review": "classify_sentiment_v4",
  "model": "gpt-4o-mini",
  "pipeline": "classify_pipeline_v1"
}
```

실을 수 있는 근거는 `app/batch/daily.py` `_ASPECT_SQL` 의 활성 버전 필터다. 그 필터가
"이 알림에 기여한 **모든 행**의 버전 3종이 활성 값" 임을 쿼리로 강제하므로, 주장이 아니라
관측이다. 그래서 `_classifier_versions_for()` 는 값을 `_active_version_params()` 에서
가져온다 — **필터가 쓴 것과 같은 값**이라야 payload 가 실제로 읽은 것을 말한다.

⚠️ `--input-source golden` 은 그 필터를 안 타므로 `null` 이 나간다. 골든은 분류 오차가 0 인
oracle 입력이라 애초에 분류기를 안 거쳤다 — `null` 이 정확한 답이지 누락이 아니다.

**payload 스키마는 백엔드 §4.1 합의 대상이다.**
