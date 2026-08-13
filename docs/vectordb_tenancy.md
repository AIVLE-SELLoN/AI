# 벡터DB 회사 범위 격리(테넌시) 설계 노트

- 상태: **적용 완료** — 컬렉션2는 PR #77(2026-08-13), 컬렉션1은 이 변경.
- 작성: 2026-08-13
- 코드 정본: `app/core/vectordb.py` (`current_tenant` · `scoped_document_id` ·
  `TENANT_METADATA_KEY`)
- 대상: Chroma 컬렉션 2개 — `detail_pages`(컬렉션1) · `rejection_reasons`(컬렉션2)

> ⚠️ **이 문서는 "왜 이 기제인가" 의 정본이다.** 기제를 바꾸려는 사람은 §3 의 실측을
> 먼저 볼 것 — 다른 두 기제는 몰라서 안 고른 게 아니라 **재보고 반려한 것**이다.

---

## 1. 왜 필요한가

`product_group_id` 는 **회사별 시퀀스**다 — A사에도 `P001` 이 있고 B사에도 `P001` 이
있다(2026-08-12 백엔드 민준님 확인). 그래서 그 값에서 파생되는 것들이 전부 **회사
안에서만 유일**하다:

```
alert_id           = ALT-{window_end}-{product_group_id}-{ASPECT}-{channel}
recommendation_id  = REC-{같은 꼬리}
컬렉션1 문서 키     = {product_group_id}:{channel}:{aspect}
```

백엔드는 `(companyId, alert_id)` **복합 유니크**로 이걸 흡수한다. **벡터DB엔 그 축이
없었다.** 회사 두 곳이 같은 조합을 만들면:

| 컬렉션 | 축이 없을 때 무슨 일이 나나 |
|---|---|
| 컬렉션1 (상세페이지) | 나중 시딩이 앞 회사 문서를 **덮는다**. 그리고 조회가 필터를 못 걸어 **다른 회사 상세페이지가 개선안의 인용 근거**가 되고, 그 문장이 `citations` 에 박제돼 셀러 화면까지 나간다 |
| 컬렉션2 (반려 사유) | 나중 HITL 결과가 앞 회사 문서를 **덮는다**. 조회가 `aspect` 하나로만 좁혀 다른 회사 반려 사례가 `similar_case` 로 **새어 나온다** |

⚠️ **지금 데모는 1회사라 위 경로가 도달 불가다**(`mq_company_id` 가 config 스칼라라
**배포 1개 = 회사 1개**). 그래도 넣은 이유는 **컬렉션2가 0건인 지금이 비용 0**이고,
HITL 이 돌기 시작하면 기존 문서 이관 작업이 되기 때문이다. 컬렉션1도 지금은 504건
재시딩이면 끝난다.

### 왜 두 번에 나눠서 했나

PR #77 은 컬렉션2만 닫고 컬렉션1을 후속으로 남겼다. **설계가 달라서가 아니라 비용
모양이 달라서다:**

| | 컬렉션2 | 컬렉션1 |
|---|---|---|
| 기존 문서 | **0건** — 이관 대상이 없다 | 504건 |
| 머지 후 남이 할 일 | 없음 | **팀 전원 재시딩 1회** |

컬렉션2는 코드만 바뀌면 끝나서 리뷰 한 번에 닫을 수 있었고, 컬렉션1은 **돈이 아니라
조율**이 든다(각자 로컬에 `.chroma/` 가 있고 gitignore 다). #77 에 같이 넣었으면 그
PR 의 리뷰 범위에 "전원 재시딩" 이 딸려와 판단이 섞였을 것이다.

🔴 **그리고 축 자체와 기제 선택도 되돌리기 비용이 비대칭이다** — 축을 데이터에 넣는 건
어느 기제로 가도 전제라 싸고, 기제를 잘못 고르면 **두 번 이관**한다. 그래서 #77 은 축만
넣고 기제를 미뤘고, 이 변경에서 실측(§3)으로 확정했다.

---

## 2. 무엇을 넣었나 — 셋은 짝이다

```
쓰기 문서 ID   scoped_document_id(tenant, raw_id)   →  "SLN-aaa:P001:COUPANG:색상"
쓰기 metadata  {TENANT_METADATA_KEY: tenant}        →  {"company_id": "SLN-aaa", ...}
조회 where     {TENANT_METADATA_KEY: tenant}        →  $and 절에 같이 넣는다
```

🔴 **하나만 지워도 격리가 깨지는데 모양이 다르다** — ID 축을 지우면 다른 회사 문서를
*덮고*(유실), 조회 필터를 지우면 덮지 않은 채 *새어 나온다*(누출). **독립적인 두 사고라
뮤테이션 테스트도 따로 잡아야 한다.** PR #77 에서 실측으로 확인됐다: 조회 필터를 지우는
뮤테이션이 ID·metadata 와 **다른 테스트**에 걸린다.

🔴 **`tenant` 는 호출부가 한 번 읽어 셋에 넘긴다.** `scoped_document_id()` 안에서
`current_tenant()` 를 부르면 호출부가 metadata 용으로 한 번 더 읽어 **tenant 를 읽는
곳이 둘**이 된다(PR #77 구현 중에 실제로 그 상태였고 monkeypatch 가 한쪽만 먹으면서
드러났다).

⚠️ **조회 필터를 `query_documents`/`get_documents` 헬퍼 안에서 자동 적용하지 않는다.**
그러면 축 없이 시딩된 기존 문서가 **전건 0건**이 되고, 무엇보다 컬렉션마다 필터 축이
달라(컬렉션1은 상품·채널·aspect, 컬렉션2는 aspect) 한 자리에서 못 정한다.

### 적용 지점

| 파일 | 역할 |
|---|---|
| `scripts/seed_vectordb.py` | 컬렉션1 쓰기 — ID + metadata |
| `app/recommendation/pipeline.py` `_get_detail_page_text` | 컬렉션1 조회 필터 |
| `app/recommendation/pipeline.py` `record_hitl_outcome` | 컬렉션2 쓰기 (PR #77) |
| `app/recommendation/pipeline.py` `retrieve_context` | 컬렉션2 조회 필터 (PR #77) |
| `eval/run_recommendation_eval.py` | 컬렉션1 조회 — **운영과 같은 조건이어야** 실험이 운영보다 후한 수치를 내지 않는다 |

⚠️ `eval/run_embedding_eval.py` 는 대상이 아니다 — 매 실행 자기 소유 일회용 컬렉션
(`embed_eval_{tag}_{uuid}`)을 만들어 쓴다.

---

## 3. 분리 기제 — 셋을 재보고 골랐다

Chroma **1.5.9** 기준 실측이다(`PersistentClient`, 로컬 파일 모드).

| | metadata + ID 접두어 ✅ | 컬렉션명 분리 | Chroma `tenant`/`database` |
|---|---|---|---|
| 사전 생성 필요 | 없음 | 없음 | **있음** — `AdminClient` |
| 없는 값으로 열면 | (해당 없음) | **조용히 빈 컬렉션 생성** | `NotFoundError` (시끄럽게 실패) |
| 격리 실측 | 검증됨 | 검증됨 | 검증됨 |
| HTTP 모드(k8s) | 그대로 됨 | 그대로 됨 | **서버 admin 권한 필요** |
| 컬렉션2와 일관성 | 동일 | 갈림 | 갈림 |
| 되돌리기 비용 | 낮음 | 중간(재시딩) | 높음(데이터가 별도 DB로) |

실측 원문:

```
PersistentClient(tenant='SLN-aaa')    → NotFoundError: Tenant [SLN-aaa] not found
PersistentClient(database='SLN-aaa')  → NotFoundError: Database [SLN-aaa] not found
AdminClient(Settings(is_persistent=True, persist_directory=...)).create_tenant(...)
                                      → OK, 이후 PersistentClient(tenant=..., database=...) 동작
같은 path 의 dbA/dbB 에 같은 컬렉션명·같은 문서 ID → 문서가 안 겹침 (격리 O)
컬렉션명 분리: get_or_create 라 사전 생성 없이 즉시 만들어짐, 64자 이름도 통과 (격리 O)
```

### 왜 metadata + ID 접두어인가

1. **Chroma `tenant`/`database` 는 지금 고를 수 없다.** 사전 생성이 필요한데, HTTP
   모드(k8s 배포)에서 그건 **우리가 소유하지 않은 서버의 admin 권한**을 요구한다.
   RabbitMQ 토폴로지가 같은 계열이다 — 운영 exchange·큐는 백엔드 소유라 우리 계정에
   `configure` 권한이 **없을 수 있고**, 그래서 `consume()` 이 운영에서 바인딩을 아예
   안 건다(`app/core/mq.py`). 권한이 확보되기 전에 이 기제를 고르면 **배포가 뜨는 것
   자체가 남의 프로비저닝에 묶인다.**
   - ⚠️ 우리 Chroma 는 현재 **로컬 파일 모드**(`CHROMA_PERSIST_DIR`)라 admin 접근이
     실제로 막힌 상태는 아니다. 반려 사유는 "지금 막혀 있다" 가 아니라 **"HTTP 모드로
     가는 순간 남의 서버 설정에 묶인다"** 는 쪽이다.
   - `get_client()` 가 `tenant`/`database` 인자를 안 쓰고 기본값으로 여는 건 **미구현이
     아니라 이 선택의 결과다.**
2. **컬렉션명 분리는 실패가 조용하다.** 이름이 어긋나면 `get_or_create_collection` 이
   **빈 컬렉션을 새로 만든다** — 조회 0건이 "미등록" 과 구분이 안 된다. 그 구분을 하려고
   `_log_detail_page_miss` 를 만들어 둔 마당에 같은 모호함을 한 겹 더 얹는 셈이다.
   `reset_collections()` 가 고정 2개 이름을 도는 것도 못 쓰게 된다.
3. **컬렉션2가 이미 이 기제다.** 컬렉션1만 다른 기제로 가면 **두 컬렉션이 새로운 방식으로
   비대칭**이 된다. 지금 닫고 있는 게 바로 그 비대칭이다.

### 다시 볼 조건

**배포 하나가 회사 여럿을 담당하게 되면** 이 결정을 다시 본다. 그때는
`mq_company_id` 가 config 스칼라라는 전제부터 깨지고, `get_client()` 의 `@lru_cache`
(인자 없음 = 프로세스당 클라이언트 1개)도 같이 봐야 한다. 그 시점엔 Chroma
`tenant`/`database` 의 **"틀린 범위면 시끄럽게 실패한다"** 가 metadata 필터의
"조용히 새어 나간다" 보다 확실히 낫다 — 프로비저닝 비용을 치를 값어치가 생긴다.

**축을 데이터에 넣는 것은 어느 기제로 가도 전제라**, 그때 이관하는 건 기제뿐이고 이
문서·테스트·데이터 축은 그대로 쓴다.

---

## 4. 재시딩 — 팀 전원 대상

🔴 **이 변경이 머지되면 각자 한 번 돌려야 한다.**

```
python scripts/seed_vectordb.py --reset
```

- 축 없이 시딩된 기존 문서엔 `company_id` metadata 가 없어서 **조회 필터가 전건을
  걸러낸다.** 504건이 멀쩡히 들어 있는데 조회는 0건이다.
- 그 상태를 `_log_detail_page_miss` 가 **WARNING 으로 갈라준다**(`--reset` 을 안내한다).
  안 갈라주면 `collection.count()` 가 504라 첫 분기에 안 걸리고 **"상세페이지
  미등록"(INFO)** 으로 조용히 오진한다 — 사람은 상품 등록 쪽을 파는데 실제 조치는
  재시딩이다.
- `.chroma/` 는 gitignore 라 **각자 로컬이 곧 환경**이다. 비용은 임베딩 504건.

⚠️ **`--reset` 은 컬렉션2도 지운다**(임베딩 모델이 컬렉션별 설정이라). 지금은 0건이라
잃을 게 없지만, **HITL 실사용이 시작된 뒤엔 반려 사례가 복구 불가로 날아간다.**
그래서 **지금이 최저 비용 시점**이다.

---

## 5. 남아 있는 것

- **운영 `product_group_id` 형식 미정** — 지금 `P001~P042` 는 목 전용 편법이다
  (`golden_mapping.csv` ⋈ `input_channel_products.csv` 조인 결과, 2026-08-13).
  백엔드 매퍼가 자기 네임스페이스로 값을 내면 **컬렉션1을 또 한 번 재시딩**해야 한다.
  민준님 회신 대기.
  - ⚠️ **이 건과 회사 축은 별개다.** 우연히 같은 작업(재시딩)을 요구할 뿐 사유가 다르다.
    회사 축은 상품ID 형식과 무관하게 필요하다.
  - 🔴 **두 건이 각각 재시딩을 요구하므로, 따로 하면 팀 전원이 두 번 돌린다.** 매핑
    형식이 확정되면 그때 한 번에 묶는 것이 낫다.
- **`LOCAL_TENANT` 폴백(`_local`)** — `MQ_COMPANY_ID` 가 비어 있는 개발 환경용이다.
  빈 값으로 쌓은 뒤 값을 채우면 그 전 문서는 조회에서 빠진다(운영은 처음부터 값이 있다).
- **회사 간 조회는 여전히 코드로 가능하다** — 필터를 안 걸면 다 보인다. 이 기제는
  **실수를 막는 것**이지 권한 경계가 아니다. 진짜 경계가 필요해지면 §3 「다시 볼 조건」.
