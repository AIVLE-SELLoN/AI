# SELLoN AI Service

멀티채널(쿠팡·네이버·지그재그) CS 문의와 리뷰를 **분류 → 이상탐지 → 개선안 생성** 으로
흘려, 셀러에게 *무엇이* / *왜* 문제이고 / *어떻게 고치면 되는지* 까지 내려주는 FastAPI 서비스.

```
CS 문의 · 리뷰 원문
      │
      ├─ ① classification   aspect(색상·사이즈·소재)와 감성을 분류
      │
      ├─ ② detection        채널별 부정률 급증을 통계로 탐지 (Fisher + BH-FDR)
      │                     → 편중형 / 전역형 판정 → 원인 라벨 진단
      │
      └─ ③ recommendation   상세페이지 근거(RAG)를 찾아 개선안 생성
                            → 자기검증 3기준, 실패 시 재시도 → 확신도 부여
                            → 셀러 승인·반려(HITL) 결과를 학습 자료로 적재
```

핵심은 **"근거 없는 개선안은 내놓지 않는다"** 입니다. 개선안이 인용한 상세페이지 문구가
실제 원문에 있는지 코드가 문자 단위로 대조하고, 통과하지 못하면 재시도하거나
일반 가이드로 내려갑니다.

---

## 지금 상태

| 모듈 | 담당 | 상태 |
| --- | --- | --- |
| `classification` (Agent1) | 현진 | ✅ 구현 — `POST /api/v1/classify` |
| `detection` (Agent2) | 서영 | ✅ 구현 — `POST /api/v1/detect`, 로직 [0]~[8]을 단계별 모듈로 분리 |
| `recommendation` (Agent3) | 지인 | ✅ 구현 — `POST /api/v1/recommendations/generate`, `/hitl` |
| `reporting` | 용준 | 🚧 서비스 로직은 작성됨, 라우터는 아직 `501` |

- 테스트 **521개 / 38파일** (`pytest`)
- ⚠️ **CI 워크플로는 아직 없습니다.** `tests/` 는 현재 로컬에서 수동 실행합니다.

## 정량 실험 하이라이트

Agent3 실측 — 2026-07-29 · gpt-4o-mini · 표본 n=15/11/4

| 실험 | 결과 |
| --- | --- |
| Retrieval hit rate | **15/15** (100%) |
| Grounding precision | **4/4** (100%) |
| **RAG 유무 대조** | RAG 없음 **0/4** → RAG 있음 **4/4** |
| 라우팅 정확도 | golden **11/11**, 실제 CS **201/201** |
| 재시도 분포 | 1차 통과 11건 / 2차 통과 4건 / 3차·fallback 0건 |

RAG 없이 원인 라벨만 주면 LLM은 상세페이지에 **있지도 않은 문구를 그럴듯하게 지어냈고**(0/4),
근거를 붙여주자 4건 전부 실제 원문을 인용했습니다. 재시도 4건은 1차 실패 후 통과한
케이스로, 재시도 로직이 장식이 아니라는 증거입니다.

> 표본이 작습니다(n=15/11/4). 100%를 "완벽"이 아니라 "아직 실패를 못 봤다"로 읽어야 합니다.
> 실험 설계·한계·나머지 5종은 [`eval/README.md`](eval/README.md).

---

## 빠른 시작

**요구 버전: Python 3.12** (개발·검증 기준 3.12.13)

```bash
# 1. 가상환경 (반드시 3.12 로)
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS/Linux

# 2. 의존성
pip install -r requirements.txt

# 3. 환경변수
cp .env.example .env            # 그리고 LLM_API_KEY 채우기

# 4. 실행
uvicorn app.main:app --reload
```

| | |
| --- | --- |
| API 문서 | http://localhost:8000/docs |
| 헬스체크 | http://localhost:8000/health |
| 테스트 | `pytest` |

> **Python 3.13+ 는 검증 안 됐습니다.** scipy·chromadb 등 바이너리 패키지가 3.12 기준으로
> 고정돼 있어 설치가 깨질 수 있습니다. 패키지 버전은 `requirements.txt` 에 고정돼 있고,
> 실제 기동·테스트 통과가 확인된 조합이니 임의로 올리지 마세요.

<details>
<summary><b>Windows 에서 <code>python</code> 명령이 안 먹을 때</b> (스토어가 뜨는 경우)</summary>

**Windows 는 기본적으로 Python 이 없습니다.** 터미널에 `python` 을 쳤을 때 버전이 안 나오거나
마이크로소프트 스토어가 뜨면 아직 없는 것입니다 (스토어에 뜨는 건 가짜 스텁이니 그걸로 설치하지 마세요).

1. [python.org/downloads](https://www.python.org/downloads/) 에서 **Python 3.12** 다운로드
2. 설치 첫 화면에서 **"Add python.exe to PATH" 체크** ← 놓치면 `python` 명령이 계속 안 먹습니다
3. **새 터미널** 을 열고 (기존 창은 PATH 갱신이 안 됨) `python --version` 확인

여전히 스토어가 뜬다면: Windows 설정 → 앱 → 고급 앱 설정 → **앱 실행 별칭** →
`python.exe` / `python3.exe` 를 **끄기**.

</details>

<details>
<summary><b>VSCode 인터프리터 선택</b></summary>

`Ctrl+Shift+P` → `Python: Select Interpreter` → `.\.venv\Scripts\python.exe`.
안 하면 "Package not installed" 경고가 뜹니다.

</details>

---

## 구조

```
app/                   # 실제 서비스 코드 (운영 컨테이너에 포함되는 유일한 폴더)
├── main.py            # 앱 생성 + 라우터 등록만
├── config.py          # 환경변수
├── core/              # 공유 계층 — 수정 시 팀 합의 필수 (schemas.py = 계약서)
├── classification/    # 현진 (Agent1) — aspect·감성 분류
├── detection/         # 서영 (Agent2) — 이상탐지 + 원인분류 ([0]~[8] 단계별 모듈)
├── recommendation/    # 지인 (Agent3) — 개선안 생성 (pipeline.py 가 본체)
└── reporting/         # 용준 — 월간 리포트 + CS 답변 초안

tests/                 # pytest — 비용 0
└── fixtures/          # 개발용 소규모 계약 예시 — golden 아님, 정답 없음

eval/                  # 정량 실험 ①~⑥ — 사람이 수동 실행, LLM 과금, CI 금지
data/                  # 파이프라인 규모 mock (~26만행) — git 제외, 아래 참고
scripts/               # 데이터 생성·적재·검산 (mock_producer, seed_vectordb 등)
docs/                  # 확정 스펙 (노션 정본의 코드측 사본)
```

**`tests/` 와 `eval/` 은 성격이 다릅니다.**

| | `tests/` | `eval/` |
| --- | --- | --- |
| 목적 | 코드가 안 깨졌나 | 성능이 얼마나 나오나 |
| 실행 | 개발 중 수시로 | 필요할 때만 |
| 비용 | 0 | **LLM 과금** |

## 데이터

`data/` 는 **git 에 포함되지 않습니다.** 클론 후 직접 만들어야 합니다:

```
data/
├── input/      # input_*.csv   — Mock Producer 발행용 원본
├── config/     # config_*.csv  — 생성기 설정 (사람이 직접 작성)
└── golden/     # golden_*.csv  — 채점 정답지
```

> ⚠️ **`data/golden/` 은 `eval/` 만 읽습니다.** `app/` 코드가 import 하면 컨닝이고,
> 정확도 측정이 무의미해집니다.

---

## 개발 규칙

전문은 [`개발환경-컨벤션.md`](개발환경-컨벤션.md) 4·5장. 자주 걸리는 것만:

1. **LLM 호출은 `core/llm_client.py` 경유.** 각자 `openai` 직접 import 금지.
2. **프롬프트는 `prompts/` 파일로.** 하드코딩 금지, 구버전 삭제 금지 (버전 비교가 곧 정량 실험).
3. **모듈끼리 import 금지.** 데이터는 `core/schemas.py` 의 Pydantic 모델로만 주고받습니다.
4. **`core/`·`schemas.py` 변경은 팀 채팅 선공지 후.**
5. **LLM 호출 함수는 `async def`.**
6. **매직넘버 금지.** `0.05` 대신 `constants.ALPHA`.
7. **로그에 추적 키(`item_id` 등) 항상 포함.**

**브랜치**는 모듈 단위 (`feat/agent2-detection`), PR 은 1명 승인이면 머지.

## 열린 항목

- **`alert_id` 기준 중복 생성 방지** — 개선안을 선생성 방식으로 확정하면서 생긴 요구사항.
  Kafka 재전달 시 같은 알림에 서로 다른 개선안이 쌓일 수 있습니다.
  백엔드 upsert vs 결정론적 ID 파생 중 미확정.
- **CS 원문(`raw_text`) 조회 경로** — 인용(`Citation`)을 채우려면 원문이 필요한데,
  목서버 온디맨드 조회로 방침만 정하고 엔드포인트는 대기 중입니다.
- **CI 워크플로 부재** — `tests/` 가 자동으로 돌지 않습니다.

## 문서

| 문서 | 내용 |
| --- | --- |
| [`개발환경-컨벤션.md`](개발환경-컨벤션.md) | 버전·컨벤션·환경 세팅 (**시작 전 "2-1. 꼭 읽어주세요" 필독**) |
| [`eval/README.md`](eval/README.md) | 정량 실험 6종 가이드·비용 통제 |
| [`docs/`](docs/) | 확정 스펙 (스키마·탐지 로직·개선안 로직) |
| 팀 노션 | 설계 의도, 실험 결과 정본, Mock 데이터 정의서 |
