# SELLoN AI Service

CS·리뷰 데이터를 **분류 → 이상탐지 → 개선안 생성** 으로 흘리는 FastAPI 서비스.

📄 **설계 의도 (`기술 정리` 문서)** → **팀 노션**
📄 **버전·컨벤션·환경 세팅** → 저장소 루트의 [`개발환경-컨벤션.md`](개발환경-컨벤션.md)
　　↑ **개발 시작 전에 "⚠️ 2-1. 꼭 읽어주세요" 절은 반드시 보세요.**
　　　chromadb·langgraph·pydantic 이 메이저 버전이 올라가서 검색되는 예제가 안 맞습니다.

## 요구 버전

| 항목 | 버전 |
| --- | --- |
| **Python** | **3.12** (개발·검증 기준: 3.12.13) |
| pip | 23 이상이면 무난 |

패키지 버전은 `requirements.txt` 에 고정돼 있습니다. 실제 설치·기동·테스트 통과가
확인된 조합이라 임의로 올리지 마세요. 자세한 표는
[`개발환경-컨벤션.md`](개발환경-컨벤션.md) 2장.

> Python 3.13+ 는 검증 안 됐습니다. scipy·chromadb 등 바이너리 패키지가 3.12 기준으로
> 고정돼 있어, 3.13 에서는 설치가 깨질 수 있습니다. **3.12 를 쓰세요.**

## 실행

### 0. Python 3.12 설치 (안 깔려 있으면)

**Windows 는 기본적으로 Python 이 없습니다.** 터미널에 `python` 을 쳤을 때
버전이 안 나오거나 **마이크로소프트 스토어가 뜨면** 아직 없는 것입니다
(스토어에 뜨는 건 가짜 스텁이니 그걸로 설치하지 마세요).

1. [python.org/downloads](https://www.python.org/downloads/) 에서 **Python 3.12** 다운로드
2. 설치 첫 화면에서 **"Add python.exe to PATH" 체크** ← 이걸 놓치면 `python` 명령이 계속 안 먹습니다
3. **새 터미널** 을 열고 (기존 창은 PATH 갱신이 안 됨) 확인:
   ```bash
   python --version        # Python 3.12.x 가 나와야 함
   ```

<details>
<summary>여전히 마이크로소프트 스토어가 뜬다면 (스텁 끄기)</summary>

Windows 설정 → 앱 → 고급 앱 설정 → **앱 실행 별칭** →
`python.exe` / `python3.exe` 항목을 **끄기**. 그 뒤 새 터미널에서 다시 확인.
</details>

### 1~4. 프로젝트 세팅

```bash
# 1. 가상환경 (반드시 Python 3.12 로)
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

- API 문서: http://localhost:8000/docs
- 헬스체크: http://localhost:8000/health

> **VSCode 인터프리터 선택:** `Ctrl+Shift+P` → `Python: Select Interpreter` →
> `.\.venv\Scripts\python.exe`. 안 하면 "Package not installed" 경고가 뜹니다.

## 테스트

```bash
pytest
```

`tests/test_app_boot.py` 는 4명의 라우터가 앱 1개에 다 붙는지 확인합니다.
라우터를 건드렸으면 이것부터 돌려보세요.

## 구조

```
app/
├── main.py            # 앱 생성 + 라우터 등록만
├── config.py          # 환경변수
├── core/              # 공유 계층 — 수정 시 팀 합의 필수 (schemas.py = 계약서)
├── classification/    # 현진 (Agent1) — aspect·감성 분류
├── detection/         # 서영 (Agent2) — 이상탐지 + 원인분류
├── recommendation/    # 지인 (Agent3) — 개선안 생성 (LangGraph)
└── reporting/         # 용준 — 월간 리포트 + CS 답변 초안
tests/                 # 공용 fixtures + 모듈별 테스트
scripts/               # seed_vectordb.py 등
```

## 현재 상태 (0주차)

⚠️ **아직 뼈대만 있습니다.** 라우터는 `501 Not Implemented` 를 반환하고,
`core/schemas.py` 와 `tests/fixtures/*.json` 은 비어 있습니다.

**의도된 상태입니다.** 문서 9장:
> 전원 회의: `schemas.py` 확정 — **완료 전 개발 시작 금지**

스키마는 4명이 주고받는 계약이라 회의 전에 채우면 안 됩니다.

### 0주차 체크리스트

- [x] 리포지토리 생성 + 폴더 구조 커밋
- [x] `core/llm_client.py` 작성
- [x] 각자 hello world 라우터 1개 → 앱 1개가 4명 코드로 뜨는지 확인
- [ ] **전원 회의: `schemas.py` 확정** ← 다음 할 일
- [ ] fixture 생성 (진양성/위양성함정/경계선/노이즈 4케이스)

## 개발 규칙 (요약)

전문은 [`개발환경-컨벤션.md`](개발환경-컨벤션.md) 4·5장 참고. 자주 걸리는 것만:

1. **LLM 호출은 `core/llm_client.py` 경유.** 각자 `openai` 직접 import 금지.
2. **프롬프트는 `prompts/` 파일로.** 코드에 하드코딩 금지, 구버전 삭제 금지
   (버전 비교가 곧 정량 실험).
3. **모듈끼리 import 금지.** 데이터는 `core/schemas.py` 의 Pydantic 모델로만.
4. **`core/`·`schemas.py` 변경은 팀 채팅 선공지 후.**
5. **LLM 호출 함수는 `async def`.**
6. **매직넘버 금지.** `0.05` 대신 `constants.ALPHA`.
7. **로그에 추적 키(`cs_id`/`sku`) 항상 포함.**

## 브랜치

모듈 단위: `feat/agent1-classification`, `feat/agent2-detection`,
`feat/agent3-recommendation`, `feat/reporting`

PR 은 1명 승인이면 머지.

## 미결정 사항

회의에서 정해야 할 것들:

- `schemas.py` 전체 (ClassifiedItem / AnomalyResult / Recommendation)
- `core/prompts.py` — 문서 4장 목록에 없는데 4개 모듈이 다 필요해서 추가함. 합의 필요
- fixture `.json` 과 정량 실험용 `.csv` 를 어디에 둘지 (`tests/fixtures/` vs 별도 `data/`)
- LangGraph HITL 상태를 어디에 영속화할지 (checkpointer vs Spring Boot DB) — 5주차 안건