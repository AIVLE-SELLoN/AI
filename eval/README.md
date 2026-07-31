# `eval/` — 정량 실험 6종

> ⚠️ **사람이 수동으로 실행한다. CI 에 넣지 말 것.**
> LLM 비용이 발생하고 몇 분에서 수십 분이 걸린다. `tests/` 와 성격이 완전히 다르다.

| | `tests/` | `eval/` |
| --- | --- | --- |
| 목적 | 코드가 안 깨졌나 | 성능이 얼마나 나오나 |
| 실행 | 개발 중 수시로 | **필요할 때만** |
| 비용 | 0 | **LLM 과금** |
| 데이터 | `tests/fixtures/` (소규모 예시) | `data/` (파이프라인 규모) |

---

## 실험 6종

| 스크립트 | 무엇을 재나 | 규모 | 비용 | 담당 | 상태 |
| --- | --- | --- | --- | --- | --- |
| `run_detection_eval.py` | ① 탐지율·오탐률·편중/전역 판정 정확도 | 31케이스 | **$0** (oracle) | 서영 | 🔴 뼈대 |
| `run_pipeline_eval.py` | ② 분류 오류가 탐지를 얼마나 깎는지 (①−②) | 시드 3벌 | ~$90 | 서영·현진 | 🔴 뼈대 |
| `run_classify_eval.py` | ③ 프롬프트1 aspect 분류 정확도 | 1,000건 × 반복 | ~$20 | 현진 | 🔴 뼈대 |
| `run_review_eval.py` | ④ 프롬프트2 리뷰 aspect별 긍부정 | AI Hub 71603 | ~$20 | 현진 | 🔴 뼈대 |
| `run_recommendation_eval.py` | ⑤ Retrieval·Grounding·RAG 유무 비교·라우팅 | golden 15건 + 실제 CS 201건 | 실측 소액 | 지인 | 🟢 실측 완료 |
| `run_cause_eval.py` | ⑥ 프롬프트3 원인분류 정확도 ([6] 원인 진단) | 채점 케이스 × 20건 | ~$20 | 서영 | 🟡 구현·실행 이력 있음 |

- ⑤는 개선안 로직 §5-3(Mock 정확도 실험 설계) 정의 — 스크립트는 개선안 모듈 소관
- ⑥은 지인 승인으로 추가(2026-07-23), 실행 결과는 `eval/results/` 참고
- 비용은 ①~④ 추정치. **평가 비용 소계 ~$150, 전체 상한 $250로 관리** — 재산정 필요

## 실행

저장소 루트에서 실행한다 (`app` 을 import 하므로). `data/` 와 ChromaDB 시딩이
먼저 돼 있어야 한다 (`scripts/seed_vectordb.py`).

```bash
python eval/run_detection_eval.py
```

⑤는 **비용이 드는 부분을 플래그로 분리**해뒀다. 아무 플래그 없이 돌리면 $0 구간만 돈다.

```bash
python eval/run_recommendation_eval.py                # Retrieval hit rate만, $0
python eval/run_recommendation_eval.py --grounding    # + Grounding precision
python eval/run_recommendation_eval.py --rag-baseline # + RAG 유무 비교
python eval/run_recommendation_eval.py --routing      # + 라우팅 정확도 (golden 15건)
python eval/run_recommendation_eval.py --routing-real # + 라우팅 정확도 (실제 CS 201건)
```

---

## ⑤ 실측 결과 (2026-07-29 · gpt-4o-mini)

| 지표 | 결과 | 비고 |
| --- | --- | --- |
| Retrieval hit rate (컬렉션1) | **15/15** (100%) | $0, LLM 미사용 |
| Grounding precision (RAG 있음) | **4/4** (100%) | copy_draft 라우팅 4건이 분모 |
| **RAG 유무 베이스라인** | 없음 **0/4** → 있음 **4/4** | §5-3 대조 실험 |
| 라우팅 정확도 | golden **11/11**, 실제 CS **201/201** | SCOPE_LIMIT·원인 미지정 제외 |
| Evaluator consistency / actionability | 15/15 (각 100%) | 🔻 프롬프트가 직접 지시하는 항목이라 **순환적** |
| **재시도(attempts) 분포** | 1차 11건 / 2차 4건 / 3차·fallback 0건 | 🔺 지시 안 한 결과값이라 신뢰 가능 |

**읽는 법 두 가지.**

- **RAG 유무 대조가 이 실험의 핵심.** RAG 없이 원인 라벨만 주면 LLM은 상세페이지에
  있지도 않은 문구를 지어냈다(0/4). 근거를 붙이자 4건 전부 실제 원문을 인용했다.
- **재시도 분포가 두 번째로 의미 있다.** 4건이 1차 실패 후 재시도로 통과했다 =
  재시도 temperature 상승·실패 피드백 로직이 라이브에서 실제로 작동한다는 증거.

**한계 (인용할 때 반드시 같이 말할 것).**

- 표본이 작다 (n=15 / 11 / 4). 100%는 "완벽"이 아니라 "아직 실패를 못 봤다"다.
- 라우팅 정확도는 원인 라벨 이름 자체가 답을 암시한다(`사진_색감_오차`에 "사진").
  판단력 테스트라기보다 **"지시 따르기" 테스트**에 가깝다.
- consistency·actionability는 프롬프트가 지시하는 항목을 채점해서 순환적이다.
- 컬렉션2(반려사유) 지표는 HITL 실사용 전까지 **구조적으로 측정 불가**.

설계 의도(왜 이렇게 재는지)는 `run_recommendation_eval.py` 소스의 docstring에 있다.

> 📊 **결과 정본은 노션 「정량 실험 지표·결과」** (AI 허브 하위).
> 위 ⑤ 수치는 그 페이지와 같아야 한다 — **한쪽만 고치지 말 것.**

---

## 비용 통제 4원칙

1. **반복 실험은 oracle 모드로** — 정답 라벨을 직접 입력해 LLM 을 안 태운다 ($0)
2. **분류는 배치 호출** — 건당 호출하면 비용 2배 + 속도 제한 초과
3. **분류 결과는 시드별 캐싱** — 같은 걸 두 번 돌리지 않는다
4. **모델 티어링** — 호출 많은 분류는 저가 모델, 호출 적고 중요한 개선안 생성만 상위 모델

## 원칙

- **`data/golden/` 은 `eval/` 만 읽는다.** `app/` 코드가 import 하면 컨닝이다.
- **재현 가능해야 한다** — 결과와 함께 **실행일·모델명·프롬프트 버전·시드·표본 수 n**
  다섯 개를 반드시 기록할 것. 하나라도 빠지면 발표에 못 쓴다.
- **분모를 밝힐 것.** "100%"가 아니라 "15/15 (100%)"로.
