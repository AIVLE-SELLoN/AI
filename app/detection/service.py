"""담당: 서영 (Agent2) — 이상탐지 파이프라인 [0]~[7].

성격: 통계 + LLM 워크플로우 → scipy + 순수 Python (프레임워크 없음).
근거 문서: `이상탐지 로직` V2.1 (반확정 — 바뀌면 여기도 같이 고칠 것).

역할 분리:
  - statistics.py : 순수 통계 계산 (scipy). LLM·DB 모름 → 단위테스트 쉬움.
  - service.py    : 파이프라인 조립. 통계 결과 + LLM 원인분류를 엮는다.

파이프라인:
    [0] 집계        상품 × aspect × 채널 × source 별 부정 비율
    [1] 과거 기준    각 채널의 과거 윈도우(28일) 실측 부정률 확보
    [2] 통계 검정    채널별 "현재 vs 자기 과거" Fisher 단측 (→ statistics.py)
    [3] 편중/전역    유의 채널 수로 판정 (1~2개=편중 / 전부=전역 / 0=정상)
    [4] 주 aspect    여러 aspect 동시 상승 시 delta 최대인 것 선택
    [5] 스코프 필터  개선안 생성 가능 aspect 인지 판정
    [6] 원인 분류    (편중형 & 스코프 내) 문의 텍스트 → 원인 후보 (프롬프트3)
    [7] 결과 발행    탐지 확신도와 함께 알림 (기각 없음)

CS 와 리뷰는 [0]~[3] 을 **각각 독립 수행** 후 [7] 에서 종합. 합산하지 않는다(분모가 다름).
baseline 도 source 별로 분리.

TODO(서영): schemas.py 확정 후 구현.
  - detect_anomaly(items) -> AnomalyResult
  - _is_biased_channel(...) -> bool     (bool 반환이므로 is_ 접두어)
  - 원인 분류는 load_prompt("detection", "classify_cause_v1") + get_llm_client()
  - 임계값은 전부 constants 사용 (ALPHA / MIN_DELTA / MIN_SAMPLE_SIZE /
    CURRENT_WINDOW_DAYS / BASELINE_WINDOW_DAYS / CAUSE_CONSISTENCY_* / ALERT_*)

──────────────────────────────────────────────────────────────────────────
[6] 원인 분류 — 프롬프트3 (classify_cause_v1.md) 사용법
──────────────────────────────────────────────────────────────────────────

편중형 & 스코프 내로 판정된 aspect 의 부정 문의 텍스트를 사전정의 원인 후보로
**분류**한다(추출 아님 — 그래야 정확도 측정이 된다).
대상 aspect: 색상 / 사이즈 / 소재. 핏 불만은 사이즈 aspect 안에서 처리한다.

■ 입출력 계약

  입력 (문의 1건): {cs_id, aspect, raw_text}
      aspect 는 상류([2]~[5])에서 이미 확정된 값.
  출력 (문의 1건): JSON 1개
      {
        "cs_id": "…",
        "aspect": "색상",
        "cause": "사진_색감_오차",     # 해당 aspect 의 후보 중 하나 또는 "기타"
        "confidence": 0.0,             # 0~1
        "evidence": "판단 근거가 된 원문 구절 그대로",   # 없으면 빈 문자열
        "aspect_match": true           # false = 상류 aspect 오분류 신호
      }

■ 프롬프트 호출

  프롬프트 파일에 JSON 이 많아 str.format() 을 쓰면 중괄호에서 깨진다.
  string.Template 과 $input_json 플레이스홀더를 쓸 것:

      import json
      from string import Template
      from app.core.prompts import load_prompt
      from app.core.llm_client import get_llm_client

      template = Template(load_prompt("detection", "classify_cause_v1"))
      input_json = json.dumps(
          {"cs_id": cs_id, "aspect": aspect, "raw_text": raw_text},
          ensure_ascii=False,
      )
      prompt = template.substitute(input_json=input_json)
      result = await get_llm_client().complete_json(prompt, trace_key=f"cs_id={cs_id}")

  raw_text 에 따옴표·줄바꿈이 들어올 수 있으므로 반드시 json.dumps 로 직렬화할 것.
  문자열 포매팅으로 끼워 넣으면 프롬프트의 JSON 이 깨진다.

  호출 단위는 문의 1건씩, 대량 처리는 병렬로.
  (mock data 정의서의 "분류는 배치 호출 필수(50건/호출)" 원칙은 12.5만 건을 돌리는
   프롬프트1 대상이다. 프롬프트3 은 케이스당 ~30건 × 36 = ~1,080건이라 예외로 둔다.
   → 지인님과 합의 필요.)

  온도는 0~0.2. 분류 태스크라 흔들리면 안 된다.

■ 후처리 (프롬프트 밖, 이 파일에서 구현)

  1. aspect_match=false 인 건은 **집계에서 제외**한다.
     해당 aspect 불만이 아니므로 원인 분포에 들어가면 분모가 오염된다.
     confidence 와 무관하게 제외 (LLM 이 false 인데도 높은 confidence 를 줄 수 있다).
  2. 저신뢰 필터(§7-4): confidence < τ 인 문의를 집계에서 제외.
     τ 는 실험으로 확정 (초기값 예: 0.6). ⚠️ 아래 '미결' 참고.
  3. 집계: 남은 문의를 cause 별 빈도로 → "N건 중 M건이 사진_색감_오차".
  4. 일관 판정(8-6): 최다 cause 비율 >= CAUSE_CONSISTENCY_MIN_RATIO
     AND 해당 건수 >= CAUSE_CONSISTENCY_MIN_COUNT
     → 일관([7] 확신도 상향 가능). 미만 → "원인 특정 안 됨"([7] 확신도 낮음).
  5. 상류 오분류 신호: aspect_match=false 비율이 높으면(예: >20%) 해당
     (상품, aspect, 채널)의 [2]~[5] aspect 분류를 재점검.

■ 미결 / 주의

  - **confidence 캘리브레이션 [서영, 프롬프트1·2 테스트 때 함께 수행]:**
    few-shot 예시의 confidence 가 "구체 후보=0.82~0.94 / 기타=0.3~0.4" 두 덩어리라
    0.5~0.8 구간이 비어 있다. LLM 은 few-shot 을 강하게 모방하므로 confidence 가
    사실상 `cause == 기타` 의 재표현이 될 수 있고, 그러면 τ 필터는 "기타 제외 필터"와
    같아진다 (= `if cause != "기타"` 한 줄로 될 일).

    또한 §7-4 가 걱정한 것은 **상류 aspect 오분류 전파**인데, confidence 는 "이 aspect
    안에서 어느 원인인가"에 대한 값이라 층이 다르다. 배송 불만이 색상으로 잘못 들어와도
    "화면"이라는 단어에 낚여 confidence 0.8 을 줄 수 있다. 그건 aspect_match 가 잡는다.

    → golden_cs_labels.true_cause 로 confidence 구간별 실제 정확도를 그린다.
      판정 기준: ① 구간이 올라갈수록 정확도가 단조 증가하는가
                ② 값이 0.5~0.8 구간에도 분포하는가 (양극단만이면 사실상 이진 플래그)
      둘 중 하나라도 실패하면 confidence 를 버리고 aspect_match 만 쓴다.
      (그 경우 후처리 2번 τ 필터 삭제, 프롬프트에서 confidence 필드 제거)
  - **파손·오배송 원인분류 상충:** [5] 스코프표는 파손·오배송도 원인분류 ✅ 로 표기했으나
    [6] 에 이 둘의 원인 후보가 정의돼 있지 않다. 개선안이 없어 결과를 쓸 곳도 없으므로
    → [5]표의 원인분류를 —(불필요)로 정정하거나, 쓸 거면 후보를 정의해야 함.
      현 프롬프트는 색상/사이즈/소재만 처리.
  - **핏 흡수:** 핏은 별도 aspect 가 아니라 사이즈에 포함하기로 확정. 다만 사이즈의
    원인 후보 3종은 치수·표기 기준이라 순수 핏 불만("붕 뜬다")은 '기타'로 떨어진다.
    기타 비율이 높게 나오면 후보 추가를 검토할 것.
  - **경계 혼동 모니터링:** 사진_색감 vs 조명, 표기_오타 vs 실측_표기_편차는 태생적으로
    겹친다 → Golden Label 로 이 쌍의 혼동행렬을 별도 확인.
  - **평가:** golden_cs_labels.true_cause 와 대조. per-문의 정확도 + 일관 판정 정확도
    둘 다 측정 (SC-033 은 "미특정"이 정답).
  - **evidence 용도 미정:** 채점(golden_cs_labels)에도 후처리에도 안 쓰인다.
    디버깅·데모용이면 그대로 두고, 아니면 빼서 토큰을 아낄 수 있다.

주의 — 놓치기 쉬운 것들:
  - **집계 단위는 상품×채널.** 문의·리뷰 원본에 옵션 정보가 없어서 옵션(SKU) 단위
    집계는 원천적으로 불가능하고, 최소표본 규칙상 옵션 단위는 만성 보류가 된다. §7-2
  - **분모는 "해당 (상품, 채널) 의 총 문의 수".** 채널 전체로 나누면 잘 팔리는 상품의
    재입고·배송 문의 폭증에 부정 신호가 희석된다 (SC-022). 주문 수가 아니다. §[0]
  - **기준선 오염 방지:** 알림이 발행된 (상품, aspect, 채널) 의 알림 구간 날짜는 과거
    윈도우 집계에서 제외. 안 그러면 지속되는 이상이 "새로운 평소"가 되어 알림이 꺼진다.
    제외로 과거 표본이 절반 이하로 줄면 설정값 baseline 으로 폴백. §[1]
  - **기각이 없다.** 각 관문에서 걸러진 것도 알림은 발행한다 (전역형 → "상품 자체 점검
    권장", 스코프 밖 → "물류/운영 점검 권장"). §6
  - **[7] 의 확신도는 '탐지 확신도'** (높음/중간/낮음). 셀러 화면(B4/B5)에 뜨는
    '개선안 확신도'(Agent3)와는 **다른 값**이다. 탐지 확신도는 Agent3 에 입력으로
    전달되어 Agent3 의 캡핑 규칙에서 쓰인다. §[7]

⚠️ 입력 의존성 — 백엔드와의 계약:
  master_sku_code 는 상품 매핑의 산출물이고, **매핑은 백엔드(Spring Boot) 담당**이다.
  detection 은 매핑이 끝났다고 가정하고 그 이후만 다룬다. §1

  **master_sku_code 는 옵션(SKU) 이 아니라 '상품 그룹' 레벨이어야 한다.** (§7-2 확정)
  이름은 SKU 지만 실제로는 상품 그룹 ID 다 — 헷갈리기 쉬우니 주의.
  detection 은 상품×채널로 집계하므로 옵션 레벨 ID 를 받으면 집계가 성립하지 않는다
  (문의·리뷰 원본에 옵션 정보가 없어서 옵션 단위로 되돌릴 수도 없다).

  → 백엔드에 이 레벨을 명시적으로 확인할 것. 매핑이 방금 백엔드로 넘어가서
    아직 계약이 문서화되지 않았다.
"""