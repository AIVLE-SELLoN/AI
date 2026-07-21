"""담당: 서영 (Agent2) — 이상탐지 + 원인분류.

성격: 통계 + LLM 워크플로우 → scipy/statsmodels + 순수 Python (프레임워크 없음).

역할 분리:
  - statistics.py : 순수 통계 계산. LLM·DB 모름 → 단위테스트 쉬움.
  - service.py    : 파이프라인 조립. 통계 결과 + LLM 원인분류를 엮는다.

TODO(서영): 설계 문서 확정 + schemas.py 확정 후 구현.
  파이프라인 단계·판정 규칙·집계 단위는 `이상탐지 로직` / `이상탐지 시나리오` 문서를
  따르되, **개발 착수 시점의 최신본을 다시 확인할 것.** 문서가 개정 중이라 여기에
  단계별 상세를 미리 적어두지 않는다 (적어두면 문서가 바뀔 때마다 같이 틀어진다).

  - detect_anomaly(...) -> AnomalyResult
  - 임계값은 전부 core/constants.py 경유 (매직넘버 금지)
  - 원인 분류는 아래 프롬프트3 사용법 참고

⚠️ 입력 의존성 — 백엔드와의 계약:
  상품 매핑은 백엔드(Spring Boot) 담당이고, detection 은 그 산출물인 상품 식별자를
  입력으로 받는다. 이 식별자는 **옵션(SKU) 이 아니라 '상품 그룹' 레벨**이어야 한다.
  문의·리뷰 원본에 옵션 정보가 없어서, 옵션 레벨 ID 를 받으면 집계가 성립하지 않는다.
  → 필드명·레벨을 백엔드와 확정할 것 (아직 계약이 문서화되지 않았다).

──────────────────────────────────────────────────────────────────────────
원인 분류 — 프롬프트3 (classify_cause_v1.md) 사용법
──────────────────────────────────────────────────────────────────────────

부정 문의 텍스트를 사전정의 원인 후보로 **분류**한다(추출 아님 — 그래야 정확도
측정이 된다). 대상 aspect: 색상 / 사이즈 / 소재.
핏 불만은 별도 aspect 가 아니라 사이즈 안에서 처리한다.

■ 입출력 계약

  입력 (문의 1건): {cs_id, aspect, raw_text}   ← aspect 는 상류에서 확정된 값
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

  호출 단위는 문의 1건씩, 대량 처리는 병렬로. 온도는 0~0.2 (분류 태스크).

■ 후처리 (프롬프트 밖, 이 파일에서 구현)

  1. **aspect_match=false 인 건은 집계에서 제외한다.**
     해당 aspect 불만이 아니므로 원인 분포에 들어가면 분모가 오염된다.
     confidence 와 무관하게 제외 (LLM 이 false 인데도 높은 confidence 를 줄 수 있다).
  2. 남은 문의를 cause 별 빈도로 집계 → "N건 중 M건이 사진_색감_오차".
  3. 일관 판정(최다 원인 비율·건수 기준) → 확신도 경로 분기.
     기준값은 문서 확정 후 constants.py 에 추가할 것.
  4. aspect_match=false 비율이 높으면(예: >20%) 상류 aspect 분류를 재점검.

■ 미결 / 주의

  - **confidence 캘리브레이션 [서영, 프롬프트1·2 테스트 때 함께 수행]:**
    few-shot 예시의 confidence 가 "구체 후보=0.82~0.94 / 기타=0.3~0.4" 두 덩어리라
    0.5~0.8 구간이 비어 있다. LLM 은 few-shot 을 강하게 모방하므로 confidence 가
    사실상 `cause == 기타` 의 재표현이 될 수 있다 (= `if cause != "기타"` 한 줄로 될 일).

    또한 상류 aspect 오분류 전파는 confidence 가 아니라 aspect_match 가 잡는다.
    배송 불만이 색상으로 잘못 들어와도 "화면"이라는 단어에 낚여 confidence 0.8 을
    줄 수 있기 때문이다. 층이 다르다.

    → golden 라벨로 confidence 구간별 실제 정확도를 그린다.
      판정 기준: ① 구간이 올라갈수록 정확도가 단조 증가하는가
                ② 값이 0.5~0.8 구간에도 분포하는가 (양극단만이면 사실상 이진 플래그)
      둘 중 하나라도 실패하면 confidence 를 버리고 aspect_match 만 쓴다.
  - **핏 흡수:** 사이즈의 원인 후보 3종은 치수·표기 기준이라 순수 핏 불만("붕 뜬다")은
    '기타'로 떨어진다. 기타 비율이 높게 나오면 후보 추가를 검토할 것.
  - **경계 혼동 모니터링:** 사진_색감 vs 조명, 표기_오타 vs 실측_표기_편차는 태생적으로
    겹친다 → golden 라벨로 이 쌍의 혼동행렬을 별도 확인.
  - **evidence 용도 미정:** 채점에도 후처리에도 안 쓰인다. 디버깅·데모용이면 그대로 두고,
    아니면 빼서 토큰을 아낄 수 있다.
"""