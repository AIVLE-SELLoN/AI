"""분류 산출물의 **버전 신원**. 적재와 조회가 같은 값을 봐야 하는 상수.

왜 `constants.py` 가 아닌가
---------------------------
그 파일 머리말은 "정량 실험 때 바꿔가며 돌려야 하는 값 + 변경 전 팀 합의 필수" 를
선언한다. 여기 값은 성격이 다르다 — **스윕 대상이 아니고**, 바꾸면 실험 결과가
달라지는 게 아니라 **이미 적재된 행이 통째로 탐지 대상에서 빠진다.** 같은 파일에 두면
"여기 있는 건 전부 튜닝 손잡이" 라는 신호가 흐려진다. (2026-08-12)

왜 `app/core/` 인가
-------------------
쓰는 쪽(`scripts/classification_worker.py`)과 읽는 쪽(`app/batch/daily.py`)이 갈려 있다.
어느 한쪽에 두면 반대쪽이 모듈 간 import 금지(CLAUDE.md 아키텍처 원칙 2)를 어겨야 한다.
`raw_db.py`·`ids.py`·`KST` 가 core 로 온 것과 같은 사유다.

⚠️ 프롬프트 버전은 여기 없다. 그건 분류 모듈의 것이고(`app/classification/service.py`),
   core 가 모듈을 import 하면 계층이 뒤집힌다. **호출부가 셋을 모아서** 술어에 넘긴다
   (`raw_schema.version_params`).
"""

from __future__ import annotations

CLASSIFIER_PIPELINE_VERSION = "classify_pipeline_v1"
"""프롬프트 **밖** 분류 로직의 버전. 후처리·정규화·폴백·허용 aspect 집합을 바꾸면 올린다.

프롬프트 버전만으로는 못 잡는 변화가 실제로 있다. `classification.service._cs_empty_fallback`
은 LLM 이 빈 배열을 냈을 때 `기타` 를 채운다. **이 폴백을 끄고 켜는 것만으로 CS 의 aspect
분포가 움직인다** — 프롬프트 파일은 한 글자도 안 바뀌었는데 숫자가 달라진다.

관련 실측은 **표본 284건 중 6건(2.1%)** 이다(2026-08-04 서영님, 원문은 그 폴백 함수와
`app/detection/loader.py` docstring). ⚠️ `cs` 전체 행수를 분모로 쓴 수치가 아니고, 전량
기준 비율은 측정된 적이 없다. 다만 **결론은 규모와 무관하다** — 폴백이 존재한다는 것만으로
켜고 끌 때 분포가 갈리기 때문이다.

⚠️ **올리면 그 순간부터 탐지가 옛 행을 안 읽는다.** 탐지는 35일(현재 7 + 과거 28)을 한 번에
   읽어서, 그 안에 서로 다른 분류기의 결과가 섞이면 **분류기 개선이 고객 이상 알림으로
   발화한다**(Fisher 검정은 둘을 못 가른다). 그래서 `app/batch/daily.py` 가 활성 버전 행만
   읽고, 안 맞는 행은 `scripts/classification_worker.py --reclassify-stale` 로 맞춰야 한다.
   **올리기 전에 backfill 계획을 먼저 세울 것** — 대상 1건이 곧 LLM 호출 1회다.
"""
