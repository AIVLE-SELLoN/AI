"""분류 산출물의 **버전 신원**. 적재와 조회가 같은 값을 봐야 하는 상수.

`constants.py` 가 아닌 이유: 그쪽은 "정량 실험 때 바꿔가며 돌리는 값 + 변경 전 팀 합의"
를 선언하는데, 여기 값은 스윕 대상이 아니고 바꾸면 실험 결과가 아니라 **이미 적재된 행이
통째로 탐지 대상에서 빠진다.**

`app/core/` 인 이유: 쓰는 쪽(`scripts/classification_worker.py`)과 읽는 쪽
(`app/batch/daily.py`)이 갈려 있어 어느 한쪽에 두면 반대쪽이 모듈 간 import 금지를 어긴다.

프롬프트 버전은 여기 없다 — 그건 분류 모듈의 것이고(`app/classification/service.py`),
core 가 모듈을 import 하면 계층이 뒤집힌다. **호출부가 셋을 모아서** 술어에 넘긴다
(`raw_schema.version_params`).
"""

from __future__ import annotations

CLASSIFIER_PIPELINE_VERSION = "classify_pipeline_v1"
"""프롬프트 **밖** 분류 로직의 버전. 허용 aspect 집합·후처리·정규화를 바꾸면 올린다.

기준은 **분자를 직접 바꾸는** 변경이다 — 허용 aspect 집합이 바뀌어 안 내던 aspect 의
분자가 생기거나, 후처리·정규화가 라벨을 다른 값으로 정규화해 집계가 갈리는 경우.
둘 다 프롬프트 파일은 한 글자도 안 바뀌었는데 숫자가 달라진다.

**`_cs_empty_fallback` 을 "분포가 갈리는 예"로 쓰지 말 것.** 파이프라인 변경 사례이긴
하지만 **집계에는 no-op 이다** — 그 함수 docstring 이 그렇게 적고 있고 재현해도 분모·
분자·커버리지가 전부 같다(`build_rows` 가 문서 1건 = 행 1개, `기타/중립` 은 `sentiment=0`
이라 분자에 안 들어감). 이 자리에 그 예를 세웠다가 결과가 안 바뀌는 변경에 전량 재분류를
치를 뻔했다.

**올리면 그 순간부터 탐지가 옛 행을 안 읽는다.** 탐지는 35일(현재 7 + 과거 28)을 한 번에
읽어서, 그 안에 서로 다른 분류기의 결과가 섞이면 **분류기 개선이 고객 이상 알림으로
발화한다**(Fisher 검정은 둘을 못 가른다). 그래서 `app/batch/daily.py` 가 활성 버전 행만
읽고, 안 맞는 행은 `scripts/classification_worker.py --reclassify-stale` 로 맞춘다.
**올리기 전에 backfill 계획을 먼저 세울 것** — 대상 1건이 곧 LLM 호출 1회다.
"""
