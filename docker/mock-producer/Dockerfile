# mock producer 전용 이미지 — 레지스트리 `y0njunch0i/sellon-mock-producer`.
#
# 🔴 **AI 노드 이미지(루트 `Dockerfile`)와 별개다.** 예전엔 compose 가 producer 를 그
#    이미지로 띄웠는데, producer 가 쓰는 것은 CSV 로드·Kafka 발행·raw DB 적재뿐이라
#    weasyprint 네이티브 의존성도, `fonts-noto-cjk`(~120MB)도, chromadb·scipy·
#    statsmodels 도 한 줄도 안 쓴다. 1.57GB 중 대부분이 producer 에겐 죽은 무게였다.
#
# ⚠️ **`app/` 전체가 아니라 `app/core/` 와 `app/config.py` 만 넣는다.** producer 는
#    `app.core.{raw_db,raw_schema,console,constants,schemas}` 만 import 하고, 그중
#    `raw_db` 가 `app.config` 를 부른다. 분류·탐지·개선안·리포팅 모듈은 안 들어간다.
#
# ⚠️ `app/core/` 는 **디렉터리째** 넣는다 — 파일을 골라 적으면 그 목록이 조용히 낡는다
#    (같은 사유로 콘솔 인코딩 가드도 하드코딩 목록을 버리고 유도로 바꿨다). 안 쓰는
#    모듈(`llm_client` · `vectordb` · `mq`)이 같이 들어가지만 **import 되지 않으므로**
#    설치되지 않은 openai·chromadb·aio-pika 를 끌지 않는다. 그 전제가 깨지는 순간
#    CI 스모크의 import 확인이 잡는다.
FROM python:3.12-slim

WORKDIR /app

# 의존성 레이어 분리 — 소스만 바뀌면 캐시 재사용.
COPY requirements-mock-producer.txt .
RUN pip install --no-cache-dir -r requirements-mock-producer.txt

COPY app/__init__.py app/config.py ./app/
COPY app/core/ ./app/core/
COPY scripts/mock_producer.py ./scripts/

# 🔴 데이터는 **넣지 않는다.** 대본 CSV(`data/input/`)는 gitignore 라 이미지에 없고,
#    있어서도 안 된다 — 26만 행이고 `data/golden/` 과 한 트리에 있다(CLAUDE.md 9).
#    실행할 때 `-v ./data:/app/data` 로 물리거나 `--data-dir` 로 가리킨다.
#
# ENTRYPOINT + CMD 로 나눈 이유: `docker run <이미지> --dry-run --data-dir /data` 처럼
# 인자만 이어 붙이면 되고, 인자 없이 띄우면 **브로커에 붙지 않고** 도움말만 찍는다.
# (기본값으로 localhost:9092 에 붙으러 가는 것보다 안전하다.)
ENTRYPOINT ["python", "-u", "scripts/mock_producer.py"]
CMD ["--help"]
