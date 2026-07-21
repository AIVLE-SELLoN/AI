FROM python:3.12-slim

WORKDIR /app

# 의존성 레이어 분리 — requirements.txt 안 바뀌면 캐시 재사용
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app/ 만 넣는다. data/(~30MB) · eval/(수동 실행) · tests/ 는 운영 이미지에 불필요.
# 특히 data/golden/ 은 채점 정답지라 컨테이너에 들어가면 안 된다.
COPY app/ ./app/

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
