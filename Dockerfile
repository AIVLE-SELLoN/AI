FROM python:3.12-slim

WORKDIR /app

# 의존성 레이어 분리 — requirements.txt 안 바뀌면 캐시 재사용
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
