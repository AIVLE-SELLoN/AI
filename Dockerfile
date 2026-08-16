FROM python:3.12-slim

WORKDIR /app

# WeasyPrint(PDF 렌더링) 네이티브 의존성. **pip 로는 안 들어온다** — weasyprint 는
# 파이썬 휠이지만 Pango/HarfBuzz 를 dlopen 으로 찾으므로, 이게 없으면 pip 설치가
# 성공해도 `compile_report_to_pdf()` 호출 순간 OSError 로 터진다.
#
# ⚠️ **이 이미지는 PDF 경로를 탄다.** 아래 셋 다 이 이미지 안에서 도는 코드다:
#      - `POST /api/v1/replies` (CMD 의 uvicorn) → cs_reply_service → compile_report_to_pdf
#      - `python -m app.batch.daily`             → generate_guideline → 같은 함수
#      - `python scripts/generate_monthly_reports.py` (compose 의 ./scripts 마운트 경유)
#    셋 중 무엇도 "컨테이너는 PDF 를 안 만든다" 가 아니다. 리포팅만 별도 이미지로
#    가르려면 위 REST 엔드포인트까지 같이 옮겨야 하므로, 지금은 한 이미지에 둔다.
#
# ⚠️ weasyprint 53+ 는 Cairo·GDK-PixBuf 를 더 이상 쓰지 않는다(Pango 로 대체). 옛
#    가이드를 보고 libcairo2·libgdk-pixbuf 를 넣지 말 것 — 이미지만 커진다.
#    libharfbuzz-subset0 은 PDF 폰트 서브셋에 쓰인다(빠지면 서브셋 단계에서 실패).
#
# ⚠️ 폰트가 없어도 **에러가 안 난다** — 그 자리에서 네모(두부)로 그려진 PDF 가 그냥
#    나간다. 리포트가 전량 한글이라 fonts-noto-cjk 는 선택이 아니다(~120MB).
#    _BASE_CSS 의 'Noto Sans CJK KR' 이 이 패키지가 설치하는 실제 패밀리명이고, 그 줄과
#    이 패키지는 **세트다** — CSS 쪽만 지우면 Pango 가 한국어 대신 Noto Sans CJK JP 로
#    폴백한다(실측). 자세한 내용은 pdf_compiler._BASE_CSS 주석.
RUN apt-get update && apt-get install --no-install-recommends -y \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        libharfbuzz-subset0 \
        fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# 의존성 레이어 분리 — requirements.txt 안 바뀌면 캐시 재사용
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app/ 와 scripts/ 를 넣는다. data/(~30MB) · eval/(수동 실행) · tests/ 는 운영 이미지에 불필요.
# 특히 data/golden/ 은 채점 정답지라 컨테이너에 들어가면 안 된다.
#
# scripts/ 가 필요한 이유: 분류 워커(`python scripts/classification_worker.py`)가
# k8s CronJob 으로 돌게 되어, compose 의 ./scripts 마운트가 없는 클러스터에서는
# 이미지 안에 실물이 있어야 한다. 프롬프트는 app/**/prompts/ 라 COPY app/ 에 이미
# 포함된다. scripts/ 에 들어가는 건 코드와 scripts/prompts/ 의 스크립트 전용 프롬프트
# 뿐이다 — 입력 CSV·raw.db 는 여전히 볼륨으로 공급한다.
COPY app/ ./app/
COPY scripts/ ./scripts/

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
