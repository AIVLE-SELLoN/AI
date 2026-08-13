from __future__ import annotations

import os
import sys
from enum import Enum
from typing import Any

from jinja2 import BaseLoader, Environment, select_autoescape

from app.core import constants
from app.reporting.charts import (
    render_divergence_gauge,
    render_sentiment_donut,
)

# Windows 환경일 경우 GTK3 DLL 경로 강제 추가.
# ⚠️ weasyprint import 보다 **먼저** 실행돼야 한다. weasyprint 는 import 시점에
#    libgobject 등 GTK DLL 을 찾으므로, PATH 를 나중에 넣으면 이미 늦다.
if sys.platform == "win32":
    gtk_path = r"C:\Program Files\GTK3-Runtime Win64\bin"
    if os.path.exists(gtk_path) and gtk_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = gtk_path + os.path.pathsep + os.environ.get("PATH", "")


# 게이지 폭(px) — A4 가로(297mm) - 좌우 여백(24mm) 의 우측 열(75%)에서 열 간격과
# 카드 안쪽 여백을 뺀 값. CSS 로 늘리면 viewBox 비율 때문에 높이까지 커져 페이지가
# 넘치므로, 생성 시점에 맞춘다.
#   (297 - 24 - 4) × 0.75 - 5 ≒ 197mm ≒ 745px @96dpi
GAUGE_WIDTH_PX = 745

# 채널 코드 → 화면 표기. 리포트는 셀러가 읽는 문서라 영문 코드를 그대로 쓰지 않는다.
CHANNEL_LABEL = {"COUPANG": "쿠팡", "NAVER": "네이버", "ZIGZAG": "지그재그"}


class ReportType(str, Enum):
    CS_GUIDELINE = "cs_guideline"
    MONTHLY_REPORT = "monthly_report"


_BASE_CSS = """
    /* 가로(landscape) — 세로로 두면 좌우 시각자료가 잘려 한 눈에 안 들어온다(2026-08-04) */
    /* ⚠️ 여백 12mm → 8mm. 상품 1건이 **정확히 한 페이지**여야 한다(2026-08-09) —
       화면이 첫 페이지만 미리보기로 띄우는데 내용이 두 장으로 갈리면 뒷장이 안 보인다.
       세로 여유가 8mm 늘어난다. */
    @page { size: A4 landscape; margin: 8mm 12mm; }
    /* ⚠️ 'Noto Sans CJK KR' 은 리눅스 컨테이너용이고 **빼면 안 된다.** Docker 이미지가
       설치하는 fonts-noto-cjk 의 실제 패밀리명이 이것이다. 'Malgun Gothic'(개발자
       Windows 용)과 'Noto Sans KR' 은 **컨테이너에서 둘 다 안 잡힌다** — fontconfig 에
       'Noto Sans KR' → 'Noto Sans CJK KR' 별칭이 없어서, 실측하면 둘 다 DejaVu Sans 로
       해석된다(`fc-match "Noto Sans KR"` → DejaVuSans.ttf).

       그때 무슨 일이 나는지가 함정이다. DejaVu 에 한글 글리프가 없으니 Pango 가 글리프
       단위로 폴백하는데, **한국어 폰트를 고른다는 보장이 없다** — 이 스택으로 실측하면
       `Noto Sans CJK JP` + `DejaVu Sans` 두 벌이 박힌다(2026-08-12 컨테이너 측정).
       한글은 그려지지만 한자가 일본어 자형이고 폰트가 둘로 갈린다. **에러는 안 난다.**
       CJK 폰트가 아예 없는 환경이면 같은 자리에서 네모(두부)가 된다.
       명시하면 `Noto Sans CJK KR` 한 벌로 정리된다(Regular/Bold 만 임베드). */
    body { font-family: 'Malgun Gothic', 'Noto Sans KR', 'Noto Sans CJK KR', sans-serif; font-size: 9pt; color: #1a1a1a; }
    /* ⚠️ 한글 어절 단위 줄바꿈(word-break: keep-all)은 weasyprint 63.1 이 무시한다.
       줄 끝에 한 글자만 넘어가는 것은 폭·글자 크기로 조절할 수밖에 없다. */
    h1 { font-size: 13pt; margin: 0 0 1mm; }
    h2 { font-size: 10pt; margin: 0.5mm 0 1mm; border-bottom: 1px solid #ccc; padding-bottom: 0.6mm; }
    .meta { color: #666; font-size: 8pt; margin-bottom: 1.5mm; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 3mm; }
    th, td { border: 1px solid #ddd; padding: 2mm 2.5mm; text-align: left; font-size: 9.5pt; }
    th { background: #f4f4f4; }
    .risk { color: #c0392b; font-weight: bold; }
    ul { margin: 0 0 3mm; padding-left: 5mm; }
    .quote { background: #f8f8f8; border-left: 3px solid #999; padding: 2mm 3mm; margin-bottom: 2mm; }

    /* KPI 카드 — 대시보드 상단과 같은 구성 */
    .kpi-row { display: flex; gap: 2.5mm; margin-bottom: 1.2mm; }
    .kpi { flex: 1; border: 1px solid #e9ecef; border-radius: 3mm; padding: 1.4mm 2.5mm; }
    .kpi .label { font-size: 7.5pt; color: #868e96; letter-spacing: .3px; }
    .kpi .value { font-size: 12pt; font-weight: bold; margin-top: 0.3mm; }
    .kpi .sub { font-size: 8pt; color: #868e96; margin-top: 1mm; }
    .kpi.accent { background: #4c6ef5; border-color: #4c6ef5; color: #fff; }
    .kpi.accent .label, .kpi.accent .sub { color: #dbe4ff; }

    /* 속성 카드 3열 — 도넛 + 범례 */
    .aspect-row { display: flex; gap: 3mm; margin-bottom: 4mm; }
    .aspect-card { flex: 1; border: 1px solid #e9ecef; border-radius: 3mm; padding: 2.2mm 2.5mm; }
    .aspect-card .head { display: flex; justify-content: space-between; align-items: center; }
    .aspect-card .title { font-size: 9.5pt; font-weight: bold; }
    .badge { font-size: 7.5pt; font-weight: bold; padding: 0.6mm 2mm; border-radius: 2mm;
             background: #e6fcf5; color: #0ca678; }
    .badge.risk { background: #fff5f5; color: #f03e3e; }
    .aspect-card .chart { text-align: center; margin: 2mm 0; }
    .legend { font-size: 8pt; color: #495057; line-height: 1.5; }
    .legend .dot { display: inline-block; width: 2mm; height: 2mm; border-radius: 50%; margin-right: 1.2mm; }
    .aspect-card .note { font-size: 7.5pt; color: #495057; margin-top: 1.5mm; line-height: 1.35; }
    .aspect-card .note.risk { background: #fff5f5; border-radius: 2mm; padding: 2mm; }
    .gauge-box { border: 1px solid #e9ecef; border-radius: 3mm; padding: 3mm; margin-bottom: 3mm; }

    /* 상품 페이지 2단 — 좌: VOC·감성분포 / 우: 채널 격차 */
    /* KPI 는 좌측 열(감성 분포) 위에만 놓이므로 폭을 왼쪽 셀에 맞춘다. */
    .kpi-row.section { width: 25%; }
    .gauge-block svg { display: block; }
    /* 행 단위 그리드 — 좌(감성 카드) : 우(게이지)를 같은 행에 묶어 세로 정렬을 보장한다 */
    /* ⚠️ 좌 29%→25%. 왼쪽(도넛+범례)은 고정 높이라 폭이 남지만, 오른쪽 원인·조치 카드는
       폭이 좁을수록 줄바꿈이 늘어 **행 높이가 커진다**. 페이지 넘침은 항상 오른쪽에서
       난다(2026-08-09). 폭을 옮기면 같은 글자 수를 더 적은 줄에 담는다. */
    .grid-row { display: flex; gap: 4mm; align-items: stretch; }
    .grid-row .cell-left { width: 25%; display: flex; }
    .grid-row .cell-right { width: 75%; display: flex; }
    .grid-row .cell-left > *, .grid-row .cell-right > * { width: 100%; }
    /* 제목 행은 세로로 쌓아야 밑줄이 열 전체로 이어진다(가로 flex 면 부제가 옆에 붙는다) */
    .grid-row.head { align-items: flex-start; }
    .grid-row.head .cell-left, .grid-row.head .cell-right { flex-direction: column; }
    .summary-line { font-size: 8pt; color: #495057; background: #f8f9fa; border-radius: 2mm;
                    padding: 1mm 2.5mm; margin-bottom: 1.2mm; line-height: 1.35; }
    /* 범례는 왼쪽, 도넛은 오른쪽 정렬 */
    .aspect-card .row { display: flex; align-items: center; gap: 2mm; }
    .aspect-card .row .chart { margin: 0 0 0 auto; }
    /* 감성 분포 카드와 게이지 블록의 높이를 맞춰 좌우가 나란히 떨어지게 한다 */
    .aspect-card, .gauge-block { margin-bottom: 0.8mm; }
    .gauge-block { border: 1px solid #e9ecef; border-radius: 3mm; padding: 1.2mm 2.5mm; margin-bottom: 0.8mm; }
    .gauge-block .pair { font-size: 9.5pt; font-weight: bold; margin-bottom: 0.8mm; }
    /* 게이지 아래 원인·조치는 각각 **독립된 카드**로 감싼다(2026-08-04 화면 확정) */
    .gauge-block .pair-note { display: flex; gap: 2.5mm; margin-top: 1mm; }
    .note-card { flex: 1; border: 1px solid #e9ecef; border-radius: 2.5mm;
                 background: #fbfbfc; padding: 1.2mm 2mm; }
    .note-card h4 { font-size: 8pt; margin: 0 0 0.8mm; color: #212529; }
    .note-card ol { margin: 0; padding-left: 4mm; font-size: 7.5pt; color: #495057;
                    line-height: 1.35; }
    .note-card li { margin-bottom: 0.5mm; }
    .note-card li:last-child { margin-bottom: 0; }

    /* 보류 상품 페이지 — 사유만 적는다. 수록 상품과 같은 머리글을 써서 같은 책으로 읽힌다 */
    .hold-box { border: 1px solid #ffe3e3; background: #fff5f5; border-radius: 3mm;
                padding: 8mm 10mm; margin-top: 4mm; text-align: center; }
    .hold-title { font-size: 11pt; font-weight: bold; color: #f03e3e; margin-bottom: 3mm; }
    .hold-msg { font-size: 10pt; color: #212529; line-height: 1.6; }
    .hold-sub { font-size: 8.5pt; color: #868e96; margin-top: 3mm; line-height: 1.5; }

    /* 합본: 상품마다 새 페이지 — 첫 상품 페이지를 화면 미리보기로 쓴다 */
    .product-page { page-break-before: always; }
    .product-page.first { page-break-before: auto; }  /* 첫 상품 앞의 빈 페이지 방지 */
"""

# ⚠️ 통계 수치(p_value 등)는 §4-4 금지 표현이라 템플릿에 넣지 않는다.
#    입력 모델에는 있지만 문서에 렌더링하면 그대로 셀러에게 노출된다.
CS_TEMPLATE_HTML = (
    """<!DOCTYPE html><html><head><meta charset="utf-8"><title>CS 대응 가이드라인</title>
<style>"""
    + _BASE_CSS
    + """</style></head>
<body>
    <h1>{{ guideline.summary.issue_title }}</h1>
    <div class="meta">
        가이드라인 ID {{ guideline.guideline_id }} · 알림 ID {{ guideline.alert_id }}<br>
        {{ input.product_group_id }}{% if input.product_name %} ({{ input.product_name }}){% endif %}
        · {{ input.channel }} · {{ input.main_aspect }} · 위험등급
        <span class="risk">{{ guideline.summary.risk_level }}</span>
    </div>

    <h2>지표 요약</h2>
    <p>{{ guideline.summary.key_metric_text }}</p>
    <p>{{ guideline.root_cause_summary }}</p>

    <h2>표준 응대 가이드</h2>
    <p>{{ guideline.standard_guideline.core_message }}</p>
    <div class="quote">{{ guideline.standard_guideline.draft_reply }}</div>
    <ul>
    {% for point in guideline.standard_guideline.key_talking_points %}
        <li>{{ point }}</li>
    {% endfor %}
    </ul>

    <h2>운영 조치 가이드</h2>
    <p>{{ guideline.ops_action_guide }}</p>

    <h2>문의별 맞춤 응대</h2>
    <table>
        <tr><th style="width:28%">문의 ID</th><th>응대 포인트</th></tr>
    {% for guide in guideline.inquiry_specific_guides %}
        <tr><td>{{ guide.item_id }}</td><td>{{ guide.recommended_point }}</td></tr>
    {% endfor %}
    </table>
</body></html>
"""
)

# 상품 1개분 본문. 단건 PDF 와 월간 합본이 **같은 마크업을 공유**한다 —
# 합본만 고치고 단건을 놓치면 두 문서의 수치 표기가 갈라진다.
#
# 레이아웃(2026-08-04 화면 확정):
#   좌 — TOTAL VOC · BRAND SENTIMENT · 항목별 고객 감성 분포
#   우 — 채널쌍 3종 평판 격차 게이지(한 화면에 전부)
#   하 — 원인 분석 결과 · 권장 조치 사항
# 삭제된 것: CRITICAL RISKS 카드, RISK/STABLE 배지, CRISIS DETECTED 심각도 표시,
#            채널쌍 캐러셀(한 번에 하나만 보던 방식), 전월 대비 변동 막대,
#            aspect 별 AI 요약 문장(카드 안 note).
_MONTHLY_SECTION_HTML = """
    <h1>{{ input.product_name }} <span style="color:#868e96">({{ input.product_group_id }})</span></h1>
    <div class="meta">
        {{ report.report_month }} 월간 분석 · {{ input.start_date }} ~ {{ input.end_date }}
        · 보고서 ID {{ report.report_id }}
        {% if input.channel_divergence and input.channel_divergence.calculated_at %}
        · 마지막 업데이트 {{ input.channel_divergence.calculated_at[:16]|replace('T', ' ') }}
        {% endif %}
    </div>

    <div class="summary-line">{{ report.channel_divergence_cause.cause_title }} —
        {{ report.channel_divergence_cause.cause_description }}</div>

    {#- ⚠️ **라벨은 계산식을 그대로 말한다** (2026-08-13 시연 검토).
        예전 라벨 두 개가 값을 잘못 읽게 만들었다:
          · `TOTAL VOC INSIGHT` — "INSIGHT" 가 아무 뜻이 없고(그냥 건수),
            **전 상품 합계로 오해**되기 쉬웠다. 실제로는 이 상품 1건의 해당 월 수치다
            (`reporting_schema.md` §7-1 의 목업 125,000건이 그 오해의 산물이다).
          · `BRAND SENTIMENT` — 감성 점수로 읽히는데 실제 값은 **긍정+중립**,
            즉 "부정이 아닌 비율"이다. 66.7% 를 보면 긍정이 3분의 2로 이해하지만
            같은 데이터의 실제 긍정은 38.9% 로 거의 두 배 차이였다.
            "브랜드" 도 틀렸다 — 상품 단위 지표다.
        둘째 줄에 근거를 적는 이유: 카드에 숫자만 있으면 정의를 물을 곳이 없다. -#}
    <div class="kpi-row section">
        <div class="kpi">
            <div class="label">이번 달 수집 VOC</div>
            <div class="value">{{ '{:,}'.format(input.total_voc_count) }}건</div>
            <div class="sub">문의·리뷰 합계</div>
        </div>
        <div class="kpi accent">
            <div class="label">비부정 의견 비율</div>
            <div class="value">{{ '%.1f'|format(brand_sentiment) }}%</div>
            <div class="sub">긍정+중립 · 건수 가중</div>
        </div>
    </div>

    {#- 좌우를 각각 하나의 열로 쌓으면 카드 높이가 달라 행이 어긋난다.
        n 번째 감성 카드와 n 번째 게이지를 **같은 행**에 넣어 항상 나란히 떨어지게 한다. -#}
    {% set pairs = input.channel_divergence.pairs %}
    {% set dists = input.aspect_distributions %}
    <div class="grid-row head">
        <div class="cell-left"><h2>항목별 고객 감성 분포</h2></div>
        <div class="cell-right"><h2>채널 간 평판 격차 분석</h2></div>
    </div>
    {% for k in range([dists|length, pairs|length]|max) %}
    <div class="grid-row">
        <div class="cell-left">
            {% if k < dists|length %}{% set dist = dists[k] %}
            <div class="aspect-card">
                <div class="title">{{ dist.aspect }}</div>
                <div style="font-size:8pt;color:#868e96">총 {{ '{:,}'.format(dist.total_count) }}건의 피드백</div>
                <div class="row">
                    <div class="legend">
                        <span class="dot" style="background:#12b886"></span>좋아요 {{ '%.0f'|format(dist.positive_ratio * 100) }}%<br>
                        <span class="dot" style="background:#ced4da"></span>보통 {{ '%.0f'|format(dist.neutral_ratio * 100) }}%<br>
                        <span class="dot" style="background:#f03e3e"></span>별로예요 {{ '%.0f'|format(dist.negative_ratio * 100) }}%
                    </div>
                    <div class="chart">{{ donut_by_aspect.get(dist.aspect, '')|safe }}</div>
                </div>
            </div>
            {% endif %}
        </div>
        <div class="cell-right">
            {% if k < pairs|length %}{% set pair = pairs[k] %}
            {% set analysis = analysis_by_pair.get(pair.comparison_pair) %}
            <div class="gauge-block">
                <div class="pair">{{ pair_label.get(pair.comparison_pair, pair.comparison_pair) }}</div>
                {{ gauge_by_pair.get(pair.comparison_pair, '')|safe }}
                {% if analysis %}
                <div class="pair-note">
                    <div class="note-card">
                        <h4>원인 분석 결과</h4>
                        <ol>{% for t in analysis.cause_analysis %}<li>{{ t }}</li>{% endfor %}</ol>
                    </div>
                    <div class="note-card">
                        <h4>권장 조치 사항</h4>
                        <ol>{% for t in analysis.recommended_actions %}<li>{{ t }}</li>{% endfor %}</ol>
                    </div>
                </div>
                {% endif %}
            </div>
            {% endif %}
        </div>
    </div>
    {% endfor %}

"""

_HTML_HEAD = (
    """<!DOCTYPE html><html><head><meta charset="utf-8"><title>월간 분석 보고서</title>
<style>"""
    + _BASE_CSS
    + """</style></head>
<body>"""
)

# 단건(디버그·REST 확인용). 운영 산출물은 아래 합본이다.
MONTHLY_TEMPLATE_HTML = _HTML_HEAD + _MONTHLY_SECTION_HTML + "</body></html>"

# 월간 합본 — **운영 산출물**. 표지 없이 상품 페이지로 바로 시작하며(2026-08-04 확정),
# 화면에는 **첫 상품 페이지**만 미리보기로 띄우고 전체는 presigned URL 로 내려받는다.
# 상품마다 페이지를 나눠 목차 없이도 넘겨볼 수 있게 한다.
MONTHLY_BOOK_HTML = (
    _HTML_HEAD
    # ⚠️ 총합 요약(표지) 페이지는 만들지 않는다 (2026-08-04 확정). 화면에 띄우는 첫
    #    페이지가 곧 첫 상품의 리포트다. 보류·실패 상품 안내는 PDF 대신 콜백의
    #    notice_message 로 전달한다 — 표지를 지우면서 그 정보가 사라지면 안 된다.
    + """
{% for item in items %}<section class="product-page{% if loop.first %} first{% endif %}">"""
    + """{% with report=item.report, input=item.input,
            donut_by_aspect=item.donut_by_aspect, gauge_by_pair=item.gauge_by_pair,
            pair_label=item.pair_label, analysis_by_pair=item.analysis_by_pair,
            brand_sentiment=item.brand_sentiment %}"""
    + _MONTHLY_SECTION_HTML
    + """{% endwith %}</section>{% endfor %}"""
    # 보류 상품 — 사유만 적은 페이지. 수록 상품 뒤에 이어 붙인다(2026-08-09).
    # ⚠️ 이 페이지가 없으면 PDF 만 받아보는 사람은 자기 상품이 왜 빠졌는지 알 수 없다.
    #    표지도 목차도 없는 구조라 "빠졌다"는 사실 자체가 안 보인다.
    + """
{% for input in held %}<section class="product-page{% if loop.first and not items %} first{% endif %}">
    <h1>{{ input.product_name }} <span style="color:#868e96">({{ input.product_group_id }})</span></h1>
    <div class="meta">
        {{ report_month }} 월간 분석 · {{ input.start_date }} ~ {{ input.end_date }}
    </div>
    <div class="hold-box">
        <div class="hold-title">리포트 생성 보류</div>
        <div class="hold-msg">{{ hold_notice }}</div>
        <div class="hold-sub">
            이번 달 수집된 VOC {{ '{:,}'.format(input.total_voc_count) }}건 —
            데이터가 쌓이면 다음 호부터 분석이 재개됩니다.
        </div>
    </div>
</section>{% endfor %}</body></html>"""
)

def _build_monthly_chart_context(context: dict[str, Any]) -> dict[str, Any]:
    """월간 템플릿이 쓰는 차트 SVG·파생 지표를 만든다.

    KPI 카드 값(브랜드 감성)은 **입력 수치에서 그대로 계산**한다 — LLM 이 만든 값을
    쓰면 팩트체크를 통과한 문장과 카드 숫자가 어긋날 수 있다.

    ⚠️ 여기서 만드는 값은 **템플릿이 실제로 읽는 것만** 둔다. 화면 개편으로 참조가
       사라진 값(전월 대비 변동 막대·RISK 배지·CRITICAL RISKS·최다 부정 속성 KPI)은
       계산까지 함께 지웠다. 남겨두면 매 상품마다 쓰지도 않을 SVG 를 만들어 버리고,
       나중에 본문과 어긋난 채로 되살아난다.
    """
    input_data = context.get("input", {})

    distributions = input_data.get("aspect_distributions", [])
    pairs = input_data.get("channel_divergence", {}).get("pairs", [])

    donut_by_aspect = {
        d["aspect"]: render_sentiment_donut(
            positive_ratio=d["positive_ratio"],
            neutral_ratio=d["neutral_ratio"],
            negative_ratio=d["negative_ratio"],
        )
        for d in distributions
    }
    # ⚠️ `jsd_baseline` 을 **반드시 같이 넘긴다.** 마커는 `jsd_score` 절대 위치인데
    #    severity 판정은 `excess = jsd_score - jsd_baseline` 기준이라, 기준선이 없으면
    #    같은 페이지에서 문장("위험 단계")과 그림(SAFETY ZONE)이 반대가 된다.
    #    근거는 `charts.render_divergence_gauge` docstring. (2026-08-13)
    gauge_by_pair = {
        p["comparison_pair"]: render_divergence_gauge(
            jsd_score=p.get("jsd_score"),
            comparison_pair=p["comparison_pair"],
            jsd_baseline=p.get("jsd_baseline"),
            sample_size=p.get("sample_size"),
            width=GAUGE_WIDTH_PX,
        )
        for p in pairs
    }

    total_count = sum(d.get("total_count", 0) for d in distributions) or 1
    # 비부정 의견 비율 = 전 속성 (긍정+중립) 가중평균. 건수로 가중해야 표본이 큰 속성이
    # 제대로 반영된다(단순 평균은 피드백 10건짜리 속성이 200건짜리와 같은 무게가 된다).
    # ⚠️ 변수명은 `brand_sentiment` 로 남겨 둔다 — 템플릿·테스트가 이 키를 쓰고, 대시보드
    #    카드②(`DashboardMonthlySummary.brand_sentiment_ratio`)와 이름을 맞춰 둔 것이다.
    #    **지면 라벨만** "비부정 의견 비율"로 고쳤다(2026-08-13). 이름이 값을 설명하지
    #    못하는 상태라, 이 계산식을 인용할 때는 반드시 (긍정+중립)임을 같이 적을 것.
    brand_sentiment = (
        sum(
            (d.get("positive_ratio", 0.0) + d.get("neutral_ratio", 0.0)) * d.get("total_count", 0)
            for d in distributions
        )
        / total_count
        * 100
    )
    return {
        # 채널쌍 라벨을 화면과 같은 한글 표기로 (COUPANG_VS_NAVER → 쿠팡 vs 네이버)
        "pair_label": {
            p["comparison_pair"]: " vs ".join(
                CHANNEL_LABEL.get(part, part) for part in p["comparison_pair"].split("_VS_")
            )
            for p in pairs
        },
        "analysis_by_pair": {
            a["comparison_pair"]: a
            for a in context.get("report", {}).get("channel_pair_analyses", [])
        },
        "donut_by_aspect": donut_by_aspect,
        "gauge_by_pair": gauge_by_pair,
        "brand_sentiment": brand_sentiment,
    }


def build_book_context(
    report_month: str,
    items: list[dict[str, Any]],
    held: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """월간 합본(상품별 섹션) 컨텍스트.

    items 원소는 `{"input": MonthlyReportInput.model_dump(mode="json"),
                   "report": MonthlyReportOutput.model_dump(mode="json")}`.
    held  원소는 `MonthlyReportInput.model_dump(mode="json")` — 표본 부족으로 보류된 상품.

    ⚠️ 총합 요약(표지) 페이지는 만들지 않는다 (2026-08-04 확정). 표지가 쓰던 값
       (전사 합계·상품 목록·기간·생성 시각)은 **계산까지 함께 지웠다** — 렌더링하지 않는
       값을 컨텍스트에 남겨두면 나중에 본문과 어긋난 채로 되살아난다.

    ⚠️ 보류 상품은 **지면에도 남긴다**(2026-08-09). 예전에는 합본에서 통째로 빼고 콜백
       `notice_message` 로만 알렸는데, 표지도 목차도 없는 구조라 **PDF 만 받아보는 사람은
       자기 상품이 왜 없는지 알 방법이 없었다.** 콜백 안내는 그대로 두고(메인 화면용),
       지면에도 사유를 적어 문서 자체로 설명이 되게 한다.
    """
    # ⚠️ `report_month` 는 **보류 페이지 전용**이다(2026-08-09). 수록 상품 페이지의
    #    `{{ report.report_month }}` 는 item.report 에서 오는 **별개 값**이라 이것과
    #    무관하다 — 보류 상품은 report(LLM 산출물)가 없어서 머리글에 쓸 연월을 따로
    #    받아야 한다. 지우면 보류 페이지의 meta 줄이 빈칸이 된다.
    #    (예전 주석은 "여기 담지 않는다"였는데, 보류 페이지가 생기면서 담게 됐다.)
    return {
        "items": [
            {
                "input": item["input"],
                "report": item["report"],
                **_build_monthly_chart_context({"input": item["input"], "report": item["report"]}),
            }
            for item in items
        ],
        "held": list(held or []),
        "hold_notice": constants.HOLD_IN_BOOK_NOTICE,
        "report_month": report_month,
    }


def compile_monthly_book(context: dict[str, Any]) -> bytes:
    """월간 합본 PDF. `build_book_context()` 결과를 그대로 넣는다."""
    import weasyprint

    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html", "xml"]))
    rendered = env.from_string(MONTHLY_BOOK_HTML).render(**context)
    return weasyprint.HTML(string=rendered).write_pdf()


def compile_report_to_pdf(report_type: ReportType, context: dict[str, Any]) -> bytes:
    """HTML Jinja2 템플릿에 컨텍스트 데이터를 바인딩하여 PDF 바이너리를 생성한다.

    weasyprint 를 함수 안에서 import 하는 이유: weasyprint 는 import 만 해도 GTK
    네이티브 DLL(libgobject 등)을 찾는다. Windows 에서 GTK3 런타임이 없으면 그
    자리에서 OSError 가 나는데, 모듈 최상단에 두면 **PDF 를 만들지 않는 코드까지**
    전부 못 불러온다. 실제로 tests/ 전체가 수집 단계에서 중단됐다(테스트는
    이 함수를 mock 하므로 weasyprint 가 필요 없는데도).
    PDF 를 실제로 만들 때만 GTK 가 필요하도록 여기로 내렸다.
    """
    import weasyprint

    env = Environment(
        loader=BaseLoader(),
        autoescape=select_autoescape(["html", "xml"]),
    )

    template_str = (
        CS_TEMPLATE_HTML
        if report_type == ReportType.CS_GUIDELINE
        else MONTHLY_TEMPLATE_HTML
    )

    render_context = dict(context)
    if report_type == ReportType.MONTHLY_REPORT:
        render_context.update(_build_monthly_chart_context(context))

    template = env.from_string(template_str)
    rendered_html = template.render(**render_context)

    return weasyprint.HTML(string=rendered_html).write_pdf()