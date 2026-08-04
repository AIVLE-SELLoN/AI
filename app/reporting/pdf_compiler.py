from __future__ import annotations

import os
import sys
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from jinja2 import BaseLoader, Environment, select_autoescape

from app.reporting.charts import (
    render_divergence_gauge,
    render_drift_bars,
    render_sentiment_donut,
)

# Windows 환경일 경우 GTK3 DLL 경로 강제 추가.
# ⚠️ weasyprint import 보다 **먼저** 실행돼야 한다. weasyprint 는 import 시점에
#    libgobject 등 GTK DLL 을 찾으므로, PATH 를 나중에 넣으면 이미 늦다.
if sys.platform == "win32":
    gtk_path = r"C:\Program Files\GTK3-Runtime Win64\bin"
    if os.path.exists(gtk_path) and gtk_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = gtk_path + os.path.pathsep + os.environ.get("PATH", "")


class ReportType(str, Enum):
    CS_GUIDELINE = "cs_guideline"
    MONTHLY_REPORT = "monthly_report"


# 월간 합본 표지 — **화면에 띄우는 건 이 첫 페이지 하나뿐**이다(2026-08-03 확정).
# 전체 PDF 는 presigned URL 로 내려받으므로, 표지만 봐도 그 달의 상태가 파악되도록
# 전사 요약(총 VOC·위험 상품 수·최다 위험 상품)과 상품 목록을 여기 담는다.
MONTHLY_COVER_HTML = """
    <section class="cover">
        <h1>{{ report_month }} 월간 CS·품질 분석 보고서</h1>
        <div class="meta">
            대상 상품 {{ product_count }}개 · 분석 기간 {{ period }} · 생성 {{ generated_at }}
        </div>

        <div class="kpi-row">
            <div class="kpi">
                <div class="label">전체 VOC</div>
                <div class="value">{{ '{:,}'.format(total_voc) }}건</div>
                <div class="sub">{{ product_count }}개 상품 합계</div>
            </div>
            <div class="kpi">
                <div class="label">RISK 속성 보유 상품</div>
                <div class="value">{{ risk_product_count }}개</div>
                <div class="sub">전월 대비 +3%p 이상</div>
            </div>
            <div class="kpi">
                <div class="label">채널 격차 위험</div>
                <div class="value">{{ crisis_product_count }}개</div>
                <div class="sub">CRISIS 단계 채널쌍 보유</div>
            </div>
            <div class="kpi accent">
                <div class="label">BRAND SENTIMENT</div>
                <div class="value">{{ '%.1f'|format(brand_sentiment) }}%</div>
                <div class="sub">전 상품 긍정+중립 가중평균</div>
            </div>
        </div>

        <h2>상품별 요약</h2>
        <table>
            <tr><th>상품</th><th>VOC</th><th>최다 부정 속성</th><th>부정률</th><th>RISK</th><th>채널 격차</th></tr>
        {% for row in summary_rows %}
            <tr>
                <td>{{ row.product_name }} <span style="color:#868e96">({{ row.code }})</span></td>
                <td>{{ '{:,}'.format(row.total_voc) }}</td>
                <td>{{ row.worst_aspect }}</td>
                <td>{{ '%.0f'|format(row.worst_ratio * 100) }}%</td>
                <td>{% if row.risk_count %}<span class="risk">{{ row.risk_count }}건</span>{% else %}-{% endif %}</td>
                <td>{{ row.severity or '판정 보류' }}</td>
            </tr>
        {% endfor %}
        </table>
        {% if held_products %}
        <div class="meta" style="margin-top:4mm">
            표본 부족으로 보류된 상품 {{ held_products|length }}개:
            {{ held_products|join(', ') }} — VOC 10건 미만이라 분석하지 않았습니다.
        </div>
        {% endif %}
        {% if failed_products %}
        <div class="meta" style="margin-top:2mm">
            생성에 실패해 이번 호에서 빠진 상품 {{ failed_products|length }}개:
            {{ failed_products|join(', ') }} — 데이터는 정상이며 운영자가 확인 중입니다.
        </div>
        {% endif %}
    </section>
"""


_BASE_CSS = """
    @page { size: A4; margin: 16mm 14mm; }
    body { font-family: 'Malgun Gothic', 'Noto Sans KR', sans-serif; font-size: 10.5pt; color: #1a1a1a; }
    h1 { font-size: 17pt; margin: 0 0 4mm; }
    h2 { font-size: 12pt; margin: 7mm 0 2mm; border-bottom: 1px solid #ccc; padding-bottom: 1mm; }
    .meta { color: #666; font-size: 9pt; margin-bottom: 6mm; }
    table { width: 100%; border-collapse: collapse; margin-bottom: 3mm; }
    th, td { border: 1px solid #ddd; padding: 2mm 2.5mm; text-align: left; font-size: 9.5pt; }
    th { background: #f4f4f4; }
    .risk { color: #c0392b; font-weight: bold; }
    ul { margin: 0 0 3mm; padding-left: 5mm; }
    .quote { background: #f8f8f8; border-left: 3px solid #999; padding: 2mm 3mm; margin-bottom: 2mm; }

    /* KPI 카드 — 대시보드 상단과 같은 구성 */
    .kpi-row { display: flex; gap: 3mm; margin-bottom: 5mm; }
    .kpi { flex: 1; border: 1px solid #e9ecef; border-radius: 3mm; padding: 3mm; }
    .kpi .label { font-size: 7.5pt; color: #868e96; letter-spacing: .3px; }
    .kpi .value { font-size: 15pt; font-weight: bold; margin-top: 1mm; }
    .kpi .sub { font-size: 8pt; color: #868e96; margin-top: 1mm; }
    .kpi.accent { background: #4c6ef5; border-color: #4c6ef5; color: #fff; }
    .kpi.accent .label, .kpi.accent .sub { color: #dbe4ff; }

    /* 속성 카드 3열 — 도넛 + 범례 */
    .aspect-row { display: flex; gap: 3mm; margin-bottom: 4mm; }
    .aspect-card { flex: 1; border: 1px solid #e9ecef; border-radius: 3mm; padding: 3mm; }
    .aspect-card .head { display: flex; justify-content: space-between; align-items: center; }
    .aspect-card .title { font-size: 10.5pt; font-weight: bold; }
    .badge { font-size: 7.5pt; font-weight: bold; padding: 0.6mm 2mm; border-radius: 2mm;
             background: #e6fcf5; color: #0ca678; }
    .badge.risk { background: #fff5f5; color: #f03e3e; }
    .aspect-card .chart { text-align: center; margin: 2mm 0; }
    .legend { font-size: 8.5pt; color: #495057; }
    .legend .dot { display: inline-block; width: 2mm; height: 2mm; border-radius: 50%; margin-right: 1.2mm; }
    .aspect-card .note { font-size: 8.5pt; color: #495057; margin-top: 2mm; line-height: 1.45; }
    .aspect-card .note.risk { background: #fff5f5; border-radius: 2mm; padding: 2mm; }
    .gauge-box { border: 1px solid #e9ecef; border-radius: 3mm; padding: 3mm; margin-bottom: 3mm; }

    /* 합본: 표지 다음부터 상품마다 새 페이지 — 첫 페이지만 화면 미리보기로 쓴다 */
    .product-page { page-break-before: always; }
    .cover h1 { font-size: 20pt; }
    .cover table td, .cover table th { font-size: 9pt; }
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
_MONTHLY_SECTION_HTML = """
    <!-- ⚠️ 이 PDF 가 월간 리포트의 **유일한 산출물**이다(2026-08-03 확정).
         데이터를 DB 에 적재하지 않고 S3 링크만 전달하며, **UI 는 이 PDF 를 뷰어로 그대로
         띄운다**(값을 따로 렌더링하지 않는다). 즉 화면에서 볼 수치는 여기 전부 들어 있어야
         하고, 표·차트를 빼면 그 수치는 어디에도 남지 않는다. -->
    <h1>{{ input.product_name }} 월간 분석 보고서</h1>
    <div class="meta">
        보고서 ID {{ report.report_id }} · {{ report.report_month }}
        ({{ input.start_date }} ~ {{ input.end_date }})<br>
        마스터 상품 코드 {{ report.master_product_code }} · 월간 총 VOC {{ input.total_voc_count }}건
        {% if input.channel_divergence and input.channel_divergence.calculated_at %}
        · 집계 기준 {{ input.channel_divergence.calculated_at[:16]|replace('T', ' ') }}
        {% endif %}
    </div>

    <div class="kpi-row">
        <div class="kpi">
            <div class="label">TOTAL VOC</div>
            <div class="value">{{ '{:,}'.format(input.total_voc_count) }}건</div>
            <div class="sub">{{ input.start_date }} ~ {{ input.end_date }}</div>
        </div>
        <div class="kpi">
            <div class="label">CRITICAL RISKS</div>
            <div class="value">{{ risk_count }}건</div>
            <div class="sub">변동 +3%p 이상 속성</div>
        </div>
        <div class="kpi">
            <div class="label">최다 부정 속성</div>
            <div class="value">{{ worst_aspect.aspect }}</div>
            <div class="sub">부정 {{ '%.0f'|format(worst_aspect.negative_ratio * 100) }}%</div>
        </div>
        <div class="kpi accent">
            <div class="label">BRAND SENTIMENT</div>
            <div class="value">{{ '%.1f'|format(brand_sentiment) }}%</div>
            <div class="sub">전 속성 긍정+중립 비중</div>
        </div>
    </div>

    <h2>항목별 고객 감성 분포</h2>
    <div class="aspect-row">
    {% for dist in input.aspect_distributions %}
        {% set drift = drift_by_aspect.get(dist.aspect) %}
        <div class="aspect-card">
            <div class="head">
                <span class="title">{{ dist.aspect }}</span>
                {% if drift and drift.status == 'RISK' %}
                <span class="badge risk">RISK</span>{% else %}<span class="badge">STABLE</span>{% endif %}
            </div>
            <div class="sub" style="font-size:8pt;color:#868e96">총 {{ '{:,}'.format(dist.total_count) }}건의 피드백</div>
            <div class="chart">{{ donut_by_aspect.get(dist.aspect, '')|safe }}</div>
            <div class="legend">
                <span class="dot" style="background:#12b886"></span>좋아요 {{ '%.0f'|format(dist.positive_ratio * 100) }}%<br>
                <span class="dot" style="background:#ced4da"></span>보통 {{ '%.0f'|format(dist.neutral_ratio * 100) }}%<br>
                <span class="dot" style="background:#f03e3e"></span>별로예요 {{ '%.0f'|format(dist.negative_ratio * 100) }}%
            </div>
            {% set summary = summary_by_aspect.get(dist.aspect) %}
            {% if summary %}
            <div class="note{% if drift and drift.status == 'RISK' %} risk{% endif %}">{{ summary }}</div>
            {% endif %}
        </div>
    {% endfor %}
    </div>

    <h2>전월 대비 변동</h2>
    {{ drift_chart|safe }}

    <h2>{{ report.channel_divergence_cause.cause_title }}</h2>
    <p>{{ report.channel_divergence_cause.cause_description }}</p>
    {% for pair in input.channel_divergence.pairs %}
    <div class="gauge-box">{{ gauge_by_pair.get(pair.comparison_pair, '')|safe }}</div>
    {% endfor %}
    <table>
        <tr><th>채널쌍</th><th>표본</th><th>분열 점수</th><th>기준값</th><th>단계</th></tr>
    {% for pair in input.channel_divergence.pairs %}
        <tr>
            <td>{{ pair.comparison_pair }}</td>
            <td>{{ pair.sample_size }}</td>
            <td>{{ '%.2f'|format(pair.jsd_score) if pair.jsd_score is not none else '판정 보류' }}</td>
            <td>{{ '%.2f'|format(pair.jsd_baseline) if pair.jsd_baseline is not none else '-' }}</td>
            <td>{{ pair.severity if pair.severity else pair.hold_reason }}</td>
        </tr>
    {% endfor %}
    </table>

    <h2>핵심 원인 분석</h2>
    <ul>
    {% for item in report.cause_analysis_results %}<li>{{ item }}</li>{% endfor %}
    </ul>

    <h2>권장 조치</h2>
    <ul>
    {% for item in report.recommended_actions %}<li>{{ item }}</li>{% endfor %}
    </ul>

    <div class="meta" style="margin-top:6mm">
        · 변동(%p)은 전월 대비 부정 비율 변화이며, 3%p 이상이면 RISK 로 표시합니다.<br>
        · 채널 분열 단계는 SAFE(안정) · CAUTION(주의) · CRISIS(위험) 순이며,
          표본이 부족한 채널쌍은 "판정 보류"로 표기합니다.
    </div>
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

# 월간 합본 — **운영 산출물**. 첫 페이지(표지)만 화면에 띄우고 전체는 presigned URL 로
# 내려받는다(2026-08-03 확정). 상품마다 페이지를 나눠 목차 없이도 넘겨볼 수 있게 한다.
MONTHLY_BOOK_HTML = (
    _HTML_HEAD
    + MONTHLY_COVER_HTML
    + """
{% for item in items %}<section class="product-page">"""
    + """{% with report=item.report, input=item.input, drift_by_aspect=item.drift_by_aspect,
            donut_by_aspect=item.donut_by_aspect, gauge_by_pair=item.gauge_by_pair,
            summary_by_aspect=item.summary_by_aspect, drift_chart=item.drift_chart,
            risk_count=item.risk_count, worst_aspect=item.worst_aspect,
            brand_sentiment=item.brand_sentiment %}"""
    + _MONTHLY_SECTION_HTML
    + """{% endwith %}</section>{% endfor %}</body></html>"""
)

def _build_monthly_chart_context(context: dict[str, Any]) -> dict[str, Any]:
    """월간 템플릿이 쓰는 차트 SVG·파생 지표를 만든다.

    분포와 드리프트는 스키마상 별도 배열이라 aspect 로 짝지어 준다. KPI 카드 값(위험
    속성 수·최다 부정 속성·브랜드 감성)은 **입력 수치에서 그대로 계산**한다 — LLM 이
    만든 값을 쓰면 팩트체크를 통과한 문장과 표지 숫자가 어긋날 수 있다.
    """
    input_data = context.get("input", {})
    report = context.get("report", {})

    distributions = input_data.get("aspect_distributions", [])
    drifts = input_data.get("sentiment_drifts", [])
    pairs = input_data.get("channel_divergence", {}).get("pairs", [])

    drift_by_aspect = {d["aspect"]: d for d in drifts}
    donut_by_aspect = {
        d["aspect"]: render_sentiment_donut(
            positive_ratio=d["positive_ratio"],
            neutral_ratio=d["neutral_ratio"],
            negative_ratio=d["negative_ratio"],
        )
        for d in distributions
    }
    gauge_by_pair = {
        p["comparison_pair"]: render_divergence_gauge(
            jsd_score=p.get("jsd_score"),
            severity=p.get("severity"),
            comparison_pair=p["comparison_pair"],
        )
        for p in pairs
    }

    total_count = sum(d.get("total_count", 0) for d in distributions) or 1
    # 브랜드 감성 = 전 속성 (긍정+중립) 가중평균. 건수로 가중해야 표본이 큰 속성이
    # 제대로 반영된다(단순 평균은 피드백 10건짜리 속성이 200건짜리와 같은 무게가 된다).
    brand_sentiment = (
        sum(
            (d.get("positive_ratio", 0.0) + d.get("neutral_ratio", 0.0)) * d.get("total_count", 0)
            for d in distributions
        )
        / total_count
        * 100
    )
    worst_aspect = max(
        distributions, key=lambda d: d.get("negative_ratio", 0.0), default={"aspect": "-", "negative_ratio": 0.0}
    )

    return {
        "drift_by_aspect": drift_by_aspect,
        "donut_by_aspect": donut_by_aspect,
        "gauge_by_pair": gauge_by_pair,
        "summary_by_aspect": {
            s["aspect"]: s["summary_text"] for s in report.get("aspect_summaries", [])
        },
        "drift_chart": render_drift_bars(drifts),
        "risk_count": sum(1 for d in drifts if d.get("status") == "RISK"),
        "worst_aspect": worst_aspect,
        "brand_sentiment": brand_sentiment,
    }


def build_book_context(
    report_month: str,
    items: list[dict[str, Any]],
    *,
    held_products: list[str] | None = None,
    failed_products: list[str] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """월간 합본 표지 + 상품별 섹션 컨텍스트.

    items 원소는 `{"input": MonthlyReportInput.model_dump(mode="json"),
                   "report": MonthlyReportOutput.model_dump(mode="json")}`.

    표지 수치는 **입력 집계에서 직접 계산**한다. LLM 문장을 재해석해 표지를 만들면
    본문과 표지가 어긋날 수 있다.
    """
    enriched, summary_rows = [], []
    total_voc = risk_products = crisis_products = 0
    weighted_sentiment = weighted_total = 0.0

    for item in items:
        input_data, report = item["input"], item["report"]
        charts = _build_monthly_chart_context({"input": input_data, "report": report})
        enriched.append({"input": input_data, "report": report, **charts})

        distributions = input_data.get("aspect_distributions", [])
        severities = [
            p.get("severity") for p in input_data["channel_divergence"]["pairs"] if p.get("severity")
        ]
        worst_pair_severity = (
            "CRISIS" if "CRISIS" in severities else ("CAUTION" if "CAUTION" in severities else
            ("SAFE" if severities else None))
        )

        total_voc += input_data.get("total_voc_count", 0)
        risk_products += 1 if charts["risk_count"] else 0
        crisis_products += 1 if worst_pair_severity == "CRISIS" else 0
        section_total = sum(d.get("total_count", 0) for d in distributions)
        weighted_sentiment += charts["brand_sentiment"] * section_total
        weighted_total += section_total

        summary_rows.append(
            {
                "code": report.get("master_product_code"),
                "product_name": input_data.get("product_name"),
                "total_voc": input_data.get("total_voc_count", 0),
                "worst_aspect": charts["worst_aspect"].get("aspect"),
                "worst_ratio": charts["worst_aspect"].get("negative_ratio", 0.0),
                "risk_count": charts["risk_count"],
                "severity": worst_pair_severity,
            }
        )

    first = items[0]["input"] if items else {}
    return {
        "items": enriched,
        "report_month": report_month,
        "product_count": len(items),
        "period": f"{first.get('start_date', '')} ~ {first.get('end_date', '')}",
        "generated_at": generated_at or datetime.now(UTC).astimezone().strftime("%Y-%m-%d %H:%M"),
        "total_voc": total_voc,
        "risk_product_count": risk_products,
        "crisis_product_count": crisis_products,
        "brand_sentiment": (weighted_sentiment / weighted_total) if weighted_total else 0.0,
        "summary_rows": sorted(summary_rows, key=lambda r: -r["worst_ratio"]),
        # ⚠️ 보류(표본 부족)와 실패(검증 미통과)를 **합치지 않는다**. 합치면 VOC 500건인
        #    상품이 표지에 "VOC 10건 미만이라 분석하지 않았다"고 잘못 인쇄된다.
        "held_products": held_products or [],
        "failed_products": failed_products or [],
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