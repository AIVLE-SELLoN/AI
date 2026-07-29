from __future__ import annotations

import os
import sys
from enum import Enum
from typing import Any

import weasyprint
from jinja2 import BaseLoader, Environment, select_autoescape

# Windows 환경일 경우 GTK3 DLL 경로 강제 추가
if sys.platform == "win32":
    gtk_path = r"C:\Program Files\GTK3-Runtime Win64\bin"
    if os.path.exists(gtk_path) and gtk_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = gtk_path + os.path.pathsep + os.environ.get("PATH", "")


class ReportType(str, Enum):
    CS_GUIDELINE = "cs_guideline"
    MONTHLY_REPORT = "monthly_report"


CS_TEMPLATE_HTML = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>CS Guideline</title></head>
<body>
    <h1>CS Guideline: {{ guideline.guideline_id }}</h1>
    <p>Alert ID: {{ guideline.alert_id }}</p>
    <p>Issue: {{ guideline.summary.issue_title }}</p>
</body>
</html>
"""

MONTHLY_TEMPLATE_HTML = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Monthly Report</title></head>
<body>
    <h1>Monthly Report: {{ report.report_id }}</h1>
    <p>Month: {{ report.report_month }}</p>
</body>
</html>
"""


def compile_report_to_pdf(report_type: ReportType, context: dict[str, Any]) -> bytes:
    """HTML Jinja2 템플릿에 컨텍스트 데이터를 바인딩하여 PDF 바이너리를 생성한다."""
    env = Environment(
        loader=BaseLoader(),
        autoescape=select_autoescape(["html", "xml"]),
    )

    template_str = (
        CS_TEMPLATE_HTML
        if report_type == ReportType.CS_GUIDELINE
        else MONTHLY_TEMPLATE_HTML
    )
    template = env.from_string(template_str)
    rendered_html = template.render(**context)

    return weasyprint.HTML(string=rendered_html).write_pdf()