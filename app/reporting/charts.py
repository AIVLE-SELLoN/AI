"""월간 리포트 PDF 차트 — 인라인 SVG 생성.

**왜 SVG 직접 생성인가**: matplotlib 같은 렌더러를 새로 붙이지 않기 위해서다.
  - 의존성이 늘지 않는다(requirements 변경은 팀 합의 대상이고, 이미지 렌더러는 무겁다).
  - 벡터라 인쇄·확대에서 깨지지 않고, PNG 를 base64 로 박는 것보다 파일이 훨씬 작다
    (차트 3개 + 게이지가 수 KB 수준).
  - weasyprint 가 인라인 SVG 를 그대로 그린다.

⚠️ weasyprint 의 SVG 지원은 기본 도형에 강하고 고급 필터·애니메이션은 약하다.
   그래서 그라디언트 대신 **구간 색 분할**을 쓴다 — 인쇄물에서 단계 경계가 더 잘 읽히는
   효과도 같이 얻는다.
"""

from __future__ import annotations

from app.core.constants import SEVERITY_STAGE_LABEL

# 대시보드와 같은 팔레트 (긍정 초록 / 중립 회색 / 부정 빨강)
COLOR_POSITIVE = "#12b886"
COLOR_NEUTRAL = "#ced4da"
COLOR_NEGATIVE = "#f03e3e"
COLOR_TEXT = "#212529"
COLOR_MUTED = "#868e96"

# 게이지 구간 색 (SAFE / CAUTION / CRISIS)
SEVERITY_COLOR = {
    "SAFE": "#12b886",
    "CAUTION": "#fab005",
    "CRISIS": "#f03e3e",
}


def _arc_dasharray(ratio: float, circumference: float) -> str:
    """도넛 조각 길이. 비율이 0이면 0 길이로 그려 선이 삐져나오지 않게 한다."""
    length = max(0.0, min(1.0, ratio)) * circumference
    return f"{length:.3f} {circumference - length:.3f}"


def render_sentiment_donut(
    *,
    positive_ratio: float,
    neutral_ratio: float,
    negative_ratio: float,
    size: int = 132,
) -> str:
    """감성 분포 도넛. 가운데에 **부정 비율**을 크게 박는다.

    가운데 수치를 부정으로 잡은 이유: 이 리포트에서 셀러가 확인해야 하는 값은 "얼마나
    좋은가"가 아니라 "얼마나 나빠졌는가"다. 대시보드 카드와 같은 기준으로 맞춘다.
    """
    radius = size / 2 - 14
    circumference = 2 * 3.141592653589793 * radius
    cx = cy = size / 2

    # 12시 방향에서 시작해 시계방향으로: 부정 → 중립 → 긍정
    segments = [
        (negative_ratio, COLOR_NEGATIVE),
        (neutral_ratio, COLOR_NEUTRAL),
        (positive_ratio, COLOR_POSITIVE),
    ]

    parts = [
        (
            f'<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" '
            f'xmlns="http://www.w3.org/2000/svg">'
        ),
        f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="none" stroke="#f1f3f5" stroke-width="16"/>',
    ]

    offset = 0.0
    for ratio, color in segments:
        if ratio <= 0:
            continue
        parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="none" stroke="{color}" '
            f'stroke-width="16" stroke-dasharray="{_arc_dasharray(ratio, circumference)}" '
            f'stroke-dashoffset="{-offset * circumference:.3f}" '
            f'transform="rotate(-90 {cx} {cy})"/>'
        )
        offset += max(0.0, min(1.0, ratio))

    parts.append(
        f'<text x="{cx}" y="{cy - 2}" text-anchor="middle" font-size="9" fill="{COLOR_MUTED}">부정</text>'
        f'<text x="{cx}" y="{cy + 16}" text-anchor="middle" font-size="20" font-weight="bold" '
        f'fill="{COLOR_TEXT}">{negative_ratio * 100:.0f}%</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def render_divergence_gauge(
    *,
    jsd_score: float | None,
    severity: str | None,
    comparison_pair: str,
    width: int = 640,
) -> str:
    """채널 간 평판 격차 게이지. 안전(0.0) → 위험(1.0) 구간 위에 현재 점수를 찍는다.

    판정이 보류된 채널쌍(severity=None)은 눈금만 그리고 마커를 찍지 않는다 —
    표본 부족으로 판정하지 않은 값을 위치로 표시하면 없는 근거를 만드는 셈이다.
    """
    height = 74
    bar_y, bar_h = 26, 26
    pad = 12
    bar_w = width - pad * 2

    # 구간 경계: δ_min=0.10, 2δ_min=0.20 을 0~1 스케일에 얹은 시각 눈금
    # (실제 판정은 excess 기준이고, 게이지는 "지금 어디쯤인지"를 보여주는 용도다)
    zones = [(0.0, 0.33, SEVERITY_COLOR["SAFE"]), (0.33, 0.66, SEVERITY_COLOR["CAUTION"]),
             (0.66, 1.0, SEVERITY_COLOR["CRISIS"])]

    parts = [
        (
            f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'xmlns="http://www.w3.org/2000/svg">'
        )
    ]
    for start, end, color in zones:
        x = pad + bar_w * start
        w = bar_w * (end - start)
        parts.append(
            f'<rect x="{x:.1f}" y="{bar_y}" width="{w:.1f}" height="{bar_h}" '
            f'fill="{color}" opacity="0.85"/>'
        )

    parts.append(
        f'<text x="{pad}" y="{bar_y - 8}" font-size="9" fill="{COLOR_MUTED}">안전 (0.0)</text>'
        f'<text x="{width - pad}" y="{bar_y - 8}" text-anchor="end" font-size="9" '
        f'fill="{COLOR_MUTED}">위험 (1.0)</text>'
        f'<text x="{pad}" y="{height - 6}" font-size="10" fill="{COLOR_TEXT}">{comparison_pair}</text>'
    )

    if jsd_score is not None and severity is not None:
        x = pad + bar_w * max(0.0, min(1.0, jsd_score))
        label = SEVERITY_STAGE_LABEL.get(severity, severity)
        parts.append(
            f'<line x1="{x:.1f}" y1="{bar_y - 4}" x2="{x:.1f}" y2="{bar_y + bar_h + 4}" '
            f'stroke="{COLOR_TEXT}" stroke-width="2"/>'
            f'<circle cx="{x:.1f}" cy="{bar_y + bar_h / 2}" r="7" fill="#ffffff" '
            f'stroke="{COLOR_TEXT}" stroke-width="2"/>'
            f'<text x="{width - pad}" y="{height - 6}" text-anchor="end" font-size="10" '
            f'font-weight="bold" fill="{SEVERITY_COLOR.get(severity, COLOR_TEXT)}">'
            f'{label} · Score {jsd_score:.2f}</text>'
        )
    else:
        parts.append(
            f'<text x="{width - pad}" y="{height - 6}" text-anchor="end" font-size="10" '
            f'fill="{COLOR_MUTED}">판정 보류 (표본 부족)</text>'
        )

    parts.append("</svg>")
    return "".join(parts)


def render_drift_bars(drifts: list[dict], width: int = 300) -> str:
    """속성별 전월 대비 변동(%p) 막대. 0을 가운데 두고 좌우로 뻗는다.

    증감이 한눈에 보여야 해서 0 기준 양방향으로 그린다 — 상승만 빨강으로 칠해
    "나빠진 항목"이 즉시 눈에 띄게 한다.
    """
    row_h, pad_top = 26, 14
    height = pad_top + row_h * len(drifts) + 6
    mid = width * 0.55  # 0 기준선 (오른쪽 상승 폭을 더 넓게 쓴다)
    scale = width * 0.4 / 10.0  # 10%p 를 화면 폭 40% 로 매핑

    parts = [
        (
            f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'xmlns="http://www.w3.org/2000/svg">'
        ),
        f'<line x1="{mid}" y1="6" x2="{mid}" y2="{height - 4}" stroke="#dee2e6" stroke-width="1"/>',
    ]
    for i, drift in enumerate(drifts):
        y = pad_top + row_h * i
        value = float(drift.get("drift_rate", 0.0)) * 100
        bar_len = min(abs(value) * scale, width * 0.4)
        color = COLOR_NEGATIVE if value >= 0 else COLOR_POSITIVE
        x = mid if value >= 0 else mid - bar_len
        parts.append(
            f'<text x="4" y="{y + 12}" font-size="10" fill="{COLOR_TEXT}">{drift.get("aspect", "")}</text>'
            f'<rect x="{x:.1f}" y="{y + 3}" width="{bar_len:.1f}" height="12" fill="{color}" rx="2"/>'
            f'<text x="{mid + width * 0.4 + 4:.1f}" y="{y + 13}" font-size="10" '
            f'fill="{color}">{value:+.1f}%p</text>'
        )
    parts.append("</svg>")
    return "".join(parts)
