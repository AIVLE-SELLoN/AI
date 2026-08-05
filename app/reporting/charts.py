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

from itertools import pairwise

# 대시보드와 같은 팔레트 (긍정 초록 / 중립 회색 / 부정 빨강)
COLOR_POSITIVE = "#12b886"
COLOR_NEUTRAL = "#ced4da"
COLOR_NEGATIVE = "#f03e3e"
COLOR_TEXT = "#212529"
COLOR_MUTED = "#868e96"

# 게이지 구간 색 (SAFE / CAUTION / CRISIS)
def _arc_dasharray(ratio: float, circumference: float) -> str:
    """도넛 조각 길이. 비율이 0이면 0 길이로 그려 선이 삐져나오지 않게 한다."""
    length = max(0.0, min(1.0, ratio)) * circumference
    return f"{length:.3f} {circumference - length:.3f}"


def render_sentiment_donut(
    *,
    positive_ratio: float,
    neutral_ratio: float,
    negative_ratio: float,
    size: int = 68,
) -> str:
    """감성 분포 도넛. 가운데에 **부정 비율**을 크게 박는다.

    가운데 수치를 부정으로 잡은 이유: 이 리포트에서 셀러가 확인해야 하는 값은 "얼마나
    좋은가"가 아니라 "얼마나 나빠졌는가"다. 대시보드 카드와 같은 기준으로 맞춘다.
    """
    radius = size / 2 - 9
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
        f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="none" stroke="#f1f3f5" stroke-width="10"/>',
    ]

    offset = 0.0
    for ratio, color in segments:
        if ratio <= 0:
            continue
        parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{radius}" fill="none" stroke="{color}" '
            f'stroke-width="10" stroke-dasharray="{_arc_dasharray(ratio, circumference)}" '
            f'stroke-dashoffset="{-offset * circumference:.3f}" '
            f'transform="rotate(-90 {cx} {cy})"/>'
        )
        offset += max(0.0, min(1.0, ratio))

    parts.append(
        f'<text x="{cx}" y="{cy - 2}" text-anchor="middle" font-size="7" fill="{COLOR_MUTED}">부정</text>'
        f'<text x="{cx}" y="{cy + 12}" text-anchor="middle" font-size="15" font-weight="bold" '
        f'fill="{COLOR_TEXT}">{negative_ratio * 100:.0f}%</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def _gradient_slices(x: float, width: float, y: float, height: float, slices: int = 48) -> list[str]:
    """초록→노랑→빨강 그라디언트를 가는 사각형으로 근사한다.

    SVG linearGradient 대신 슬라이스를 쓰는 이유: weasyprint 의 SVG 지원은 기본 도형에
    강하고 그라디언트는 렌더러 버전을 탄다. 인쇄물에서 색이 통째로 빠지는 사고를 피하려고
    확실히 그려지는 방식으로 만든다.
    """
    # slices=1 이면 아래 i/(slices-1) 이 0 나눗셈이 된다. 지금 호출부는 기본값뿐이라
    # 도달하지 않지만, 인자를 열어둔 함수라 방어한다.
    slices = max(2, slices)
    stops = [(0.0, (18, 184, 134)), (0.5, (250, 176, 5)), (1.0, (240, 62, 62))]

    def color_at(t: float) -> str:
        for (t0, c0), (t1, c1) in pairwise(stops):
            if t <= t1:
                ratio = 0.0 if t1 == t0 else (t - t0) / (t1 - t0)
                r, g, b = (round(a + (z - a) * ratio) for a, z in zip(c0, c1, strict=True))
                return f"#{r:02x}{g:02x}{b:02x}"
        return "#f03e3e"

    parts, slice_w = [], width / slices
    for i in range(slices):
        parts.append(
            f'<rect x="{x + i * slice_w:.2f}" y="{y}" width="{slice_w + 0.6:.2f}" '
            f'height="{height}" fill="{color_at(i / (slices - 1))}"/>'
        )
    return parts


def render_divergence_gauge(
    *,
    jsd_score: float | None,
    comparison_pair: str,
    sample_size: int | None = None,
    width: int = 430,
) -> str:
    """채널 간 평판 격차 게이지 — 안전(0.0) ~ 위험(0.6+) 위에 현재 점수를 찍는다.

    ⚠️ 게이지에는 severity 배지(CRISIS DETECTED 등)를 **달지 않는다**(2026-08-04 확정).
       여기서는 점수와 위치만 보여준다.

       단, 단계 라벨 자체가 문서에서 사라진 것은 아니다 — 페이지 상단 요약 문장
       (`channel_divergence_cause.cause_title`)에는 "위험 단계"처럼 그대로 들어가고,
       검증기 `_validate_stage_label` 이 그 포함을 **강제**한다. 그 라벨은 게이지 색과
       문구가 어긋나는 것을 잡는 유일한 앵커라 빼면 검증이 약해진다.
       (2026-08-05 리뷰 확인: 배지만 삭제, 문장은 유지)

    판정이 보류된 채널쌍은 마커를 찍지 않는다 — 표본 부족으로 판정하지 않은 값을
    위치로 표시하면 없는 근거를 만드는 셈이다.
    """
    # 막대 높이는 구간 라벨 두 줄(ZONE + 점수 범위)이 **안쪽에** 들어갈 만큼 필요하다.
    # 여기를 줄이면 라벨 baseline 이 막대 밖으로 나가 잘린다.
    height = 58
    bar_y, bar_h = 20, 27
    pad = 10
    bar_w = width - pad * 2

    parts = [
        (
            f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'xmlns="http://www.w3.org/2000/svg">'
        )
    ]
    parts.extend(_gradient_slices(pad, bar_w, bar_y, bar_h))

    # 구간 라벨은 막대 안쪽에 흰 글씨로 (화면과 동일)
    parts.append(
        f'<text x="{pad + 8}" y="{bar_y + 11}" font-size="7" font-weight="bold" '
        f'fill="#ffffff">SAFETY ZONE</text>'
        f'<text x="{pad + 8}" y="{bar_y + 22}" font-size="8" fill="#ffffff">안전 (0.0)</text>'
        f'<text x="{width - pad - 8}" y="{bar_y + 11}" text-anchor="end" font-size="7" '
        f'font-weight="bold" fill="#ffffff">DANGER ZONE</text>'
        f'<text x="{width - pad - 8}" y="{bar_y + 22}" text-anchor="end" font-size="8" '
        f'fill="#ffffff">위험 (0.6+)</text>'
    )

    if jsd_score is not None:
        x = pad + bar_w * max(0.0, min(1.0, jsd_score))
        label = f"Score {jsd_score:.2f}"
        box_w = 96  # 좌우를 넉넉히 — 좁으면 "Score 0.54" 가 답답하게 붙는다
        box_x = min(max(x - box_w / 2, pad), width - pad - box_w)
        parts.append(
            f'<rect x="{box_x:.1f}" y="1" width="{box_w}" height="17" rx="8.5" '
            f'fill="#ffffff" stroke="#dee2e6"/>'
            f'<circle cx="{box_x + 14:.1f}" cy="9.5" r="3.5" fill="{COLOR_NEGATIVE}"/>'
            f'<text x="{box_x + 23:.1f}" y="13" font-size="9" fill="{COLOR_TEXT}">{label}</text>'
            f'<line x1="{x:.1f}" y1="{bar_y}" x2="{x:.1f}" y2="{bar_y + bar_h}" '
            f'stroke="#ffffff" stroke-width="2"/>'
        )
    else:
        parts.append(
            f'<text x="{width / 2}" y="13" text-anchor="middle" font-size="8.5" '
            f'fill="{COLOR_MUTED}">표본 부족 — 판정 보류</text>'
        )

    if sample_size is not None:
        parts.append(
            f'<text x="{width - pad}" y="{height - 2}" text-anchor="end" font-size="7.5" '
            f'fill="{COLOR_MUTED}">표본 {sample_size}건</text>'
        )

    parts.append("</svg>")
    return "".join(parts)


