"""월간 리포트 PDF 차트 — 인라인 SVG 생성.

matplotlib 같은 이미지 렌더러를 붙이지 않는다: 의존성이 늘지 않고, 벡터라 인쇄·확대에
안 깨지며, PNG 를 base64 로 박는 것보다 파일이 훨씬 작다(차트 3개 + 게이지가 수 KB).
weasyprint 가 인라인 SVG 를 그대로 그린다.

weasyprint 의 SVG 지원은 기본 도형에 강하고 고급 필터는 약하다. 그래서 그라디언트
대신 구간 색 분할을 쓴다 — 인쇄물에서 단계 경계가 더 잘 읽히는 효과도 같이 얻는다.
"""

from __future__ import annotations

from itertools import pairwise

# 대시보드와 같은 팔레트 (긍정 초록 / 중립 회색 / 부정 빨강)
COLOR_POSITIVE = "#12b886"
COLOR_NEUTRAL = "#ced4da"
COLOR_NEGATIVE = "#f03e3e"
COLOR_TEXT = "#212529"
COLOR_MUTED = "#868e96"

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
    """감성 분포 도넛. 가운데에 부정 비율을 크게 박는다.

    셀러가 확인해야 하는 값은 "얼마나 좋은가"가 아니라 "얼마나 나빠졌는가"다.
    대시보드 카드와 같은 기준이다.
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

    # 가운데 수치는 구멍 안에 완전히 들어가야 한다. 링 안쪽 반지름은
    # radius - stroke/2 = 20 이고, 15px 로 그리면 "50%" 폭 40px 의 반폭이 그 값과 같아져
    # 글자 끝이 링을 파고든다. 12px 이면 반폭 16.2px · 그 지점 구멍 반높이 11.7px 이라
    # 아래끝(baseline cy+8.5)과 약 3px 뜬다. 링 두께(10)는 건드리지 않는다.
    parts.append(
        f'<text x="{cx}" y="{cy - 5}" text-anchor="middle" font-size="7" fill="{COLOR_MUTED}">부정</text>'
        f'<text x="{cx}" y="{cy + 8.5}" text-anchor="middle" font-size="12" font-weight="bold" '
        f'fill="{COLOR_TEXT}">{negative_ratio * 100:.0f}%</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def _gradient_slices(x: float, width: float, y: float, height: float, slices: int = 48) -> list[str]:
    """초록-노랑-빨강 그라디언트를 가는 사각형으로 근사하고 양 끝을 둥글게 막는다.

    SVG linearGradient 도 `clipPath` 도 쓰지 않는다 — weasyprint 에서 렌더러 버전을 타
    인쇄물에서 색이 통째로 빠질 수 있다. 양 끝은 반원 path 로 따로 그린다(호는 기본
    도형이라 확실히 그려진다). 가운데 슬라이스는 반원 반지름만큼 안쪽에서 시작해 사각
    모서리가 반원 밖으로 비어져 나오지 않게 한다.
    """
    # slices=1 이면 아래 i/(slices-1) 이 0 나눗셈이다. 인자를 열어둔 함수라 방어한다.
    slices = max(2, slices)
    stops = [(0.0, (18, 184, 134)), (0.5, (250, 176, 5)), (1.0, (240, 62, 62))]

    def color_at(t: float) -> str:
        for (t0, c0), (t1, c1) in pairwise(stops):
            if t <= t1:
                ratio = 0.0 if t1 == t0 else (t - t0) / (t1 - t0)
                r, g, b = (round(a + (z - a) * ratio) for a, z in zip(c0, c1, strict=True))
                return f"#{r:02x}{g:02x}{b:02x}"
        return "#f03e3e"

    radius = height / 2  # 알약(pill) 형태 — 막대 높이의 절반
    inner_x, inner_w = x + radius, max(0.0, width - height)

    parts = [
        # 왼쪽 반원 (위→아래, sweep=0 이라 왼쪽으로 부푼다)
        (
            f'<path d="M {inner_x:.2f},{y} A {radius:.2f},{radius:.2f} 0 0 0 '
            f'{inner_x:.2f},{y + height} Z" fill="{color_at(0.0)}"/>'
        ),
        # 오른쪽 반원 (위→아래, sweep=1 이라 오른쪽으로 부푼다)
        (
            f'<path d="M {inner_x + inner_w:.2f},{y} A {radius:.2f},{radius:.2f} 0 0 1 '
            f'{inner_x + inner_w:.2f},{y + height} Z" fill="{color_at(1.0)}"/>'
        ),
    ]

    slice_w = inner_w / slices
    for i in range(slices):
        # 위치(t)는 **막대 전체** 기준으로 잡는다 — 반원 색과 이어지도록.
        t = (radius + i * slice_w) / width
        parts.append(
            f'<rect x="{inner_x + i * slice_w:.2f}" y="{y}" width="{slice_w + 0.6:.2f}" '
            f'height="{height}" fill="{color_at(min(1.0, t))}"/>'
        )
    return parts


def render_divergence_gauge(
    *,
    jsd_score: float | None,
    comparison_pair: str,
    jsd_baseline: float | None = None,
    sample_size: int | None = None,
    width: int = 430,
) -> str:
    """채널 간 평판 격차 게이지 — 절대 격차 축(0.0~0.6+) 위에 점수와 기준선을 찍는다.

    이 그림은 판정을 말하지 않는다. 절대 척도에서의 위치만 말한다. 마커는 `jsd_score`
    절대값인데 severity 판정은 `excess = jsd_score - jsd_baseline` 기준이라
    (`metrics_calculator.decide_severity`, 경계 `JSD_DELTA_MIN`=0.10 / 0.20), 두 축이
    서로 다른 것을 잰다. 이런 쌍이 정상적으로 나온다:

        jsd   baseline  excess  판정      절대 축에서 마커가 찍히는 곳
        0.54  0.50      0.04    SAFE      막대 오른쪽(54%)
        0.25  0.02      0.23    CRISIS    막대 왼쪽(25%)

    그래서 구간 이름에 판정어를 쓰면 안 된다. "SAFETY/DANGER ZONE" 으로 부르면 CRISIS
    인 두 번째 쌍의 마커가 "SAFETY ZONE" 안에 찍히는데, 같은 페이지 상단 `cause_title`
    에는 `_validate_stage_label` 이 "위험 단계"를 강제하므로 문서 안에서 문장과 그림이
    정면으로 반대가 된다. 기준선을 같이 그려도 이 모순은 안 풀린다 — 간격에서 이유를
    추론할 수 있게 될 뿐, 마커가 "SAFETY" 구역 안에 있다는 사실은 그대로다. 그래서
    구간 이름이 절대 축의 말("절대 격차 낮음/높음")이다.

    기준선을 함께 찍는 이유는 따로 있다. 두 마커 사이 간격이 곧 excess 라 "얼마나
    초과했는지"가 그림에 남는다. 절대 점수도 그대로 두는 것은 `pair_analysis` 문장이
    `jsd_score` 를 인용하기 때문이다 — excess 로 갈아끼우면 문장과 그림이 다시 갈린다.

    게이지는 유의성도, severity 배지도 보여주지 않는다. `decide_severity` 는
    `bh_significant` 가 False 면 excess 가 아무리 커도 SAFE 로 보므로 간격이 넓은데
    판정은 SAFE 인 쌍이 가능하다. 그림이 아니라 문장이 책임진다 — 렌더러에 severity·bh
    를 넘기면 판정 규칙이 `metrics_calculator` 와 여기 두 곳으로 갈린다.

    단계 라벨 자체가 문서에서 사라진 것은 아니다. 페이지 상단 요약 문장
    (`channel_divergence_cause.cause_title`)에는 "위험 단계"처럼 들어가고 검증기
    `_validate_stage_label` 이 그 포함을 강제한다. 그 라벨은 게이지 색과 문구가
    어긋나는 것을 잡는 유일한 앵커라 빼면 검증이 약해진다.

    판정이 보류된 채널쌍은 마커를 찍지 않는다 — 표본 부족으로 판정하지 않은 값을
    위치로 표시하면 없는 근거를 만드는 셈이다. 스키마가 게이트 미충족 시 판정 6개 값을
    전부 `null` 로 못박아서(`ChannelDivergencePair._GATED_FIELDS`) `jsd_baseline` 도
    같이 `None` 이 되므로, 기준선만 덩그러니 남는 경우는 생기지 않는다.
    """
    # 막대 높이는 구간 라벨 두 줄이 안쪽에 들어갈 만큼 필요하다. 줄이면 라벨 baseline
    # 이 막대 밖으로 나가 잘린다.
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

    # 구간 라벨은 막대 안쪽에 흰 글씨로. 판정어(안전/위험/SAFETY/DANGER)는 쓰지 않는다 —
    # 사유는 함수 docstring 참고. 대안이던 "severity·bh 를 렌더러에 넘겨 판정을 직접
    # 그리기" 는 배지를 되살리는 셈이라 기각했다. 지금 라벨은 "이 값이 절대 척도에서
    # 어디쯤인가"만 말하고 판정은 문장이 한다.
    #
    # 화면(프론트)은 아직 SAFETY/DANGER 를 쓴다. 같은 모순이 거기에도 있으므로 공유가
    # 필요하다. 지면을 화면에 맞추려고 되돌리지 말 것 — 틀린 쪽에 맞추는 것이다.
    parts.append(
        f'<text x="{pad + 8}" y="{bar_y + 11}" font-size="7" font-weight="bold" '
        f'fill="#ffffff">절대 격차 낮음</text>'
        f'<text x="{pad + 8}" y="{bar_y + 22}" font-size="8" fill="#ffffff">0.0</text>'
        f'<text x="{width - pad - 8}" y="{bar_y + 11}" text-anchor="end" font-size="7" '
        f'font-weight="bold" fill="#ffffff">절대 격차 높음</text>'
        f'<text x="{width - pad - 8}" y="{bar_y + 22}" text-anchor="end" font-size="8" '
        f'fill="#ffffff">0.6+</text>'
    )

    if jsd_score is not None:
        x = pad + bar_w * max(0.0, min(1.0, jsd_score))
        label = f"Score {jsd_score:.2f}"

        # 기준선(귀무 기댓값)을 점수 마커보다 먼저 그려, 겹칠 때 점수가 위로 오게 한다
        # — 둘이 붙어 있을 때(= excess 가 작을 때) 판정의 주인공이 가려지지 않게.
        #
        # 모양도 다르게 한다. 같은 흰 실선이면 어느 쪽이 기준인지 알 수 없다. 점수는
        # 흰 실선, 기준선은 어두운 점선이다(둘 다 기본 도형이라 확실히 그려진다).
        # 막대 밖으로 내리지 않는다 — 아래 줄의 "표본 N건"(오른쪽 정렬)과 겹친다.
        if jsd_baseline is not None:
            bx = pad + bar_w * max(0.0, min(1.0, jsd_baseline))
            parts.append(
                f'<line x1="{bx:.1f}" y1="{bar_y}" x2="{bx:.1f}" y2="{bar_y + bar_h}" '
                f'stroke="{COLOR_TEXT}" stroke-width="1.4" stroke-dasharray="3 2" '
                f'opacity="0.9"/>'
            )
            # 라벨은 막대 아래 줄. "표본 N건"(7.5px, 약 56px)이 오른쪽 끝을 쓰므로
            # 그만큼 + 자기 반폭(약 20px)을 비워 클램프한다 — 안 그러면 기준선이 큰
            # 쌍에서 두 글자가 겹쳐 둘 다 못 읽는다.
            label_x = min(max(bx, pad + 20), width - pad - 80)
            parts.append(
                f'<text x="{label_x:.1f}" y="{height - 2}" text-anchor="middle" '
                f'font-size="7.5" fill="{COLOR_MUTED}">기준 {jsd_baseline:.2f}</text>'
            )
        # 폭은 실측 기준이다: "Score 0.42" 는 9px 산세리프에서 49.1px.
        #   점(cx +11, r 3.5) -> 텍스트 시작 +19 -> 끝 약 +68 -> 우측 여백 약 10
        # 점수는 항상 `%.2f` 라 글자 수가 고정이므로 폭이 흔들리지 않는다.
        box_w = 78
        box_x = min(max(x - box_w / 2, pad), width - pad - box_w)
        parts.append(
            f'<rect x="{box_x:.1f}" y="1" width="{box_w}" height="17" rx="8.5" '
            f'fill="#ffffff" stroke="#dee2e6"/>'
            f'<circle cx="{box_x + 11:.1f}" cy="9.5" r="3.5" fill="{COLOR_NEGATIVE}"/>'
            f'<text x="{box_x + 19:.1f}" y="13" font-size="9" fill="{COLOR_TEXT}">{label}</text>'
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


