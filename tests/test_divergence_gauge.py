"""채널 격차 게이지(`charts.render_divergence_gauge`)가 판정과 같은 근거를 보여주는가.

🔴 **여기서 재는 것 하나: 그림이 문장과 반대로 보이지 않는가.**

게이지 마커는 `jsd_score` **절대값** 위치인데, severity 판정은
`excess = jsd_score - jsd_baseline` 기준이다(`metrics_calculator.decide_severity`).
두 기준이 달라 이런 쌍이 정상적으로 나온다:

    jsd   baseline  excess  판정      기준선 없이 본 게이지
    0.54  0.50      0.04    SAFE      붉은 구역(54%) — 위험해 보인다
    0.25  0.02      0.23    CRISIS    녹색 구역(25%) — 안전해 보인다

두 번째가 더 나쁘다 — 같은 페이지 `cause_title` 에는 `_validate_stage_label` 이
"위험 단계"를 강제로 넣는데 그림은 SAFETY ZONE 을 가리킨다. `jsd_baseline` 은 표본이
작을수록 커지므로 드문 경우가 아니다. (2026-08-13 서영님 시연 검토)

기준선을 같이 찍으면 **두 마커 사이 간격이 곧 excess** 라 초과분이 그림에 남는다.
"""

from __future__ import annotations

import re

import pytest

from app.core import constants
from app.reporting.charts import render_divergence_gauge
from app.reporting.metrics_calculator import decide_severity
from app.reporting.pdf_compiler import _build_monthly_chart_context

PAIR = "COUPANG_VS_NAVER"
WIDTH = 745  # pdf_compiler.GAUGE_WIDTH_PX 와 같은 실사용 폭

# 마커 x 좌표를 뽑는다. 점수는 흰 실선, 기준선은 어두운 점선이라 stroke 로 가른다.
_SCORE_LINE = re.compile(r'<line x1="([\d.]+)"[^>]*stroke="#ffffff"')
_BASELINE_LINE = re.compile(r'<line x1="([\d.]+)"[^>]*stroke-dasharray')


def _gauge(score: float | None, baseline: float | None) -> str:
    return render_divergence_gauge(
        jsd_score=score,
        comparison_pair=PAIR,
        jsd_baseline=baseline,
        sample_size=203,
        width=WIDTH,
    )


@pytest.mark.parametrize(
    ("score", "baseline", "expected_stage"),
    [
        # 붉은 구역인데 SAFE — 기준선이 같이 높아서 초과분이 작다
        (0.54, 0.50, "SAFE"),
        # 녹색 구역인데 CRISIS — 기준선이 낮아서 초과분이 크다 (더 위험한 쪽)
        (0.25, 0.02, "CRISIS"),
    ],
)
def test_baseline_marker_explains_the_verdict(score, baseline, expected_stage) -> None:
    """🔴 절대 위치가 판정과 어긋나는 쌍에서 **기준선이 그 이유를 설명**한다.

    판정을 여기서 다시 계산하지 않고 `decide_severity` 를 그대로 부른다 — 테스트가
    판정식을 베껴 적으면 그쪽이 바뀌었을 때 이 테스트만 옛 기준으로 남는다.
    """
    severity, _ = decide_severity(score, baseline, bh_significant=True)
    assert severity.name == expected_stage, "픽스처 전제가 깨졌다 — 판정식이 바뀌었다"

    svg = _gauge(score, baseline)

    score_x = float(_SCORE_LINE.search(svg).group(1))
    baseline_x = float(_BASELINE_LINE.search(svg).group(1))

    # 두 마커 사이 간격이 곧 excess 다. 폭에 비례하므로 방향과 크기를 함께 본다.
    gap = score_x - baseline_x
    expected_gap = (score - baseline) * (WIDTH - 20)  # pad=10 양쪽
    assert gap == pytest.approx(expected_gap, abs=0.5)

    # 초과분이 CRISIS 경계를 넘었으면 간격도 그만큼 넓어야 한다 — 그림과 판정이 같은 축
    if expected_stage == "CRISIS":
        assert gap > 2 * constants.JSD_DELTA_MIN * (WIDTH - 20) * 0.99


def test_score_marker_still_shows_the_absolute_value() -> None:
    """점수 마커는 **절대값 자리에 그대로** 남는다.

    `pair_analysis` 문장이 `jsd_score` 를 그대로 인용하므로, 게이지를 excess 로 갈아끼우면
    문장과 그림이 다시 갈린다. 기준선을 **더하는** 방식을 고른 이유다.
    """
    svg = _gauge(0.42, 0.10)

    assert "Score 0.42" in svg
    assert float(_SCORE_LINE.search(svg).group(1)) == pytest.approx(
        10 + (WIDTH - 20) * 0.42, abs=0.5
    )


def test_held_pair_draws_neither_marker() -> None:
    """판정 보류(게이트 미충족)면 **기준선도 안 찍는다.**

    스키마가 게이트 미충족 시 판정 6개 값을 전부 `null` 로 못박아서
    (`ChannelDivergencePair._GATED_FIELDS`) `jsd_baseline` 도 같이 `None` 이 된다.
    기준선만 덩그러니 남으면 "판정하지 않았다"가 "기준 대비 0"으로 읽힌다.
    """
    svg = _gauge(None, None)

    assert _SCORE_LINE.search(svg) is None
    assert _BASELINE_LINE.search(svg) is None
    assert "기준" not in svg
    assert "표본 부족 — 판정 보류" in svg


def test_baseline_label_never_collides_with_the_sample_text() -> None:
    """기준선이 오른쪽 끝이어도 라벨이 "표본 N건" 위로 겹치지 않는다.

    둘 다 같은 줄(`height - 2`)에 있고 표본 텍스트는 오른쪽 정렬이라, 클램프가 없으면
    기준선이 큰 쌍에서 **두 글자가 겹쳐 둘 다 못 읽는다.**
    """
    svg = _gauge(0.99, 0.97)

    label_x = float(
        re.search(r'<text x="([\d.]+)"[^>]*text-anchor="middle"[^>]*font-size="7.5"', svg).group(1)
    )
    # 표본 텍스트가 쓰는 오른쪽 영역(약 56px) + 라벨 반폭(약 20px) 을 비워 둔다
    assert label_x <= WIDTH - 10 - 80


def test_pdf_context_actually_passes_the_baseline() -> None:
    """🔴 **호출부가 `jsd_baseline` 을 넘기는지**까지 본다 — 원래 버그가 여기였다.

    `render_divergence_gauge` 만 테스트하면 인자를 안 넘기는 회귀를 못 잡는다. 실제로
    이 함수가 `jsd_score`·`sample_size` 만 넘기고 있어서, 게이지는 판정을 반영할 방법
    자체가 없었다. 렌더 함수 단위 테스트 3개가 전부 통과하는 상태였다.
    (뮤테이션 확인: 이 단언이 없으면 호출부에서 인자를 지워도 스위트가 초록이다)
    """
    pair = {
        "comparison_pair": PAIR,
        "jsd_score": 0.25,
        "jsd_baseline": 0.02,
        "sample_size": 203,
    }
    context = _build_monthly_chart_context(
        {"input": {"channel_divergence": {"pairs": [pair]}, "aspect_distributions": []}}
    )

    svg = context["gauge_by_pair"][PAIR]
    assert _BASELINE_LINE.search(svg) is not None, "호출부가 jsd_baseline 을 안 넘겼다"
    assert "기준 0.02" in svg


def test_gauge_without_baseline_still_renders() -> None:
    """`jsd_baseline` 이 없어도 깨지지 않는다 — 인자가 선택이라 호출부가 빠뜨릴 수 있다.

    그 경우 점수 마커만 나온다(예전 동작). 값이 없다고 그리기를 실패하면 리포트 전체가
    안 나가므로, 여기서는 조용히 덜 보여주는 쪽이 맞다.
    """
    svg = render_divergence_gauge(jsd_score=0.42, comparison_pair=PAIR, width=WIDTH)

    assert _SCORE_LINE.search(svg) is not None
    assert _BASELINE_LINE.search(svg) is None
