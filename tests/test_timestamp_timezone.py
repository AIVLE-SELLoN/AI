"""타임스탬프가 **호스트 시간대에 의존하지 않는지** 고정한다.

왜 소스를 훑는 테스트인가
------------------------
같은 사고가 **네 번** 났다 — `daily._to_kst`(PR #53) · 탐지 시각 기본값(PR #68) ·
`mock_producer` KST 정의(PR #70) · 리포팅/워커 타임스탬프(2026-08-13). 매번 다른 파일이고
매번 "개발 머신이 KST 라 로컬 테스트로는 영원히 안 잡힌다"가 원인이었다.

값을 하나씩 단언하는 테스트로는 **다음 파일**을 못 막는다. 새로 쓰는 코드가 같은 관용구를
쓰면 그만이기 때문이다. 그래서 관용구 자체를 금지한다.

무엇이 문제인가
--------------
`datetime.now()` · `date.today()` 는 naive 이고, `.astimezone()` 을 **인자 없이** 부르면
**실행 호스트의 로컬 시간대**로 변환한다. 셋 다 KST 노트북에서는 맞는 값을 내고 UTC
컨테이너에서는 9시간 어긋난다 — 확정 문서 §3 이 날짜 경계를 Asia/Seoul 로 못박았으므로
이건 환경에 따라 계약을 어기는 코드다.

⚠️ **UTC 를 명시한 것은 대상이 아니다.** `datetime.now(timezone.utc)` 는 호스트에 의존하지
   않고, 두 곳은 그렇게 두는 것이 맞다:
     - `app/core/mq.py` `_now_iso()` — Envelope `occurredAt` 은 계약이 UTC·`Z` 접미다(§3)
     - `app/batch/daily.py` `started` — 벽시계가 아니라 **차이**만 쓰는 경과시간 기준점
   이 둘은 표현이 UTC 일 뿐 같은 순간이고, KST 로 바꾸면 계약이 깨지거나(전자) 아무것도
   안 달라진다(후자).
"""

from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

SCANNED_DIRS = ("app", "scripts")

# 호스트 로컬에 의존하는 관용구 셋.
#   - `datetime.now()`  / `date.today()`      : naive = 호스트 로컬
#   - `.astimezone()`   (인자 없음)           : 호스트 로컬로 변환
# `datetime.now(KST)` · `datetime.now(timezone.utc)` · `.astimezone(KST)` 는 안 걸린다.
FORBIDDEN = re.compile(
    r"datetime\.now\(\s*\)|date\.today\(\s*\)|\.astimezone\(\s*\)"
)


def _offending_lines(path: Path) -> list[tuple[int, str]]:
    """그 파일에서 금지 관용구가 **코드로** 쓰인 줄. 주석·문자열은 뺀다.

    ⚠️ 주석을 빼는 이유: 이 관용구들이 **왜 위험한지**를 설명하는 주석이 여러 파일에 있고
       (그게 있어야 다음 사람이 안 되돌린다), 그것까지 잡으면 설명을 지워야 통과하게 된다.

    ⚠️ **`tokenize` 를 쓴다 — 줄 단위로 직접 세지 않는다.** 처음엔 `\"\"\"` 로 docstring
       구간을 손으로 따라갔는데 `daily.py` 의 여러 줄 docstring 안 주석을 오탐했다.
       파서를 흉내 내면 이런 구멍이 계속 생긴다.
    """
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    # 주석·문자열 토큰 자리를 공백으로 지운 사본. 남은 것이 곧 코드다.
    blanked = list(lines)

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, SyntaxError):  # pragma: no cover - 파싱 불가 파일은 건너뛴다
        return []

    for tok in tokens:
        if tok.type not in (tokenize.COMMENT, tokenize.STRING):
            continue
        (r1, c1), (r2, c2) = tok.start, tok.end
        for row in range(r1, r2 + 1):
            i = row - 1
            if i >= len(blanked):
                continue
            start = c1 if row == r1 else 0
            end = c2 if row == r2 else len(blanked[i])
            blanked[i] = blanked[i][:start] + " " * (end - start) + blanked[i][end:]

    return [
        (no, lines[no - 1].strip())
        for no, code in enumerate(blanked, start=1)
        if FORBIDDEN.search(code)
    ]


@pytest.mark.parametrize("folder", SCANNED_DIRS)
def test_no_host_local_timestamps(folder: str) -> None:
    """🔴 호스트 로컬 시각을 쓰는 코드가 없어야 한다.

    걸렸다면 `app/core/constants.py` 의 `KST` 를 명시할 것:

        datetime.now()              ->  datetime.now(KST)
        date.today()                ->  datetime.now(KST).date()
        <aware>.astimezone()        ->  <aware>.astimezone(KST)

    경과시간 기준점처럼 **차이만 쓰는** 값이면 `datetime.now(timezone.utc)` 도 된다 —
    호스트에 의존하지 않는다는 것이 요건이다.
    """
    hits: list[str] = []
    for path in sorted((ROOT / folder).rglob("*.py")):
        for no, line in _offending_lines(path):
            hits.append(f"{path.relative_to(ROOT).as_posix()}:{no}: {line}")

    assert not hits, "호스트 로컬 시각 사용:\n  " + "\n  ".join(hits)


def test_the_guard_actually_matches_the_forbidden_forms() -> None:
    """가드가 실제로 무엇을 잡고 무엇을 통과시키는지 고정한다.

    이게 없으면 정규식이 조용히 아무것도 안 잡게 바뀌어도 위 테스트가 통과한다 —
    "테스트가 있는데 안 잡는" 상태가 제일 나쁘다.
    """
    for bad in (
        "x = datetime.now()",
        "x = datetime.now( )",
        "x = date.today()",
        "x = value.astimezone()",
        "x = datetime.now(timezone.utc).astimezone().isoformat()",
    ):
        assert FORBIDDEN.search(bad), bad

    for good in (
        "x = datetime.now(KST)",
        "x = datetime.now(timezone.utc)",
        "x = value.astimezone(KST)",
        "x = datetime.now(KST).date()",
    ):
        assert not FORBIDDEN.search(good), good
