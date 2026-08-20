"""콘솔 출력 인코딩. **윈도우 기본 콘솔(cp949)에서 죽는 걸 막는다.**

우리 CLI·스크립트는 요약과 진단 문구에 `⚠️`(U+26A0) · `ℹ️`(U+2139) · `❌`(U+274C) ·
`—`(U+2014, em dash) 를 쓰는데 **cp949 에 전부 없다**(`←`(U+2190) 는 있어서 통과한다 —
그래서 한 줄만 골라 터지는 것처럼 보인다).

사소하지 않은 이유는 크래시가 **할 일을 다 끝낸 뒤 마지막 print 에서** 난다는 것이다.
배치는 실패 보고(`sys.exit(EXIT_RUNTIME_ERROR)`)에 도달하지 못하고 셋업 스크립트는 큐를
다 만들어 놓고 정상 완료를 exit 1 로 보고한다 — 종료코드가 "성공"과 "아무것도 못 함"을
구분하지 못하게 되고, 보는 사람은 traceback 을 믿고 **이미 끝난 일을 되돌리러 간다.**
진입점마다 사설 사본을 두면 같은 사고가 반복되므로 한 곳에 둔다.
"""

from __future__ import annotations

import contextlib
import sys


def force_utf8_output() -> None:
    """표준 출력·에러를 UTF-8 로 돌린다. **출력이 나가기 전에 부를 것.**

    `errors="replace"` 는 최후 보루다 — 콘솔이 어떤 코드페이지든 메시지는 나와야 한다.
    `reconfigure` 가 없는 스트림(pytest 캡처 등)에서는 조용히 건너뛴다. 여기서 예외가
    새면 진입점이 **아무 일도 하기 전에** 죽는데, 인코딩 편의 때문에 그러는 건 원래
    문제보다 나쁘다.

    로깅 설정과의 순서는 무관하다 — `reconfigure` 는 스트림 객체를 교체하지 않고 제자리에서
    바꾸므로, `logging.basicConfig()` 가 먼저 핸들러를 만들어도 같은 객체를 들고 있다.
    """
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError, ValueError):
            stream.reconfigure(encoding="utf-8", errors="replace")
