"""담당: 지인 — 콘솔 출력 인코딩(`app/core/console.py`).

배선(각 진입점이 실제로 부르는지)은 그 진입점 테스트가 본다 —
`tests/test_batch_daily.py::test_main_switches_encoding_before_printing`.
헬퍼만 덮으면 호출 한 줄이 빠져도 안 죽는다.
"""

import sys

from app.core.console import force_utf8_output


def test_output_streams_are_switched_to_utf8(monkeypatch):
    """출력이 나가기 전에 stdout·stderr 을 UTF-8 로 돌린다.

    윈도우 기본 콘솔(cp949)에 `⚠️`(U+26A0)·`ℹ️`(U+2139)·`❌`(U+274C)·`—`(U+2014)가
    없어서 진입점들이 `UnicodeEncodeError` 로 터졌다. **할 일을 다 끝낸 뒤에** 죽는 게
    더 나쁘다 — 종료코드가 "성공"과 "아무것도 못 함"을 구분하지 못하게 된다.
    """

    class _Reconfigurable:
        def __init__(self) -> None:
            self.kwargs: dict = {}

        def reconfigure(self, **kwargs) -> None:
            self.kwargs = kwargs

    out, err = _Reconfigurable(), _Reconfigurable()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)

    force_utf8_output()

    # stderr 도 같이 — 로그 레코드와 traceback 이 그쪽으로 나간다.
    for stream in (out, err):
        assert stream.kwargs["encoding"] == "utf-8"
        assert stream.kwargs["errors"] == "replace"


def test_utf8_switch_is_safe_on_streams_that_cannot_reconfigure(monkeypatch):
    """pytest 캡처 스트림처럼 reconfigure 가 없는 곳에서도 터지지 않는다.

    여기서 예외가 새면 진입점이 **아무 일도 하기 전에** 죽는다 — 인코딩 편의 때문에
    배치를 못 띄우는 건 원래 문제보다 나쁘다.
    """
    monkeypatch.setattr(sys, "stdout", object())
    monkeypatch.setattr(sys, "stderr", object())

    force_utf8_output()  # 예외가 나면 이 줄에서 실패한다
