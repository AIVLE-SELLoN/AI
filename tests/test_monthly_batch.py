"""월간 리포트 배치(`scripts/generate_monthly_reports.py`) 진입점 테스트.

여기서 재는 것은 **배선** 하나다 — `main()` 이 출력보다 먼저 인코딩을 돌리는가.

이 스크립트의 로그·도움말·요약 print 에는 `—`(U+2014)가 들어 있는데 cp949 에 없다.
`main()`·`run_generate()` 를 **코드에서 직접 부르는 경로**(배치 러너·검증 하네스)에서
그 줄에 닿으면 `UnicodeEncodeError` 로 죽는다. 하필 그런 줄이 실패 경로에 몰려 있어
(발행 실패 안내 등) 정상 동작만 확인하면 영영 못 본다.

⚠️ **인코딩은 pytest 안에서 검증할 수 없다.** pytest 가 캡처한 stdout 에는
   `reconfigure` 가 없어서 `console.force_utf8_output()` 이 조용히 건너뛰고, 그 스트림은
   원래 utf-8 이라 무엇을 하든 안 죽는다. 실제로 호출을 통째로 지워도 in-process
   테스트는 전부 통과했다(2026-08-09 리뷰 지적). 그래서 **서브프로세스로 진짜 cp949
   콘솔을 만들어** 확인한다.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import generate_monthly_reports as batch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate_monthly_reports.py"


def _run_script(*args: str, io_encoding: str) -> subprocess.CompletedProcess:
    """자식 프로세스로 스크립트를 돌린다. stdout 인코딩을 지정한다.

    `PYTHONIOENCODING=cp949` 면 한국어 윈도우 기본 콘솔과 같은 상태다. 여기서는
    `reconfigure` 가 살아 있어서 `force_utf8_output()` 이 실제로 일을 하고, 안 하면
    진짜로 죽는다 — pytest 캡처 stdout 과 다른 점이 이것이다.
    """
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        cwd=str(ROOT),
        env={**os.environ, "PYTHONIOENCODING": io_encoding},
        timeout=120,
        check=False,  # 종료코드 자체가 검증 대상이라 여기서 던지면 안 된다
    )


def test_help_survives_cp949_console():
    """cp949 콘솔에서 `--help` 가 죽지 않는다.

    ⚠️ 이 테스트가 **세 가지 배치를 전부 구분한다** — 호출이 제자리(통과) / 아예 없음
       (실패) / `parse_args()` 뒤로 밀림(실패). in-process 테스트는 셋 다 통과시킨다.
       argparse 는 도움말을 stdout 으로 **바로** 쓰므로, 전환이 파싱보다 뒤면 그 시점에
       이미 늦다.
    """
    result = _run_script("--help", io_encoding="cp949")

    assert result.returncode == 0, (
        "cp949 콘솔에서 --help 가 죽었다. force_utf8_output() 이 "
        "parse_args() 보다 먼저 불리는지 확인할 것:\n"
        + result.stderr.decode("utf-8", "replace")[-800:]
    )


def test_help_output_actually_needs_the_switch():
    """위 cp949 테스트의 **전제** — `--help` 출력에 cp949 로 못 내보내는 문자가 실린다.

    ⚠️ 이 전제가 깨지면 위 테스트가 조용히 무력해진다. 실제로 `--help` 출력에 실리는
       cp949 밖 문자는 `--permutations` help 문구의 `—` **하나뿐**이라, 누가 그 문구만
       다듬으면 `force_utf8_output()` 을 통째로 지워도 위 테스트가 통과한다
       (2026-08-10 리뷰에서 실증). 나머지 20여 개는 docstring·주석이라 화면에 안 나온다.

    그래서 소스 파일 전체가 아니라 **출력**을 본다. 파일을 재면 docstring 의 `—` 때문에
    전제가 계속 참인 것처럼 보인다 — 정작 필요한 건 "`--help` 출력에 있나" 다.
    """
    result = _run_script("--help", io_encoding="utf-8")
    assert result.returncode == 0

    with pytest.raises(UnicodeEncodeError):
        result.stdout.decode("utf-8").encode("cp949")


def test_missing_required_arg_exits_with_argparse_code():
    """필수 인자가 없으면 argparse 종료코드 2 로 끝난다 — 스크립트가 뜨고 파싱까지 간다.

    ⚠️ **인코딩 배선과는 무관하다.** `force_utf8_output()` 을 통째로 지워도 통과한다
       (2026-08-10 리뷰에서 확인). argparse 의 usage 와
       `the following arguments are required: --month` 는 전부 ASCII 라 cp949 에서도
       멀쩡하고, 그 경로에서는 로깅도 아직 안 돈다.

    남겨 두는 이유는 **스모크**로서의 값이다 — import 가 깨지거나 파서 정의가 망가지면
    2 가 아닌 코드로 끝난다. 인코딩은 위 두 테스트가 맡는다.
    """
    result = _run_script(io_encoding="cp949")  # --month 누락

    assert result.returncode == 2, (
        "argparse 인자 오류(2)가 아닌 코드로 끝났다:\n"
        + result.stderr.decode("utf-8", "replace")[-800:]
    )


def test_main_switches_encoding_before_parsing(monkeypatch):
    """`main()` 이 **인자 파싱보다 먼저** `force_utf8_output()` 을 부른다.

    ⚠️ 예전 판은 `calls` 에 파싱 이벤트가 없어서 실제로는 `utf8 < aggregate` 만 쟀다.
       그래서 호출을 `parse_args()` **뒤로** 옮겨도 통과했다(2026-08-09 지적).
       argparse 를 감싸 파싱 시점을 기록해 순서를 진짜로 고정한다.
    """
    calls: list[str] = []
    real_parse = argparse.ArgumentParser.parse_args

    def spy_parse(self, *a, **kw):
        calls.append("parse")
        return real_parse(self, *a, **kw)

    monkeypatch.setattr(batch, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", spy_parse)
    monkeypatch.setattr(batch, "run_aggregate", lambda _args: calls.append("aggregate") or 0)
    monkeypatch.setattr(batch.sys, "argv", ["batch", "--stage", "aggregate", "--month", "2026-07"])

    with pytest.raises(SystemExit) as exit_info:
        batch.main()

    assert exit_info.value.code == 0
    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    assert calls.index("utf8") < calls.index("parse"), (
        f"인코딩 전환이 인자 파싱보다 뒤에 있습니다: {calls}"
    )


def test_aggregate_missing_db_returns_nonzero(tmp_path):
    """raw DB 가 없으면 비-0 — cron 이 알아채야 한다."""
    args = argparse.Namespace(
        db=str(tmp_path / "없는.db"), month="2026-07", products=None,
        permutations=None, deadline=None,
    )

    assert batch.run_aggregate(args) == 1
