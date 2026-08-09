"""월간 리포트 배치(`scripts/generate_monthly_reports.py`) 진입점 테스트.

여기서 재는 것은 **배선** 하나다 — `main()` 이 출력보다 먼저 인코딩을 돌리는가.

이 스크립트의 로그·도움말·요약 print 에는 `—`(U+2014)가 들어 있는데 cp949 에 없다.
한국어 윈도우 콘솔에서 그 줄에 닿으면 `UnicodeEncodeError` 로 죽는다. 하필 그런 줄이
**실패 경로에 몰려 있어**(발행 실패 안내 등) 로컬에서 가장 안 밟힌다 — 정상 동작만
확인하면 영영 못 본다.
"""

from __future__ import annotations

import argparse

import pytest

from scripts import generate_monthly_reports as batch


def test_main_switches_encoding_before_parsing(monkeypatch):
    """⚠️ `main()` 이 실제로 `force_utf8_output()` 을 부른다 — 배선까지 고정한다.

    헬퍼만 테스트하면 호출 한 줄이 빠져도 아무것도 안 깨진다. 예전에는 이 호출이
    `if __name__ == "__main__"` 안에 있어서 **테스트로 고정할 방법 자체가 없었다**
    (2026-08-09 리뷰 지적). `app/batch/daily.py` 와 같은 구조로 맞췄다.
    """
    calls: list[str] = []

    monkeypatch.setattr(batch, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(
        batch, "run_aggregate", lambda _args: calls.append("aggregate") or 0
    )
    monkeypatch.setattr(batch.sys, "argv", ["batch", "--stage", "aggregate", "--month", "2026-07"])

    with pytest.raises(SystemExit) as exit_info:
        batch.main()

    assert exit_info.value.code == 0
    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    # 인자 파싱·실행보다 먼저여야 한다 — argparse 의 --help 출력도 이 문자를 쓴다.
    assert calls.index("utf8") < calls.index("aggregate")


def test_help_does_not_crash_on_cp949_console(monkeypatch, capsys):
    """`--help` 가 죽지 않는다.

    argparse 는 도움말을 stdout 으로 바로 쓴다. 인코딩 전환이 파싱보다 뒤에 있으면
    **`--help` 조차** UnicodeEncodeError 로 끝난다(실제로 그랬다).
    """
    monkeypatch.setattr(batch.sys, "argv", ["batch", "--help"])

    with pytest.raises(SystemExit) as exit_info:
        batch.main()

    assert exit_info.value.code == 0
    assert "월간 리포트 일괄 생성" in capsys.readouterr().out


def test_publish_failure_line_survives_cp949(monkeypatch):
    """발행 실패 안내에 쓰인 문자가 cp949 로도 나간다 — 실패 사유가 안 보이면 안 된다.

    ⚠️ 이 줄은 **실패 경로에만** 나오므로 정상 실행에서는 절대 안 밟힌다. 인코딩이
       안 돌아간 상태에서 여기 닿으면, 이미 실패한 배치가 "왜 실패했는지"조차 못 남긴다.
    """
    batch.force_utf8_output()

    # 요약 print 가 쓰는 문자열을 그대로 인코딩해 본다(cp949 는 U+2014 를 모른다).
    line = " 발행 ai.report.generated: **실패** — RuntimeError: 브로커 접속 실패"
    with pytest.raises(UnicodeEncodeError):
        line.encode("cp949")  # 전제 확인: 이 문자열은 cp949 로 표현 불가다

    # force_utf8_output() 뒤에는 stdout 이 utf-8 이라 그대로 나간다.
    assert batch.sys.stdout.encoding.lower().replace("-", "") == "utf8"


def test_aggregate_missing_db_returns_nonzero(tmp_path):
    """raw DB 가 없으면 비-0 — cron 이 알아채야 한다."""
    args = argparse.Namespace(
        db=str(tmp_path / "없는.db"), month="2026-07", products=None,
        permutations=None, deadline=None,
    )

    assert batch.run_aggregate(args) == 1
