"""담당: 지인 — 웹 진입점(`app/main.py`)의 설정 오류 처리.

`app/consumer.py` 와 같은 계약을 웹 쪽에도 건다(PR #91 후속). 다만 **모양이 다르다** —
컨슈머는 `main()` 이 있어서 우리가 종료코드의 주인이지만, 여기는 uvicorn 이 import 하는
모듈이라 `sys.exit()` 를 **import 시점**에 부른다. 그래서 "그 값이 정말 프로세스
종료코드로 나가는가" 가 이 파일에서 새로 확인해야 하는 것이고,
`test_the_exit_code_survives_the_real_launcher` 가 그걸 잡는다.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from app import consumer, main
from app.core import exit_codes

ROOT = Path(__file__).resolve().parents[1]


class _PortOnly(BaseModel):
    """`MQ_PORT=abc` 를 흉내내기 위한 최소 모델 — 진짜 `ValidationError` 를 얻는다."""

    port: int


def _bad_log_level():
    """설정은 읽히는데 레벨 값이 틀린 경우 — 진짜 `basicConfig` 가 던진다."""
    return SimpleNamespace(log_level="info")


def _unloadable_settings():
    """설정 로딩 자체가 실패하는 경우. `ValidationError` 는 `ValueError` 하위다."""
    return _PortOnly(port="abc")


@pytest.mark.parametrize(
    "fake_get_settings, why",
    [
        (_bad_log_level, "logging 이 거부하는 레벨 — 진짜 basicConfig 가 던진다"),
        (_unloadable_settings, "설정 로딩 자체가 실패 — get_settings() 가 던진다"),
    ],
)
def test_config_failures_exit_as_config_error(monkeypatch, capsys, fake_get_settings, why):
    """🔴 설정 오류는 exit 2(설정 문제)여야 한다 — exit 1(일시적 오류)이면 안 된다.

    예전엔 `get_settings()`·`basicConfig()` 가 모듈 최상단에 맨몸으로 있어서 미포착
    예외로 나가 종료코드가 **1** 이 됐다. 1 은 *"재시작하면 나을 수 있다"* 로 정의한
    값이라(`app/core/exit_codes.py`), 같은 환경변수를 다시 읽어 영원히 실패하는데도
    k8s 가 계속 재시작만 한다.

    ⚠️ **두 갈래를 다 돈다.** 이 분기가 덮는 실패는 둘인데(로깅 설정 / 설정 로딩) 한쪽만
       재면, 누가 `settings = get_settings()` 를 `try` 위로 올리는 "정리" 를 해도
       스위트가 초록이고 `MQ_PORT` 오타가 다시 exit 1 이 된다.

    ⚠️ **`get_settings` 를 갈아끼운다 — `basicConfig` 가 아니다.** `basicConfig` 를
       가짜로 바꾸면 *"소문자 레벨이 정말 `ValueError` 를 내는가"* 라는 전제를 아무도
       안 재게 된다. 이렇게 두면 **진짜 `logging.basicConfig` 가 진짜 예외를 던지는
       경로**를 인프로세스에서 탄다.
    """
    # 🔴 root 핸들러를 비우고 시작한다. `logging.basicConfig()` 는 **핸들러가 이미 있으면
    #    아무것도 안 하고 반환**하는데, pytest 의 logging 플러그인이 붙여 둔 것이 있어서
    #    비우지 않으면 예외가 아예 안 난다 — 검증 대상을 안 타고도 초록이 된다.
    #    (PR #91 에서 실제로 밟은 자리다.)
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(main, "get_settings", fake_get_settings)

    with pytest.raises(SystemExit) as exc:
        main._configure_logging()

    assert exc.value.code == exit_codes.EXIT_CONFIG_ERROR, why

    err = capsys.readouterr().err
    assert "설정을 읽지 못해" in err, why
    # raw traceback 이 아니라 **한 줄** 이어야 한다 — 로그 수집기에서 흩어지지 않게.
    assert len(err.strip().splitlines()) == 1, f"여러 줄로 나갔습니다: {err!r}"
    # 🔴 이 진단이 cp949 콘솔에서도 나가야 한다. `app/main.py` 는 `force_utf8_output()`
    #    을 일부러 안 부르므로(스트림 소유자가 uvicorn), 메시지에 `—`·`⚠️` 가 섞이면
    #    이 print 자체가 `UnicodeEncodeError` 로 터져 **exit 1 로 되돌아간다.**
    assert err.encode("cp949"), why


def test_valid_config_applies_the_configured_level(monkeypatch):
    """반대편 — 정상 설정이면 종료하지 않고 레벨이 실제로 걸린다.

    ⚠️ 이게 없으면 `_configure_logging()` 이 **아무것도 안 하게** 바뀌어도(예: try 블록을
       통째로 비움) 위 테스트만으로는 안 걸린다. 위 두 갈래는 "던지는가" 만 보기 때문이다.
    """
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(main, "get_settings", lambda: SimpleNamespace(log_level="WARNING"))

    main._configure_logging()  # SystemExit 이 나면 안 된다

    assert logging.root.level == logging.WARNING
    assert logging.root.handlers, "basicConfig 가 핸들러를 안 붙였습니다"


def test_both_entrypoints_share_one_exit_contract():
    """🔴 두 진입점의 종료코드가 갈리면 안 된다.

    같은 실패(설정 문제)를 웹은 2, 컨슈머는 3 으로 보고하기 시작하면 k8s 쪽 대응이
    진입점마다 달라지는데, **그 차이는 운영에서만 드러난다** — 로컬에서 종료코드를 보는
    사람은 없다. 값을 한쪽만 고치면 여기서 걸린다.
    """
    assert main.EXIT_CONFIG_ERROR == consumer.EXIT_CONFIG_ERROR == 2
    assert consumer.EXIT_RUNTIME_ERROR == 1
    # 재시작해도 같은 실패와 나을 수 있는 실패는 **서로 달라야** 한다.
    assert exit_codes.EXIT_CONFIG_ERROR != exit_codes.EXIT_RUNTIME_ERROR


@pytest.mark.parametrize(
    "argv, why",
    [
        (
            [sys.executable, "-c", "import app.main"],
            "우리 코드만 — uvicorn 이 바뀌어도 이건 우리 책임이다",
        ),
        (
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "18080",
            ],
            "Dockerfile CMD 와 같은 배포 형태 — 실제로 k8s 가 보는 종료코드",
        ),
    ],
)
def test_the_exit_code_survives_the_real_launcher(argv, why):
    """🔴 종료코드가 **OS 까지** 2 로 나가는지 — 진짜 프로세스로 확인한다.

    인프로세스 테스트는 `SystemExit` 까지만 본다. 여기는 `main()` 이 없어서
    **import 시점에** `sys.exit()` 를 부르는데, 그 값이 uvicorn 을 거쳐서도 프로세스
    종료코드로 남는지는 **띄워 봐야 안다**(컨슈머에는 없던 위험이다). 실측으로 남는 것을
    확인하고 이 테스트로 고정한다.

    ⚠️ **`--reload`·`--workers` 는 일부러 안 쓴다.** 그 형태는 부모가 감독 프로세스라
       자식이 죽어도 부모가 안 끝난다(실측: 무한 대기). uvicorn 쪽 동작이라 우리가
       어쩌지 못하고, Dockerfile `CMD` 에 두 플래그가 없어 운영 경로도 아니다.

    🔴 **`설정을 읽지 못해` 를 반드시 본다.** 종료코드만 보면 **다른 이유로도 초록**이
       된다 — 파이썬이 소문자 레벨을 받아들이게 바뀌거나 누가 `log_level` 정규화를 넣으면
       테스트는 통과하면서 검증 대상이 사라진다.

    ⚠️ `encoding="utf-8"` 을 명시한다 — 자식이 한글을 내는데 부모가 자기 locale 로
       디코드하면 mojibake 나 `UnicodeDecodeError` 가 난다(PR #66 건).
    ⚠️ `env` 를 통째로 교체하지 않는다 — 교체하면 자식이 `PATH` 를 잃는다(같은 건).
    """
    proc = subprocess.run(
        argv,
        cwd=ROOT,
        env={
            **os.environ,
            "LOG_LEVEL": "info",  # 소문자 — logging 이 거부한다
            "PYTHONIOENCODING": "utf-8",
        },
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,  # 0 이 아닌 종료코드가 **기대값**이다 — check=True 면 안 된다
    )

    assert proc.returncode == exit_codes.EXIT_CONFIG_ERROR, (
        f"{why}: 설정 오타인데 exit {proc.returncode} 입니다 "
        f"(stderr 마지막 줄: {proc.stderr.strip().splitlines()[-1:]})"
    )
    assert "설정을 읽지 못해" in proc.stderr, why
    assert "Traceback" not in proc.stderr, why
