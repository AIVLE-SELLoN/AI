"""담당: 지인 — 웹 진입점(`app/main.py`)의 설정 오류 처리.

`app/consumer.py` 와 같은 계약을 웹 쪽에도 건다(PR #91 후속). 다만 **모양이 다르다** —
컨슈머는 `main()` 이 있어서 우리가 종료코드의 주인이지만, 여기는 uvicorn 이 import 하는
모듈이라 `sys.exit()` 를 **import 시점**에 부른다. 그래서 "그 값이 정말 프로세스
종료코드로 나가는가" 가 이 파일에서 새로 확인해야 하는 것이고,
`test_the_exit_code_survives_the_real_launcher` 가 그걸 잡는다.
"""

from __future__ import annotations

import ast
import logging
import os
import re
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from app.core import exit_codes, logging_setup

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    """빈 포트를 OS 에게 받는다.

    ⚠️ 고정 포트를 박으면 두 방향으로 샌다 — 그 포트가 이미 쓰이면 **무관한 이유로**
       실패하고, 회귀로 서버가 정말 뜨면 그 포트를 점유한다(용준님 PR #96 리뷰 잔가지).
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _launch(argv: list[str]) -> subprocess.CompletedProcess[str]:
    """설정 오타(`LOG_LEVEL=info`)를 준 채로 진짜 프로세스를 띄운다.

    ⚠️ `encoding="utf-8"` 을 명시한다 — 자식이 한글을 내는데 부모가 자기 locale 로
       디코드하면 mojibake 나 `UnicodeDecodeError` 가 난다(PR #66 건).
    ⚠️ `env` 를 통째로 교체하지 않는다 — 교체하면 자식이 `PATH` 를 잃는다(같은 건).
    """
    return subprocess.run(
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
    "fake_get_settings, expect_in_message, why",
    [
        (
            _bad_log_level,
            "Unknown level",
            "logging 이 거부하는 레벨 — 진짜 basicConfig 가 던진다",
        ),
        (
            _unloadable_settings,
            # 🔴 **필드명이 나와야 한다.** pydantic 은 첫 줄이 `1 validation error for
            #    Settings` 라는 개수 헤더뿐이라, 그것만 남기면 운영자가 **어느 값이 틀렸는지
            #    모른다** — 이 PR 이 없애려던 "원인을 모르는 실패" 가 형태만 바뀌어 남는다.
            #    하필 `MQ_PORT`·`CHROMA_PORT` 처럼 제일 흔한 오타가 전부 이쪽이다.
            #    (용준님 PR #96 리뷰 ①, 재현 확인)
            "port",
            "설정 로딩 자체가 실패 — get_settings() 가 던진다",
        ),
    ],
)
def test_config_failures_exit_as_config_error(
    monkeypatch, capsys, fake_get_settings, expect_in_message, why
):
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
    monkeypatch.setattr(logging_setup, "get_settings", fake_get_settings)

    with pytest.raises(SystemExit) as exc:
        logging_setup.configure_logging_or_exit("서버")

    assert exc.value.code == exit_codes.EXIT_CONFIG_ERROR, why

    err = capsys.readouterr().err
    assert "설정을 읽지 못해" in err, why
    # 🔴 사유가 **실제로 식별 가능**해야 한다 — 위 파라미터 주석 참고.
    assert expect_in_message in err, f"{why}: 사유를 특정할 수 없는 문구입니다 — {err!r}"
    # raw traceback 이 아니라 **한 줄** 이어야 한다 — 로그 수집기에서 흩어지지 않게.
    assert len(err.strip().splitlines()) == 1, f"여러 줄로 나갔습니다: {err!r}"
    # ⚠️ cp949 콘솔에서도 **읽혀야** 한다.
    #    🔴 이건 "안 그러면 죽는다" 가 아니다 — CPython 이 stderr 의 error handler 를
    #    `backslashreplace` 로 못박아 둬서(`PYTHONIOENCODING=cp949:strict` 로도 그렇다)
    #    `—`·`⚠️` 를 찍어도 예외가 안 난다. 죽는 건 `strict` 인 stdout 쪽이다.
    #    그래서 이 단언이 지키는 것은 **가독성**이다 — `—` 처럼 이스케이프돼 나가면
    #    운영자가 사유를 못 읽는다. (용준님 PR #96 리뷰 ②, 실측으로 근거 정정)
    assert err.encode("cp949"), why


def test_the_message_never_echoes_the_offending_value(monkeypatch, capsys):
    """🔴 **어느 키가 틀렸는지는 알리되, 그 값은 절대 안 싣는다.**

    `Settings` 에 `llm_api_key`·`mq_password` 가 있다. 그 값이 검증에 걸리는 날 진단
    문구가 **비밀값을 로그로 내보낸다** — 로그는 수집기로 흘러가고 지우기 어렵다.

    같은 이유로 `str(exc)` 를 통째로 이어붙이면 안 된다(거기엔 `input_value='abc'` 가
    들어 있다). `loc` + `msg` 만으로 "어느 키가 왜 틀렸는지" 는 충분히 나온다.
    ⚠️ 리뷰에서 *"원값도 넣자"* 는 제안이 있었는데(용준님 PR #96 ①) 이 사유로 안 넣었다.
    """
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(logging_setup, "get_settings", _unloadable_settings)

    with pytest.raises(SystemExit):
        logging_setup.configure_logging_or_exit("서버")

    err = capsys.readouterr().err
    assert "port" in err, "어느 키가 틀렸는지는 나와야 합니다"
    assert "abc" not in err, f"입력값이 그대로 새고 있습니다: {err!r}"


def test_valid_config_applies_the_configured_level(monkeypatch):
    """반대편 — 정상 설정이면 종료하지 않고 레벨이 실제로 걸린다.

    ⚠️ 이게 없으면 `configure_logging_or_exit()` 이 **아무것도 안 하게** 바뀌어도(예: try
       블록을 통째로 비움) 위 테스트만으로는 안 걸린다. 위 두 갈래는 "던지는가" 만 본다.
    """
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(
        logging_setup, "get_settings", lambda: SimpleNamespace(log_level="WARNING")
    )

    logging_setup.configure_logging_or_exit("서버")  # SystemExit 이 나면 안 된다

    assert logging.root.level == logging.WARNING
    assert logging.root.handlers, "basicConfig 가 핸들러를 안 붙였습니다"


@pytest.mark.parametrize(
    "fake_get_settings, expectation",
    [
        (lambda: SimpleNamespace(log_level="ERROR"), "applies"),
        (_bad_log_level, "exits"),
    ],
)
def test_the_guard_survives_a_preexisting_root_handler(
    monkeypatch, capsys, fake_get_settings, expectation
):
    """🔴 root 에 핸들러가 이미 있어도 레벨이 걸리고, 틀린 레벨이면 여전히 exit 2 다.

    `logging.basicConfig` 는 핸들러가 하나라도 있으면 **`level` 처리까지 통째로
    건너뛴다**(CPython 이 그 인자를 `if len(root.handlers) == 0` 안에서만 읽는다).
    그러면 두 가지가 동시에 조용해진다 — `LOG_LEVEL` 이 무시되고, **잘못된 레벨에도
    예외가 안 나서 exit 2 가드가 죽는다.**

    `app/main.py` 는 라우터 4개를 이 호출보다 **먼저** import 하므로, 그중 하나가 언젠가
    import 시점에 핸들러를 붙이기 시작하면 바로 그 상태가 된다. 지금은 붙이는 게 없어
    무해하지만 **무해한 채로 잠가 둔다**(용준님 PR #96 리뷰).

    ⚠️ 해법이 `force=True` 가 **아닌** 이유: 그건 기존 핸들러를 지우며 `close()` 까지
       불러서 pytest 의 `caplog` 핸들러를 닫는다 — 로그는 나가는데 `caplog.records` 가
       비는 상태가 되어 기존 테스트 2개가 실제로 깨졌다(실측). 지금은 `setLevel` 한 줄로
       남의 핸들러를 파괴하지 않고 같은 것을 잠근다.
    """
    monkeypatch.setattr(logging.root, "handlers", [logging.NullHandler()])
    monkeypatch.setattr(logging_setup, "get_settings", fake_get_settings)

    if expectation == "applies":
        logging_setup.configure_logging_or_exit("서버")
        assert logging.root.level == logging.ERROR
    else:
        with pytest.raises(SystemExit) as exc:
            logging_setup.configure_logging_or_exit("서버")
        assert exc.value.code == exit_codes.EXIT_CONFIG_ERROR
        assert "설정을 읽지 못해" in capsys.readouterr().err


# ── 진입점이 자기 종료코드를 선언하지 않는지 ──────────────────────────────

CMD_MODULE = re.compile(r"uvicorn[\"',\s]+([\w.]+):")
"""Dockerfile `CMD` 가 uvicorn 으로 띄우는 모듈 — `app/main.py` 는 `__main__` 블록이
없어서 이것 말고는 진입점으로 잡을 방법이 없다."""

ENTRYPOINTS_STILL_ON_THEIR_OWN = {"app/batch/daily.py"}
"""🔴 **아직 `exit_codes.py` 계약을 안 쓰는 배포 진입점.** 여기 있다는 건 *"설정 오류가
exit 1 + raw traceback 으로 나간다"* 는 뜻이다 — `exit_codes.py` 가 없애려던 그 상태다.

`app/batch/daily.py` 는 로깅 레벨을 `logging.INFO` 로 하드코딩해서 `LOG_LEVEL` 오타에는
안 걸리지만, `get_settings()` 가 `run_batch` **안쪽**에서 불려서 `MQ_PORT=abc` 같은 값
오류는 그대로 샌다(실측). 실패 지점이 부팅이 아니라 실행 중이라
`configure_logging_or_exit()` 모양으로는 안 덮이고, 별건이 필요하다.

⚠️ **고쳤으면 이 집합에서 지울 것** — 남겨두면 이 테스트가 실패해서 알려준다(부분집합이
   아니라 **정확히 일치**를 본다). 목록이 조용히 자라지 않게 하려는 것이다.
"""


def _deployed_app_entrypoints() -> set[str]:
    """배포되는 `app/**` 진입점. 손으로 적지 않고 유도한다.

    `__main__` 블록이 있는 모듈 + Dockerfile `CMD` 가 uvicorn 으로 띄우는 모듈.
    (`tests/test_console_encoding.py` 가 인코딩에 대해 하는 것과 같은 방식이다.)
    """
    found = set()
    for path in (ROOT / "app").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if any(
            isinstance(n, ast.If)
            and isinstance(n.test, ast.Compare)
            and isinstance(n.test.left, ast.Name)
            and n.test.left.id == "__name__"
            for n in tree.body
        ):
            found.add(path.relative_to(ROOT).as_posix())

    for dotted in CMD_MODULE.findall((ROOT / "Dockerfile").read_text(encoding="utf-8")):
        candidate = ROOT / (dotted.replace(".", "/") + ".py")
        if candidate.exists():
            found.add(candidate.relative_to(ROOT).as_posix())
    return found


def test_entrypoints_do_not_declare_their_own_exit_codes():
    """🔴 진입점이 종료코드를 **자기 리터럴로** 선언하면 안 된다.

    상수를 `app/core/exit_codes.py` 로 모은 이유가 *"각자 두면 한쪽만 바뀌어도 조용히
    갈린다"* 인데, 그 위험은 값을 한쪽에서 고치는 게 아니라 **새 진입점이 자기 숫자를
    쓰기 시작하는 것**으로 온다(용준님 PR #96 리뷰 잔가지 — 정의가 한 곳뿐이면 값 비교
    단언은 동어반복이라 그걸 못 본다).

    유도한 집합을 쓰므로 **진입점이 하나 늘면 자동으로 여기 걸린다.**
    """
    offenders = set()
    for rel in _deployed_app_entrypoints():
        tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            # ① `sys.exit(1)` 처럼 숫자를 직접 넘기는 것
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "exit"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, int)
            ):
                offenders.add(rel)
            # ② `EXIT_CONFIG_ERROR = 2` 처럼 사설 상수를 다시 선언하는 것.
            #    🔴 **런타임 단언으로는 이걸 못 잡는다** — 값이 같으면 `==` 도 `is` 도
            #    통과한다(작은 int 는 CPython 이 캐싱해서 `2 is 2` 가 True 다). 그래서
            #    소스로 본다.
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id.startswith("EXIT_") for t in node.targets
            ):
                offenders.add(rel)

    assert offenders == ENTRYPOINTS_STILL_ON_THEIR_OWN, (
        "진입점이 종료코드를 리터럴로 씁니다(app/core/exit_codes.py 를 쓸 것). "
        f"발견={sorted(offenders)} / 문서화된 예외={sorted(ENTRYPOINTS_STILL_ON_THEIR_OWN)}"
    )


def test_the_contract_values_are_what_k8s_expects():
    """계약값 자체를 못박는다 — 이 PR 의 전제가 "2 = 설정 문제" 다.

    ⚠️ *"두 진입점의 값이 같은지"* 를 재는 단언은 **동어반복이라 넣지 않았다** — 정의가
       한 곳뿐이라 `==` 도 `is` 도 갈릴 수가 없다(용준님 PR #96 리뷰 잔가지). 진짜 위험인
       "새 진입점이 자기 숫자를 쓴다" 는 위 소스 가드가 본다.
    """
    assert exit_codes.EXIT_CONFIG_ERROR == 2
    assert exit_codes.EXIT_RUNTIME_ERROR == 1
    # 재시작해도 같은 실패와 나을 수 있는 실패는 **서로 달라야** 한다.
    assert exit_codes.EXIT_CONFIG_ERROR != exit_codes.EXIT_RUNTIME_ERROR


@pytest.mark.parametrize(
    "argv, why",
    [
        (
            ["-c", "import app.main"],
            "우리 코드만 — uvicorn 이 바뀌어도 이건 우리 책임이다",
        ),
        (
            ["-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "{port}"],
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

    ⚠️ 포트는 OS 에게 빈 것을 받는다. 고정 포트를 박으면 이미 쓰이는 경우 **무관한 이유로**
       실패하고, 회귀로 서버가 정말 뜨면 그 포트를 점유한다(용준님 리뷰 잔가지).
    """
    argv = [sys.executable, *(a.format(port=_free_port()) for a in argv)]
    try:
        proc = _launch(argv)
    except subprocess.TimeoutExpired:
        # 🔴 회귀(=설정 오류인데 서버가 그냥 뜸)는 타임아웃으로 나타난다. 그대로 두면
        #    `TimeoutExpired` 만 뜨고 **무엇이 틀렸는지 안 나온다**(용준님 리뷰 잔가지).
        pytest.fail(
            f"{why}: 설정 오타인데 프로세스가 끝나지 않았습니다 — "
            "부팅이 막히지 않고 서버가 떴다는 뜻입니다."
        )

    assert proc.returncode == exit_codes.EXIT_CONFIG_ERROR, (
        f"{why}: 설정 오타인데 exit {proc.returncode} 입니다 "
        f"(stderr 마지막 줄: {proc.stderr.strip().splitlines()[-1:]})"
    )
    assert "설정을 읽지 못해" in proc.stderr, why
    assert "Traceback" not in proc.stderr, why
