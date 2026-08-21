"""담당: 지인 — 컨슈머 실행 진입점(`app/consumer.py`).

브로커 없이 돈다 — `consume()` 을 몽키패치해서 종료 경로와 종료 코드만 본다.
예외로 `test_real_bad_log_level_exits_two` 만 실제 프로세스를 띄운다(사유는 그 docstring).
"""

import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app import consumer
from app.core import logging_setup
from app.core.exceptions import MqDisabledError
from tests.conftest import bad_log_level_settings, pin_settings, unloadable_settings


def test_config_error_exits_nonzero(monkeypatch, caplog):
    """설정 문제로 못 뜨면 0 이 아닌 코드로 끝난다.

    0 으로 끝내면 k8s 가 "할 일 끝나고 정상 종료"로 보고 조용히 넘어간다 — 컨슈머가
    영영 안 떠 있는데 아무도 못 알아챈다. HITL 피드백이 통째로 안 들어오는 상태다.
    """

    async def disabled():
        raise MqDisabledError("MQ_ENABLED=false 라 컨슈머를 띄우지 않습니다")

    pin_settings(monkeypatch)
    monkeypatch.setattr(consumer, "consume", disabled)

    with pytest.raises(SystemExit) as exc:
        consumer.main()

    assert exc.value.code == consumer.EXIT_CONFIG_ERROR
    assert any("띄우지 못했습니다" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "fake_get_settings, why",
    [
        (bad_log_level_settings, "logging 이 거부하는 레벨 — 진짜 basicConfig 가 던진다"),
        (unloadable_settings, "설정 로딩 자체가 실패 — get_settings() 가 던진다"),
    ],
)
def test_config_failures_exit_as_config_error(monkeypatch, capsys, fake_get_settings, why):
    """설정 오류는 exit 2(설정 문제)여야 한다 — exit 1(일시적 오류)이면 안 된다.

    예전엔 `get_settings()`·`basicConfig()` 가 `try` **밖**이라 미포착 예외로 나가
    종료코드가 **1** 이 됐다. 1 은 이 파일 맨 위에서 *"재시작하면 나을 수 있다"* 로
    정의한 값이라, 같은 `.env` 를 다시 읽어 영원히 실패하는데도 계속 재시작만 한다(재현 확인).

    **`get_settings` 를 갈아끼운다 — `basicConfig` 가 아니다.** 초안은 `basicConfig` 를
       가짜로 바꿔서 **우리 처리만** 쟀는데, 그러면 *"소문자 레벨이 정말 `ValueError` 를
       내는가"* 라는 전제는 아무도 안 잰다. 이렇게 두면 **진짜 `logging.basicConfig` 가
       진짜 예외를 던지는 경로**를 인프로세스에서 탄다.

    **몽키패치 대상이 `consumer` 가 아니라 `logging_setup` 이다**. 처리 본체가
       `app/core/logging_setup.py` 로 옮겨가서, 웹 진입점과 **한 블록을 공유**한다.
       `consumer` 에 걸면 조용히 아무 효과가 없다 — 여기서 이름을 되돌리지 말 것.

    **두 갈래를 다 돈다.** 분기가 덮는 실패는 둘인데(로깅 설정 / 설정 로딩) 초안은
       로깅 쪽만 쟀다. 그러면 누가 `settings = get_settings()` 를 `try` 위로 올리는
       "정리" 를 해도 스위트가 초록이고, `MQ_PORT` 오타가 다시 exit 1 이 된다.
    """
    # root 핸들러를 비우고 시작한다. `logging.basicConfig()` 는 **핸들러가 이미 있으면
    #    아무것도 안 하고 반환**하는데, pytest 의 logging 플러그인이 붙여 둔 것이 있어서
    #    비우지 않으면 예외가 아예 안 난다. 그러면 흐름이 그대로 흘러가
    #    `MqDisabledError` → 같은 exit 2 가 나와 **검증 대상을 안 타고도 초록**이 된다.
    #    (실측 — 아래 `설정을 읽지 못해` 단언이 실제로 이걸 잡아냈다.)
    monkeypatch.setattr(logging.root, "handlers", [])
    monkeypatch.setattr(logging_setup, "get_settings", fake_get_settings)

    with pytest.raises(SystemExit) as exc:
        consumer.main()

    assert exc.value.code == consumer.EXIT_CONFIG_ERROR, why
    assert "설정을 읽지 못해" in capsys.readouterr().err, why


def test_real_bad_log_level_exits_two():
    """실제 프로세스로 확인한다 — 종료코드가 **OS 까지** 2로 나가는지.

    위 테스트는 `SystemExit` 까지만 본다. 실제로 그 값이 프로세스 종료코드가 되는지는
    띄워 봐야 안다.

    **`설정을 읽지 못해` 를 반드시 본다.** 종료코드만 보면 이 테스트는 **다른 이유로도
       초록**이 된다 — `MQ_ENABLED=false` 만으로 `MqDisabledError` → 같은 exit 2 가
       나오기 때문이다:

           LOG_LEVEL=info (오타)  exit=2  Traceback=0  '설정을 읽지 못해' 있음  ← 이 분기
           LOG_LEVEL=INFO (정상)  exit=2  Traceback=0  '설정을 읽지 못해' 없음  ← 딴 분기

       즉 파이썬이 소문자 레벨을 받아들이게 바뀌거나 누가 `log_level` 정규화를 넣으면
       **테스트는 초록인 채 검증 대상이 사라진다.** 이 한 줄이 그걸 가른다.
       (한때 *"수정 전 코드에서도 통과한다"* 로 알려졌는데 재현되지 않는다 —
       수정 전에서는 두 단언이 다 실패한다. 다만 **가드 자체는 필요하다.**)

    `encoding="utf-8"` 을 명시한다 — 자식이 한글을 내는데 부모가 cp949 로 디코드하면
       mojibake 나 `UnicodeDecodeError` 가 난다. 부모가 UTF-8 모드일 때
       디코드 스레드가 죽어 `proc.stderr` 가 **`None`** 이 되는 형태로 나타났다.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "app.consumer"],
        cwd=Path(__file__).resolve().parents[1],
        env={
            **os.environ,
            "MQ_ENABLED": "false",
            "LOG_LEVEL": "info",  # 소문자 — logging 이 거부한다
            "PYTHONIOENCODING": "utf-8",
        },
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,  # 0 이 아닌 종료코드가 **기대값**이다 — check=True 면 안 된다
    )

    assert proc.returncode == consumer.EXIT_CONFIG_ERROR, (
        f"설정 오타인데 exit {proc.returncode} 입니다 "
        f"(stderr 마지막 줄: {proc.stderr.strip().splitlines()[-1:]})"
    )
    # 어느 분기가 처리했는지까지 고정한다(위 docstring 참고).
    assert "설정을 읽지 못해" in proc.stderr
    # raw traceback 이 아니라 한 줄 메시지로 나가야 한다.
    assert "Traceback" not in proc.stderr


def test_interrupt_is_a_clean_shutdown(monkeypatch, caplog):
    """Ctrl+C 는 실패가 아니다 — 0 으로 끝난다.

    여기서 예외가 새면 로컬에서 끌 때마다 스택트레이스가 뜨고, 컨테이너에서는
    정상 종료가 실패로 기록된다.
    """

    caplog.set_level(logging.INFO)

    async def interrupted():
        raise KeyboardInterrupt

    pin_settings(monkeypatch)
    monkeypatch.setattr(consumer, "consume", interrupted)

    consumer.main()  # SystemExit 이 나면 안 된다

    assert any("종료 신호" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_stops_without_raising(monkeypatch, caplog):
    """SIGTERM(=취소)으로 멈춰도 예외로 터지지 않고 조용히 끝난다.

    컨테이너가 보내는 종료 신호는 정상 종료다. 여기서 CancelledError 가 밖으로 나가면
    종료 로그에 스택트레이스가 찍혀 진짜 장애와 구분이 안 된다.
    """
    caplog.set_level(logging.INFO)
    started = asyncio.Event()

    async def forever():
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(consumer, "consume", forever)

    run = asyncio.create_task(consumer._run())
    await started.wait()
    run.cancel()

    await run  # 예외 없이 끝난다

    assert any("종료 신호" in r.getMessage() for r in caplog.records)


def test_unexpected_error_is_logged_as_one_record(monkeypatch, caplog):
    """접속 실패 같은 예상 밖 오류도 한 레코드로 남기고 1 로 끝난다.

    잡지 않으면 stderr 에 raw traceback 이 찍혀 로그 수집기에서 여러 줄로 흩어지고,
    상시 프로세스라 그 상태로 죽으면 무슨 일이 있었는지 추적이 어렵다.
    """

    async def broker_down():
        raise ConnectionResetError("broker down")

    pin_settings(monkeypatch)
    monkeypatch.setattr(consumer, "consume", broker_down)

    with pytest.raises(SystemExit) as exc:
        consumer.main()

    assert exc.value.code == consumer.EXIT_RUNTIME_ERROR
    assert any("예기치 않게" in r.getMessage() for r in caplog.records)


def test_main_switches_encoding_before_running(monkeypatch):
    """`main()` 이 실제로 `force_utf8_output()` 을 부른다 — 배선까지 고정한다.

    **빠지면 조용히 사라지는 게 아니라 시끄럽게 잃는다.** cp949 콘솔에서 `—` 가 든
       경고는 `emit` 이 실패해 **안 나가고**, 대신 `--- Logging error ---` 블록이
       건당 10줄쯤 stderr 에 쌓인다(실측 613자/10줄). 진단 로그를 잃으면서
       소음은 는다.

       그리고 **한 경로는 호출부로 탈출한다.** `handleError()` 가 traceback 을 같은
       스트림에 쓰는데 거기 실린 소스 라인에 `—` 가 있으면 또 터지고, handleError 는
       `OSError` 만 삼킨다. `UnicodeEncodeError` 는 `ValueError` 하위라 `consume()` 이
       계약 위반으로 분류해 `nack(requeue=False)` → **DLX**. 메시지가 유실된다.

       탈출에는 **두 조건이 다 맞아야** 한다. 하나만 바꿔도 재현이 안 된다:
       ① 실패한 호출의 **소스 라인**에 `—` 리터럴이 있을 것 — 여러 줄로 쪼갠 호출은
          traceback 에 첫 줄(`logger.warning(`)만 실려서 traceback 자체는 인코딩된다
       ② `sys.stderr` 가 그 실패하는 스트림일 것 — `basicConfig` 가 그렇게 묶는다
          (`handlers[0].stream is sys.stderr` 확인함)

       **당시 근거로 든 `handle_report_feedback` 예시는 이제 재현되지 않는다.**
          한때는 naive `submittedAt` 경로(단일행 warning)가 실제로 탈출했는데,
          **그 모듈의 로그 메시지에서 `—` 를 걷어내면서** 조건 ①이
          사라졌다(세 경로 모두 탈출 없음·메시지 정상 기록으로 재확인).

          **그렇다고 이 호출이 필요 없어진 게 아니다.** 그건 그 파일 하나를 고친
          것이고 조건 ①은 **누가 로그 문구에 `—`·`⚠️` 를 하나 넣는 순간 되돌아온다.**
          메시지 위생은 파일마다 사람이 지켜야 하지만 진입점 전환은 한 줄로 전부를
          덮는다 — 그래서 두 겹으로 둔다(`tests/test_console_encoding.py` 가 진입점
          쪽을 기계적으로 강제한다).

    이 테스트가 유일한 방어선이다. 호출이 빠져도 **리눅스에서는 아무것도 안 변해서**
       (운영이 리눅스라 프로덕션은 무사하다) 종료코드도 다른 테스트도 안 걸린다.
    """
    from app.core import mq_consumer

    calls: list[str] = []

    async def interrupted():
        calls.append("consume")
        raise KeyboardInterrupt

    # 이 테스트가 전역 HANDLERS 를 더럽히지 않게 — main() 이 wire_handlers() 를 탄다.
    monkeypatch.setattr(mq_consumer, "HANDLERS", {})
    monkeypatch.setattr(consumer, "force_utf8_output", lambda: calls.append("utf8"))
    pin_settings(monkeypatch)
    monkeypatch.setattr(consumer, "consume", interrupted)

    consumer.main()

    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    # 출력이 나가기 전에 불려야 한다.
    assert calls.index("utf8") < calls.index("consume")


def test_wiring_log_reports_what_was_actually_registered(monkeypatch, caplog):
    """배선 로그가 **실제로 등록된 곳**을 읽는지 — 사본을 읽으면 거짓말을 한다.

    `from app.core.mq_consumer import HANDLERS` 로 가져오면 import 시점의 dict 객체가
    `app.consumer` 네임스페이스에 값으로 묶인다. 운영에서는 같은 객체라 안 드러나지만
    (`consumer.HANDLERS is mq_consumer.HANDLERS`), 테스트가 `HANDLERS` 를 갈아끼우면
    **등록은 새 dict 에 되고 로그는 옛 dict 를 읽어 "비었다"고 말한다.**

    이 로그가 배선을 눈으로 확인하는 유일한 수단이라, 거짓이면 운영에서 "핸들러가 안
    꽂혔나" 를 엉뚱한 데서 찾게 된다.
    """
    from app.core import mq_consumer

    caplog.set_level(logging.INFO)
    monkeypatch.setattr(mq_consumer, "HANDLERS", {})

    consumer.wire_handlers()

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert mq_consumer.REPORT_CREATED in logged
    assert mq_consumer.RECOMMENDATION_REVIEWED in logged


def test_wiring_registers_the_hitl_handler(monkeypatch):
    """배선이 실제로 HITL 핸들러를 꽂는지 확인한다.

    다른 테스트들은 전부 자기가 등록하고 자기가 쓰기 때문에, 운영 배선이 비어 있어도
    통과한다. 실제로 그 구멍으로 한 번 깨졌다 — 스모크 스크립트가 dispatch() 를 직접
    부르는데 HANDLERS 가 비어 KeyError 가 났다.
    """
    from app.core import mq_consumer

    monkeypatch.setattr(mq_consumer, "HANDLERS", {})

    consumer.wire_handlers()

    assert mq_consumer.RECOMMENDATION_REVIEWED in mq_consumer.HANDLERS
