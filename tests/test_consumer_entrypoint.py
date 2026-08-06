"""담당: 지인 — 컨슈머 실행 진입점(`app/consumer.py`).

브로커 없이 돈다 — `consume()` 을 몽키패치해서 종료 경로와 종료 코드만 본다.
"""

import asyncio
import logging

import pytest

from app import consumer
from app.core.exceptions import MqDisabledError


def test_config_error_exits_nonzero(monkeypatch, caplog):
    """⚠️ 설정 문제로 못 뜨면 0 이 아닌 코드로 끝난다.

    0 으로 끝내면 k8s 가 "할 일 끝나고 정상 종료"로 보고 조용히 넘어간다 — 컨슈머가
    영영 안 떠 있는데 아무도 못 알아챈다. HITL 피드백이 통째로 안 들어오는 상태다.
    """

    async def disabled():
        raise MqDisabledError("MQ_ENABLED=false 라 컨슈머를 띄우지 않습니다")

    monkeypatch.setattr(consumer, "consume", disabled)

    with pytest.raises(SystemExit) as exc:
        consumer.main()

    assert exc.value.code == consumer.EXIT_CONFIG_ERROR
    assert any("띄우지 못했습니다" in r.getMessage() for r in caplog.records)


def test_interrupt_is_a_clean_shutdown(monkeypatch, caplog):
    """Ctrl+C 는 실패가 아니다 — 0 으로 끝난다.

    여기서 예외가 새면 로컬에서 끌 때마다 스택트레이스가 뜨고, 컨테이너에서는
    정상 종료가 실패로 기록된다.
    """

    caplog.set_level(logging.INFO)

    async def interrupted():
        raise KeyboardInterrupt

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

    monkeypatch.setattr(consumer, "consume", broker_down)

    with pytest.raises(SystemExit) as exc:
        consumer.main()

    assert exc.value.code == consumer.EXIT_RUNTIME_ERROR
    assert any("예기치 않게" in r.getMessage() for r in caplog.records)
