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


def test_main_switches_encoding_before_running(monkeypatch):
    """⚠️ `main()` 이 실제로 `force_utf8_output()` 을 부른다 — 배선까지 고정한다.

    ⚠️ **다른 진입점과 실패 모양이 다르다.** 스크립트는 인코딩이 어긋나면 크래시로
       드러나는데(`setup_local_mq` 가 큐를 다 만들고 종료 메시지에서 죽어 exit 1 이
       났던 그것), 이 프로세스가 내는 출력은 거의 전부 `logger` 라 logging 이 예외를
       삼킨다. 프로세스는 멀쩡히 돌고 **그 줄만 조용히 사라진다.**

       그래서 이 호출이 빠져도 종료코드도 테스트도 아무것도 안 변한다 — 이 테스트가
       유일한 방어선이다. 사라지는 줄이 하필 계약 어긋남 경고들이라(`—` 를 쓴다),
       백엔드와 처음 붙여보는 그 주에 정작 안 보인다.
    """
    from app.core import mq_consumer

    calls: list[str] = []

    async def interrupted():
        calls.append("consume")
        raise KeyboardInterrupt

    # 이 테스트가 전역 HANDLERS 를 더럽히지 않게 — main() 이 wire_handlers() 를 탄다.
    monkeypatch.setattr(mq_consumer, "HANDLERS", {})
    monkeypatch.setattr(consumer, "force_utf8_output", lambda: calls.append("utf8"))
    monkeypatch.setattr(consumer, "consume", interrupted)

    consumer.main()

    assert "utf8" in calls, "main() 이 force_utf8_output() 을 부르지 않았습니다"
    # 출력이 나가기 전에 불려야 한다.
    assert calls.index("utf8") < calls.index("consume")


def test_wiring_log_reports_what_was_actually_registered(monkeypatch, caplog):
    """⚠️ 배선 로그가 **실제로 등록된 곳**을 읽는지 — 사본을 읽으면 거짓말을 한다.

    `from app.core.mq_consumer import HANDLERS` 로 가져오면 import 시점의 dict 객체가
    `app.consumer` 네임스페이스에 값으로 묶인다. 운영에서는 같은 객체라 안 드러나지만
    (`consumer.HANDLERS is mq_consumer.HANDLERS`), 테스트가 `HANDLERS` 를 갈아끼우면
    **등록은 새 dict 에 되고 로그는 옛 dict 를 읽어 "비었다"고 말한다.**

    이 로그가 배선을 눈으로 확인하는 유일한 수단이라, 거짓이면 운영에서 "핸들러가 안
    꽂혔나" 를 엉뚱한 데서 찾게 된다. (용준님 PR #86 리뷰, 2026-08-14)
    """
    from app.core import mq_consumer

    caplog.set_level(logging.INFO)
    monkeypatch.setattr(mq_consumer, "HANDLERS", {})

    consumer.wire_handlers()

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert mq_consumer.REPORT_CREATED in logged
    assert mq_consumer.RECOMMENDATION_REVIEWED in logged


def test_wiring_registers_the_hitl_handler(monkeypatch):
    """⚠️ 배선이 실제로 HITL 핸들러를 꽂는지 확인한다.

    다른 테스트들은 전부 자기가 등록하고 자기가 쓰기 때문에, 운영 배선이 비어 있어도
    통과한다. 실제로 그 구멍으로 한 번 깨졌다 — 스모크 스크립트가 dispatch() 를 직접
    부르는데 HANDLERS 가 비어 KeyError 가 났다(2026-08-07).
    """
    from app.core import mq_consumer

    monkeypatch.setattr(mq_consumer, "HANDLERS", {})

    consumer.wire_handlers()

    assert mq_consumer.RECOMMENDATION_REVIEWED in mq_consumer.HANDLERS
