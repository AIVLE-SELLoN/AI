"""담당: 지인 — 인바운드 컨슈머 실행 진입점. **상시 프로세스다.**

    python -m app.consumer

왜 FastAPI 안이 아니라 별도 프로세스인가
----------------------------------------
uvicorn worker 를 2개 이상 띄우면 **컨슈머도 2개가 되어 같은 메시지를 두 번 처리**한다.
`record_hitl_outcome()` 이 `recommendation_id` 로 upsert 라 결과는 같지만, 벡터DB 쓰기가
두 배로 나가고 워커 수를 늘릴 때마다 조용히 배가된다. 웹 요청 처리와 수명주기도 다르다
— 웹은 재배포가 잦고 컨슈머는 계속 붙어 있어야 한다. 배치(`app.batch.daily`)도 같은
이유로 별도 프로세스다.

종료
----
Ctrl+C(SIGINT) 또는 SIGTERM(k8s) 으로 멈춘다. **처리 중이던 메시지는 유실되지 않는다** —
Manual ACK 라 확인하지 않은 메시지는 연결이 끊기는 순간 브로커가 큐로 되돌린다.

종료 코드는 `app/core/exit_codes.py` 가 정본이다(배포 진입점이 모두 같은 계약을 쓴다).
여기서 0 은 "종료 신호를 받아 정상 종료" 를 뜻한다.

⚠️ **Git Bash 에서 `MQ_VHOST=/` 를 인라인으로 주면 안 된다.** MSYS 경로 변환이 `/` 를
   `C:/Program Files/Git/` 로 바꿔서 엉뚱한 vhost 로 접속한다. `.env` 에 적거나
   `MSYS_NO_PATHCONV=1` 을 앞에 붙일 것. (운영 vhost 는 `app` 이라 슬래시가 없어 무관하다.)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

from app.core import mq_consumer
from app.core.console import force_utf8_output
from app.core.exceptions import AiServiceError
from app.core.exit_codes import EXIT_CONFIG_ERROR, EXIT_RUNTIME_ERROR
from app.core.logging_setup import configure_logging_or_exit
from app.core.mq_consumer import (
    RECOMMENDATION_REVIEWED,
    REPORT_CREATED,
    consume,
    register_handler,
)

logger = logging.getLogger(__name__)


def wire_handlers() -> None:
    """처리 함수를 컨슈머에 꽂는다. **core 가 컴포넌트를 모르게 하는 배선 지점이다.**

    ⚠️ **이걸 안 부르면 `HANDLERS` 가 비어 있어 모든 이벤트가 DLX 로 간다.** 컨슈머를
    직접 띄우지 않고 `dispatch()` 만 쓰는 도구(스모크 스크립트 등)도 이 함수를 먼저
    불러야 한다 — 공개 함수인 이유가 그것이다.

    등록 안 된 이벤트는 ACK 하지 않고 DLX 로 보낸다 — 처리한 적 없는 걸 처리했다고
    하면 그 메시지가 사라진다. 인바운드 2종(§8)이 여기 다 꽂혀 있어야 한다.

    ⚠️ import 를 **함수 안에서** 한다. 모듈 최상단으로 올리면 컨슈머를 띄우지 않는
       테스트·도구까지 recommendation·reporting 을 끌고 들어온다.
    """
    from app.recommendation.service import handle_recommendation_reviewed
    from app.reporting.feedback_service import handle_report_feedback

    register_handler(RECOMMENDATION_REVIEWED, handle_recommendation_reviewed)
    register_handler(REPORT_CREATED, handle_report_feedback)
    # ⚠️ `mq_consumer.HANDLERS` 를 **모듈 경유로** 읽는다. `from … import HANDLERS` 로
    #    가져오면 import 시점의 객체가 이 네임스페이스에 값으로 묶여, 테스트가
    #    `monkeypatch.setattr(mq_consumer, "HANDLERS", {})` 로 갈아끼웠을 때
    #    등록은 새 dict 에 되고 이 로그는 옛 dict 를 읽어 "비었다"고 말한다.
    logger.info("등록된 처리 함수: %s", ", ".join(sorted(mq_consumer.HANDLERS)))


async def _run() -> None:
    """컨슈머를 띄우고 종료 신호를 기다린다."""
    wire_handlers()
    task = asyncio.create_task(consume())

    # SIGTERM 은 컨테이너 종료 신호다. 윈도우 이벤트 루프는 이 API 가 없어서 무시한다
    # (로컬 개발은 Ctrl+C 로 충분하고, 운영은 리눅스다).
    with contextlib.suppress(NotImplementedError, AttributeError):
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("종료 신호를 받았습니다 — 컨슈머를 닫습니다")


def main() -> None:
    # 출력이 나가기 전에. 사유는 app/core/console.py 참고.
    # ⚠️ 빠지면 조용히 사라지는 게 아니라 **시끄럽게 잃는다** — 진단 경고가 안 나가는
    #    대신 `--- Logging error ---` 블록이 건당 10줄 쌓인다. 게다가 단일행 warning
    #    (`_format_submitted_at`)은 **호출부로 탈출해 메시지까지 잃는다**:
    #    `UnicodeEncodeError` 가 `ValueError` 하위라 `consume()` 이 계약 위반으로 보고
    #    `nack(requeue=False)` → DLX. 조건·실측은 테스트 docstring 참고.
    force_utf8_output()

    # ⚠️ **설정 로딩·로깅 설정이 여기(try 안)에 있어야 한다.** 밖에 두면 `LOG_LEVEL`
    #    오타 하나가 미포착 예외로 나가 exit **1**(일시적 오류)이 된다 — 재시작해도 같은
    #    값을 읽으니 k8s 는 영원히 재시작만 하고 아무도 못 알아챈다.
    #    처리 본체와 사유는 `app/core/logging_setup.py` 에 있다 — 다른 진입점과 **같은
    #    블록을 복사**하고 있었고, 그래서 진단 문구 결함을 두 번 고쳐야 했다(PR #96 리뷰 ④).
    configure_logging_or_exit("컨슈머")

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("종료 신호를 받았습니다 — 컨슈머를 닫습니다")
    except AiServiceError as exc:
        # 설정 문제(MQ_ENABLED=false 등)는 재시작해도 같다. 0 으로 끝내면 k8s 가
        # "할 일 끝나고 정상 종료"로 보고 조용히 넘어가서 아무도 못 알아챈다.
        logger.error("컨슈머를 띄우지 못했습니다: %s", exc)
        sys.exit(EXIT_CONFIG_ERROR)
    except Exception:
        # 접속 실패(브로커 다운·잘못된 vhost 등)가 여기로 온다. 그냥 두면 stderr 에
        # 날 raw traceback 이 찍혀 로그 수집기에서 여러 줄로 흩어진다 — logger.exception
        # 은 traceback 을 유지하면서 한 레코드로 남긴다.
        logger.exception("컨슈머가 예기치 않게 종료됐습니다")
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == "__main__":
    main()
