"""진입점 공통 부팅 처리 — 설정을 읽고 로깅을 걸거나, 못 하면 사유 한 줄 남기고 끝낸다.

왜 core 에 있나
---------------
진입점마다 **같은 블록을 복사**하고 있었다(`app/main.py`·`app/consumer.py` 가 명사
하나만 달랐다). `exit_codes.py` 가 상수를 이관한 사유(*"각자 두면 한쪽만 바뀌어도 조용히
갈린다"*)가 이 블록에도 그대로 적용된다 — 실제로 아래 `_describe()` 결함을 두 번 고쳐야
했다(용준님 PR #96 리뷰 ①·④).

**배포 진입점이 전부 이 함수를 쓴다.** ⚠️ 여기에 목록이나 개수를 적지 말 것 — 다음
진입점이 생길 때 조용히 거짓이 된다(`exit_codes.py` 의 같은 경고 참고).

⚠️ **이 함수가 덮는 범위는 "부팅 시점 설정" 까지다.** `Settings` 로 읽히는 값(`LOG_LEVEL`
   오타, `MQ_PORT=abc` 등)은 여기서 걸리지만, **실행 중에 드러나는 환경 전제**(raw DB 경로
   부재, 옛 스키마, 분류 결과 테이블 없음)는 여기 안 온다. 배치는 그게 제일 잦은 실패라
   `daily.main()` 이 `run_batch` 를 감싸는 **바깥 껍질**을 따로 두고 같은 exit 2 로 가른다
   (`app/consumer.py` 가 `asyncio.run()` 을 감싸는 것과 같은 모양이다).
   → 즉 **"부팅 가드 + 바깥 껍질" 두 겹**이고, 이 함수는 그 앞쪽 한 겹이다.
"""

from __future__ import annotations

import logging
import sys

from pydantic import ValidationError

from app.config import get_settings
from app.core.exit_codes import EXIT_CONFIG_ERROR

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def _describe(exc: ValueError) -> str:
    """예외를 **한 줄**로 줄인다. 로그 수집기에서 흩어지지 않게.

    🔴 **pydantic 은 첫 줄만 집으면 아무 정보도 안 남는다.** `str(exc).splitlines()[0]`
       이 `'1 validation error for Settings'` 라는 **개수 헤더**라, 운영자가 어느 값이
       틀렸는지 알 수 없다. 하필 `MQ_PORT`·`CHROMA_PORT` 처럼 **제일 자주 나는 오타**가
       전부 이쪽이다(용준님 PR #96 리뷰 ①, 재현 확인). `LOG_LEVEL` 쪽은 첫 줄이
       `Unknown level: 'info'` 라 멀쩡해서 안 드러났다.

    🔴 **`input`(입력값)은 일부러 안 싣는다.** `Settings` 에 `llm_api_key`·`mq_password`
       가 있어서, 그 값이 검증에 걸리는 날 **비밀값이 로그로 나간다.** 같은 이유로
       `str(exc)` 전체를 이어붙이는 것도 안 된다 — 거기엔 `input_value='abc'` 가 들어
       있다. `loc` 과 `msg` 만으로도 "어느 키가 왜 틀렸는지" 는 다 나온다.

    ⚠️ **`loc` 이 비는 경우가 있다 — `model_validator` 는 필드 하나에 안 매인다.**
       (`raw_db` 원자값 조합 검사가 그렇다.) 그때 `loc` 을 그대로 붙이면 `": 사유"` 처럼
       **빈 접두어**가 남아 사람이 읽다 멈칫한다. 그런 검사는 메시지 자체가 키 이름을
       담고 있으므로 접두어를 아예 뺀다.
    """
    if isinstance(exc, ValidationError):
        parts = [
            f"{loc}: {err['msg']}" if (loc := ".".join(str(p) for p in err["loc"])) else err["msg"]
            for err in exc.errors()
        ]
        if parts:
            return " / ".join(parts)

    lines = str(exc).splitlines()
    return lines[0] if lines else type(exc).__name__


def configure_logging_or_exit(what: str) -> None:
    """설정을 읽고 로깅을 건다. **실패하면 한 줄 남기고 `exit 2` 로 끝낸다.**

    `what` 은 메시지에 들어갈 이름이다(`"서버"` · `"컨슈머"`).

    🔴 **왜 이 처리가 필요한가 — 진입점이 CrashLoopBackOff 로 죽던 자리다.**
       이 두 줄이 맨몸으로 있으면 `LOG_LEVEL=info`(소문자) 하나가 미포착 예외로 나가
       종료코드가 **1**(일시적 오류)이 된다. k8s 는 그걸 보고 재시작하고 **같은
       환경변수를 다시 읽어 또 죽는다.** 사유는 raw traceback 으로만 남는다.

    ⚠️ **`ValueError` 만 잡는다.** 덮는 실패가 둘인데 둘 다 그 하위라 정확히 덮인다 —
       `basicConfig(level=...)` 의 잘못된 레벨과 `get_settings()` 의 pydantic
       `ValidationError`. 더 넓히면 진짜 예상 밖 오류까지 "설정 문제" 로 보고하게 된다.

    ⚠️ **`logger` 가 아니라 `print` 다.** 두 갈래 중 `get_settings()` 쪽에서 터지면
       로깅이 아직 아무것도 설정되지 않았다. 두 갈래가 같은 모양으로 나가게 통일한다.

    ⚠️ **메시지는 한글·ASCII 로 유지한다 — 다만 "안 그러면 죽는다" 는 아니다.**
       CPython 이 **stderr 의 error handler 를 `backslashreplace` 로 못박아** 둬서
       (`PYTHONIOENCODING=cp949:strict` 로도 그렇다 — 실측), 여기서 `—`·`⚠️` 를 찍어도
       예외가 안 난다. 죽는 건 `strict` 인 **stdout** 쪽이다. 그래서 진짜 이유는
       **읽히느냐**다 — cp949 콘솔에서 `\\u2014` 로 이스케이프돼 나가면 사유를 못 읽는다.
       (용준님 PR #96 리뷰 ②. 처음엔 "안 그러면 exit 1 로 되돌아간다" 고 적었는데
       그 근거는 **틀렸다** — 결론만 같다.)
    """
    try:
        settings = get_settings()
        logging.basicConfig(level=settings.log_level, format=LOG_FORMAT)
        # 🔴 **레벨을 명시적으로 한 번 더 건다 — 이게 없으면 가드가 조용히 죽는다.**
        #    `basicConfig` 는 root 에 핸들러가 이미 있으면 **레벨 처리까지 통째로
        #    건너뛴다**(CPython 이 `level` 을 `if len(root.handlers) == 0` 안에서만 읽는다).
        #    그러면 `LOG_LEVEL` 이 조용히 무시될 뿐 아니라 **잘못된 레벨에도 예외가 안 나서
        #    아래 except 가 안 탄다.** `app/main.py` 는 라우터 4개를 이 호출보다 **먼저**
        #    import 하므로, 그중 하나가 import 시점에 핸들러를 붙이기 시작하면 바로 그
        #    상태가 된다(용준님 PR #96 리뷰 — 지금은 붙이는 게 없어 무해하다).
        #
        #    ⚠️ **`force=True` 로 풀지 말 것.** 그건 기존 핸들러를 지우면서 `close()` 까지
        #       불러서, pytest 의 `caplog` 핸들러를 닫아 **로그는 나가는데 `caplog.records`
        #       가 빈** 상태를 만든다(실측: 기존 테스트 2개가 그렇게 깨졌다). 남의 핸들러를
        #       파괴하지 않으면서 같은 것을 잠그는 게 이 한 줄이다.
        logging.getLogger().setLevel(settings.log_level)
    except ValueError as exc:
        print(f"설정을 읽지 못해 {what}를 띄우지 못했습니다: {_describe(exc)}", file=sys.stderr)
        sys.exit(EXIT_CONFIG_ERROR)
