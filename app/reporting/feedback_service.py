"""담당: 용준 — 월간 리포트 셀러 피드백 수신(`feedback.report.created`).

계약은 `docs/mq_events.md` §8.

이 핸들러는 로그만 남기고 아무 데도 저장하지 않는다. 정본은 `feedbackId` 를 발급한
백엔드에 이미 있고, 우리 쪽 저장소는 서비스 DB(main server 경유)뿐이라 AI 가 사본을
들 자리가 없다. 벡터DB 도 아니다 — 컬렉션 둘(`detail_pages`, `rejection_reasons`)이
전부라, 리포트 피드백을 후자에 넣으면 개선안 RAG 결과에 섞여 들어간다.

적재를 붙일 때는 멱등 처리가 같이 와야 한다. 멱등 키는 `feedbackId` 인데 컨슈머는
중복 제거를 하지 않는다. 로그만 남기는 동안은 중복이 줄 하나를 더 만들 뿐이지만,
저장을 시작하는 순간 `feedbackId` 기준 upsert 가 필요하다.

등록 자체가 적재 로직보다 먼저다: 등록되지 않은 `eventType` 은 `dispatch()` 에서
`KeyError` → nack → DLX 로 간다. 그 정책은 의도한 것이지만(ACK 하면 담당자가 영영
못 받는다) **DLQ 는 안전망이 아니다** — 백엔드 큐의 TTL 과 delivery-limit
(`mq_consumer.resolve_queue`) 때문에 쌓아뒀다 처리하는 게 아니라 결국 만료된다.
"""

from __future__ import annotations

import logging
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.core.constants import KST

logger = logging.getLogger(__name__)

# 아래 `logger.*` 메시지에는 cp949 로 인코딩 안 되는 문자를 쓰지 않는다(`—`·`⚠️` 등).
# 주석·docstring 은 콘솔로 안 나가므로 해당 없다. 메시지 리터럴만이다.
#
# 인코딩 편의가 아니라 메시지 유실 문제다. cp949 콘솔에서 진입점이
# `force_utf8_output()` 을 안 부른 채 이 핸들러가 돌면: `emit()` 이 인코딩에 실패해
# 경고가 안 나가고, `handleError()` 가 traceback 을 같은 스트림에 쓰다 소스 라인의 그
# 문자에서 또 터진다. handleError 는 `OSError` 만 삼키므로 `UnicodeEncodeError` 가
# 호출부로 탈출하고, 그건 `ValueError` 하위라 `mq_consumer.consume()` 이 계약 위반으로
# 분류해 `nack(requeue=False)` → DLX 로 보낸다. 피드백이 유실된다.
#
# 탈출은 실패한 호출이 한 줄일 때만 난다(여러 줄이면 traceback 에 첫 줄만 실려
# 인코딩된다). 그 차이에 기대지 않는다 — 한 줄로 합치는 것은 정상적인 정리 작업이다.
# `app/consumer.py` 가 `force_utf8_output()` 을 불러 실제 경로는 이미 막혀 있고,
# 여기서 또 막는 건 모듈이 진입점의 기억력에 기대지 않게 하려는 것이다.

# 계약이 정한 값. 벗어나도 죽이지 않고 경고만 한다 — 아래 핸들러 참고.
FEEDBACK_TYPES = frozenset({"POSITIVE", "NEGATIVE", "NEUTRAL"})
RATING_MIN, RATING_MAX = 1, 5


class ReportFeedbackCreated(BaseModel):
    """`feedback.report.created` payload.

    인바운드 2종 중 이 이벤트만 payload 가 camelCase 다
    (`feedback.recommendation.reviewed` 는 안이 snake_case). 그래서 `alias_generator`
    로 한 줄에 숨기지 않고 필드마다 alias 를 적는다 — 예외라는 사실이 코드에서 보여야
    한다. `rating`·`comment` 는 한 단어라 두 표기가 같아 alias 가 없는 게 맞다.

    필수는 식별자 둘뿐이다. 나머지를 필수로 올리면 백엔드가 한 필드만 빠뜨려도 파싱
    단계 `ValidationError` 로 전량 DLQ 행이 된다. 가르는 기준은 그게 없으면 기록이
    무의미해지는가다 — `feedbackId`·`reportId` 가 없으면 어느 리포트의 무슨 피드백인지
    알 수 없으므로 DLX 로 보내 백엔드가 고쳐 재발행하게 한다.

    이 관대함은 필드 누락 한정이고 타입이 틀리면 여전히 죽는다. 손수 만든 검사 둘
    (`feedbackType` 계약값·`rating` 범위)만 경고로 끝나고 그 밖은 pydantic 타입 강제가
    먼저 걸린다. 실측:

        rating 0 / 6 / -1                          경고만 (의도대로)
        rating 4.5 / "good" · feedbackType 3       ValidationError -> DLQ
        comment 12345 · submittedAt "어제"          ValidationError -> DLQ
        rating "5" · submittedAt 1755000000        통과 (강제 변환)

    `rating` 이 한 필드 안에서 갈린다 — 범위 밖이면 살고 타입이 틀리면 죽는다. 백엔드가
    타입 있는 Spring DTO(`Integer`·enum·`OffsetDateTime`)로 직렬화하는 한 위 경우는 잘
    안 나오므로 그대로 둔다. 어떤 값이 와도 안 죽게 하려면 전 필드를 `Any` 로 받아
    손수 검사해야 하고, 그러면 계약 문서화 효과를 잃는다.
    """

    model_config = ConfigDict(populate_by_name=True)

    feedback_id: str = Field(alias="feedbackId")
    report_id: str = Field(alias="reportId")

    # `userId` 는 일부러 파싱만 하고 아무 데도 안 쓴다. 계약에 있는 필드라 모델에 두어
    # 문서 역할을 하게 한다. 로그에는 넣지 않는다 — `comment` 를 안 찍는 것과 같은
    # 판단이다. 안 쓴다고 지우면 계약과 코드가 갈린다.
    user_id: str | None = Field(default=None, alias="userId")
    feedback_type: str | None = Field(default=None, alias="feedbackType")
    rating: int | None = None
    comment: str | None = None
    submitted_at: datetime | None = Field(default=None, alias="submittedAt")


def _format_submitted_at(value: datetime | None) -> str | None:
    """`submittedAt` 을 KST 표기로. 타임존이 없으면 경고하고 원문 그대로 남긴다.

    백엔드는 `OffsetDateTime` 으로 보내므로 오프셋이 붙어서 온다. KST 로 맞추는 것은
    우리 로그의 다른 시각이 전부 KST 라, 여기만 UTC 로 남으면 같은 사건을 두 시각으로
    읽게 되기 때문이다.

    타임존이 없으면 추측해서 붙이지 않는다. naive 값에 KST 를 씌우면 백엔드가 UTC 로
    보낸 경우 9시간이 조용히 밀린다. 모르면 원문을 남겨 읽는 사람이 판단하게 한다.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        logger.warning("submittedAt 에 타임존이 없습니다. 원문 그대로 기록합니다: %s", value)
        return value.isoformat()
    return value.astimezone(KST).isoformat()


def handle_report_feedback(payload: dict) -> None:
    """`feedback.report.created` 1건을 기록한다. 저장하지 않는다 — 모듈 docstring 참고.

    컨슈머(`app/core/mq_consumer.py`)가 부르지만 core 가 이 함수를 알지는 않는다 —
    실행 진입점(`app/consumer.py`)이 `register_handler()` 로 꽂아 준다.

    워커 스레드에서 도는 동기 함수다. 안에서 블로킹 I/O 를 해도 이벤트 루프를 막지
    않는다(`dispatch()` 가 `asyncio.to_thread` 로 돌린다).

    값이 이상한 것과 식별자가 없는 것을 다르게 다룬다. `feedbackType` 이 계약 밖이거나
    `rating` 이 1~5 를 벗어나면 경고만 하고 넘긴다 — 숫자 하나가 어긋났다고 메시지를
    DLQ 로 보내면 로그만 남기는 핸들러에서 손해가 더 크다. 식별자가 없으면 위 모델이
    `ValidationError` 로 죽는다.

    Raises:
        ValidationError: `feedbackId`·`reportId` 가 없음 (계약 위반).
    """
    event = ReportFeedbackCreated.model_validate(payload)

    if event.feedback_type is not None and event.feedback_type not in FEEDBACK_TYPES:
        logger.warning(
            "reportId=%s: 계약에 없는 feedbackType 입니다(%s). 그대로 기록합니다. "
            "계약값: %s",
            event.report_id,
            event.feedback_type,
            ", ".join(sorted(FEEDBACK_TYPES)),
        )

    if event.rating is not None and not RATING_MIN <= event.rating <= RATING_MAX:
        logger.warning(
            "reportId=%s: rating 이 %d~%d 를 벗어났습니다(%s). 그대로 기록합니다.",
            event.report_id,
            RATING_MIN,
            RATING_MAX,
            event.rating,
        )

    # 추적 키(reportId)를 반드시 포함한다. 이 로그가 지금은 유일한 기록이라, 나중에
    # "그 달 리포트 평이 어땠나"를 물으면 여기서만 답이 나온다.
    #
    # `comment` 는 원문을 찍지 않는다. 셀러가 쓴 자유 텍스트라 연락처나 주문 정보가
    # 섞일 수 있고 로그는 수집기로 흘러가 우리가 지울 수 없는 곳에 남는다. 있었는지만
    # 남겨도 "코멘트가 붙은 피드백이 얼마나 되나"는 답할 수 있고, 원문이 필요하면
    # 정본을 가진 백엔드에 `feedbackId` 로 물으면 된다.
    logger.info(
        "리포트 피드백 수신 reportId=%s feedbackId=%s type=%s rating=%s submittedAt=%s "
        "comment=%s (기록만 하고 저장하지 않습니다)",
        event.report_id,
        event.feedback_id,
        event.feedback_type,
        event.rating,
        _format_submitted_at(event.submitted_at),
        "있음" if (event.comment or "").strip() else "없음",
    )
