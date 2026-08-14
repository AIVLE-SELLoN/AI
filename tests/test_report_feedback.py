"""`feedback.report.created` 핸들러 — 파싱·관대함의 경계·배선.

이 핸들러는 저장을 하지 않아서 "무엇이 남았는가"로 검증할 수가 없다. 그래서 확인할 것이
셋이다: **camelCase 를 제대로 읽는가**, **어디까지 봐주고 어디서 죽는가**, 그리고
**운영 배선에 실제로 꽂혀 있는가**.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.core.constants import KST
from app.reporting.feedback_service import (
    ReportFeedbackCreated,
    handle_report_feedback,
)

PAYLOAD = {
    "feedbackId": "FB-0001",
    "reportId": "RPT-202607",
    "userId": "U-77",
    "feedbackType": "POSITIVE",
    "rating": 5,
    "comment": "격차 그래프가 이해하기 쉬웠습니다",
    "submittedAt": "2026-08-13T08:30:00+09:00",
}


def test_camel_case_payload_is_parsed() -> None:
    """🔴 인바운드 2종 중 이 이벤트만 camelCase 다 — 그걸 실제로 읽는지 본다."""
    event = ReportFeedbackCreated.model_validate(PAYLOAD)

    assert event.feedback_id == "FB-0001"
    assert event.report_id == "RPT-202607"
    assert event.user_id == "U-77"
    assert event.feedback_type == "POSITIVE"
    assert event.rating == 5
    assert event.submitted_at == datetime(2026, 8, 13, 8, 30, tzinfo=KST)


def test_snake_case_also_works() -> None:
    """`populate_by_name` — 백엔드가 표기를 바꿔도 계약 위반으로 죽지 않는다."""
    event = ReportFeedbackCreated.model_validate(
        {"feedback_id": "FB-0002", "report_id": "RPT-202607"}
    )

    assert event.feedback_id == "FB-0002"


@pytest.mark.parametrize("missing", ["feedbackId", "reportId"])
def test_missing_identifier_raises(missing: str) -> None:
    """🔴 식별자가 없으면 **죽어야 한다** — DLX 로 가서 백엔드가 고쳐 재발행한다.

    어느 리포트의 무슨 피드백인지 모르면 로그를 남길 이유가 없다. 여기서 봐주면
    빈 줄만 쌓이고 아무도 계약이 깨진 걸 모른다.
    """
    payload = {k: v for k, v in PAYLOAD.items() if k != missing}

    with pytest.raises(ValidationError):
        handle_report_feedback(payload)


def test_optional_fields_may_be_absent() -> None:
    """식별자만 있으면 통과한다. 값 하나 빠졌다고 전량 DLQ 가 되면 안 된다."""
    handle_report_feedback({"feedbackId": "FB-0003", "reportId": "RPT-202607"})


def test_unknown_feedback_type_warns_but_survives(caplog) -> None:
    """🔴 값이 이상한 것과 식별자가 없는 것은 다르게 다룬다 — 여기서는 죽지 않는다."""
    with caplog.at_level(logging.WARNING):
        handle_report_feedback({**PAYLOAD, "feedbackType": "AWESOME"})

    assert any("feedbackType" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("rating", [0, 6, -1])
def test_out_of_range_rating_warns_but_survives(caplog, rating: int) -> None:
    """rating 범위 밖도 경고만. 숫자 하나 어긋났다고 메시지를 잃는 건 손해가 크다."""
    with caplog.at_level(logging.WARNING):
        handle_report_feedback({**PAYLOAD, "rating": rating})

    assert any("rating" in r.getMessage() for r in caplog.records)


def test_submitted_at_is_logged_in_kst(caplog) -> None:
    """🔴 UTC 로 와도 KST 로 찍는다 — 로그의 다른 시각이 전부 KST 라 여기만 어긋나면 안 된다."""
    utc_noon = "2026-08-13T03:30:00+00:00"  # = KST 12:30

    with caplog.at_level(logging.INFO):
        handle_report_feedback({**PAYLOAD, "submittedAt": utc_noon})

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "12:30:00+09:00" in logged
    assert "03:30:00+00:00" not in logged


def test_naive_submitted_at_is_not_guessed(caplog) -> None:
    """🔴 타임존이 없으면 KST 를 씌우지 않는다 — UTC 였다면 9시간이 조용히 밀린다.

    ⚠️ `at_level` 을 **INFO 로 잡는다.** WARNING 으로 잡으면 경고만 보이고 시각이 찍히는
       기록 줄은 캡처되지 않아, KST 를 씌우는 회귀를 놓친다(변이 검증에서 실제로 새어
       나갔다). 경고가 났는지와 무엇이 찍혔는지를 **둘 다** 봐야 한다.
    """
    with caplog.at_level(logging.INFO):
        handle_report_feedback({**PAYLOAD, "submittedAt": "2026-08-13T08:30:00"})

    messages = "\n".join(r.getMessage() for r in caplog.records)
    assert "타임존" in messages
    assert "submittedAt=2026-08-13T08:30:00 " in messages
    assert "+09:00" not in messages


def test_comment_text_is_not_logged(caplog) -> None:
    """🔴 셀러가 쓴 자유 텍스트는 원문을 남기지 않는다.

    연락처·주문정보가 섞여 들어올 수 있고, 로그는 우리가 지울 수 없는 수집기로 간다.
    있었는지만 남긴다.
    """
    secret = "010-1234-5678 로 연락주세요"

    with caplog.at_level(logging.INFO):
        handle_report_feedback({**PAYLOAD, "comment": secret})

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert secret not in logged
    assert "있음" in logged


def test_blank_comment_is_reported_as_absent(caplog) -> None:
    """공백만 있는 코멘트는 '없음'이다 — 빈 값을 '있음'으로 세면 집계가 부풀려진다."""
    with caplog.at_level(logging.INFO):
        handle_report_feedback({**PAYLOAD, "comment": "   "})

    assert "없음" in "\n".join(r.getMessage() for r in caplog.records)


def test_duplicate_delivery_is_harmless() -> None:
    """멱등 키는 feedbackId 지만 컨슈머는 중복 제거를 하지 않는다(§10).

    저장을 하지 않는 동안은 같은 이벤트가 두 번 와도 줄 하나가 더 생길 뿐이다.
    적재를 붙이는 순간 이 테스트는 upsert 검증으로 바뀌어야 한다.
    """
    handle_report_feedback(PAYLOAD)
    handle_report_feedback(PAYLOAD)


def test_wiring_registers_both_inbound_handlers(monkeypatch) -> None:
    """🔴 운영 배선에 실제로 꽂혀 있는지 — 이게 없으면 위 테스트가 전부 통과해도 DLQ 로 간다.

    다른 테스트는 핸들러를 직접 부르기 때문에 `wire_handlers()` 가 비어 있어도 통과한다.
    같은 구멍으로 한 번 깨진 적이 있다(2026-08-07, `test_consumer_entrypoint`).
    """
    from app import consumer
    from app.core import mq_consumer

    monkeypatch.setattr(mq_consumer, "HANDLERS", {})

    consumer.wire_handlers()

    assert mq_consumer.REPORT_CREATED in mq_consumer.HANDLERS
    assert mq_consumer.RECOMMENDATION_REVIEWED in mq_consumer.HANDLERS


@pytest.mark.asyncio
async def test_dispatch_routes_the_event_end_to_end(monkeypatch) -> None:
    """🔴 이벤트 이름이 실제로 이 핸들러로 라우팅되는지 — 상수 오타를 잡는 유일한 자리.

    `REPORT_CREATED` 를 잘못 적어도 등록 테스트는 통과한다(등록한 이름으로 확인하므로).
    브로커가 보내는 **문자열 그대로** 넣어 봐야 걸린다.
    """
    import json

    from app import consumer
    from app.core import mq_consumer

    monkeypatch.setattr(mq_consumer, "HANDLERS", {})
    consumer.wire_handlers()

    body = json.dumps({"eventType": "feedback.report.created", "payload": PAYLOAD})
    await mq_consumer.dispatch("feedback.report.created", body.encode("utf-8"))


def test_offset_other_than_kst_is_converted_not_relabeled() -> None:
    """오프셋이 다른 값도 **같은 순간**으로 변환된다 — 라벨만 갈아끼우면 시각이 틀어진다."""
    from app.reporting.feedback_service import _format_submitted_at

    aware = datetime(2026, 8, 13, 0, 0, tzinfo=timezone(timedelta(hours=-5)))

    assert _format_submitted_at(aware) == "2026-08-13T14:00:00+09:00"


# ── 관대함의 경계 — 누락은 봐주고 타입은 안 봐준다 ──────────────

@pytest.mark.parametrize(
    "field, value",
    [
        ("rating", 4.5),        # 정수가 아닌 수
        ("rating", "good"),     # 숫자가 아닌 문자열
        ("feedbackType", 3),    # 문자열이 아님
        ("comment", 12345),     # 문자열이 아님
        ("submittedAt", "어제"),  # 파싱 불가한 시각
    ],
)
def test_wrong_type_still_raises(field: str, value) -> None:
    """🔴 **관대함은 "필드 누락" 한정이다.** 타입이 틀리면 pydantic 이 먼저 죽인다.

    docstring 의 "나머지를 필수로 올리면 전량 DLQ" 가 일반 보증처럼 읽히기 쉬운데,
    실제로는 손수 만든 검사 둘(feedbackType 계약값·rating 범위)에서만 성립한다.
    `rating` 은 **한 필드 안에서 갈린다** — 0/6/-1 은 경고만이고 4.5/"good" 은 DLQ 다.
    지금은 백엔드가 타입 있는 Spring DTO 라 실제 위험이 낮다고 보고 그대로 둔다.
    이 테스트는 그 경계가 어디인지를 **고정해 두는 것**이 목적이다. (2026-08-14 리뷰)
    """
    with pytest.raises(ValidationError):
        handle_report_feedback({**PAYLOAD, field: value})


@pytest.mark.parametrize(
    "field, value",
    [
        ("rating", "5"),                # 문자열이지만 정수로 강제 변환된다
        ("submittedAt", 1755000000),    # epoch 초도 datetime 으로 읽힌다
        ("unknownField", "x"),          # 모르는 필드는 무시된다(계약이 늘어도 안 죽는다)
    ],
)
def test_coercible_or_unknown_fields_survive(field: str, value) -> None:
    """반대편도 고정한다 — 강제 변환되는 값과 모르는 필드는 통과한다.

    백엔드가 계약에 필드를 추가해도(§11 은 옵셔널 추가만 허용) 이 핸들러는 안 죽는다.
    """
    handle_report_feedback({**PAYLOAD, field: value})
