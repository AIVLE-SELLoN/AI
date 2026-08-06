"""담당: 지인 — RabbitMQ 발행기(`app/core/mq.py`).

지금은 시그니처 스텁이라 **호출부가 기대하는 모양**만 고정한다. 팀원 2명이 이 시그니처에
맞춰 호출부를 짜고 있어서, 여기가 깨지면 배치가 import 단계에서 조용히 폴백으로 빠진다.
"""

import inspect

import pytest

from app.core import mq


def test_trace_id_is_unique_per_call():
    """배치 1회 = traceId 1개. 배치가 한 번 만들어 그 배치의 모든 메시지에 붙인다(§3)."""
    assert mq.new_trace_id() != mq.new_trace_id()


def test_publishers_take_trace_id_as_argument():
    """`trace_id` 를 발행 함수가 자체 생성하면 배치 1회 = traceId 1개 규약이 깨진다.

    인자 이름·순서는 `app/batch/daily.py` 호출부와 맞춰져 있다.
    """
    assert list(inspect.signature(mq.publish_anomaly_analyzed).parameters) == [
        "alert",
        "rec",
        "trace_id",
    ]
    assert list(inspect.signature(mq.publish_guideline_generated).parameters) == [
        "callback",
        "trace_id",
    ]


@pytest.mark.asyncio
async def test_publishers_are_not_silent_stubs():
    """미구현 발행기가 조용히 성공하면 안 된다.

    호출부는 예외가 없으면 발행 성공으로 보고 그 알림을 prior_alerts 캐시에 넣는다 —
    안 나간 메시지를 성공 처리하면 셀러가 그 알림을 억제 기간(7일) 내내 못 본다.
    """
    with pytest.raises(NotImplementedError):
        await mq.publish_anomaly_analyzed(None, None, "trace-x")
    with pytest.raises(NotImplementedError):
        await mq.publish_guideline_generated(None, "trace-x")
