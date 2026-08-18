"""담당: 지인 — REST 진입점 `service.generate_recommendation` 의 **배선**.

운영 경로가 아니다(개선안은 탐지 배치가 선생성해 `ai.anomaly.analyzed` 에 싣는다).
그래도 배선을 고정하는 이유는 `test_generate_for_alert.py` 에 적어둔 것과 같다 —
**끊겨도 결과 객체는 똑같이 나오는데 실제로는 image_guide 가 근거를 잃는다**(조용한 열화).

라우터 테스트 4개는 이 함수를 통째로 monkeypatch 하므로 이 끊김을 못 잡는다.
`fetch_linked_inquiries(alert)` 호출을 지워도 그쪽은 전부 통과한다. (2026-08-11 리뷰 ④)

LLM·DB 를 안 탄다 — `pipeline.run` 과 조회 함수를 몽키패치로 막는다.
"""

import pytest

from app.recommendation import service


@pytest.mark.asyncio
async def test_cs_inquiries_are_fetched_and_passed_to_pipeline(monkeypatch, biased_alert):
    """조회한 CS 원문이 `run()` 까지 그대로 간다.

    body 로 alert 만 받으므로 여기서 조회하지 않으면 `cs_quotes` 가 0건이 되고,
    image_guide 로 라우팅된 알림은 **항상 None** 이 된다(2026-08-10 이전 동작).
    """
    fetched = ["원문-1", "원문-2"]
    seen: dict = {}

    monkeypatch.setattr(service, "fetch_linked_inquiries", lambda alert: fetched)

    async def fake_run(alert, inquiries=()):
        seen["alert"] = alert
        seen["inquiries"] = inquiries
        return "개선안"

    monkeypatch.setattr(service.pipeline, "run", fake_run)

    assert await service.generate_recommendation(biased_alert) == "개선안"
    assert seen["alert"] is biased_alert
    assert seen["inquiries"] is fetched, "조회 결과가 파이프라인까지 안 갔다"


@pytest.mark.asyncio
async def test_missing_raw_db_degrades_with_a_warning(monkeypatch, biased_alert, caplog):
    """raw DB 가 없으면 **500 대신** 원문 없이 진행하고 경고를 남긴다.

    그 환경에서 던지면 DB 없이 쓰던 copy_draft 디버깅까지 같이 막힌다. 대신 조용히
    넘기지 않는다 — 근거가 빠진 결과를 "개선안이 안 만들어진다" 로 오해하지 않게.
    """

    def boom(alert):
        raise FileNotFoundError("raw DB 가 없습니다: /없는/경로/raw.db")

    monkeypatch.setattr(service, "fetch_linked_inquiries", boom)

    async def fake_run(alert, inquiries=()):
        assert inquiries == [], "조회 실패면 빈 리스트로 진행해야 한다"

    monkeypatch.setattr(service.pipeline, "run", fake_run)

    assert await service.generate_recommendation(biased_alert) is None
    assert any("raw DB 를 읽지 못해" in r.getMessage() for r in caplog.records), (
        "조용히 넘기면 근거 없이 나온 결과를 아무도 못 알아챈다"
    )