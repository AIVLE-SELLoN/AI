"""담당: 지인 — REST 진입점 `service.generate_recommendation` 의 **배선**.

운영 경로가 아니다(개선안은 탐지 배치가 선생성해 `ai.anomaly.analyzed` 에 싣는다).
그래도 배선을 고정하는 이유는 `test_generate_for_alert.py` 에 적어둔 것과 같다 —
**끊겨도 결과 객체는 똑같이 나오는데 실제로는 image_guide 가 근거를 잃는다**(조용한 열화).

라우터 테스트 4개는 이 함수를 통째로 monkeypatch 하므로 이 끊김을 못 잡는다.
`fetch_linked_inquiries(alert)` 호출을 지워도 그쪽은 전부 통과한다.

LLM·DB 를 안 탄다 — `pipeline.run` 과 조회 함수를 몽키패치로 막는다.
"""

import psycopg
import pytest

from app.recommendation import service


@pytest.mark.asyncio
async def test_cs_inquiries_are_fetched_and_passed_to_pipeline(monkeypatch, biased_alert):
    """조회한 CS 원문이 `run()` 까지 그대로 간다.

    body 로 alert 만 받으므로 여기서 조회하지 않으면 `cs_quotes` 가 0건이 되고,
    image_guide 로 라우팅된 알림은 **항상 None** 이 된다(조회를 붙이기 전 동작).
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc, why",
    [
        (
            psycopg.OperationalError("connection failed: server closed the connection"),
            "DB 미기동·호스트 오타·비밀번호 틀림 — OperationalError 계열",
        ),
        (
            psycopg.errors.UndefinedTable('relation "voc_document" does not exist'),
            "뷰 없음·GRANT 누락·DSN 형식 오타 — ProgrammingError 계열",
        ),
    ],
)
async def test_unreachable_postgres_degrades_too(
    monkeypatch, biased_alert, caplog, exc, why
):
    """같은 degrade 가 **Postgres 에서도** 돌아야 한다 — 안 그러면 여기만 500 이다.

    위 테스트가 잠그는 `FileNotFoundError` 는 **sqlite 파일 부재**의 모양이다. raw DB 가
    Postgres 로 가면 같은 상황(못 읽는다)이 `psycopg.Error` 로 오는데 그건
    `FileNotFoundError` 가 아니라, `connection_error_types()` 가 빠지면 이 엔드포인트가
    조용히 500 으로 바뀐다 — 배치의 exit 1 회귀와 **같은 결함이 문만 다른 것**이다.

    **두 베이스를 다 넣었다** — `OperationalError` 로 좁히면 `UndefinedTable` 이
       혼자 실패해서 알려준다.
    """

    def boom(alert):
        raise exc

    monkeypatch.setattr(service, "fetch_linked_inquiries", boom)

    async def fake_run(alert, inquiries=()):
        assert inquiries == [], why

    monkeypatch.setattr(service.pipeline, "run", fake_run)

    assert await service.generate_recommendation(biased_alert) is None
    assert any("raw DB 를 읽지 못해" in r.getMessage() for r in caplog.records), why