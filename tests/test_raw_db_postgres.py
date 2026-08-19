"""담당: 지인 — raw DB **Postgres** 조회 경로(이식 1단계).

`RAW_DB_TEST_DSN` 이 없으면 통째로 skip 한다. **그게 계약이다** — 노션 CI_방향성 §2 가
"테스트가 자립적이다 · 외부 의존 없음 · 시크릿 0개" 를 CI 를 AI팀이 갖는 근거로 삼고
있어서, 여기서 DB 를 요구하면 그 근거가 깨진다. 대신 skip 이 아니라 **돌 수 있게** 해
두는 것이 요점이다 — 이식 검증이 한 번 해보고 마는 수작업 기록이 아니라 재현 가능한
절차로 남는다.

    docker compose up -d rawdb
    RAW_DB_TEST_DSN="postgresql://sellon:sellon@localhost:5433/rawdb?sslmode=disable" \
        pytest tests/test_raw_db_postgres.py

⚠️ **`sslmode=disable` 을 붙인다.** compose 의 Postgres 는 SSL 을 안 켜는데 우리 기본값이
   `require` 라, 안 붙이면 접속 자체가 거부된다. 운영 기본값을 로컬 편의로 되돌리지 않으려고
   여기서 명시하는 쪽을 골랐다(`config.raw_db_sslmode` 주석 참고).

⚠️ **`RAW_DB_TEST_DSN` 만 단일 문자열이다.** 운영 경로는 원자값 5개를 우리가 조립하는데
   (`raw_db.conninfo_from_settings`), 이 키는 `.env` 에 안 들어가는 테스트 게이트라 운영
   경로와 섞이지 않는다. 아래 `pg` 픽스처가 psycopg 로 파싱해 **원자값으로 되돌려** 넣으므로
   실연결 검증이 조립 경로를 그대로 통과한다 — 게이트만 우회하면 그 경로가 안 걸린다.

여기서 잠그는 것은 **sqlite 에서는 원리적으로 못 잡는 것들**이다. 넷 다 조회는 성공한
뒤에 터지거나 조용히 틀리는 종류라, DB 별 분기를 되돌려도 나머지 테스트는 전부 초록이다:
  1. `?` 바인딩이 `%s` 로 옮겨진다                (안 옮기면 `ProgrammingError`)
  2. `IS NOT DISTINCT FROM` 이 Postgres 에서도 널 안전 비교다 (`IS` 면 구문 오류)
  3. 스키마 가드가 `information_schema` 로 돈다   (`PRAGMA`·`sqlite_master` 면 죽는다)
  4. `TIMESTAMPTZ` 가 `datetime` 으로 와도 날짜 절단이 된다 (문자열만 받으면 `TypeError`)

⚠️ LLM·네트워크 없음. 로컬 Postgres 에 표식 행 몇 개를 넣고 지운다.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest

from app.batch.inputs import load_inputs_from_db
from app.classification.service import PROMPT_ASPECT_VERSION, PROMPT_SENTIMENT_VERSION
from app.config import get_settings
from app.core import raw_db, raw_schema
from app.core.inquiries import fetch_linked_inquiries
from app.core.schemas import (
    Aspect,
    Channel,
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    RecommendedAction,
    Source,
    SourceSignals,
    Verdict,
)
from app.core.versions import CLASSIFIER_PIPELINE_VERSION

DSN = os.getenv("RAW_DB_TEST_DSN", "")

pytestmark = pytest.mark.skipif(
    not DSN, reason="RAW_DB_TEST_DSN 없음 — 로컬 Postgres 검증 전용"
)

# 표식 접두사. 남의 행을 건드리지 않고, 정리도 이 접두사로만 한다.
PREFIX = "PGT"
WINDOW_END = date(2026, 8, 28)


def _alert(inquiry_ids: list[str]) -> DetectionAlert:
    return DetectionAlert(
        alert_id="ALT-20260828-P001-COLOR-COUPANG",
        detected_at=datetime(2026, 8, 28, 9, 0, tzinfo=timezone.utc),
        product_group_id="P001",
        channel=Channel.COUPANG,
        window_start=date(2026, 8, 22),
        window_end=WINDOW_END,
        verdict=Verdict.BIASED,
        significant_channels=[Channel.COUPANG],
        main_aspect=Aspect.COLOR,
        stats=DetectionStats(
            source=Source.CS,
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=1e-4,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(cs=True, review=None, interpretation="CS 선행"),
        detection_confidence=DetectionConfidence.HIGH,
        scope_in=True,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        evidence=Evidence(inquiry_ids=inquiry_ids),
    )


@pytest.fixture
def pg(monkeypatch):
    """접속 원자값을 Postgres 로 돌리고 표식 행을 심는다. 끝나면 지운다.

    🔴 **게이트 DSN 을 원자값으로 되돌려 넣는다 — `dsn=` 로 우회하지 않는다.** 우회하면
       `conninfo_from_settings()` 조립 경로가 **실연결 검증을 통째로 안 탄다.** 값이
       빠지거나 `sslmode` 가 안 실리는 회귀가 여기서도 안 걸리면, sqlite 에서도 안 걸리니
       아무 데서도 안 걸린다. 파싱은 psycopg 에 맡긴다(우리 게이트 문자열이라 남의 형식을
       파싱하는 것과 다르다).

    ⚠️ **별도 연결로 심는다.** 우리 조회 연결은 읽기 전용이라 자기 자신으로는 못 넣고,
       심는 트랜잭션이 열려 있으면 다른 연결에서 안 보이므로 commit 까지 해야 한다.
    """
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    settings = get_settings()
    atoms = conninfo_to_dict(DSN)
    # `monkeypatch.setattr` 은 pydantic 검증을 안 거치므로 타입을 여기서 맞춘다 —
    # 문자열 포트로도 붙기는 하지만, 필드 타입과 어긋난 채로 두면 다음 사람이 헷갈린다.
    atoms["port"] = int(atoms["port"]) if atoms.get("port") else None
    for field, key, default in (
        ("raw_db_host", "host", "localhost"),
        ("raw_db_port", "port", 5432),
        ("raw_db_name", "dbname", "rawdb"),
        ("raw_db_username", "user", ""),
        ("raw_db_password", "password", ""),
        # compose 는 SSL 을 안 켠다 — 게이트 DSN 이 지정하지 않으면 기본값 require 로
        # 붙었다가 거부당한다. 그 실패는 "이식이 깨졌다" 처럼 보인다.
        ("raw_db_sslmode", "sslmode", "disable"),
    ):
        # `raising=True`(기본)로 둔다 — 필드명이 바뀌면 여기서 터지는 게 맞다. False 면
        # 오타가 조용히 새 속성을 만들고, 접속은 **빈 계정**으로 시도돼 인증 실패로만 보인다
        # (`tests/conftest.block_local_raw_db` 와 같은 사유).
        monkeypatch.setattr(settings, field, atoms.get(key) or default)

    versions = (PROMPT_ASPECT_VERSION, settings.llm_model, CLASSIFIER_PIPELINE_VERSION)
    review_versions = (
        PROMPT_SENTIMENT_VERSION,
        settings.llm_model,
        CLASSIFIER_PIPELINE_VERSION,
    )

    with psycopg.connect(DSN, autocommit=True) as seed:
        _cleanup(seed)
        seed.execute(
            "INSERT INTO channel (channel_id, display_name) VALUES (%s, %s)"
            " ON CONFLICT DO NOTHING",
            ("COUPANG", "쿠팡"),
        )
        # 현재 윈도우 안(=window_end) 문의 2건 + 리뷰 1건.
        seed.execute(
            "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id,"
            " content, inquired_at, created_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                f"{PREFIX}-INQ-1",
                "CP-1",
                "P001",
                "COUPANG",
                "사진이랑 색이 너무 달라요",
                "2026-08-27T10:00:00+09:00",
                "2026-08-27T10:00:05+09:00",
            ),
        )
        seed.execute(
            "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id,"
            " content, inquired_at, created_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                f"{PREFIX}-INQ-2",
                "CP-1",
                "P001",
                "COUPANG",
                "받아보니 화면보다 어둡네요",
                "2026-08-28T09:00:00+09:00",
                "2026-08-28T09:00:05+09:00",
            ),
        )
        seed.execute(
            "INSERT INTO reviews (id, channel_product_id, product_group_id, channel_id,"
            " content, rating, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                f"{PREFIX}-RVW-1",
                "CP-1",
                "P001",
                "COUPANG",
                "색감은 괜찮습니다",
                5,
                "2026-08-28T11:00:00+09:00",
            ),
        )
        # 분류 결과 — CS 2건은 aspect 를 달고, 리뷰 1건은 **aspect 0개**(정상 출력).
        for item_id, source, ver in (
            (f"{PREFIX}-INQ-1", "cs", versions),
            (f"{PREFIX}-INQ-2", "cs", versions),
            (f"{PREFIX}-RVW-1", "review", review_versions),
        ):
            seed.execute(
                "INSERT INTO classified_item (item_id, source, classified_at,"
                " prompt_version, model_version, pipeline_version)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                (item_id, source, "2026-08-28T12:00:00+09:00", *ver),
            )
        for item_id in (f"{PREFIX}-INQ-1", f"{PREFIX}-INQ-2"):
            seed.execute(
                "INSERT INTO classified_item_aspect (item_id, aspect, sentiment,"
                " mixed_signal) VALUES (%s, %s, %s, %s)",
                (item_id, "색상", -1, None),
            )

    yield

    with psycopg.connect(DSN, autocommit=True) as cleanup:
        _cleanup(cleanup)


def _cleanup(conn) -> None:
    conn.execute("DELETE FROM classified_item_aspect WHERE item_id LIKE %s", (f"{PREFIX}-%",))
    conn.execute("DELETE FROM classified_item WHERE item_id LIKE %s", (f"{PREFIX}-%",))
    conn.execute("DELETE FROM cs WHERE id LIKE %s", (f"{PREFIX}-%",))
    conn.execute("DELETE FROM reviews WHERE id LIKE %s", (f"{PREFIX}-%",))


def test_connection_is_read_only(pg):
    """계약① — 읽는 쪽은 원문을 못 고친다.

    sqlite 는 `mode=ro` 가, Postgres 는 GRANT 가 막는다. 로컬 compose 는 우리가
    superuser 라 GRANT 가 아무것도 안 막으므로, `connect_readonly()` 가 세션에
    read-only 를 거는 것이 유일한 방어선이다 — **그게 빠지면 조용히 사라진다.**
    """
    import psycopg

    conn = raw_db.connect_readonly()
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("DELETE FROM cs WHERE id = ?", (f"{PREFIX}-INQ-1",))
    finally:
        conn.close()


def test_schema_guards_run_on_postgres(pg):
    """스키마 가드가 `information_schema` 로 돈다.

    `PRAGMA`·`sqlite_master` 를 직접 쓰면 **가드 자신이** 구문 오류로 먼저 죽는다 —
    스키마가 멀쩡한지 보려던 코드가 터지는 모양이라 원인이 메시지에 안 드러난다.
    """
    conn = raw_db.connect_readonly()
    try:
        assert raw_schema.find_legacy_tables(conn) == []
        assert raw_db.existing_tables(
            conn, ("classified_item", "classified_item_aspect")
        ) == {"classified_item", "classified_item_aspect"}
        # 뷰는 세지 않는다 — 원문만 있는 DB 가 "워커 돌았음" 으로 통과하면 안 된다.
        assert raw_db.existing_tables(conn, (raw_schema.VOC_DOCUMENT,)) == set()
        assert "pipeline_version" in raw_db.table_columns(conn, "classified_item")
        assert raw_db.table_columns(conn, "없는테이블") == set()
    finally:
        conn.close()


def test_load_inputs_from_db_reads_postgres(pg):
    """조회①② — 분모(documents)와 분자(items)를 Postgres 에서 읽는다.

    한 번에 네 가지가 걸린다: `?`→`%s` 바인딩, 활성 버전 필터의 널 안전 비교,
    `_check_version_cutover` 의 집계, `TIMESTAMPTZ` → KST 날짜 절단.
    """
    items, documents = load_inputs_from_db(window_end=WINDOW_END)

    mine = {d["id"] for d in documents if d["id"].startswith(PREFIX)}
    assert mine == {f"{PREFIX}-INQ-1", f"{PREFIX}-INQ-2", f"{PREFIX}-RVW-1"}

    doc = next(d for d in documents if d["id"] == f"{PREFIX}-INQ-2")
    assert doc["product"] == "P001"
    assert doc["channel"] == "COUPANG"
    assert doc["source"] == "cs"
    # TIMESTAMPTZ 가 aware datetime 으로 와도 KST 로 접혀야 한다.
    assert doc["created_at"].date() == date(2026, 8, 28)
    assert doc["created_at"].utcoffset().total_seconds() == 9 * 3600

    by_id = {i.item_id: i for i in items if i.item_id.startswith(PREFIX)}
    assert set(by_id) == {f"{PREFIX}-INQ-1", f"{PREFIX}-INQ-2", f"{PREFIX}-RVW-1"}
    assert [a.aspect.value for a in by_id[f"{PREFIX}-INQ-1"].aspects] == ["색상"]
    assert [a.sentiment.value for a in by_id[f"{PREFIX}-INQ-1"].aspects] == [-1]
    # aspect 0개인 리뷰도 item 으로 살아남는다(LEFT JOIN 계약) — 미분류와 구분된다.
    assert by_id[f"{PREFIX}-RVW-1"].aspects == []


def test_active_version_filter_excludes_stale_rows_on_postgres(pg):
    """활성 버전 필터가 Postgres 에서도 **거른다.**

    🔴 이게 통과만 보고 넘어가면 안 되는 이유: `IS NOT DISTINCT FROM` 을 `=` 로
       되돌려도 위 테스트는 그대로 초록이다(값이 다 NOT NULL 이라). 널이 섞였을 때만
       갈리므로 `model_version = NULL` 인 옛 행을 일부러 넣어 확인한다.

    ⚠️ 옛 행이 윈도우 안에 있으면 `_check_version_cutover` 가 배치를 세운다(fail-closed).
       여기서는 그 **세우는 동작 자체**가 관측 대상이다 — 세우지 않으면 과거 구간
       분자가 0 이 되어 최대 강도 오탐이 난다(2026-08-12 실측).
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as seed:
        seed.execute(
            "INSERT INTO cs (id, channel_product_id, product_group_id, channel_id,"
            " content, inquired_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                f"{PREFIX}-INQ-OLD",
                "CP-1",
                "P001",
                "COUPANG",
                "옛 분류기가 라벨링한 문의",
                "2026-08-27T13:00:00+09:00",
            ),
        )
        seed.execute(
            "INSERT INTO classified_item (item_id, source, prompt_version,"
            " model_version, pipeline_version) VALUES (%s, %s, %s, NULL, %s)",
            (
                f"{PREFIX}-INQ-OLD",
                "cs",
                PROMPT_ASPECT_VERSION,
                CLASSIFIER_PIPELINE_VERSION,
            ),
        )

    with pytest.raises(RuntimeError, match="옛 분류기 기준"):
        load_inputs_from_db(window_end=WINDOW_END)


def test_fetch_linked_inquiries_reads_postgres(pg):
    """조회③ — `evidence.inquiry_ids` → CS 원문. IN 절 바인딩이 옮겨져야 한다."""
    alert = _alert([f"{PREFIX}-INQ-2", f"{PREFIX}-RVW-1"])

    inquiries = fetch_linked_inquiries(alert)

    assert [i.item_id for i in inquiries] == [f"{PREFIX}-INQ-2", f"{PREFIX}-RVW-1"]
    assert inquiries[0].raw_text == "받아보니 화면보다 어둡네요"
    assert inquiries[0].source.value == "cs"
    # 리뷰도 근거로 쓴다(2026-08-11 확정 정책). 출처만 갈라서 실어 보낸다.
    assert inquiries[1].source.value == "review"
    assert inquiries[0].created_at.date() == date(2026, 8, 28)
