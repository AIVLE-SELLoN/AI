"""공용 pytest 픽스처. pytest가 자동 수집하므로 각 tests/test_*.py에서 import 불필요."""

import logging
import os

# pipeline.py의 @traceable은 LLM이 mock이어도 트레이스를 실제로 전송한다 — 테스트가
# LangSmith 월 한도를 소진한다(2026-08-04 초과 확인). app import보다 먼저 꺼야 하고,
# app.config의 load_dotenv()는 override=False라 여기서 박아두면 .env가 못 덮는다.
# setdefault라 `LANGSMITH_TRACING=true pytest`로 그 실행만 켤 수 있다.
os.environ.setdefault("LANGSMITH_TRACING", "false")
os.environ.setdefault("LANGCHAIN_TRACING_V2", "false")

from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from app.config import get_settings
from app.core.schemas import (
    DetectionAlert,
    DetectionConfidence,
    DetectionStats,
    Evidence,
    LinkedCSInquiry,
    RecommendedAction,
    RootCause,
    SourceSignals,
    Verdict,
)


class _PortOnly(BaseModel):
    """`MQ_PORT=abc` 를 흉내내기 위한 최소 모델 — 진짜 `ValidationError` 를 얻는다."""

    port: int


def bad_log_level_settings():
    """설정은 읽히는데 **레벨 값이 틀린** 경우 — 진짜 `basicConfig` 가 던진다.

    진입점의 부팅 가드(`app/core/logging_setup.configure_logging_or_exit`)가 덮는 실패는
    둘인데 이게 그 하나다. `monkeypatch.setattr(logging_setup, "get_settings", ...)` 로
    끼워 쓴다.

    ⚠️ **여기(conftest)에 둔 이유** — 진입점이 셋이라 같은 가짜가 테스트 파일 3개에
       복제될 참이었다. `Settings` 검증이 바뀌면 세 곳을 같이 고쳐야 하는데, 한 곳만
       고치면 **나머지 둘은 조용히 옛 실패 모드를 재는** 테스트가 된다.
    """
    return SimpleNamespace(log_level="info")


def unloadable_settings():
    """**설정 로딩 자체가 실패**하는 경우 — 부팅 가드가 덮는 다른 갈래.

    `ValidationError` 는 `ValueError` 하위라 같은 `except` 에 걸린다.
    """
    return _PortOnly(port="abc")


def pin_settings(monkeypatch, log_level: str = "INFO") -> None:
    """진입점의 부팅 가드가 **개발자 환경을 안 타게** 못박는다.

    🔴 **이게 없으면 `LOG_LEVEL=info` 가 걸린 머신에서 무관한 테스트가 빨개진다** —
       `main()` 이 설정 오류로 exit 2 하고, 화면에는 *"인코딩 배선이 깨졌다"* 처럼 보인다.
       PR #79 의 `.env` 의존 사고와 같은 계열이고, **개발자 로컬이 곧 CI 인 구조**라
       사람마다 결과가 갈린다.

    🔴 **root 핸들러는 일부러 안 비운다.** 비우면 pytest 의 `caplog` 핸들러가 사라져서
       **로그는 나가는데 `caplog.records` 가 비는** 상태가 된다 — #96 에서 `force=True`
       로 밟았던 그 함정이고, 여기서도 컨슈머 테스트 3개가 같은 모양으로 깨졌다(실측).
       비우는 것이 필요한 쪽은 *"잘못된 레벨로 `basicConfig` 를 **실제로** 터뜨리는"*
       테스트뿐이라, 그건 각 테스트가 직접 한다.

    ⚠️ `main()` 을 부르는 테스트는 **전부** 이걸 써야 한다. 안 쓰면 "그 머신에서만"
       실패해서 원인을 엉뚱한 데서 찾게 된다.
    """
    from app.core import logging_setup

    monkeypatch.setattr(
        logging_setup, "get_settings", lambda: SimpleNamespace(log_level=log_level)
    )


@pytest.fixture(autouse=True)
def restore_root_logger():
    """root 로거 상태(레벨·핸들러)를 테스트마다 되돌린다.

    🔴 **왜 필요한가 — 실제로 다른 파일 테스트 2개를 깨뜨렸다**(2026-08-16 실측).
       진입점의 `configure_logging_or_exit()`(`app/core/logging_setup.py`)이 `LOG_LEVEL`
       을 **항상** 적용하도록 바뀌면서, 그걸 태우는 테스트가 root 레벨을 바꾼 채 끝나면
       **뒤에 도는 테스트의 `caplog` 가 조용히 달라진다.** 레벨이 올라간 상태면 기대하던
       INFO/WARNING 레코드가 아예 안 잡힌다.

    ⚠️ `monkeypatch` 로는 못 막는다 — `handlers` 리스트는 되돌려주지만 **`level` 은
       되돌리지 않는다.** 그래서 별도 픽스처가 필요하다.

    ⚠️ 실패가 **테스트 단독 실행에서는 재현되지 않는다**(그때는 앞에서 레벨을 바꾼 게
       없다). 순서에 따라 갈리는 형태라 눈에 잘 안 띈다 — 그래서 전역으로 잠근다.
    """
    root = logging.getLogger()
    level = root.level
    handlers = root.handlers[:]
    yield
    root.setLevel(level)
    root.handlers[:] = handlers


@pytest.fixture(autouse=True)
def block_local_raw_db(monkeypatch, tmp_path):
    """테스트가 **개발자 로컬의 `data/raw.db` 를 읽지 않게** 막는다.

    `fetch_linked_inquiries`(REST 경로)가 raw DB 를 직접 조회하는데, 그 파일은
    gitignore 라 **있는 사람과 없는 사람의 결과가 갈린다.** 있으면 실제 원문이 섞여
    들어오고 없으면 안 섞이는데, 둘 다 통과해 버려서 무엇을 검증한 건지 알 수 없다.
    CI 가 따로 없어 **각자 로컬이 곧 CI** 라(`block_real_s3` 와 같은 사유) 기본값을
    "없는 경로" 로 고정한다.

    DB 를 실제로 쓰는 테스트는 `db_path=` 로 자기 임시 파일을 명시해서 이 기본값을
    덮는다(`tests/test_load_inputs_from_db.py`).
    """
    # `raising=True`(기본)로 둔다 — 필드명이 바뀌면 여기서 터지는 게 맞다. False 면
    # 이 가드가 조용히 죽고 테스트가 개발자 로컬 DB 를 다시 읽기 시작한다.
    monkeypatch.setattr(get_settings(), "raw_db_path", str(tmp_path / "없는-raw.db"))


TEST_COMPANY_ID = "SLN-test"
"""테스트가 보는 회사 식별자. 실제 `.env` 값과 **일부러 다르게** 둔다 — 개발자 값이
새어 들어오면 assert 가 그 값을 통과시켜 버린다."""


@pytest.fixture(autouse=True)
def pin_company_id(monkeypatch):
    """`MQ_COMPANY_ID` 를 테스트 고정값으로 못박는다. **개발자 `.env` 를 못 보게 한다.**

    🔴 **이걸로 막는 사고**: `core/vectordb.current_tenant()` 가 이 설정을 읽어 벡터DB
       문서 ID·metadata·조회 필터의 회사 축을 만든다. 그래서 `.env` 가 있는 사람과 없는
       사람이 **다른 ID 를 보고**, 같은 테스트가 한쪽에서만 통과한다.
       실제로 PR #77 이 그렇게 통과했다 — 워크트리엔 `.env` 가 없어 폴백(`_local`)이
       나왔고, `.env` 가 있는 메인 작업 폴더에서는 `SLN-local` 이 나와 **머지 후 main 이
       빨개졌다.** CI 가 따로 없어 **각자 로컬이 곧 CI** 인 구조에서 반복되는 계열이다
       (`block_local_raw_db`·`block_real_s3` 와 같은 사유. 08-07 S3 키 9 failed,
       08-11 MQ `mq_host` 도 같은 모양이었다).

    폴백 자체를 재는 테스트는 이 값을 `""` 로 덮어 쓴다
    (`test_record_hitl_outcome.test_local_fallback_when_company_id_is_unset`).

    🔴 **`get_settings.cache_clear()` 를 부르는 테스트를 추가하면 이 가드가 조용히
       무력화된다.** 여기서 고정하는 건 `@lru_cache` 가 들고 있는 **그 인스턴스**이고
       (`app/config.get_settings`), 캐시를 비우면 다음 호출이 **새 Settings 를 만들어
       `.env` 를 다시 읽는다.** 그 순간 위 사고가 그대로 재발한다 — 실패가 아니라
       **환경에 따라 갈리는 통과**로 나타나므로 눈에 안 띈다.
       → 설정 재로딩을 재는 테스트는 `monkeypatch.setenv("MQ_COMPANY_ID", ...)` 로
       **환경변수까지 명시적으로 고정**할 것. (서영님 PR #79 사후 리뷰 지적. 현재
       저장소에 `cache_clear()` 호출은 0건이라 지금은 안전하다.)
    """
    monkeypatch.setattr(get_settings(), "mq_company_id", TEST_COMPANY_ID)


@pytest.fixture
def linked_inquiries() -> list[LinkedCSInquiry]:
    """biased_alert.evidence.inquiry_ids 에 대응하는 CS 원문.

    배치(`app/core/inquiries.py`)가 만들어 Agent3·가이드라인에 같이 넘기는 그 리스트다.
    image_guide 의 근거이자 citations 의 출처라, ID 가 alert 의 것과 어긋나면
    `validate_citations_grounded()` 가 잡는다 — 일부러 같은 ID 를 쓴다.
    """
    return [
        LinkedCSInquiry(
            item_id="INQ-000412",
            raw_text="사진이랑 색이 너무 달라요. 화면에서 본 아이보리가 아니에요.",
            created_at="2026-05-25T09:12:00",
        ),
        LinkedCSInquiry(
            item_id="INQ-000415",
            raw_text="조명 때문인지 실물 색이 훨씬 어둡습니다.",
            created_at="2026-05-26T14:03:00",
        ),
    ]


@pytest.fixture
def biased_alert() -> DetectionAlert:
    """편중형 — 색상 + 원인 명확 → 개선안 생성 트리거."""
    return DetectionAlert(
        alert_id="ALT-20260528-0001",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P001",
        channel="COUPANG",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.BIASED,
        significant_channels=["COUPANG"],
        main_aspect="색상",
        sub_aspects=[
            {"aspect": "파손", "delta": 0.07, "recommended_action": "물류 점검 권장"}
        ],
        stats=DetectionStats(
            source="cs",
            cur_rate=0.13,
            past_rate=0.05,
            delta=0.08,
            p_value=0.00013,
            bh_significant=True,
            cur_total=200,
        ),
        source_signals=SourceSignals(
            cs=True, review=False, interpretation="CS 선행 신호 — 리뷰는 시차로 미반영 가능"
        ),
        root_cause=RootCause(label="사진_색감_오차", count=14, total=20, consistent=True),
        detection_confidence=DetectionConfidence.HIGH,
        scope_in=True,
        recommended_action=RecommendedAction.GENERATE_RECOMMENDATION,
        evidence=Evidence(inquiry_ids=["INQ-000412", "INQ-000415"], linked_change_id="CHG-0009"),
    )


@pytest.fixture
def global_alert() -> DetectionAlert:
    """전역형 — 상품 1건, channel=ALL, root_cause 없음."""
    return DetectionAlert(
        alert_id="ALT-20260528-0002",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P002",
        channel="ALL",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.GLOBAL,
        significant_channels=["COUPANG", "NAVER", "ZIGZAG"],
        main_aspect="파손",
        stats=DetectionStats(
            source="cs",
            cur_rate=0.20,
            past_rate=0.06,
            delta=0.14,
            p_value=0.00002,
            bh_significant=True,
            cur_total=180,
        ),
        source_signals=SourceSignals(cs=True, review=True, interpretation="강한 신호(양 소스)"),
        root_cause=None,
        detection_confidence=DetectionConfidence.NOT_APPLICABLE,
        scope_in=False,
        recommended_action=RecommendedAction.PRODUCT_CHECK,
        evidence=Evidence(inquiry_ids=["INQ-000501", "INQ-000502"]),
    )


@pytest.fixture
def indeterminate_alert() -> DetectionAlert:
    """구분불가 — 채널 표본 부족으로 편중/전역 판정 불가."""
    return DetectionAlert(
        alert_id="ALT-20260528-0003",
        detected_at="2026-05-28T10:30:00",
        product_group_id="P003",
        channel="COUPANG",
        window_start="2026-05-22",
        window_end="2026-05-28",
        verdict=Verdict.INDETERMINATE,
        significant_channels=["COUPANG"],
        excluded_channels=["NAVER", "ZIGZAG"],
        main_aspect="사이즈",
        stats=DetectionStats(
            source="cs",
            cur_rate=0.15,
            past_rate=0.06,
            delta=0.09,
            p_value=0.0009,
            bh_significant=True,
            cur_total=40,
        ),
        source_signals=SourceSignals(
            cs=True, review=None, interpretation="CS 선행 신호 — 리뷰는 시차로 미반영 가능"
        ),
        root_cause=None,
        detection_confidence=DetectionConfidence.MEDIUM,
        scope_in=True,
        recommended_action=RecommendedAction.SCOPE_UNDETERMINED,
        evidence=Evidence(inquiry_ids=["INQ-000601"]),
    )
