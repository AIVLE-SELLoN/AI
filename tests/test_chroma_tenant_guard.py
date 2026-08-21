"""`config._check_chroma_tenant` — 운영 Chroma 를 보는데 회사 축이 비었을 때 부팅에서 막는다.

이 가드가 없던 동안 매니페스트의 웹·컨슈머 두 곳에서 `MQ_COMPANY_ID` 가 빠진 채로 작성됐고
(2026-08 인프라 문의로 발견), 그 상태는 **예외도 경고도 없이** 돌았다 — 조회는 `_local`
테넌트로 0 건이 되고, 컨슈머의 HITL 이력은 아무도 읽을 수 없는 테넌트에 쌓인다.

같은 값을 `mq._publish` 는 이미 막고 있었다. 이 파일이 고정하는 것은 **그 비대칭의 해소**다.
"""

from __future__ import annotations

import pytest

from app.config import LOCAL_CHROMA_HOSTS, Settings

CLUSTER_CHROMA = "chromadb.default.svc.cluster.local"
"""운영 매니페스트가 넣는 값(`fastapi/core/00-web-config.yaml` 등)."""

# `MQ_COMPANY_ID` 를 지우는 것만으로는 부족하다 — 아래 `_settings` 주석 참고.
_BLOCKED_ENV = ("MQ_COMPANY_ID", "CHROMA_HOST", "CHROMA_PORT", "CHROMA_PERSIST_DIR")


def _settings(monkeypatch, **overrides) -> Settings:
    """`.env` 와 `os.environ` 을 **차단하고** 넘긴 값만으로 Settings 를 만든다.

    `_env_file=None` 만으로는 안 막힌다. `app.config.load_dotenv()` 가 import 시점에
       `.env` 를 **`os.environ` 에도** 넣기 때문에, `MQ_COMPANY_ID` 를 `.env` 에 둔
       개발자에게만 이 테스트가 조용히 통과한다(실제로 이 가드를 검증하다 한 번 밟았다).
       `tests/test_raw_db._settings` 가 같은 이유로 같은 형태를 쓴다.
    """
    for key in _BLOCKED_ENV:
        monkeypatch.delenv(key, raising=False)
    return Settings(_env_file=None, **overrides)


def test_cluster_chroma_without_company_id_is_blocked(monkeypatch):
    """운영 Chroma + 빈 회사 축 = 부팅 실패.

    매니페스트에서 이 키가 빠졌을 때의 조합 그대로다. 통과시키면 조회 0 건·HITL 유실이
    **조용히** 일어난다.
    """
    with pytest.raises(ValueError) as exc:
        _settings(monkeypatch, chroma_host=CLUSTER_CHROMA)

    message = str(exc.value)
    # 어느 키를 채워야 하는지가 문구에 있어야 한다 — 없으면 운영자가 원인을 못 찾는다.
    assert "MQ_COMPANY_ID" in message
    # 값이 아니라 **이름**이 헷갈리는 자리라, 용도를 문구가 밝혀야 한다.
    assert "벡터DB" in message


@pytest.mark.parametrize(
    "overrides, why",
    [
        (
            {"chroma_host": CLUSTER_CHROMA, "mq_company_id": "SLN-0000000001"},
            "회사 축이 채워져 있으면 운영 호스트여도 정상",
        ),
        (
            {"chroma_host": CLUSTER_CHROMA, "chroma_persist_dir": ".chroma"},
            "파일 모드는 로컬이라 회사 축이 없어도 된다",
        ),
        (
            {},
            "CHROMA_HOST 미설정 → 기본값 localhost. 분류 워커·월간 리포트가 이 경우다",
        ),
    ],
)
def test_allowed_combinations(monkeypatch, overrides, why):
    """면제 조건 셋. **이게 없으면 가드가 과하게 잡는 걸 못 본다.**

    특히 세 번째가 중요하다 — Chroma 를 아예 안 쓰는 워크로드(분류 워커)까지 막으면
    이 가드가 배포를 깨는 쪽이 된다.
    """
    assert _settings(monkeypatch, **overrides) is not None, why


@pytest.mark.parametrize("host", sorted(LOCAL_CHROMA_HOSTS))
def test_every_local_host_is_exempt(monkeypatch, host):
    """`LOCAL_CHROMA_HOSTS` 의 항목은 전부 실제로 면제되어야 한다.

    목록에 넣었는데 판정에서 빠지면(대소문자·공백 처리 누락 등) 로컬 개발이 막힌다.
    """
    assert _settings(monkeypatch, chroma_host=host) is not None


def test_host_matching_ignores_case_and_space(monkeypatch):
    """`.env` 에서 흔한 대문자·앞뒤 공백을 로컬로 인정한다 — `is_local_broker_host` 와 같다."""
    assert _settings(monkeypatch, chroma_host="  LocalHost  ") is not None


def test_chromadb_is_not_treated_as_local(monkeypatch):
    """`chromadb` 는 로컬이 아니다 — **운영 k8s Service 이름**이다.

    브로커 쪽 `LOCAL_BROKER_HOSTS` 에는 `rabbitmq` 가 있어서 대칭으로 넣고 싶어지는데,
    그건 compose 서비스명이라 사정이 다르다. Chroma 는 compose 에 서비스가 없고
    (로컬은 `CHROMA_PERSIST_DIR` 파일 모드), 반대로 `chromadb` 를 로컬로 인정하면
    `default` 네임스페이스 파드가 짧은 이름으로 붙을 때 운영을 로컬로 오인한다.
    """
    assert "chromadb" not in LOCAL_CHROMA_HOSTS
    with pytest.raises(ValueError):
        _settings(monkeypatch, chroma_host="chromadb")


def test_the_guard_matches_what_mq_already_enforces():
    """같은 값을 두 곳이 **같은 방향으로** 다루는지 고정한다.

    이 단언이 이 PR 의 전제다 — `mq._publish` 는 빈 `mq_company_id` 로 발행을 거부한다.
       그쪽 사유(*"회사 미상 행은 복구할 단서가 없다"*)가 벡터DB 에도 적용되는데 거기만
       조용히 넘어가던 것이 이번에 고친 비대칭이다. 한쪽 정책이 느슨해지면 여기서 깨진다.
    """
    from app.core import mq, vectordb

    source = mq.__loader__.get_source(mq.__name__)
    assert "if not settings.mq_company_id:" in source, "MQ 쪽 차단이 사라졌습니다"
    # 벡터DB 는 같은 값을 테넌트 축으로 쓴다 — 이 연결이 끊기면 가드의 의미가 없다.
    assert "mq_company_id" in vectordb.__loader__.get_source(vectordb.__name__)
