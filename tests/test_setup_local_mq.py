"""담당: 지인 — `scripts/setup_local_mq.py` 의 로컬 브로커 가드.

이 스크립트는 **백엔드 소유 큐**(`main.inbound`·`ai.inbound`)를 만든다. 로컬 대역용이라
맞지만, 운영 브로커를 향해 돌면 남의 토폴로지를 우리 인자로 선점한다. 그 사고는 조용하다
— 큐가 만들어지고 스크립트는 성공으로 끝나며, 백엔드가 나중에 정상 토폴로지를 올릴 때
`PRECONDITION_FAILED` 로 처음 드러난다.

브로커에 붙지 않는다 — 가드 함수만 직접 부른다.
"""

from types import SimpleNamespace

import pytest

from scripts.setup_local_mq import LOCAL_BROKER_HOSTS, assert_local_broker


def _settings(host: str, declare: bool = True) -> SimpleNamespace:
    return SimpleNamespace(mq_host=host, mq_declare_topology=declare)


@pytest.mark.parametrize("host", sorted(LOCAL_BROKER_HOSTS))
def test_local_hosts_pass(host):
    """로컬 호스트는 통과한다 — docker-compose 서비스명·컨테이너 안 호스트 포함.

    통과의 정의는 "`SystemExit` 이 안 난다" 다. 반환값은 없다.
    """
    assert_local_broker(_settings(host))


def test_uppercase_and_padding_are_normalized():
    """`.env` 값에 대소문자·공백이 섞여도 같은 판정이어야 한다."""
    assert_local_broker(_settings("  LocalHost  "))


@pytest.mark.parametrize(
    "host",
    ["mq.sellon.example.com", "10.0.1.20", "sellon-rabbitmq.prod.internal"],
)
def test_remote_host_is_refused_even_when_flag_is_on(host):
    """🔴 **플래그가 켜져 있어도** 로컬이 아니면 거부한다.

    예전 가드는 `MQ_DECLARE_TOPOLOGY` 하나만 봤다. 그 플래그는 "우리가 토폴로지를
    만든다" 는 뜻이지 "여기가 로컬이다" 가 아니다 — 운영 접속정보(C1)를 넣으면서
    플래그를 같이 안 내리면 그대로 통과했다. 스크립트 자신의 docstring 이 이 구멍을
    적어두고 있었다.
    """
    with pytest.raises(SystemExit) as exc:
        assert_local_broker(_settings(host, declare=True))

    message = str(exc.value)
    assert "로컬 브로커가 아닙니다" in message
    # 운영 전환 시 같이 내려야 하는 것을 메시지가 알려줘야 한다 — 이 가드에 걸린 사람은
    # 지금 막 운영 전환을 하는 중이다.
    assert "MQ_DECLARE_TOPOLOGY=false" in message
    assert "MQ_VHOST" in message


def test_flag_off_is_still_refused():
    """플래그가 꺼져 있으면 로컬이어도 거부한다 — 기존 동작 유지."""
    with pytest.raises(SystemExit) as exc:
        assert_local_broker(_settings("localhost", declare=False))

    assert "MQ_DECLARE_TOPOLOGY=false" in str(exc.value)


def test_empty_host_is_refused():
    """빈 값(기본값)은 로컬이 아니다 — 접속이 어차피 안 되지만 여기서 먼저 세운다."""
    with pytest.raises(SystemExit):
        assert_local_broker(_settings(""))
