"""환경변수 로딩. 코드에 API 키·호스트를 하드코딩하지 말고 전부 여기를 거칠 것.

load_dotenv()가 필요한 이유: pydantic-settings의 env_file 로딩은 .env를 읽어서
Settings 객체 안에만 넣어줄 뿐 os.environ엔 안 넣는다. langsmith 같은 서드파티
SDK는 os.environ을 직접 읽으므로, 그런 라이브러리도 .env 값을 보게 하려면
os.environ에 실제로 채워 넣는 load_dotenv()가 따로 필요하다.
"""

import os
from functools import lru_cache
from typing import Self

from dotenv import load_dotenv
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

RAW_DB_SSLMODES = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)
"""libpq 가 받는 `sslmode` 값 전부. 오타를 **부팅에서** 걸러 접속 시점으로 미루지 않는다."""

RAW_DB_MIN_CONNECT_TIMEOUT = 2
"""libpq 가 실제로 존중하는 `connect_timeout` 최솟값. **그 아래는 다른 뜻이 된다.**

blackhole IP(`10.255.255.1`) 실측, libpq 18:

    connect_timeout=0    130.0초   ← 미지정과 같다. 0·음수는 "무한 대기" 다
    connect_timeout=1      2.1초   ← 조용히 2 로 올라간다
    connect_timeout=2      2.0초
    connect_timeout=3      3.0초

🔴 첫 줄이 이 상수가 있는 이유다 — "상한을 두지 않겠다" 는 뜻으로 0 을 적으면 **이 PR 이
   없애려는 무한 대기가 그대로 돌아온다.** 둘째 줄은 설정값과 실제가 어긋나는 자리라
   같이 막는다.
"""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- LLM ---
    llm_api_key: str = ""
    llm_model: str = "gpt-4o-mini"
    cause_llm_model: str = "gpt-4o"
    llm_timeout_seconds: int = 60

    # --- 벡터DB ---
    chroma_host: str = "localhost"
    chroma_port: int = 8000
    # 값이 있으면 로컬 파일 모드(PersistentClient), 없으면 HTTP 모드(HttpClient)
    chroma_persist_dir: str = ""

    # --- RabbitMQ (docs/mq_events.md) ---
    # 접속 정보는 백엔드 대기 중(C1)이라 빈 기본값이다. mq_enabled 가 꺼져 있으면
    # 발행 함수가 예외를 던진다 — 안 보낸 메시지를 성공으로 기록하지 않기 위해서다.
    # 로컬 검증은 docker-compose 의 rabbitmq 로 한다(.env.example 참고).
    mq_enabled: bool = False
    mq_host: str = ""
    mq_port: int = 5672
    mq_user: str = ""
    mq_password: str = ""
    mq_vhost: str = "/app"
    # Envelope 의 companyId. 백엔드가 회사 구분용으로 추가했고 "하드코딩으로 박아두라"고
    # 했다(MQ 컨벤션 §3). 빈 값으로 발행하면 백엔드 DB 에 회사 미상 행이 쌓이고
    # 나중에 되돌리기 어려우므로, 비어 있으면 발행을 막는다.
    mq_company_id: str = ""
    mq_exchange: str = "app.events"
    mq_publish_timeout_seconds: int = 10
    # exchange·큐를 우리가 만들지, 이미 있는 걸 쓸지. **기본은 안 만든다.**
    # 운영 토폴로지(app.events · ai.inbound)는 백엔드 인프라가 소유하고 quorum·DLX·TTL
    # 설정이 붙어 있다. 우리가 다른 인자로 선언하면 PRECONDITION_FAILED 로 거부당해
    # 아예 못 뜬다. 로컬 docker-compose 처럼 아무것도 없는 환경에서만 true 로 켠다.
    mq_declare_topology: bool = False

    # --- raw DB (「Raw DB 스키마 확정 (8/7)」) ---
    # AI 노드는 원본 DB 에 **직접 읽기** 권한이 있다(§5-2) — 서비스 DB 만 main server
    # 경유다. 목 파이프라인에서는 `scripts/mock_producer.py` 가 만든 sqlite 파일이고,
    # 쓰는 쪽(`scripts/`)은 같은 이름의 환경변수를 직접 읽으므로 키 이름을 바꾸면
    # 양쪽이 갈린다.
    #
    # ⚠️ **이 값은 파일 경로 전용이다 — 접속 정보를 넣지 말 것.**
    #    `core/raw_db.connect_readonly()` 가 `Path(...).exists()` 를 먼저 보므로 접속
    #    문자열은 `FileNotFoundError` 로 떨어진다. Postgres 는 아래 원자값을 쓴다.
    raw_db_path: str = "./data/raw.db"

    # 운영 raw DB(Postgres `rawdb`) 접속 정보. **`RAW_DB_HOST` 가 비면 위 sqlite 를 쓴다.**
    #
    # 🔴 **커넥션 문자열 한 벌이 아니라 원자값으로 나눠 받는다**(2026-08-18 확정).
    #    백엔드 Spring 이 같은 Secret 을 읽고 자기 JDBC URL 을 조립하는데, 형식을 공유하면
    #    **그쪽 드라이버 설정 하나가 우리 장애가 된다** — pgjdbc 전용 인자
    #    (`reWriteBatchedInserts`·`currentSchema`·`prepareThreshold`·`loggerLevel`)가 URL 에
    #    붙는 순간 libpq 가 `invalid URI query parameter` 로 접속을 거부한다(실측). 일부만
    #    통과하는 게 더 나쁘다 — 한동안 잘 돌다가 남의 yaml 한 줄로 우리 배치가 죽는다.
    #    → **공유하는 것은 형식이 아니라 사실이고, 조립은 각자 한다.**
    #
    # 🔴 기본값이 빈 문자열인 것이 계약이다 — 아무것도 설정하지 않은 환경(데모·팀원
    #    로컬·테스트)은 이 키들이 생기기 전과 **완전히 같은 경로**로 돈다.
    #
    # ⚠️ 읽기 전용은 **DB 권한(GRANT)이 정본**이었으나, 인프라가 AI 노드에 **RW 전면 부여**로
    #    회신했다(2026-08-18). 즉 `core/raw_db.py` 의 세션 read-only 가 이제 **유일한
    #    방어선**이다 — 로컬 superuser 용 방어선이라던 그 줄이 운영에도 해당한다.
    raw_db_host: str = ""
    raw_db_port: int = 5432
    raw_db_name: str = ""
    raw_db_username: str = ""
    raw_db_password: str = ""

    # 🔴 **`require` 가 기본값인 것이 안전장치다 — 비우면 libpq 기본값 `prefer` 로 떨어진다.**
    #    `prefer` 는 SSL 을 시도하다 **서버가 거부하면 평문으로 붙고 실패하지 않는다**
    #    (실측: `pq.Conninfo.get_defaults()` → `prefer`, libpq 18). "CA 를 안 쓴다" 를
    #    "SSL 설정을 안 한다" 로 옮기면 조용히 암호화 없이 붙는 자리다.
    #    ⚠️ 로컬 compose 의 Postgres 는 SSL 을 안 켜므로 `RAW_DB_SSLMODE=disable` 이 필요하다.
    #
    # 🔴 **`sslmode` 는 우리 전용 키다 — 백엔드와 공유하지 않는다.** pgjdbc 는 `ssl=true`,
    #    libpq 는 `sslmode=...` 로 **철자가 다르다.** 한 값을 둘이 나눠 쓰면 어느 한쪽이
    #    반드시 틀린 값을 받는다.
    raw_db_sslmode: str = "require"
    # CA 번들 경로. **비워 둔다** — `verify-*` 로 올릴 때만 채운다(2026-08-18: Spring 이
    # 이미 `require` 로 돌고 ConfigMap 에 인증서가 없어 양쪽을 맞췄다).
    raw_db_sslrootcert: str = ""

    # 접속 시도 상한(초). 🔴 **이 값이 없으면 무한 대기다** — libpq 기본값이 미지정이고
    #    (실측: `pq.Conninfo.get_defaults()` → `''`), 그때 실패는 OS 의 TCP 재시도가 끝날
    #    때까지 간다(blackhole IP 실측 **130초**). 두 호출부가 그동안 붙잡힌다:
    #      배치 — CronJob 이 자리를 잡고 앉아 있다가 `activeDeadlineSeconds` 까지 간다.
    #      REST — `service.generate_recommendation` 의 degrade 가 **발동하기 전에** 요청
    #             하나가 그만큼 매달린다(배치만의 문제가 아니다).
    #
    # ⚠️ **#101 이 닫은 것과 축이 다르다.** 그 PR 은 *"예외를 어떻게 분류하나"* 를 고쳤고
    #    이건 *"얼마나 매달리나"* 다. 130초를 기다린 뒤에도 결국 `OperationalError` 라
    #    분류는 그때도 정상 동작했다 — 그래서 그 PR 로는 안 닫혔다.
    #
    # ⚠️ **짧을 때의 대가가 더 크다** — 배치가 그날 통째로 안 돌거나(데이터 갭) REST 가
    #    CS 원문 없이 개선안을 만든다. 반대로 길 때 잃는 것은 시간뿐이다. 그래서 기대
    #    지연(VPC 안 수십 ms)의 수백 배로 넉넉히 잡는다 — **무한만 없애면 목적은 달성된다.**
    #
    # ⚠️ 이건 **접속** 상한이지 질의 상한이 아니다. 붙은 뒤 질의가 멎는 경우는 별건이다
    #    (`statement_timeout`). 지금 우리 조회는 인덱스가 있는 35일 범위라 범위 밖으로 둔다.
    raw_db_connect_timeout: int = 10

    # --- 앱 ---
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_raw_db(self) -> Self:
        """raw DB 접속 원자값이 **서로 맞는지** 부팅에서 본다.

        🔴 **왜 접속 시점이 아니라 여기인가.** 전부 재시작해도 안 낫는 설정 오류이고,
           런타임까지 미루면 libpq 메시지가 원인을 안 알려준다. 여기서 걸리면 진입점의
           `configure_logging_or_exit()` 이 사유 한 줄 + **exit 2** 로 끝낸다.

           CA 누락이 특히 그렇다 — 가드를 우회하고 SSL 켠 Postgres 에 실제로 붙여 보면
           libpq 는 **아무도 설정한 적 없는 경로**를 대며 죽는다(실측):

               root certificate file "…/AppData/Roaming/postgresql/root.crt" does not exist

           `sslrootcert` 를 비우면 libpq 가 저 기본 경로로 폴백하기 때문인데, 보안을
           조이려고 `verify-full` 만 켠 사람에게 저 문장은 원인을 안 알려준다.
           ⚠️ 반대로 CA 를 제대로 주면 붙는다(`verify-ca`·`verify-full` 둘 다 실측) —
              즉 막히는 건 **설정 조합**이지 `verify-*` 자체가 아니다.

        ⚠️ **`RAW_DB_HOST` 가 비어 있으면 아무것도 안 본다.** sqlite 데모·팀원 로컬·테스트가
           이 검사에 걸리면 안 된다 — 그게 위 "설정을 안 건드리면 이전과 같다" 계약이다.

        ⚠️ 비밀번호는 검사하지 않는다. 빈 값이 정상인 접속 방식이 있고(trust·`.pgpass`),
           틀린 비밀번호는 어차피 부팅에서 알 수 없다.

        ⚠️ 메시지에 **값을 싣지 않는다.** `logging_setup._describe()` 가 `loc`·`msg` 만
           찍어서 `raw_db_password` 가 로그로 새는 것을 이미 막고 있는데, 여기서 값을
           문장에 넣으면 그 방어선을 우회한다.
        """
        # 🔴 **폐기된 `RAW_DB_DSN` 이 남아 있으면 세운다 — 이게 제일 조용한 사고다.**
        #    `extra="ignore"` 라 그 키는 **아무 말 없이 무시된다.** 그런데 직전
        #    `.env.example` 이 이식 검증 절차로 *"RAW_DB_DSN 주석 해제 → 검증 → 다시 주석"*
        #    을 안내했으므로 **남겨둔 사람이 반드시 있다.** 그 사람은 Postgres 를 본다고
        #    믿으면서 sqlite 를 읽는다 — `tests/conftest.block_local_raw_db` 가 *"떠 있으면
        #    엉뚱한 데이터로 통과한다, 후자가 더 나쁘다"* 라고 적어둔 바로 그 모양이고,
        #    이번엔 방향만 반대다.
        #    ⚠️ 호스트 게이트 **밖**이다 — 피해자가 정확히 "원자값을 아직 안 넣은 사람" 이라
        #       게이트 안에 두면 아무도 못 본다.
        if os.environ.get("RAW_DB_DSN"):
            raise ValueError(
                "RAW_DB_DSN 은 더 이상 쓰지 않습니다 — 그대로 두면 조용히 무시되고"
                " sqlite 를 읽습니다. RAW_DB_HOST·RAW_DB_PORT·RAW_DB_NAME·"
                "RAW_DB_USERNAME·RAW_DB_PASSWORD 로 나눠 적고 그 줄은 지우세요"
            )

        if not self.raw_db_host:
            return self

        missing = [
            name
            for name, value in (
                ("RAW_DB_NAME", self.raw_db_name),
                ("RAW_DB_USERNAME", self.raw_db_username),
            )
            if not value
        ]
        if missing:
            # 없으면 libpq 가 **OS 사용자 이름**으로 채운다 — 컨테이너에서는 `root` 라
            # `database "root" does not exist` 로 죽고, 원인이 메시지에 안 드러난다.
            raise ValueError(
                f"RAW_DB_HOST 를 설정했으면 {'·'.join(missing)} 도 있어야 합니다"
            )

        if self.raw_db_sslmode not in RAW_DB_SSLMODES:
            raise ValueError(
                "RAW_DB_SSLMODE 가 libpq 값이 아닙니다"
                f" (가능: {', '.join(sorted(RAW_DB_SSLMODES))})"
            )

        if self.raw_db_sslmode.startswith("verify-") and not self.raw_db_sslrootcert:
            raise ValueError(
                f"RAW_DB_SSLMODE={self.raw_db_sslmode} 는 CA 번들이 필요합니다 —"
                " RAW_DB_SSLROOTCERT 를 채우거나 sslmode 를 require 로 두세요"
            )

        # 🔴 **0 을 "상한 없음" 으로 읽는 사람을 막는다.** libpq 에서 0·음수는 무한 대기라
        #    (실측) 그렇게 적으면 이 키가 생기기 전 상태로 조용히 되돌아간다 — 값이 있으니
        #    설정한 사람은 상한이 걸렸다고 믿는다. `RAW_DB_MIN_CONNECT_TIMEOUT` 참고.
        if self.raw_db_connect_timeout < RAW_DB_MIN_CONNECT_TIMEOUT:
            raise ValueError(
                "RAW_DB_CONNECT_TIMEOUT 은"
                f" {RAW_DB_MIN_CONNECT_TIMEOUT} 이상이어야 합니다 — libpq 에서 0·음수는"
                " 무한 대기를 뜻하고 1 은 조용히 2 로 올라갑니다"
            )

        return self


@lru_cache
def get_settings() -> Settings:
    """앱 전역에서 이 함수로만 설정을 가져온다 (프로세스당 1회 로딩)."""
    return Settings()
