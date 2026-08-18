"""환경변수 로딩. 코드에 API 키·호스트를 하드코딩하지 말고 전부 여기를 거칠 것.

load_dotenv()가 필요한 이유: pydantic-settings의 env_file 로딩은 .env를 읽어서
Settings 객체 안에만 넣어줄 뿐 os.environ엔 안 넣는다. langsmith 같은 서드파티
SDK는 os.environ을 직접 읽으므로, 그런 라이브러리도 .env 값을 보게 하려면
os.environ에 실제로 채워 넣는 load_dotenv()가 따로 필요하다.
"""

from functools import lru_cache

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


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
    # ⚠️ **지금은 파일 경로 전용이다.** `core/raw_db.connect_readonly()` 가
    #    `Path(...).exists()` 를 먼저 보므로 Postgres DSN 을 넣으면 `FileNotFoundError`
    #    가 난다. 운영 DB 로 옮길 때는 이 값의 타입이 아니라 **연결 함수를** 바꿔야
    #    한다(sqlite3 → psycopg). 그때 `?mode=ro` 도 DB 권한으로 대체된다.
    raw_db_path: str = "./data/raw.db"

    # --- 앱 ---
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """앱 전역에서 이 함수로만 설정을 가져온다 (프로세스당 1회 로딩)."""
    return Settings()
