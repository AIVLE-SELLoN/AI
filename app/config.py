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
    llm_timeout_seconds: int = 60

    # --- 벡터DB ---
    chroma_host: str = "localhost"
    chroma_port: int = 8000
    # 값이 있으면 로컬 파일 모드(PersistentClient), 없으면 HTTP 모드(HttpClient)
    chroma_persist_dir: str = ""

    # --- 앱 ---
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """앱 전역에서 이 함수로만 설정을 가져온다 (프로세스당 1회 로딩)."""
    return Settings()
