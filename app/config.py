"""환경변수 로딩. 코드에 API 키·호스트를 하드코딩하지 말고 전부 여기를 거칠 것."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


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
