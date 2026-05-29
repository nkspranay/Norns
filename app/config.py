from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # PostgreSQL
    database_url: str = "postgresql+asyncpg://scheduler:scheduler123@localhost:5432/scheduler_db"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Application
    app_env: str = "development"
    secret_key: str = "your-secret-key-change-in-production"
    log_level: str = "INFO"

    # Worker
    worker_concurrency: int = 3
    job_max_retries: int = 3
    job_retry_backoff: int = 2
    heartbeat_interval: int = 10
    heartbeat_timeout: int = 30

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()