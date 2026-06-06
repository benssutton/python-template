from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"
    data_dir: str = "./data"

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/appdb"
    db_pool_size: int = 5
    db_pool_max_overflow: int = 10

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
