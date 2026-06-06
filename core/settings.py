from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"
    data_dir: str = "./data"

    postgres_url: str = "postgresql://user:password@localhost:5432/appdb"
    postgres_pool_min_size: int = 2
    postgres_pool_max_size: int = 10

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"

    redis_url: str = "redis://localhost:6379/0"

    flight_host: str = "localhost"
    flight_port: int = 8815          # pyarrow Flight default
    flight_ticket: str = "items"
    lsm_flush_rows: int = 1000       # memtable -> run threshold
    lsm_compaction_runs: int = 4     # run count -> compaction threshold


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
