from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"
    data_dir: str = "./data"

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/appdb"