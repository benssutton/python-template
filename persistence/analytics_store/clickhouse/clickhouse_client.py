from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from core.settings import Settings

_client: AsyncClient | None = None


async def create_client(settings: Settings) -> AsyncClient:
    global _client
    _client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    return _client


def get_client() -> AsyncClient:
    if _client is None:
        raise RuntimeError("ClickHouse client not initialised — call create_client() in lifespan first")
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
