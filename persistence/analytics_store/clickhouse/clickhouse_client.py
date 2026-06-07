import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from settings import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        self._client = await clickhouse_connect.get_async_client(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            username=self._settings.clickhouse_user,
            password=self._settings.clickhouse_password,
            database=self._settings.clickhouse_database,
        )
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
