from httpx import AsyncClient

from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from settings import Settings


async def test_clickhouse_client_aexit_without_client_is_noop():
    """__aexit__ must not raise when called on an instance that never entered."""
    client = ClickHouseClient(Settings())
    await client.__aexit__(None, None, None)


async def test_get_data(test_client: AsyncClient):
    response = await test_client.get("/data")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["rows"]) == 3
    assert body["limit"] == 10
