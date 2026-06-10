from httpx import AsyncClient

from persistence.transaction_store.postgres.postgres_client import PostgresClient
from settings import Settings


async def test_postgres_client_aexit_without_pool_is_noop():
    """__aexit__ must not raise when called on an instance that never entered."""
    client = PostgresClient(Settings())
    await client.__aexit__(None, None, None)


async def test_get_config_returns_empty_list(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert response.json() == []


async def test_post_config_creates_entry(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "staging"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "staging"}


async def test_post_config_upserts_on_duplicate_key(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "production"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "production"}


async def test_get_config_returns_all_entries(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": "env", "value": "production"} in response.json()
