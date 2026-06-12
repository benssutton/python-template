from httpx import AsyncClient

from persistence.transaction_store.postgres.postgres_client import PostgresClient
from settings import Settings


async def test_postgres_client_aexit_without_pool_is_noop():
    """__aexit__ must not raise when called on an instance that never entered."""
    client = PostgresClient(Settings())
    await client.__aexit__(None, None, None)


async def test_get_config_returns_list(test_client: AsyncClient):
    """Config endpoint always returns a JSON list (may not be empty)."""
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


async def test_post_config_creates_entry(test_client: AsyncClient):
    key = "test_creates_key"
    response = await test_client.post("/config/", json={"key": key, "value": "staging"})
    assert response.status_code == 201
    assert response.json() == {"key": key, "value": "staging"}


async def test_post_config_upserts_on_duplicate_key(test_client: AsyncClient):
    key = "test_upsert_key"
    await test_client.post("/config/", json={"key": key, "value": "staging"})
    response = await test_client.post("/config/", json={"key": key, "value": "production"})
    assert response.status_code == 201
    assert response.json() == {"key": key, "value": "production"}


async def test_get_config_returns_all_entries(test_client: AsyncClient):
    key = "test_list_key"
    await test_client.post("/config/", json={"key": key, "value": "value1"})
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": key, "value": "value1"} in response.json()
