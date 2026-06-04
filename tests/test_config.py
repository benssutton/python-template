import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_config_returns_empty_list(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_post_config_creates_entry(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "staging"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "staging"}


@pytest.mark.asyncio
async def test_post_config_upserts_on_duplicate_key(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "production"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "production"}


@pytest.mark.asyncio
async def test_get_config_returns_all_entries(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": "env", "value": "production"} in response.json()
