import pytest
from httpx import AsyncClient


async def test_get_count_returns_total(test_client: AsyncClient):
    response = await test_client.get("/data/count")
    assert response.status_code == 200
    assert response.json() == {"count": 3}


async def test_get_rows_returns_all(test_client: AsyncClient):
    response = await test_client.get("/data/rows")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["rows"]) == 3
    assert body["limit"] == 10
    assert body["offset"] == 0


async def test_get_rows_with_limit(test_client: AsyncClient):
    response = await test_client.get("/data/rows?limit=2")
    assert response.status_code == 200
    body = response.json()
    assert len(body["rows"]) == 2
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 0


async def test_get_rows_with_offset(test_client: AsyncClient):
    response = await test_client.get("/data/rows?limit=2&offset=2")
    assert response.status_code == 200
    body = response.json()
    assert len(body["rows"]) == 1
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 2
