from httpx import AsyncClient


async def test_post_cache_creates_entry(test_client: AsyncClient):
    response = await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    assert response.status_code == 201
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


async def test_get_cache_returns_entry(test_client: AsyncClient):
    await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    response = await test_client.get("/cache/foo")
    assert response.status_code == 200
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


async def test_get_cache_missing_key_returns_404(test_client: AsyncClient):
    response = await test_client.get("/cache/nonexistent")
    assert response.status_code == 404


async def test_post_cache_with_ttl(test_client: AsyncClient):
    response = await test_client.post("/cache/", json={"key": "expiring", "value": "soon", "ttl_seconds": 60})
    assert response.status_code == 201
    assert response.json() == {"key": "expiring", "value": "soon", "ttl_seconds": 60}

    get_response = await test_client.get("/cache/expiring")
    assert get_response.status_code == 200
    body = get_response.json()
    assert body["key"] == "expiring"
    assert body["value"] == "soon"
    assert body["ttl_seconds"] is not None
    assert 0 < body["ttl_seconds"] <= 60
