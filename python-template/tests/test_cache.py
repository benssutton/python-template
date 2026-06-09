from httpx import AsyncClient


async def test_post_cache_without_ttl(test_client: AsyncClient):
    response = await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    assert response.status_code == 201
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


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


async def test_get_cache_returns_entry(test_client: AsyncClient):
    await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    response = await test_client.get("/cache/foo")
    assert response.status_code == 200
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


async def test_get_cache_missing_key_returns_404(test_client: AsyncClient):
    response = await test_client.get("/cache/nonexistent")
    assert response.status_code == 404


async def test_post_cache_with_dict_value(test_client: AsyncClient):
    payload = {"key": "json_dict", "value": {"nested": "object", "count": 42}}
    response = await test_client.post("/cache/", json=payload)
    assert response.status_code == 201
    body = response.json()
    assert body["key"] == "json_dict"
    assert body["value"] == {"nested": "object", "count": 42}
    assert body["ttl_seconds"] is None

    get_response = await test_client.get("/cache/json_dict")
    assert get_response.status_code == 200
    assert get_response.json()["value"] == {"nested": "object", "count": 42}


async def test_post_cache_with_list_value(test_client: AsyncClient):
    payload = {"key": "json_list", "value": [1, "two", {"three": 3}]}
    response = await test_client.post("/cache/", json=payload)
    assert response.status_code == 201
    assert response.json()["value"] == [1, "two", {"three": 3}]

    get_response = await test_client.get("/cache/json_list")
    assert get_response.status_code == 200
    assert get_response.json()["value"] == [1, "two", {"three": 3}]

