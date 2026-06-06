from httpx import AsyncClient


async def test_get_data(test_client: AsyncClient):
    response = await test_client.get("/data")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["rows"]) == 3
    assert body["limit"] == 10
