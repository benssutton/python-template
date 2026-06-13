
async def test_liveness_returns_alive(test_client):
    response = await test_client.get("/health/live")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "alive"
    assert body["uptime_seconds"] >= 0.0


async def test_status_reports_app_and_system(test_client):
    """status is overridden to 'testing' in the test fixtures."""
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    body = response.json()
    assert body["app"]["status"] == "testing"
    assert body["uptime"]["process_seconds"] >= 0.0
    assert body["system"]["process"]["memory_rss_bytes"] > 0
    assert isinstance(body["dependencies"], list)
    assert body["ingest"]["transport"] == "flight"


async def test_root_returns_non_empty_json(test_client):
    response = await test_client.get("/")
    assert response.status_code == 200
    assert response.json()
