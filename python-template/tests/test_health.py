
async def test_health_status(test_client):
    """ N.B. - the status value is defined in the Settings class
        This is overwritten to "testing" in the test fixtures in conf.py
    """
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    assert response.json() == {"status": "testing"}


async def test_root_returns_non_empty_json(test_client):
    response = await test_client.get("/")
    assert response.status_code == 200
    assert response.json()
