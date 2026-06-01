import asyncio
from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient, ASGITransport

from core.dependencies import get_health_service
from core.settings import Settings
from services.health import HealthService


@pytest.fixture(scope="session")
def override_health_service():
    test_settings = Settings(status="testing")
    health_service = HealthService(test_settings)
    yield health_service


@pytest.fixture(scope="session")
async def test_client(override_health_service):
    from main import app, mcp

    app.dependency_overrides[get_health_service] = lambda: override_health_service

    """In order to run tests async use httpx's AsyncClient.
       This doesn't create the lifespan in main, so will need
       to create it here"""
    @asynccontextmanager
    async def test_lifespan():
        async with mcp.session_manager.run():
            yield

    async with test_lifespan():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client