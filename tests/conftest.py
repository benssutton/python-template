import asyncio
from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient, ASGITransport

from core.dependencies import get_health_service, get_data_service
from core.settings import Settings
from services.health import HealthService
from services.data import DataService

@pytest.fixture(scope="session")
def test_settings():
    test_settings = Settings(status="testing",
                             data_dir="./tests/test_data")
    return test_settings

@pytest.fixture(scope="session")
def override_health_service(test_settings):
    health_service = HealthService(test_settings)
    yield health_service

@pytest.fixture(scope="session")
def override_data_service(test_settings):
    health_service = DataService(test_settings)
    yield health_service

@pytest.fixture(scope="session")
async def test_client(override_health_service, override_data_service):
    from main import app, mcp

    app.dependency_overrides[get_health_service] = lambda: override_health_service
    app.dependency_overrides[get_data_service] = lambda: override_data_service

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