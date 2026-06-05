import asyncio
from contextlib import asynccontextmanager

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from testcontainers.postgres import PostgresContainer

from core.dependencies import get_health_service, get_data_service, get_transaction_session
from core.settings import Settings
from services.health import HealthService
from services.data import DataService
from persistence.transaction_store.postgres.postgres_base import PostgresBase
import persistence.transaction_store.models.config  # noqa: F401 — registers Configuration with metadata

PG_IMAGE = "postgres:18"

@pytest.fixture(scope="session")
def test_settings():
    return Settings(status="testing", data_dir="./tests/test_data")


@pytest.fixture(scope="session")
def override_health_service(test_settings):
    yield HealthService(test_settings)


@pytest.fixture(scope="session")
def override_data_service(test_settings):
    yield DataService(test_settings)


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(PG_IMAGE) as pg:
        yield pg


@pytest.fixture(scope="session")
async def transaction_engine(postgres_container):
    url = str(make_url(postgres_container.get_connection_url()).set(drivername="postgresql+asyncpg"))
    # On Windows, asyncpg's SSPI/GSSAPI negotiation can interfere with password
    # auth. Supplying the password explicitly in connect_args bypasses SSPI.
    engine = create_async_engine(
        url, connect_args={"ssl": False, "password": postgres_container.password}
    )
    async with engine.begin() as conn:
        await conn.run_sync(PostgresBase.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def transaction_session_factory(transaction_engine):
    return async_sessionmaker(transaction_engine, expire_on_commit=False)


@pytest.fixture(scope="session")
async def test_client(override_health_service, override_data_service, transaction_session_factory):
    from main import app, mcp

    async def _get_test_transaction_session():
        async with transaction_session_factory() as session:
            yield session
            await session.commit()

    app.dependency_overrides[get_health_service] = lambda: override_health_service
    app.dependency_overrides[get_data_service] = lambda: override_data_service
    app.dependency_overrides[get_transaction_session] = _get_test_transaction_session

    @asynccontextmanager
    async def test_lifespan():
        async with mcp.session_manager.run():
            yield

    async with test_lifespan():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
