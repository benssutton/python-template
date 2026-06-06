import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer

from core.dependencies import get_health_service, get_data_service, get_transaction_session
from core.settings import Settings
from services.health import HealthService
from services.data import DataService
from persistence.transaction_store.postgres.postgres_base import PostgresBase
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
import persistence.transaction_store.models.config  # noqa: F401 — registers Configuration with metadata

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"

_SEED_ITEMS = [[1, "alpha", "a"], [2, "beta", "b"], [3, "gamma", "c"]]


@pytest.fixture(scope="session")
def test_settings():
    return Settings(status="testing", data_dir="./tests/test_data")


@pytest.fixture(scope="session")
def override_health_service(test_settings):
    yield HealthService(test_settings)


# ── ClickHouse fixtures ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def clickhouse_container():
    with ClickHouseContainer(CH_IMAGE, port=8123) as ch:
        yield ch


@pytest.fixture(scope="session")
async def test_clickhouse_client(clickhouse_container):
    http_port = int(clickhouse_container.get_exposed_port(8123))
    ch_settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=http_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
    )
    async with ClickHouseClient(ch_settings) as client:
        _DDL = (Path(__file__).parent.parent / "scripts" / "clickhouse-init.sql").read_text()
        await client.command(_DDL)
        await client.insert("items", _SEED_ITEMS, column_names=["id", "name", "value"])
        yield client


# ── Postgres fixtures ──────────────────────────────────────────────────────────

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


# ── App client ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
async def override_data_service(test_clickhouse_client):
    yield DataService(test_clickhouse_client)


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
