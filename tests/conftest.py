from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest
import pyarrow.ipc as pa_ipc
from httpx import AsyncClient, ASGITransport
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer

from core.container import service_container
from core.dependencies import get_health_service, get_data_service, get_config_service
from core.settings import Settings
from services.config import ConfigService
from services.health import HealthService
from services.data import DataService
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"


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
    schema_sql = (Path(__file__).parent.parent / "scripts" / "clickhouse-init.sql").read_text()
    async with ClickHouseClient(ch_settings) as client:
        await client.command(schema_sql)
        with pa_ipc.open_file(Path(__file__).parent / "data" / "items.arrow") as reader:
            arrow_table = reader.read_all()
        await client.insert_arrow("default.items", arrow_table)
        yield client


# ── Postgres fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(PG_IMAGE) as pg:
        yield pg


@pytest.fixture(scope="session")
async def postgres_pool(postgres_container):
    pool = await asyncpg.create_pool(
        host="localhost",
        port=int(postgres_container.get_exposed_port(5432)),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
        ssl=False,
    )
    schema_sql = (Path(__file__).parent.parent / "scripts" / "postgres-init.sql").read_text()
    async with pool.acquire() as conn:
        await conn.execute(schema_sql)
    yield pool
    await pool.close()


# ── App client ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
async def override_data_service(test_clickhouse_client):
    yield DataService(test_clickhouse_client)


@pytest.fixture(scope="session")
async def override_config_service(postgres_pool):
    yield ConfigService(postgres_pool)


@pytest.fixture(scope="session")
async def test_client(override_health_service, override_data_service, override_config_service):
    from main import app, mcp

    app.dependency_overrides[get_health_service] = lambda: override_health_service
    app.dependency_overrides[get_data_service] = lambda: override_data_service
    app.dependency_overrides[get_config_service] = lambda: override_config_service

    service_container.register_singleton(HealthService, override_health_service)
    service_container.register_singleton(DataService, override_data_service)
    service_container.register_singleton(ConfigService, override_config_service)

    @asynccontextmanager
    async def test_lifespan():
        async with mcp.session_manager.run():
            yield

    async with test_lifespan():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
