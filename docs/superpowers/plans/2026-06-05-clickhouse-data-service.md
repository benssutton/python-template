# ClickHouse Data Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the file-based IPC stream in the `/data` endpoint with ClickHouse, exposing `GET /data/count` and `GET /data/rows` as illustrative async best-practice endpoints.

**Architecture:** A `clickhouse-connect` async client is created during the FastAPI lifespan, stored as a module-level singleton in `persistence/analytics_store/clickhouse/clickhouse_client.py`, and injected into `DataService` which is registered in the DI Container. Tests use a session-scoped `testcontainers` ClickHouse container (HTTP port 8123), seed an `items` table, and override `DataService` via `dependency_overrides`.

**Tech Stack:** `clickhouse-connect[async]` (official ClickHouse async Python client via aiohttp), `testcontainers[clickhouse]`, FastAPI `Query` parameter validation.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `requirements.txt` | Modify | Add `clickhouse-connect[async]`, `testcontainers[clickhouse]` |
| `core/settings.py` | Modify | Add 5 ClickHouse connection fields |
| `persistence/analytics_store/clickhouse/clickhouse_client.py` | Create | Async client lifecycle: `create_client`, `get_client`, `close_client` |
| `schemas/data.py` | Modify | Replace `DataShapeResponse` with `DataCountResponse`, `DataRowResponse`, `DataRowsResponse` |
| `services/data.py` | Rewrite | Async `DataService`: `get_count()`, `get_rows(limit, offset)` |
| `core/container.py` | Modify | Remove `DataService` from `initialise_container` (wired in lifespan instead) |
| `main.py` | Modify | Lifespan: create CH client, register DataService singleton, add ping health check, close on shutdown |
| `routers/data.py` | Rewrite | `GET /data/count`, `GET /data/rows?limit=&offset=` |
| `tests/conftest.py` | Modify | Add `clickhouse_container`, `test_clickhouse_client`, update `override_data_service` |
| `tests/test_data.py` | Rewrite | 4 tests covering both endpoints |

---

### Task 1: Add dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add clickhouse-connect and testcontainers[clickhouse]**

Replace the `# Data` and `# Testing` sections in `requirements.txt`:

```
# Fast API Service
fastapi
pydantic
pydantic-settings
python-dotenv
httpx

# MCP
mcp

# Postgres
asyncpg
pgvector
sqlalchemy[asyncio]
alembic

# ClickHouse
clickhouse-connect[async]

# Data
polars
rapidfuzz

# Testing
pytest
pytest-asyncio
pytest-cov
testcontainers[postgresql]
testcontainers[clickhouse]
```

- [ ] **Step 2: Install the new dependencies**

```bash
pip install "clickhouse-connect[async]" "testcontainers[clickhouse]"
```

Expected: installs without errors. `clickhouse-connect[async]` pulls in `aiohttp`.

- [ ] **Step 3: Verify import**

```bash
python -c "import clickhouse_connect; print('ok')"
```

Expected: prints `ok`.

---

### Task 2: Add ClickHouse settings

**Files:**
- Modify: `core/settings.py`

- [ ] **Step 1: Add 5 ClickHouse connection fields**

Full file after change:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"
    data_dir: str = "./data"

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/appdb"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
```

- [ ] **Step 2: Verify Settings loads cleanly**

```bash
python -c "from core.settings import Settings; s = Settings(); print(s.clickhouse_host)"
```

Expected: prints `localhost`.

---

### Task 3: Create ClickHouse persistence module

**Files:**
- Create: `persistence/analytics_store/clickhouse/clickhouse_client.py`

`get_async_client` is a coroutine — it must be awaited in an async context and cannot run at module import time. This module exposes three functions that `main.py` lifespan uses to manage the client lifecycle.

- [ ] **Step 1: Create the module**

```python
from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from core.settings import Settings

_client: AsyncClient | None = None


async def create_client(settings: Settings) -> AsyncClient:
    global _client
    _client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    return _client


def get_client() -> AsyncClient:
    if _client is None:
        raise RuntimeError("ClickHouse client not initialised — call create_client() in lifespan first")
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
```

- [ ] **Step 2: Verify import**

```bash
python -c "from persistence.analytics_store.clickhouse.clickhouse_client import create_client; print('ok')"
```

Expected: prints `ok`.

---

### Task 4: Update schemas

**Files:**
- Modify: `schemas/data.py`

- [ ] **Step 1: Replace DataShapeResponse with three new types**

Full file after change:

```python
from pydantic import BaseModel


class DataCountResponse(BaseModel):
    count: int


class DataRowResponse(BaseModel):
    id: int
    name: str
    value: str


class DataRowsResponse(BaseModel):
    rows: list[DataRowResponse]
    total: int
    limit: int
    offset: int
```

Note: `tests/test_data.py` will need rewriting (done in Task 7). The existing test will fail after this step — that is expected.

---

### Task 5: Set up ClickHouse test fixtures

**Files:**
- Modify: `tests/conftest.py`

`ClickHouseContainer(port=8123)` exposes the HTTP interface port (not the native TCP port 9000). `get_exposed_port(8123)` returns the randomly assigned host port. The ClickHouse Docker image listens on both 8123 (HTTP) and 9000 (native) by default; we expose only 8123 because `clickhouse-connect` uses HTTP.

On the first run, Docker will pull `clickhouse/clickhouse-server:latest` which is several hundred MB — this is normal and only happens once.

- [ ] **Step 1: Replace tests/conftest.py with the full updated version**

```python
import asyncio
from contextlib import asynccontextmanager

import clickhouse_connect
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
import persistence.transaction_store.models.config  # noqa: F401 — registers Configuration with metadata

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"

_CREATE_ITEMS = """
CREATE TABLE IF NOT EXISTS items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id
"""

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
    client = await clickhouse_connect.get_async_client(
        host="localhost",
        port=http_port,
        username=clickhouse_container.username or "default",
        password=clickhouse_container.password or "",
        database=clickhouse_container.dbname or "default",
    )
    await client.command(_CREATE_ITEMS)
    await client.insert("items", _SEED_ITEMS, column_names=["id", "name", "value"])
    yield client
    await client.close()


@pytest.fixture(scope="session")
async def override_data_service(test_clickhouse_client):
    yield DataService(test_clickhouse_client)


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
```

- [ ] **Step 2: Verify conftest imports without error**

```bash
python -c "import tests.conftest" 2>&1 | head -5
```

Expected: no import errors (may be blank output).

---

### Task 6: Write failing tests

**Files:**
- Rewrite: `tests/test_data.py`

- [ ] **Step 1: Write the four tests**

```python
import pytest
from httpx import AsyncClient


async def test_get_count_returns_total(test_client: AsyncClient):
    response = await test_client.get("/data/count")
    assert response.status_code == 200
    assert response.json() == {"count": 3}


async def test_get_rows_returns_all(test_client: AsyncClient):
    response = await test_client.get("/data/rows")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["rows"]) == 3
    assert body["limit"] == 10
    assert body["offset"] == 0


async def test_get_rows_with_limit(test_client: AsyncClient):
    response = await test_client.get("/data/rows?limit=2")
    assert response.status_code == 200
    body = response.json()
    assert len(body["rows"]) == 2
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 0


async def test_get_rows_with_offset(test_client: AsyncClient):
    response = await test_client.get("/data/rows?limit=2&offset=2")
    assert response.status_code == 200
    body = response.json()
    assert len(body["rows"]) == 1
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 2
```

- [ ] **Step 2: Run the data tests and confirm they fail**

```bash
pytest tests/test_data.py -v -s
```

Expected: 4 failures — `404 Not Found` (endpoints do not exist yet). If you see import errors instead, fix those before proceeding.

---

### Task 7: Implement DataService

**Files:**
- Rewrite: `services/data.py`

- [ ] **Step 1: Rewrite DataService as fully async**

```python
import logging

from clickhouse_connect.driver.asyncclient import AsyncClient

from schemas.data import DataCountResponse, DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class DataService:
    def __init__(self, client: AsyncClient):
        self._client = client

    async def get_count(self) -> DataCountResponse:
        result = await self._client.query("SELECT count() FROM items")
        return DataCountResponse(count=result.first_row[0])

    async def get_rows(self, limit: int, offset: int) -> DataRowsResponse:
        count_result = await self._client.query("SELECT count() FROM items")
        total = count_result.first_row[0]

        result = await self._client.query(
            f"SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}"
        )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit, offset=offset)
```

Note: `limit` and `offset` are FastAPI-validated integers (see Task 9), so direct f-string interpolation carries no SQL-injection risk.

---

### Task 8: Update Container and wire DataService in lifespan

**Files:**
- Modify: `core/container.py`
- Modify: `main.py`

`DataService` now requires an async client that can only be created after an event loop is running. It is removed from `initialise_container` and registered as a singleton in `main.py` lifespan instead.

- [ ] **Step 1: Remove DataService from Container.initialise_container**

In `core/container.py`, change `initialise_container` to:

```python
def initialise_container(self):
    self.register_singleton(HealthService, HealthService(self.settings))
```

Also remove the `DataService` import from the top of `core/container.py`. Full file after change:

```python
import logging

from core.settings import Settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    def __init__(self):
        self._singletons = {}
        self._factories = {}
        self.settings = Settings()
        self.initialise_container()

    def initialise_container(self):
        self.register_singleton(HealthService, HealthService(self.settings))

    def get_settings(self):
        return self.settings

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def register_factory(self, service_type: type, factory_func: callable):
        self._factories[service_type] = factory_func

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        if service_type in self._factories:
            return self._factories[service_type]()
        try:
            return service_type()
        except Exception as e:
            raise ValueError(f"Cannot resolve service of type {service_type}: {e}")

    def clear(self):
        self._singletons.clear()
        self._factories.clear()


def create_container():
    return Container()


service_container = create_container()
```

- [ ] **Step 2: Update main.py lifespan**

Full `main.py` after change:

```python
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from sqlalchemy import text

from core.container import service_container
from core.settings import Settings
from persistence.analytics_store.clickhouse.clickhouse_client import create_client, close_client
from persistence.transaction_store.postgres.postgres_engine import engine
from services.data import DataService

log = logging.getLogger(__name__)

settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ch_client = await create_client(settings)
    service_container.register_singleton(DataService, DataService(ch_client))

    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    ok = await ch_client.ping()
    if not ok:
        raise RuntimeError("ClickHouse startup ping failed")

    async with mcp.session_manager.run():
        yield

    await close_client()


from routers import health, data, config

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA],
    lifespan=lifespan,
)

# REST Routers
app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")


# Root Endpoint
@app.get("/", tags=["API Root Page"])
async def get_root():
    return {
        "title": settings.app_title,
        "version": settings.app_version,
        "description": settings.app_description,
        "docs": "/docs",
    }


# MCP
from mcp_routers import tools

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)
tools.register(mcp)
app.mount("/mcp", mcp.streamable_http_app())


if __name__ == "__main__":
    log.info("Starting the application from main.py")
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=443,
        ssl_keyfile="./certs/key.pem",
        ssl_certfile="./certs/cert.pem",
    )
```

---

### Task 9: Implement the router

**Files:**
- Rewrite: `routers/data.py`

- [ ] **Step 1: Replace the router with count and rows endpoints**

```python
import logging

from fastapi import APIRouter, Query

from core.dependencies import DataServiceDep
from schemas.data import DataCountResponse, DataRowsResponse

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {"name": TAG, "description": "Endpoints for retrieving data"}

router = APIRouter(tags=[TAG])


@router.get("/count", response_model=DataCountResponse)
async def get_count(data_service: DataServiceDep):
    return await data_service.get_count()


@router.get("/rows", response_model=DataRowsResponse)
async def get_rows(
    data_service: DataServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    return await data_service.get_rows(limit=limit, offset=offset)
```

---

### Task 10: Run all tests, verify, commit

**Files:**
- No file changes — verify only

- [ ] **Step 1: Run the full test suite**

```bash
pytest -v -s
```

Expected output includes:
```
tests/test_data.py::test_get_count_returns_total PASSED
tests/test_data.py::test_get_rows_returns_all PASSED
tests/test_data.py::test_get_rows_with_limit PASSED
tests/test_data.py::test_get_rows_with_offset PASSED
tests/test_config.py::test_get_config_returns_empty_list PASSED
tests/test_config.py::test_post_config_creates_entry PASSED
tests/test_config.py::test_post_config_upserts_on_duplicate_key PASSED
tests/test_config.py::test_get_config_returns_all_entries PASSED
tests/test_health.py::... PASSED
```

All tests should pass. One pre-existing MCP cancel-scope teardown warning is expected and is not a failure.

- [ ] **Step 2: Commit**

```bash
git add requirements.txt core/settings.py core/container.py main.py \
    persistence/analytics_store/clickhouse/clickhouse_client.py \
    schemas/data.py services/data.py routers/data.py \
    tests/conftest.py tests/test_data.py
git commit -m "feat: replace IPC stream with ClickHouse, add /data/count and /data/rows"
```
