# Python Template Code Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 12 code quality issues in the python-template FastAPI app: eliminate duplicate `Settings()` instantiation, tighten the DI container, fix a HealthService bug, implement real MCP tools, move MCP registrations into lifespan, add NotImplementedError stubs, parameterize a SQL query, and clean up test noise.

**Architecture:** Four grouped tasks — each compiles cleanly and all tests pass after every commit. No new files are created; no existing public contracts change. The `get_settings()` singleton (added in Task 1) is the linchpin: every subsequent task uses it to eliminate duplicate `Settings()` calls.

**Tech Stack:** Python, FastAPI, SQLAlchemy async, pydantic-settings (`lru_cache`), pytest-asyncio, clickhouse-connect

---

## File Structure

**Modified:**
- `core/settings.py` — add `get_settings()` singleton + `db_pool_size`/`db_pool_max_overflow` fields
- `core/container.py` — use `get_settings()`, remove `_factories`/`register_factory`, fix `get()` to fail fast
- `persistence/transaction_store/postgres/postgres_engine.py` — use `get_settings()`, read pool size from settings
- `services/health.py` — fix `status()` to return `HealthStatusResponse` (not raw string), type-annotate `__init__`
- `routers/health.py` — add `response_model=HealthStatusResponse`
- `mcp_routers/tools.py` — replace function-reference stubs with real implementations
- `mcp_routers/resources.py` — drop unused `log`, remove `return None`, raise `NotImplementedError`
- `mcp_routers/prompts.py` — drop unused `log`, remove `return None`, raise `NotImplementedError`
- `main.py` — move all imports to top, create `mcp` before `lifespan`, move all mcp registrations into `lifespan`
- `services/data.py` — replace f-string SQL with parameterized query
- `tests/test_health.py` — update assertion from `"testing"` to `{"status": "testing"}`
- `tests/conftest.py` — remove unused `import asyncio`
- `tests/test_config.py` — remove all `@pytest.mark.asyncio` decorators (redundant with `asyncio_mode=auto`)

---

### Task 1: Settings singleton, pool fields, Container cleanup, postgres_engine

**Goal:** Eliminate the three separate `Settings()` calls (in `container.py`, `main.py`, and `postgres_engine.py`) with a single cached instance. Add configurable pool fields to `Settings`. Remove dead code from `Container`.

**Files:**
- Modify: `core/settings.py`
- Modify: `core/container.py`
- Modify: `persistence/transaction_store/postgres/postgres_engine.py`

---

- [ ] **Step 1: Add `get_settings()` and pool fields to `core/settings.py`**

Replace the entire file with:

```python
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"
    data_dir: str = "./data"

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/appdb"
    db_pool_size: int = 5
    db_pool_max_overflow: int = 10

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 2: Update `core/container.py` — use `get_settings()`, remove dead code, fail fast in `get()`**

Replace the entire file with:

```python
import logging

from core.settings import get_settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    def __init__(self):
        self._singletons = {}
        self.settings = get_settings()
        self.initialise_container()

    def initialise_container(self):
        self.register_singleton(HealthService, HealthService(self.settings))

    def get_settings(self):
        return self.settings

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        raise ValueError(f"No service registered for type {service_type.__name__}")

    def clear(self):
        self._singletons.clear()


def create_container():
    return Container()


service_container = create_container()
```

Key changes: `Settings()` → `get_settings()`, removed `_factories` dict and `register_factory` method, removed the silent `service_type()` fallback in `get()` (was swallowing misconfiguration silently), `clear()` no longer clears the non-existent `_factories`.

- [ ] **Step 3: Update `persistence/transaction_store/postgres/postgres_engine.py` — use `get_settings()` and pool fields**

Replace the entire file with:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from core.settings import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    pool_pre_ping=True,
    pool_size=_settings.db_pool_size,
    max_overflow=_settings.db_pool_max_overflow,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/ -v
```

Expected: all tests pass (same count as before this task).

- [ ] **Step 5: Commit**

```bash
git add core/settings.py core/container.py persistence/transaction_store/postgres/postgres_engine.py
git commit -m "refactor: add get_settings() singleton, pool fields, and fail-fast Container.get()"
```

---

### Task 2: Fix HealthService bug + type annotation + router response_model + test assertion

**Goal:** `HealthService.status()` currently creates a `HealthStatusResponse` but then discards it and returns the raw string — fix it to return the schema object. Add the `Settings` type annotation to `__init__`. Add `response_model` to the router so FastAPI validates the response. Update the test assertion to match the new JSON shape.

**Files:**
- Modify: `services/health.py`
- Modify: `routers/health.py`
- Modify: `tests/test_health.py`

---

- [ ] **Step 1: Fix `services/health.py`**

Replace the entire file with:

```python
import logging

from core.settings import Settings
from schemas.health import HealthStatusResponse

log = logging.getLogger(__name__)


class HealthService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def status(self) -> HealthStatusResponse:
        return HealthStatusResponse(status=self.settings.status)
```

The bug: the old code created `HealthStatusResponse(status=self.settings.status)` and immediately discarded it, then returned `self.settings.status` (a raw string). The router was serialising that raw string as a JSON string `"testing"` rather than the object `{"status": "testing"}`.

- [ ] **Step 2: Add `response_model` to `routers/health.py`**

Replace the entire file with:

```python
import logging

from fastapi import APIRouter

from core.dependencies import HealthServiceDep
from schemas.health import HealthStatusResponse

log = logging.getLogger(__name__)

TAG = "Application Health"
TAG_METADATA = {"name": TAG, "description": "Endpoints for checking the status and health of the application"}

router = APIRouter(tags=[TAG])


@router.get("/status", response_model=HealthStatusResponse)
async def get_health(health_service: HealthServiceDep):
    return health_service.status()
```

- [ ] **Step 3: Update `tests/test_health.py` assertion**

Replace the entire file with:

```python
async def test_health_status(test_client):
    """ N.B. - the status value is defined in the Settings class
        This is overwritten to "testing" in the test fixtures in conf.py
    """
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    assert response.json() == {"status": "testing"}
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/ -v
```

Expected: all tests pass. `test_health_status` now asserts `{"status": "testing"}` instead of `"testing"`.

- [ ] **Step 5: Commit**

```bash
git add services/health.py routers/health.py tests/test_health.py
git commit -m "fix: HealthService.status() returns HealthStatusResponse, not raw string"
```

---

### Task 3: MCP tools implementation + main.py restructure + NotImplementedError stubs

**Goal:** Three related changes that must be implemented together:

1. **`mcp_routers/tools.py`**: Replace the stubs that return function references with real implementations that call services and return typed values.
2. **`mcp_routers/resources.py` and `mcp_routers/prompts.py`**: Drop unused `log`, remove `return None`, raise `NotImplementedError` with a helpful message.
3. **`main.py`**: Move all imports to the top, create `mcp` before `lifespan`, and move ALL mcp registrations (`tools.register`, `resources.register`, `prompts.register`) INSIDE `lifespan`.

The placement of registrations in `lifespan` is critical: `resources.register` and `prompts.register` raise `NotImplementedError`. If called at module level they would fire when `conftest.py` does `from main import app, mcp`, breaking all tests. Tests use a custom `test_lifespan` that skips the real `lifespan`, so `NotImplementedError` is never raised during tests. In production, startup fails until the stubs are implemented — which is the desired behavior.

**Files:**
- Modify: `mcp_routers/tools.py`
- Modify: `mcp_routers/resources.py`
- Modify: `mcp_routers/prompts.py`
- Modify: `main.py`

---

- [ ] **Step 1: Implement `mcp_routers/tools.py`**

Replace the entire file with:

```python
from mcp.server.fastmcp import FastMCP

from core.dependencies import get_health_service, get_data_service


def register(mcp: FastMCP) -> None:
    @mcp.tool()
    def get_health_status() -> str:
        """Returns the current application health status."""
        return get_health_service().status().status

    @mcp.tool()
    async def get_data_count() -> int:
        """Returns the total number of items in the data store."""
        result = await get_data_service().get_count()
        return result.count
```

The old code returned the `get_health_service` and `get_data_service` *functions* themselves as tool results, not the data they produce. These tools now call the dependency getters (which pull the live singletons from the container) and return actual typed values.

- [ ] **Step 2: Replace `mcp_routers/resources.py` with NotImplementedError stub**

Replace the entire file with:

```python
from mcp.server.fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    raise NotImplementedError(
        "Register your MCP resources in mcp_routers/resources.py. "
        "See https://gofastmcp.com/servers/resources for examples."
    )
```

- [ ] **Step 3: Replace `mcp_routers/prompts.py` with NotImplementedError stub**

Replace the entire file with:

```python
from mcp.server.fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    raise NotImplementedError(
        "Register your MCP prompts in mcp_routers/prompts.py. "
        "See https://gofastmcp.com/servers/prompts for examples."
    )
```

- [ ] **Step 4: Restructure `main.py`**

Replace the entire file with:

```python
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from sqlalchemy import text

from core.container import service_container
from core.settings import get_settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.transaction_store.postgres.postgres_engine import engine
from routers import health, data, config
from mcp_routers import tools, resources, prompts
from services.data import DataService

log = logging.getLogger(__name__)

settings = get_settings()

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tools.register(mcp)
    resources.register(mcp)
    prompts.register(mcp)
    async with ClickHouseClient(settings) as ch_client:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        try:
            ok = await ch_client.ping()
            if not ok:
                raise RuntimeError("ClickHouse startup ping failed")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError("ClickHouse startup ping failed") from exc

        service_container.register_singleton(DataService, DataService(ch_client))
        async with mcp.session_manager.run():
            yield


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA],
    lifespan=lifespan,
)

app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")

app.mount("/mcp", mcp.streamable_http_app())


@app.get("/", tags=["API Root Page"])
async def get_root():
    return {
        "title": settings.app_title,
        "version": settings.app_version,
        "description": settings.app_description,
        "docs": "/docs",
    }


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

Key changes vs the old `main.py`:
- All imports moved to the top (previously `from routers import ...` appeared mid-file after `app = FastAPI(...)`)
- `mcp` object created before `lifespan` (previously defined after)
- `tools.register(mcp)`, `resources.register(mcp)`, `prompts.register(mcp)` all called inside `lifespan` (previously `tools.register(mcp)` was at module level)
- `Settings()` replaced with `get_settings()`

- [ ] **Step 5: Run tests**

```bash
pytest tests/ -v
```

Expected: all tests pass. The `test_lifespan` fixture in `conftest.py` bypasses the real lifespan and calls `mcp.session_manager.run()` directly, so the `NotImplementedError` stubs are never invoked during tests.

- [ ] **Step 6: Commit**

```bash
git add mcp_routers/tools.py mcp_routers/resources.py mcp_routers/prompts.py main.py
git commit -m "refactor: implement MCP tools, add NotImplementedError stubs, move mcp registration into lifespan"
```

---

### Task 4: Parameterize DataService SQL + clean up test noise

**Goal:** Replace the f-string SQL query in `DataService.get_rows()` with a parameterized query to eliminate SQL injection risk. Remove the unused `import asyncio` from `conftest.py`. Remove all `@pytest.mark.asyncio` decorators from `test_config.py` (redundant when `asyncio_mode=auto` is set in pytest config).

**Files:**
- Modify: `services/data.py`
- Modify: `tests/conftest.py`
- Modify: `tests/test_config.py`

---

- [ ] **Step 1: Parameterize the SQL query in `services/data.py`**

Replace the `get_rows` method body. The old code:

```python
result = await self._client.query(
    f"SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}"
)
```

Replace with:

```python
result = await self._client.query(
    "SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}",
    parameters={"limit": limit, "offset": offset},
)
```

The full updated file:

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
            "SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}",
            parameters={"limit": limit, "offset": offset},
        )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit, offset=offset)
```

The `{limit}` / `{offset}` placeholders are clickhouse-connect's named parameter syntax (not Python f-string). The `parameters` dict is sent separately and the driver handles escaping.

- [ ] **Step 2: Remove unused `import asyncio` from `tests/conftest.py`**

The first line of `tests/conftest.py` is `import asyncio`. Delete it. The resulting top of the file should be:

```python
from contextlib import asynccontextmanager
from pathlib import Path
...
```

- [ ] **Step 3: Remove `@pytest.mark.asyncio` decorators from `tests/test_config.py`**

Replace the entire file with:

```python
from httpx import AsyncClient


async def test_get_config_returns_empty_list(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert response.json() == []


async def test_post_config_creates_entry(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "staging"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "staging"}


async def test_post_config_upserts_on_duplicate_key(test_client: AsyncClient):
    response = await test_client.post("/config/", json={"key": "env", "value": "production"})
    assert response.status_code == 201
    assert response.json() == {"key": "env", "value": "production"}


async def test_get_config_returns_all_entries(test_client: AsyncClient):
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": "env", "value": "production"} in response.json()
```

The `import pytest` and all `@pytest.mark.asyncio` decorators are removed. With `asyncio_mode=auto` in `pytest.ini` / `pyproject.toml`, async test functions are collected automatically.

- [ ] **Step 4: Run tests**

```bash
pytest tests/ -v
```

Expected: all tests pass, same count as before.

- [ ] **Step 5: Commit**

```bash
git add services/data.py tests/conftest.py tests/test_config.py
git commit -m "fix: parameterize DataService SQL, remove unused asyncio import, drop redundant asyncio markers"
```
