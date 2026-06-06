# Replace SQLAlchemy + Alembic with asyncpg Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQLAlchemy ORM + Alembic migration stack with raw asyncpg, mirroring the ClickHouseClient pattern so both database clients use the same context-manager / singleton architecture.

**Architecture:** A `PostgresClient` async context manager wraps an asyncpg connection pool (exactly mirroring `ClickHouseClient`). The pool is a singleton registered in `service_container` at startup. `ConfigService` holds the pool and acquires connections per operation. Schema is managed by `scripts/postgres-init.sql` (mirroring `scripts/clickhouse-init.sql`) and run at startup via `CREATE TABLE IF NOT EXISTS`.

**Tech Stack:** asyncpg (direct, no SQLAlchemy), FastAPI DI, testcontainers (PostgresContainer), pytest-asyncio

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Create | `persistence/transaction_store/postgres/postgres_client.py` | `PostgresClient` context manager — mirrors `ClickHouseClient` |
| Create | `scripts/postgres-init.sql` | DDL only — mirrors `clickhouse-init.sql` |
| Modify | `services/config.py` | Takes `asyncpg.Pool`; raw SQL queries |
| Modify | `schemas/config.py` | Remove ORM-specific `from_attributes=True` |
| Modify | `core/dependencies.py` | Remove SQLAlchemy session DI; add singleton-based `ConfigServiceDep` |
| Modify | `core/settings.py` | `database_url` → `postgres_url`; pool size field names |
| Modify | `main.py` | Use `PostgresClient` in lifespan; register `ConfigService` singleton |
| Modify | `tests/conftest.py` | Replace SQLAlchemy fixtures with asyncpg pool fixture |
| Modify | `requirements.txt` | Remove `sqlalchemy[asyncio]`, `alembic`, `pgvector` |
| Modify | `docker-compose.yml` | `DATABASE_URL` → `POSTGRES_URL`; drop `+asyncpg` driver prefix |
| Modify | `CLAUDE.md` | Update architecture, stack, and patterns sections |
| Modify | `routers/data.py` | Fix pre-existing bug: `offset=offset` references undefined variable |
| Delete | `alembic/` | Entire directory |
| Delete | `alembic.ini` | |
| Delete | `persistence/transaction_store/postgres/postgres_base.py` | ORM declarative base |
| Delete | `persistence/transaction_store/postgres/postgres_engine.py` | SQLAlchemy engine |
| Delete | `persistence/transaction_store/models/config.py` | ORM model |
| Delete | `persistence/transaction_store/models/` | Directory (now empty) |

---

### Task 1: PostgresClient context manager

**Files:**
- Create: `persistence/transaction_store/postgres/postgres_client.py`

This is a direct mirror of `ClickHouseClient` in `persistence/analytics_store/clickhouse/clickhouse_client.py`. The `__aenter__` creates an asyncpg connection pool and returns it; `__aexit__` closes it.

- [ ] **Step 1: Create the file**

```python
from __future__ import annotations

import asyncpg

from core.settings import Settings


class PostgresClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self) -> asyncpg.Pool:
        self._pool = await asyncpg.create_pool(
            self._settings.postgres_url,
            min_size=self._settings.postgres_pool_min_size,
            max_size=self._settings.postgres_pool_max_size,
        )
        return self._pool

    async def __aexit__(self, *_: object) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
```

- [ ] **Step 2: Commit**

```bash
git add persistence/transaction_store/postgres/postgres_client.py
git commit -m "feat: add PostgresClient async context manager wrapping asyncpg pool"
```

---

### Task 2: postgres-init.sql schema file

**Files:**
- Create: `scripts/postgres-init.sql`

Mirrors `scripts/clickhouse-init.sql`. DDL only; no seed data.

- [ ] **Step 1: Create the file**

```sql
CREATE TABLE IF NOT EXISTS configuration (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

- [ ] **Step 2: Commit**

```bash
git add scripts/postgres-init.sql
git commit -m "feat: add postgres-init.sql DDL schema (replaces Alembic migrations)"
```

---

### Task 3: Rewrite ConfigService with asyncpg

**Files:**
- Modify: `services/config.py`
- Modify: `schemas/config.py`

`ConfigService` now takes an `asyncpg.Pool` instead of an `AsyncSession`. It acquires a connection per operation from the pool. The `set` operation uses `INSERT ... ON CONFLICT ... RETURNING` — a single atomic statement that handles upsert without needing an explicit transaction context.

`schemas/config.py`: `from_attributes=True` was needed only for SQLAlchemy ORM → Pydantic mapping. We now construct `ConfigEntry` directly from asyncpg `Record` objects, so remove it.

- [ ] **Step 1: Rewrite `services/config.py`**

```python
import asyncpg

from schemas.config import ConfigEntry


class ConfigService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_all(self) -> list[ConfigEntry]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM configuration ORDER BY key")
            return [ConfigEntry(key=row["key"], value=row["value"]) for row in rows]

    async def set(self, key: str, value: str) -> ConfigEntry:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO configuration (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                RETURNING key, value
                """,
                key,
                value,
            )
            return ConfigEntry(key=row["key"], value=row["value"])
```

- [ ] **Step 2: Simplify `schemas/config.py`**

Remove `ConfigDict` and `from_attributes=True` — no longer needed since `ConfigEntry` is constructed directly, not from an ORM object.

```python
from pydantic import BaseModel


class ConfigSetRequest(BaseModel):
    key: str
    value: str


class ConfigEntry(BaseModel):
    key: str
    value: str
```

- [ ] **Step 3: Commit**

```bash
git add services/config.py schemas/config.py
git commit -m "refactor: rewrite ConfigService to use asyncpg pool with raw SQL"
```

---

### Task 4: Update core/dependencies.py

**Files:**
- Modify: `core/dependencies.py`

Remove all SQLAlchemy session machinery (`AsyncSession`, `async_sessionmaker`, `get_transaction_session`, `TransactionSessionDep`). `ConfigService` is now a singleton in `service_container` — resolved exactly like `HealthService` and `DataService`.

- [ ] **Step 1: Replace the file contents**

```python
from typing import Annotated

from fastapi import Depends

from core.settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService


def get_health_service() -> HealthService:
    return service_container.get(HealthService)


def get_data_service() -> DataService:
    return service_container.get(DataService)


def get_config_service() -> ConfigService:
    return service_container.get(ConfigService)


SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
```

- [ ] **Step 2: Commit**

```bash
git add core/dependencies.py
git commit -m "refactor: replace SQLAlchemy session DI with singleton-based ConfigServiceDep"
```

---

### Task 5: Update core/settings.py

**Files:**
- Modify: `core/settings.py`

Three changes:
1. Rename `database_url` → `postgres_url`. The value format changes from `postgresql+asyncpg://...` (SQLAlchemy driver prefix) to `postgresql://...` (plain asyncpg URL).
2. Rename `db_pool_size` → `postgres_pool_min_size` (asyncpg's minimum connections in the pool).
3. Rename `db_pool_max_overflow` → `postgres_pool_max_size` (asyncpg's maximum pool size).

**Important:** The environment variable names change from `DATABASE_URL`, `DB_POOL_SIZE`, `DB_POOL_MAX_OVERFLOW` to `POSTGRES_URL`, `POSTGRES_POOL_MIN_SIZE`, `POSTGRES_POOL_MAX_SIZE`. Update any `.env` files accordingly.

- [ ] **Step 1: Update `core/settings.py`**

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
    postgres_url: str = "postgresql://user:password@localhost:5432/appdb"
    postgres_pool_min_size: int = 2
    postgres_pool_max_size: int = 10
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 2: Commit**

```bash
git add core/settings.py
git commit -m "refactor: rename database_url -> postgres_url and update pool size settings for asyncpg"
```

---

### Task 6: Update main.py lifespan

**Files:**
- Modify: `main.py`

Replace the SQLAlchemy `engine` import with `PostgresClient`. The lifespan now:
1. Opens the Postgres pool and runs the init SQL (idempotent `CREATE TABLE IF NOT EXISTS`)
2. Registers `ConfigService` as a singleton
3. Opens the ClickHouse client and registers `DataService` as a singleton
4. Runs the MCP session for the app lifetime

Both context managers must wrap `yield` because their clients are held by long-lived singletons.

- [ ] **Step 1: Replace `main.py`**

```python
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import service_container
from core.settings import get_settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from routers import health, data, config
from mcp_routers import tools
from services.config import ConfigService
from services.data import DataService

log = logging.getLogger(__name__)

settings = get_settings()

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)

tools.register(mcp)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with PostgresClient(settings) as pg_pool:
        schema_sql = Path("scripts/postgres-init.sql").read_text()
        async with pg_pool.acquire() as conn:
            await conn.execute(schema_sql)
        service_container.register_singleton(ConfigService, ConfigService(pg_pool))

        async with ClickHouseClient(settings) as ch_client:
            if not await ch_client.ping():
                raise RuntimeError("ClickHouse startup ping failed")
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
        "MCP": "/mcp"
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

- [ ] **Step 2: Commit**

```bash
git add main.py
git commit -m "refactor: use PostgresClient in lifespan, register ConfigService as singleton"
```

---

### Task 7: Update tests/conftest.py and verify tests pass

**Files:**
- Modify: `tests/conftest.py`

Replace all SQLAlchemy fixtures with an asyncpg pool fixture. Key changes:
- Remove `transaction_engine`, `transaction_session_factory` fixtures and all SQLAlchemy imports
- Remove `PostgresBase` and models imports (files will be deleted in Task 8)
- Add `postgres_pool` fixture: creates asyncpg pool from testcontainer, runs init SQL
- Add `override_config_service` fixture: `ConfigService(postgres_pool)` registered as singleton
- Remove `app.dependency_overrides[get_transaction_session]` — that dep no longer exists
- Use explicit host/port/user/password kwargs (not URL) to avoid Windows SSPI negotiation issues

Note: `postgres_container.username`, `postgres_container.password`, `postgres_container.dbname` are standard attributes on `PostgresContainer`. The exposed Postgres port is 5432 inside the container.

- [ ] **Step 1: Replace `tests/conftest.py`**

```python
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
```

- [ ] **Step 2: Run the tests**

```bash
pytest tests/ -v -s
```

Expected: all tests pass. If any fail, diagnose before continuing.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "refactor: replace SQLAlchemy test fixtures with asyncpg pool fixture"
```

---

### Task 8: Clean up and update supporting files

**Files:**
- Delete: `alembic/` (entire directory), `alembic.ini`
- Delete: `persistence/transaction_store/postgres/postgres_base.py`
- Delete: `persistence/transaction_store/postgres/postgres_engine.py`
- Delete: `persistence/transaction_store/models/config.py`
- Delete: `persistence/transaction_store/models/` (now empty)
- Modify: `requirements.txt`
- Modify: `docker-compose.yml`
- Modify: `CLAUDE.md`
- Modify: `routers/data.py` (fix pre-existing bug)

- [ ] **Step 1: Delete old Alembic and SQLAlchemy files**

```bash
# On Windows PowerShell:
Remove-Item -Recurse -Force alembic
Remove-Item alembic.ini
Remove-Item persistence/transaction_store/postgres/postgres_base.py
Remove-Item persistence/transaction_store/postgres/postgres_engine.py
Remove-Item persistence/transaction_store/models/config.py
Remove-Item persistence/transaction_store/models  # now empty
```

- [ ] **Step 2: Run tests to verify nothing is broken by deletions**

```bash
pytest tests/ -v
```

Expected: all tests pass (nothing imports the deleted files after Tasks 3-7).

- [ ] **Step 3: Update `requirements.txt`**

Remove `sqlalchemy[asyncio]`, `alembic`, and `pgvector`. Keep `asyncpg` (now used directly, not as a SQLAlchemy driver).

```text
# Fast API Service
fastapi
uvicorn[standard]
pydantic
pydantic-settings
python-dotenv
httpx

# MCP
mcp

# Postgres
asyncpg

# ClickHouse
clickhouse-connect[async]

# Data
polars
pyarrow
rapidfuzz

# Testing
pytest
pytest-asyncio
pytest-cov
testcontainers[postgresql]
testcontainers[clickhouse]
```

- [ ] **Step 4: Update `docker-compose.yml`**

The app service's `DATABASE_URL` env var must be renamed to `POSTGRES_URL`, and the value must drop the `+asyncpg` driver prefix.

Change line 41 from:
```yaml
      DATABASE_URL: postgresql+asyncpg://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@db:5432/${POSTGRES_DB:-appdb}
```
To:
```yaml
      POSTGRES_URL: postgresql://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@db:5432/${POSTGRES_DB:-appdb}
```

- [ ] **Step 5: Fix pre-existing bug in `routers/data.py`**

Line 24 currently calls `data_service.get_data(limit=limit, offset=offset)` but `offset` is not defined (it was removed from the function signature in a prior edit). The `get_data` method signature is `async def get_data(self, limit: int)`.

Change line 24 from:
```python
    return await data_service.get_data(limit=limit, offset=offset)
```
To:
```python
    return await data_service.get_data(limit=limit)
```

- [ ] **Step 6: Update `CLAUDE.md`**

**Architecture section** — change `alembic/` entry:

From:
```
  alembic/                      Schema migrations (async env.py)
```
To:
```
  scripts/postgres-init.sql     Postgres DDL (CREATE TABLE IF NOT EXISTS; run at startup)
```
(Note: `scripts/` already appears in the file via `clickhouse-init.sql` — fold this into that line or add alongside it.)

**Stack section** — update Postgres line:

From:
```
- SQLAlchemy (async) + Alembic — Postgres transaction store
```
To:
```
- asyncpg — Postgres transaction store (direct, no ORM)
```

**Key Patterns — Persistence — Postgres** section — replace entirely:

From:
```
**Persistence — Postgres**
- SQLAlchemy async engine and session factory live in `persistence/transaction_store/postgres/`.
- Sessions are injected via `TransactionSessionDep`; commit/rollback is managed in the dependency, not in services.
- Schema changes go through Alembic migrations in `alembic/versions/`.
```
To:
```
**Persistence — Postgres**
- `PostgresClient` in `persistence/transaction_store/postgres/postgres_client.py` mirrors `ClickHouseClient`: async context manager whose `__aenter__` returns a live `asyncpg.Pool`, `__aexit__` closes it.
- In `main.py` the lifespan wraps startup in `async with PostgresClient(settings) as pg_pool:`. `ConfigService` is registered as a singleton holding the pool.
- Schema is in `scripts/postgres-init.sql` (DDL only, `CREATE TABLE IF NOT EXISTS`). The lifespan runs it at startup — idempotent, no migration tooling needed.
- Services acquire connections per-operation via `async with pool.acquire() as conn:`. No session injection into routes.
```

**Database Investigation section** — update the Postgres instruction:

From:
```
pytest tests/test_config.py -v -s
```
Keep as-is (still correct — the test spins up a testcontainer).

Remove any reference to `alembic upgrade head` or Alembic commands if present.

- [ ] **Step 7: Final test run**

```bash
pytest tests/ -v --cov
```

Expected: all tests pass, no import errors for deleted files.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "chore: remove SQLAlchemy, Alembic, pgvector; update docs and docker-compose for asyncpg"
```
