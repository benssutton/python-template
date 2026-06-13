# Redis Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Redis-backed cache store with `POST /cache/` (set with optional TTL) and `GET /cache/{key}` (single-key lookup, 404 on miss) endpoints, following the same async context manager pattern as ClickHouseClient and PostgresClient.

**Architecture:** `RedisClient` is an async context manager whose `__aenter__` returns `redis.asyncio.Redis` directly (redis-py manages the connection pool internally, mirroring ClickHouseClient). `CacheService` holds the client and calls `set`/`get`/`ttl` on it. The lifespan nests `RedisClient` between `PostgresClient` and `ClickHouseClient`, registering `CacheService` as a singleton.

**Tech Stack:** `redis[hiredis]` (async Redis client), `testcontainers[redis]` (integration testing).

**Note for agentic workers:** Tasks 1–4 build isolated components that reference settings/types introduced across tasks — this is intentional. Code quality reviewers should treat cross-task dependencies as planned, not as bugs.

---

### Task 1: Add requirements and settings

**Files:**
- Modify: `requirements.txt`
- Modify: `core/settings.py`

- [ ] **Step 1: Add redis packages to requirements.txt**

Add two lines under the existing `# Testing` section comment:

```
# Redis
redis[hiredis]

# Testing
pytest
pytest-asyncio
pytest-cov
testcontainers[postgresql]
testcontainers[clickhouse]
testcontainers[redis]
```

The full file should be:

```
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

# Redis
redis[hiredis]

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
testcontainers[redis]
```

- [ ] **Step 2: Install the new packages**

```bash
pip install redis[hiredis] testcontainers[redis]
```

Expected: packages install without error.

- [ ] **Step 3: Add redis_url to core/settings.py**

Add `redis_url` after the ClickHouse settings block. The full file should be:

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

    redis_url: str = "redis://localhost:6379/0"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Commit**

```bash
git add requirements.txt core/settings.py
git commit -m "feat: add redis[hiredis] dependency and redis_url setting"
```

---

### Task 2: Create RedisClient

**Files:**
- Create: `persistence/cache_store/redis/redis_client.py`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p persistence/cache_store/redis
```

- [ ] **Step 2: Create persistence/cache_store/redis/redis_client.py**

```python
from __future__ import annotations

import redis.asyncio as aioredis

from core.settings import Settings


class RedisClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    async def __aenter__(self) -> aioredis.Redis:
        self._client = aioredis.Redis.from_url(self._settings.redis_url)
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
```

- [ ] **Step 3: Commit**

```bash
git add persistence/cache_store/redis/redis_client.py
git commit -m "feat: add RedisClient async context manager"
```

---

### Task 3: Create schemas and CacheService

**Files:**
- Create: `schemas/cache.py`
- Create: `services/cache.py`

- [ ] **Step 1: Create schemas/cache.py**

```python
from pydantic import BaseModel


class CacheSetRequest(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None


class CacheEntry(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None
```

- [ ] **Step 2: Create services/cache.py**

```python
import redis.asyncio as aioredis

from schemas.cache import CacheEntry


class CacheService:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def set(self, key: str, value: str, ttl_seconds: int | None) -> CacheEntry:
        await self._client.set(key, value, ex=ttl_seconds)
        return CacheEntry(key=key, value=value, ttl_seconds=ttl_seconds)

    async def get(self, key: str) -> CacheEntry | None:
        value = await self._client.get(key)
        if value is None:
            return None
        ttl = await self._client.ttl(key)
        return CacheEntry(key=key, value=value.decode(), ttl_seconds=ttl if ttl >= 0 else None)
```

`ttl()` returns `-1` for keys with no expiry and `-2` for keys that don't exist. The `ttl >= 0` guard maps both negative values to `None`.

- [ ] **Step 3: Commit**

```bash
git add schemas/cache.py services/cache.py
git commit -m "feat: add CacheEntry schema and CacheService"
```

---

### Task 4: Create router and wire dependency injection

**Files:**
- Create: `routers/cache.py`
- Modify: `core/dependencies.py`

- [ ] **Step 1: Create routers/cache.py**

```python
import logging

from fastapi import APIRouter, HTTPException

from core.dependencies import CacheServiceDep
from schemas.cache import CacheEntry, CacheSetRequest

log = logging.getLogger(__name__)

TAG = "Cache"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for reading and writing Redis cache entries",
}

router = APIRouter(tags=[TAG])


@router.post("/", response_model=CacheEntry, status_code=201)
async def set_cache(body: CacheSetRequest, cache_service: CacheServiceDep):
    return await cache_service.set(body.key, body.value, body.ttl_seconds)


@router.get("/{key}", response_model=CacheEntry)
async def get_cache(key: str, cache_service: CacheServiceDep):
    entry = await cache_service.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return entry
```

- [ ] **Step 2: Update core/dependencies.py**

Add `CacheService` import, `get_cache_service` function, and `CacheServiceDep` alias. The full file should be:

```python
from typing import Annotated

from fastapi import Depends

from core.settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService
from services.cache import CacheService


def get_health_service() -> HealthService:
    return service_container.get(HealthService)


def get_data_service() -> DataService:
    return service_container.get(DataService)


def get_config_service() -> ConfigService:
    return service_container.get(ConfigService)


def get_cache_service() -> CacheService:
    return service_container.get(CacheService)


SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
```

- [ ] **Step 3: Commit**

```bash
git add routers/cache.py core/dependencies.py
git commit -m "feat: add cache router and CacheServiceDep"
```

---

### Task 5: Update conftest and write failing tests

**Files:**
- Modify: `tests/conftest.py`
- Create: `tests/test_cache.py`

The router is not yet mounted in `main.py`, so these tests will return 404 and fail. That is the expected red state.

- [ ] **Step 1: Update tests/conftest.py**

Add `from testcontainers.redis import RedisContainer`, a `REDIS_IMAGE` constant, a `redis_container` fixture, and `redis_container` + `redis_url` to `test_client`. The full file should be:

```python
from pathlib import Path

import pytest
import pyarrow.ipc as pa_ipc
from httpx import AsyncClient, ASGITransport
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from core.container import service_container
from core.dependencies import get_health_service
from core.settings import Settings
from services.health import HealthService
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"
REDIS_IMAGE = "redis:7"


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


# ── Redis fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer(REDIS_IMAGE) as r:
        yield r


# ── Async Test Client ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
async def test_client(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    override_health_service,
):
    from main import app, create_lifespan

    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))

    test_settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
    )

    app.dependency_overrides[get_health_service] = lambda: override_health_service
    service_container.register_singleton(HealthService, override_health_service)

    async with create_lifespan(test_settings)(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
```

- [ ] **Step 2: Create tests/test_cache.py**

```python
from httpx import AsyncClient


async def test_post_cache_creates_entry(test_client: AsyncClient):
    response = await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    assert response.status_code == 201
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


async def test_get_cache_returns_entry(test_client: AsyncClient):
    response = await test_client.get("/cache/foo")
    assert response.status_code == 200
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}


async def test_get_cache_missing_key_returns_404(test_client: AsyncClient):
    response = await test_client.get("/cache/nonexistent")
    assert response.status_code == 404


async def test_post_cache_with_ttl(test_client: AsyncClient):
    response = await test_client.post("/cache/", json={"key": "expiring", "value": "soon", "ttl_seconds": 60})
    assert response.status_code == 201
    assert response.json() == {"key": "expiring", "value": "soon", "ttl_seconds": 60}
```

- [ ] **Step 3: Run the tests and confirm they fail**

```bash
pytest tests/test_cache.py -v
```

Expected: 4 FAILED — the `/cache/` route does not exist yet (router not mounted in `main.py`).

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py tests/test_cache.py
git commit -m "test: add redis_container fixture and cache endpoint tests"
```

---

### Task 6: Wire lifespan and mount router in main.py

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Update main.py**

Add `RedisClient` and `CacheService` imports, nest `RedisClient` in the lifespan, mount the cache router, and add `cache.TAG_METADATA` to `openapi_tags`. The full file should be:

```python
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import service_container
from core.settings import get_settings, Settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.cache_store.redis.redis_client import RedisClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from routers import health, data, config, cache
from mcp_routers import tools
from services.cache import CacheService
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


def create_lifespan(settings: Settings):
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async with PostgresClient(settings) as pg_pool:
            schema_sql = (Path(__file__).parent / "scripts" / "postgres-init.sql").read_text()
            async with pg_pool.acquire() as conn:
                await conn.execute(schema_sql)
            service_container.register_singleton(ConfigService, ConfigService(pg_pool))

            async with RedisClient(settings) as redis_client:
                service_container.register_singleton(CacheService, CacheService(redis_client))

                async with ClickHouseClient(settings) as ch_client:
                    if not await ch_client.ping():
                        raise RuntimeError("ClickHouse startup ping failed")
                    service_container.register_singleton(DataService, DataService(ch_client))

                    async with mcp.session_manager.run():
                        yield
    return lifespan


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA, cache.TAG_METADATA],
    lifespan=create_lifespan(settings),
)

app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")
app.include_router(cache.router, prefix="/cache")

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

- [ ] **Step 2: Run all tests and confirm they pass**

```bash
pytest tests/ -v
```

Expected: all existing tests still pass, plus the 4 new cache tests:
```
tests/test_cache.py::test_post_cache_creates_entry PASSED
tests/test_cache.py::test_get_cache_returns_entry PASSED
tests/test_cache.py::test_get_cache_missing_key_returns_404 PASSED
tests/test_cache.py::test_post_cache_with_ttl PASSED
```

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "feat: wire RedisClient into lifespan and mount cache router"
```

---

### Task 7: Update docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Update docker-compose.yml**

Add the `redis` service and wire it into the `app` service. The full file should be:

```yaml
name: python-template

services:
  db:
    image: postgres:18
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-password}
      POSTGRES_DB: ${POSTGRES_DB:-appdb}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-user} -d ${POSTGRES_DB:-appdb}"]
      interval: 5s
      timeout: 5s
      retries: 10

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
    environment:
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - ./scripts/clickhouse-init.sql:/docker-entrypoint-initdb.d/01-schema.sql
      - ./performance/data/clickhouse-seed.sql:/docker-entrypoint-initdb.d/02-seed.sql
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      POSTGRES_URL: postgresql://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@db:5432/${POSTGRES_DB:-appdb}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: "8123"
      REDIS_URL: redis://redis:6379/0
    depends_on:
      db:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8000/health/status')\""]
      interval: 5s
      timeout: 10s
      retries: 12
      start_period: 15s

volumes:
  postgres_data:
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Redis service to docker-compose and wire REDIS_URL into app"
```
