# Redis Cache Feature Design

## Goal

Add a Redis-backed cache store to the template, exposing `POST /cache/` and `GET /cache/{key}` endpoints. Illustrates a third persistence pattern alongside Postgres and ClickHouse, using the same async context manager convention.

## Architecture

Redis slots in as a third persistence store under `persistence/cache_store/redis/`. The feature follows the identical layered structure: client → service → router. `RedisClient.__aenter__` returns `redis.asyncio.Redis` directly (mirroring `ClickHouseClient`), since redis-py manages its own connection pool internally. No schema init SQL is needed — Redis is schemaless.

## New Files

| File | Responsibility |
|------|---------------|
| `persistence/cache_store/redis/redis_client.py` | Async context manager; `__aenter__` returns `redis.asyncio.Redis` |
| `services/cache.py` | `CacheService` — `set` and `get` methods, holds the client |
| `schemas/cache.py` | `CacheSetRequest`, `CacheEntry` — both include optional `ttl_seconds` |
| `routers/cache.py` | `POST /cache/` and `GET /cache/{key}`; exports `TAG` and `TAG_METADATA` |
| `tests/test_cache.py` | Four integration tests: set, get, 404, TTL |

## Modified Files

| File | Change |
|------|--------|
| `core/settings.py` | Add `redis_url: str = "redis://localhost:6379/0"` |
| `core/dependencies.py` | Add `get_cache_service()` and `CacheServiceDep` |
| `main.py` | Add `RedisClient` to `create_lifespan`; register `CacheService` singleton; mount `cache.router` at `/cache`; add `cache.TAG_METADATA` to `openapi_tags` |
| `docker-compose.yml` | Add `redis:7` service with healthcheck; add `REDIS_URL` env var to `app` service |
| `requirements.txt` | Add `redis[hiredis]` and `testcontainers[redis]` |
| `tests/conftest.py` | Add `redis_container` fixture; add `redis_url` to combined test settings in `test_client` |

## Component Details

### RedisClient

```python
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

### CacheService

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

`ttl()` returns `-1` for keys with no expiry and `-2` for missing keys. Both negative values map to `None` in the response.

### Schemas

```python
class CacheSetRequest(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None

class CacheEntry(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None
```

### Router

```python
TAG = "Cache"
TAG_METADATA = {"name": TAG, "description": "Endpoints for reading and writing Redis cache entries"}

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

### Lifespan wiring (in `create_lifespan`)

`RedisClient` is nested between `PostgresClient` and `ClickHouseClient`:

```python
async with PostgresClient(settings) as pg_pool:
    # ... schema init, ConfigService registration ...
    async with RedisClient(settings) as redis_client:
        service_container.register_singleton(CacheService, CacheService(redis_client))
        async with ClickHouseClient(settings) as ch_client:
            # ... ping, DataService registration ...
            async with mcp.session_manager.run():
                yield
```

### Docker Compose

```yaml
redis:
  image: redis:7
  ports:
    - "6379:6379"
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 5s
    timeout: 5s
    retries: 10
```

`app` service gains `REDIS_URL: redis://redis:6379/0` in `environment` and `redis: condition: service_healthy` in `depends_on`.

### Testing

**`conftest.py` additions:**

```python
from testcontainers.redis import RedisContainer

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as r:
        yield r
```

`test_client` gains `redis_container` as a parameter and adds to the combined settings:

```python
redis_url=f"redis://localhost:{redis_container.get_exposed_port(6379)}/0"
```

**`tests/test_cache.py`:**

```python
async def test_post_cache_creates_entry(test_client):
    response = await test_client.post("/cache/", json={"key": "foo", "value": "bar"})
    assert response.status_code == 201
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}

async def test_get_cache_returns_entry(test_client):
    response = await test_client.get("/cache/foo")
    assert response.status_code == 200
    assert response.json() == {"key": "foo", "value": "bar", "ttl_seconds": None}

async def test_get_cache_missing_key_returns_404(test_client):
    response = await test_client.get("/cache/nonexistent")
    assert response.status_code == 404

async def test_post_cache_with_ttl(test_client):
    response = await test_client.post("/cache/", json={"key": "expiring", "value": "soon", "ttl_seconds": 60})
    assert response.status_code == 201
    assert response.json() == {"key": "expiring", "value": "soon", "ttl_seconds": 60}
```
