# ClickHouse Data Service Implementation Design

**Goal:** Replace the file-based IPC stream in the `/data` endpoint with a ClickHouse-backed service, adding `GET /data/count` and `GET /data/rows` as illustrative best-practice toy endpoints.

**Architecture:** A `clickhouse-connect` async client singleton lives in `persistence/analytics_store/clickhouse/`, initialised from `Settings` at module import time (identical pattern to `postgres_engine.py`). `DataService` is refactored to be async, takes the client as a constructor argument, and remains a singleton in the DI `Container`. Tests use a session-scoped `testcontainers` ClickHouse container with seeded data and override `DataService` via `dependency_overrides`.

**Tech Stack:** `clickhouse-connect` (official ClickHouse async Python client), `testcontainers` (ClickHouse container for tests), FastAPI `Query` parameter validation.

---

## Table Schema

Table `items` in the `default` database:

```sql
CREATE TABLE items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id
```

---

## File Structure

| File | Change |
|---|---|
| `persistence/analytics_store/clickhouse/clickhouse_client.py` | New — async client singleton |
| `core/settings.py` | Add 5 ClickHouse connection fields |
| `core/container.py` | Register ClickHouse client + updated DataService |
| `services/data.py` | Rewrite — async, takes client, `get_count()` + `get_rows()` |
| `schemas/data.py` | Replace `DataShapeResponse` with `DataCountResponse` + `DataRowResponse` + `DataRowsResponse` |
| `routers/data.py` | Replace `GET /data/shape` with `GET /data/count` + `GET /data/rows` |
| `main.py` | Add ClickHouse `ping()` to lifespan startup health check |
| `tests/conftest.py` | Add ClickHouse container + seed fixtures, update `override_data_service` |
| `tests/test_data.py` | Rewrite for both new endpoints |

---

## Settings

Five new fields added to `core/settings.py` (`Settings` class):

```python
clickhouse_host: str = "localhost"
clickhouse_port: int = 8123
clickhouse_user: str = "default"
clickhouse_password: str = ""
clickhouse_database: str = "default"
```

---

## Persistence Layer

`persistence/analytics_store/clickhouse/clickhouse_client.py` creates the async client singleton at module import time:

```python
import clickhouse_connect
from core.settings import Settings

_settings = Settings()
clickhouse_client = clickhouse_connect.get_async_client(
    host=_settings.clickhouse_host,
    port=_settings.clickhouse_port,
    username=_settings.clickhouse_user,
    password=_settings.clickhouse_password,
    database=_settings.clickhouse_database,
)
```

---

## Schemas

```python
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

---

## DataService

Constructor takes the ClickHouse async client. Two async methods:

- `get_count()` — executes `SELECT count() FROM items`, returns `DataCountResponse`
- `get_rows(limit, offset)` — executes `SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}` plus a count query for `total`, returns `DataRowsResponse`

---

## Routers

```
GET /data/count                           → DataCountResponse
GET /data/rows?limit=10&offset=0          → DataRowsResponse
```

`limit`: default 10, validated `ge=1, le=100` via `Query`. `offset`: default 0, validated `ge=0`.

---

## Container

`core/container.py` updated to:
1. Import `clickhouse_client` from `persistence/analytics_store/clickhouse/clickhouse_client.py`
2. Register `DataService(clickhouse_client)` as a singleton (replaces the current `DataService(settings)`)

---

## Startup Health Check

`main.py` lifespan adds a ClickHouse `ping()` alongside the existing Postgres `SELECT 1`:

```python
await clickhouse_client.ping()
```

If ClickHouse is unreachable at startup the application fails fast.

---

## Testing

Session-scoped testcontainers pattern, mirroring `test_config.py`:

- `clickhouse_container` — session-scoped fixture, `ClickHouseContainer("clickhouse/clickhouse-server:latest")`
- `test_clickhouse_client` — async session-scoped fixture; connects to container, creates `items` table, seeds 3 rows
- `override_data_service` — updated to construct `DataService(test_clickhouse_client)`; overrides `get_data_service` in `dependency_overrides`

**Test cases (`tests/test_data.py`):**

| Test | Endpoint | Expected |
|---|---|---|
| `test_get_count_returns_total` | `GET /data/count` | `{"count": 3}` |
| `test_get_rows_returns_all` | `GET /data/rows` | 3 rows, `total=3` |
| `test_get_rows_with_limit` | `GET /data/rows?limit=2` | 2 rows, `total=3` |
| `test_get_rows_with_offset` | `GET /data/rows?limit=2&offset=2` | 1 row, `total=3` |

`total` always reflects the full table count so callers can implement client-side pagination.
