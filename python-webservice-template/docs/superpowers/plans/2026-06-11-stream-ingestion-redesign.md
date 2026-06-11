# Stream Ingestion Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a `BatchConsumer` Protocol that decouples Flight/Solace/HTTP transports from the `LSMStore`, replace all unit tests with real-endpoint integration tests, and add k6 performance tests for all three ingest transports.

**Architecture:** A two-method `BatchConsumer` Protocol (`batches()` + `close()`) in `ingestion/base.py` is the only abstraction. Each transport (`ingestion/flight/`, `ingestion/solace/`) is an async context manager that also implements the Protocol. `StreamIngestService` owns the ingest thread and works with any consumer. Config selects the active transport at startup.

**Tech Stack:** Python/FastAPI, pyarrow.flight, solace-pubsubplus, polars, testcontainers (DockerContainer for Solace), pytest-asyncio, k6.

**Run tests from:** `python-webservice-template/` directory.

---

## File Map

### Created
| File | Responsibility |
|------|---------------|
| `ingestion/__init__.py` | Package marker |
| `ingestion/base.py` | `BatchConsumer` Protocol |
| `ingestion/flight/__init__.py` | Package marker |
| `ingestion/flight/client.py` | `FlightBatchConsumer` — async ctx mgr + `BatchConsumer` impl |
| `ingestion/solace/__init__.py` | Package marker |
| `ingestion/solace/client.py` | `SolaceBatchConsumer` — async ctx mgr + `BatchConsumer` impl |
| `persistence/stream_store/__init__.py` | Package marker |
| `persistence/stream_store/lsm_store.py` | `LSMStore` promoted from `flight/` subfolder — logic unchanged |
| `services/stream_ingest.py` | `StreamIngestService` — ingest thread, `get_data`, `ingest_batch` |
| `tests/publishers/__init__.py` | Package marker |
| `tests/publishers/flight_server.py` | `ExampleFlightServer` + `make_batch` (replaces `example_server.py` + `flight_helpers.py`) |
| `tests/publishers/solace_publisher.py` | `SolacePublisher` — publishes Arrow IPC batches to Solace |
| `tests/test_http_ingest.py` | Integration tests for `POST /data/ingest` |
| `tests/test_solace_cache.py` | Integration tests for Solace-fed cache (self-contained fixtures) |
| `tests/performance/data/generate_ingest_batch.py` | Generates `ingest_batch.ipc` fixture once |
| `tests/performance/ingest_http.js` | k6: pumps `POST /data/ingest` |
| `tests/performance/solace_cache.js` | k6: reads `GET /data/cache` under Solace ingest |
| `tests/performance/publishers/solace_publisher.py` | Continuous Solace publisher for perf runs |

### Modified
| File | Change |
|------|--------|
| `requirements.txt` | Add `solace-pubsubplus` |
| `settings.py` | Add `ingest_transport`, Solace connection fields |
| `pytest.ini` | Register `flight`, `solace`, `http_ingest` marks |
| `main.py` | Config-driven consumer selection, `StreamIngestService` wiring |
| `core/dependencies.py` | Replace `FlightCacheServiceDep` → `StreamIngestServiceDep` |
| `routers/data.py` | Update `GET /cache` dep; add `POST /ingest` |
| `tests/conftest.py` | Update imports; update `example_flight_server` fixture |
| `tests/test_flight_cache.py` | New batch design, four assertions |
| `tests/test_example_server.py` | Update imports; remove `test_empty_script_rejected` |
| `tests/performance/lib/checks.js` | Add `checkStatus202` |
| `docker-compose.yml` | Add `solace`, `solace-publisher`; `INGEST_TRANSPORT` on `app` |

### Deleted
| File | Reason |
|------|--------|
| `persistence/stream_store/flight/flight_client.py` | Replaced by `ingestion/flight/client.py` |
| `persistence/stream_store/flight/lsm_store.py` | Promoted to `persistence/stream_store/lsm_store.py` |
| `services/flight_cache.py` | Replaced by `services/stream_ingest.py` |
| `tests/example_server.py` | Replaced by `tests/publishers/flight_server.py` |
| `tests/flight_helpers.py` | Absorbed into `tests/publishers/flight_server.py` |
| `tests/test_lsm_store.py` | Unit tests replaced by real-endpoint tests |
| `tests/test_flight_service.py` | Unit tests replaced by real-endpoint tests |
| `tests/test_flight_merge.py` | Unit tests replaced by real-endpoint tests |

---

## Task 1: Dependencies, Settings, Pytest Marks

**Files:**
- Modify: `requirements.txt`
- Modify: `settings.py`
- Modify: `pytest.ini`

- [ ] **Step 1: Add `solace-pubsubplus` to requirements**

```
# requirements.txt — add after "redis[hiredis]":
solace-pubsubplus
```

- [ ] **Step 2: Extend `settings.py` with `ingest_transport` and Solace fields**

Add after `lsm_key_columns`:
```python
    # Ingestion transport selector
    ingest_transport: str = "flight"        # "flight" | "solace"

    # Solace — only resolved when ingest_transport="solace"
    solace_host: str = "localhost"
    solace_port: int = 55555
    solace_vpn: str = "default"
    solace_username: str = "admin"
    solace_password: str = "admin"
    solace_topic: str = "ingest/batches"
```

- [ ] **Step 3: Register marks in `pytest.ini`**

```ini
[pytest]
pythonpath = .
asyncio_mode = auto
asyncio_default_fixture_loop_scope = session
asyncio_default_test_loop_scope = session
markers =
    flight: tests that require a real Flight server
    solace: tests that require a Solace broker container
    http_ingest: tests for the HTTP ingest endpoint
```

- [ ] **Step 4: Commit**

```bash
git add requirements.txt settings.py pytest.ini
git commit -m "chore: add solace-pubsubplus dep, ingest settings, pytest marks"
```

---

## Task 2: Relocate `lsm_store.py`

The LSM logic is unchanged — this task is a pure file move with import updates so
the rest of the codebase can transition without breaking the existing tests.

**Files:**
- Create: `persistence/stream_store/__init__.py`
- Create: `persistence/stream_store/lsm_store.py`
- Modify: `services/flight_cache.py` (import path only)

- [ ] **Step 1: Create package marker**

Create `persistence/stream_store/__init__.py` as an empty file.

- [ ] **Step 2: Create `persistence/stream_store/lsm_store.py`**

Copy the entire contents of `persistence/stream_store/flight/lsm_store.py` verbatim:

```python
from dataclasses import dataclass

import pyarrow as pa
import polars as pl

ORDER_COLUMN = "seqno"


def _merge_frame(frames: tuple[pl.DataFrame, ...],
                 key_columns: list[str]) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    winners = (
        combined
        .with_columns(
            pl.col(ORDER_COLUMN)
            .rank("ordinal", descending=True)
            .over(key_columns)
            .alias("_rn")
        )
        .filter(pl.col("_rn") == 1)
        .drop("_rn")
    )
    return winners


def _merge_to_rows(frames: tuple[pl.DataFrame, ...],
                   key_columns: list[str],
                   limit: int | None) -> tuple[list[dict], int]:
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit is not None:
        live = live.head(limit)
    return live.select(["id", "name", "value"]).to_dicts(), total


@dataclass(frozen=True)
class _Snapshot:
    runs: tuple[pl.DataFrame, ...]
    memtable: tuple[pl.DataFrame, ...]


class LSMStore:
    def __init__(self, flush_rows: int, compaction_runs: int,
                 key_columns: list[str] | None = None) -> None:
        self._flush_rows = flush_rows
        self._compaction_runs = compaction_runs
        self._key_columns = key_columns or ["id"]
        self._seqno = 0
        self._memtable: list[pl.DataFrame] = []
        self._memtable_rows = 0
        self._runs: list[pl.DataFrame] = []
        self._snapshot = _Snapshot(runs=(), memtable=())

    def ingest(self, batch: pa.RecordBatch) -> None:
        frame = pl.from_arrow(batch)
        n = frame.height
        frame = frame.with_columns(
            pl.Series(ORDER_COLUMN, range(self._seqno, self._seqno + n))
        )
        self._seqno += n
        self._memtable.append(frame)
        self._memtable_rows += n
        if self._memtable_rows >= self._flush_rows:
            self._flush()
            if len(self._runs) >= self._compaction_runs:
                self._compact()
        self._publish()

    def _flush(self) -> None:
        if not self._memtable:
            return
        self._runs.append(pl.concat(self._memtable, how="vertical"))
        self._memtable = []
        self._memtable_rows = 0

    def _compact(self) -> None:
        merged = _merge_frame(tuple(self._runs), self._key_columns)
        self._runs = [merged] if merged is not None else []

    def _publish(self) -> None:
        self._snapshot = _Snapshot(runs=tuple(self._runs),
                                   memtable=tuple(self._memtable))

    def query(self, limit: int) -> tuple[list[dict], int]:
        snap = self._snapshot
        return _merge_to_rows(snap.runs + snap.memtable, self._key_columns, limit)
```

- [ ] **Step 3: Update import in `services/flight_cache.py`** (line 8)

Change:
```python
from persistence.stream_store.flight.lsm_store import LSMStore
```
To:
```python
from persistence.stream_store.lsm_store import LSMStore
```

- [ ] **Step 4: Verify existing tests still pass**

```bash
pytest tests/test_flight_cache.py tests/test_lsm_store.py tests/test_flight_merge.py tests/test_flight_service.py -v
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add persistence/stream_store/__init__.py persistence/stream_store/lsm_store.py services/flight_cache.py
git commit -m "refactor: promote lsm_store.py out of flight/ subfolder"
```

---

## Task 3: `BatchConsumer` Protocol and `ingestion/` Skeleton

**Files:**
- Create: `ingestion/__init__.py`
- Create: `ingestion/base.py`
- Create: `ingestion/flight/__init__.py`
- Create: `ingestion/solace/__init__.py`

- [ ] **Step 1: Create package markers**

Create the following as empty files:
- `ingestion/__init__.py`
- `ingestion/flight/__init__.py`
- `ingestion/solace/__init__.py`

- [ ] **Step 2: Create `ingestion/base.py`**

```python
from typing import Iterator, Protocol

import pyarrow as pa


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    batches() is a blocking generator; close() must be thread-safe and unblock
    any pending batches() call so the ingest thread can exit cleanly.
    """

    def batches(self) -> Iterator[pa.RecordBatch]: ...

    def close(self) -> None: ...
```

- [ ] **Step 3: Commit**

```bash
git add ingestion/
git commit -m "feat: add ingestion/ package with BatchConsumer Protocol"
```

---

## Task 4: `FlightBatchConsumer`

**Files:**
- Create: `ingestion/flight/client.py`

- [ ] **Step 1: Create `ingestion/flight/client.py`**

```python
import asyncio
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight

from settings import Settings


class FlightBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None

    async def __aenter__(self) -> "FlightBatchConsumer":
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = self._client.do_get(ticket)
        while True:
            try:
                yield reader.read_chunk().data
            except StopIteration:
                break

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/flight/client.py
git commit -m "feat: add FlightBatchConsumer to ingestion/flight/"
```

---

## Task 5: `StreamIngestService`

**Files:**
- Create: `services/stream_ingest.py`

- [ ] **Step 1: Create `services/stream_ingest.py`**

```python
import asyncio
import logging
import threading

import pyarrow as pa

from ingestion.base import BatchConsumer
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class StreamIngestService:
    def __init__(self, consumer: BatchConsumer, store: LSMStore) -> None:
        self._consumer = consumer
        self._store = store
        self._thread: threading.Thread | None = None

    async def __aenter__(self) -> "StreamIngestService":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()

    def _ingest_loop(self) -> None:
        try:
            for batch in self._consumer.batches():
                try:
                    self._store.ingest(batch)
                except Exception:
                    log.exception("ingest failed; skipping batch")
        except Exception:
            log.exception("consumer batches() failed; stopping ingest")

    async def stop(self) -> None:
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join)
            self._thread = None

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._store.ingest, batch)
```

- [ ] **Step 2: Commit**

```bash
git add services/stream_ingest.py
git commit -m "feat: add StreamIngestService with generic BatchConsumer ingest thread"
```

---

## Task 6: Wire Up New Components

Replace `FlightCacheService` with `StreamIngestService` throughout the app wiring.
The existing `test_flight_cache.py` end-to-end tests are the verification gate.

**Files:**
- Modify: `core/dependencies.py`
- Modify: `routers/data.py`
- Modify: `main.py`

- [ ] **Step 1: Update `core/dependencies.py`**

Replace the file entirely:

```python
from typing import Annotated

from fastapi import Depends

from settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService
from services.cache import CacheService
from services.stream_ingest import StreamIngestService


def get_health_service() -> HealthService:
    return service_container.get(HealthService)


def get_data_service() -> DataService:
    return service_container.get(DataService)


def get_config_service() -> ConfigService:
    return service_container.get(ConfigService)


def get_cache_service() -> CacheService:
    return service_container.get(CacheService)


def get_stream_ingest_service() -> StreamIngestService:
    return service_container.get(StreamIngestService)


SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
StreamIngestServiceDep = Annotated[StreamIngestService, Depends(get_stream_ingest_service)]
```

- [ ] **Step 2: Update `routers/data.py`**

Replace the file entirely:

```python
import logging

import pyarrow as pa
from fastapi import APIRouter, HTTPException, Query, Request

from core.dependencies import DataServiceDep, StreamIngestServiceDep
from schemas.data import DataRowsResponse

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for retrieving data"
    }

router = APIRouter(tags=[TAG])


@router.get("", response_model=DataRowsResponse)
async def get_data(
    data_service: DataServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await data_service.get_data(limit=limit)


@router.get("/cache", response_model=DataRowsResponse)
async def get_cached_data(
    svc: StreamIngestServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await svc.get_data(limit=limit)


@router.post("/ingest", status_code=202)
async def ingest_batch(
    request: Request,
    svc: StreamIngestServiceDep,
) -> dict:
    body = await request.body()
    try:
        reader = pa.ipc.open_stream(pa.BufferReader(body))
        for batch in reader:
            await svc.ingest_batch(batch)
    except pa.ArrowInvalid as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"accepted": True}
```

- [ ] **Step 3: Update `main.py`**

Replace the file entirely:

```python
import logging
from contextlib import asynccontextmanager, AsyncExitStack
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import service_container
from settings import get_settings, Settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.cache_store.redis.redis_client import RedisClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from persistence.stream_store.lsm_store import LSMStore
from ingestion.flight.client import FlightBatchConsumer
from ingestion.solace.client import SolaceBatchConsumer
from routers import health, data, config, cache
from mcp_routers import tools
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.stream_ingest import StreamIngestService

log = logging.getLogger(__name__)

logging.getLogger("asyncio").addFilter(
    lambda r: not (r.exc_info and isinstance(r.exc_info[1], ConnectionResetError))
)

settings = get_settings()

mcp = FastMCP(
    name=settings.mcp_name,
    streamable_http_path="/",
    instructions=settings.mcp_instructions,
)

tools.register(mcp)

_CONSUMERS = {
    "flight": FlightBatchConsumer,
    "solace": SolaceBatchConsumer,
}


def create_lifespan(settings: Settings):
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async with AsyncExitStack() as stack:
            pg_pool = await stack.enter_async_context(PostgresClient(settings))
            schema_sql = (Path(__file__).parent / "scripts" / "postgres-init.sql").read_text()
            async with pg_pool.acquire() as conn:
                await conn.execute(schema_sql)
            service_container.register_singleton(ConfigService, ConfigService(pg_pool))

            redis_client = await stack.enter_async_context(RedisClient(settings))
            service_container.register_singleton(CacheService, CacheService(redis_client))

            ch_client = await stack.enter_async_context(ClickHouseClient(settings))
            if not await ch_client.ping():
                raise RuntimeError("ClickHouse startup ping failed")
            service_container.register_singleton(DataService, DataService(ch_client))

            ConsumerClass = _CONSUMERS[settings.ingest_transport]
            consumer = await stack.enter_async_context(ConsumerClass(settings))
            store = LSMStore(
                flush_rows=settings.lsm_flush_rows,
                compaction_runs=settings.lsm_compaction_runs,
                key_columns=settings.lsm_key_columns,
            )
            ingest_svc = await stack.enter_async_context(StreamIngestService(consumer, store))
            service_container.register_singleton(StreamIngestService, ingest_svc)

            await stack.enter_async_context(mcp.session_manager.run())
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
        host=settings.server_host,
        port=settings.server_port,
        ssl_keyfile=settings.ssl_keyfile,
        ssl_certfile=settings.ssl_certfile,
    )
```

- [ ] **Step 4: Verify flight tests still pass**

The `SolaceBatchConsumer` import in `main.py` will fail until Task 13. To avoid this,
temporarily stub the solace import by creating `ingestion/solace/client.py` now:

```python
# ingestion/solace/client.py — temporary stub, replaced in Task 13
from typing import Iterator
import asyncio
import pyarrow as pa
from settings import Settings


class SolaceBatchConsumer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def __aenter__(self) -> "SolaceBatchConsumer":
        return self

    async def __aexit__(self, *_: object) -> None:
        pass

    def batches(self) -> Iterator[pa.RecordBatch]:
        return iter([])

    def close(self) -> None:
        pass
```

```bash
pytest tests/test_flight_cache.py -v
```
Expected: 3 tests pass (poll will still find total=2 from existing flight server fixture).

- [ ] **Step 5: Commit**

```bash
git add core/dependencies.py routers/data.py main.py ingestion/solace/client.py
git commit -m "feat: wire StreamIngestService and POST /data/ingest into app"
```

---

## Task 7: Create `tests/publishers/flight_server.py`

Absorbs `tests/example_server.py` and `tests/flight_helpers.py` into one file.
Empty scripts are now permitted (returns zero batches — used by HTTP ingest tests).

**Files:**
- Create: `tests/publishers/__init__.py`
- Create: `tests/publishers/flight_server.py`
- Modify: `tests/test_example_server.py`

- [ ] **Step 1: Create `tests/publishers/__init__.py`** as an empty file.

- [ ] **Step 2: Create `tests/publishers/flight_server.py`**

```python
import os
import time

import pyarrow as pa
import pyarrow.flight as flight


RECORD_SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("value", pa.string()),
    pa.field("op", pa.string()),
])


def make_batch(rows: list[tuple[int, str, str, str]]) -> pa.RecordBatch:
    """rows: list of (id, name, value, op)."""
    return pa.record_batch({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "name": pa.array([r[1] for r in rows], pa.string()),
        "value": pa.array([r[2] for r in rows], pa.string()),
        "op": pa.array([r[3] for r in rows], pa.string()),
    })


class ExampleFlightServer(flight.FlightServerBase):
    def __init__(self, location, script: list[pa.RecordBatch],
                 interval: float, loop: bool = False) -> None:
        super().__init__(location)
        self._script = script
        self._interval = interval
        self._loop = loop
        # Use provided schema or fall back to module constant for empty scripts
        self._schema = script[0].schema if script else RECORD_SCHEMA

    def do_get(self, context, ticket):
        def gen():
            while True:
                for batch in self._script:
                    if self._interval:
                        time.sleep(self._interval)
                    yield batch
                if not self._loop:
                    break

        return flight.GeneratorStream(self._schema, gen())


def _default_script() -> list[pa.RecordBatch]:
    return [
        make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert"), (3, "c", "z", "upsert")]),
        make_batch([(1, "a", "new", "upsert")]),
        make_batch([(2, "b", "y", "delete")]),
    ]


def main() -> None:
    host = os.environ.get("FLIGHT_BIND_HOST", "0.0.0.0")
    port = int(os.environ.get("FLIGHT_PORT", "8815"))
    interval = float(os.environ.get("FLIGHT_INTERVAL", "0.2"))
    location = flight.Location.for_grpc_tcp(host, port)
    server = ExampleFlightServer(location, _default_script(), interval=interval, loop=True)
    server.serve()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Update `tests/test_example_server.py`**

Replace the file entirely. The `test_empty_script_rejected` test is removed (empty scripts
are now valid — they produce zero batches via the `RECORD_SCHEMA` fallback):

```python
import threading

import pyarrow.flight as flight

from tests.publishers.flight_server import ExampleFlightServer, make_batch, _default_script


def test_example_server_streams_script():
    script = [
        make_batch([(1, "a", "x", "upsert")]),
        make_batch([(2, "b", "y", "upsert")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    client = None
    try:
        client = flight.connect(f"grpc://localhost:{server.port}")
        reader = client.do_get(flight.Ticket(b"items"))
        table = reader.read_all()
        assert table.num_rows == 2
    finally:
        if client is not None:
            client.close()
        server.shutdown()


def test_empty_script_serves_zero_batches():
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, [], interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    client = None
    try:
        client = flight.connect(f"grpc://localhost:{server.port}")
        reader = client.do_get(flight.Ticket(b"items"))
        table = reader.read_all()
        assert table.num_rows == 0
    finally:
        if client is not None:
            client.close()
        server.shutdown()


def test_default_script_exercises_lsm_edge_cases():
    script = _default_script()
    assert len(script) == 3
    assert script[0].num_rows == 3
    assert script[2].column("op").to_pylist() == ["delete"]


def test_main_uses_env_and_serves(monkeypatch):
    import tests.publishers.flight_server as fs

    served = {}

    def fake_serve(self):
        served["port"] = self.port

    monkeypatch.setattr(fs.ExampleFlightServer, "serve", fake_serve)
    monkeypatch.setenv("FLIGHT_BIND_HOST", "localhost")
    monkeypatch.setenv("FLIGHT_PORT", "0")
    monkeypatch.setenv("FLIGHT_INTERVAL", "0.0")

    fs.main()

    assert "port" in served
```

- [ ] **Step 4: Run example server tests**

```bash
pytest tests/test_example_server.py -v
```
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/publishers/ tests/test_example_server.py
git commit -m "refactor: move ExampleFlightServer to tests/publishers/, support empty script"
```

---

## Task 8: Update `tests/conftest.py`

Replace the flight fixture imports and the `example_flight_server` script to use
the new batch design (BATCH_1, BATCH_2, BATCH_3 with explicit LSM annotations).

**Files:**
- Modify: `tests/conftest.py`

- [ ] **Step 1: Replace the file**

```python
import asyncio
import threading
from pathlib import Path

import pytest
import pyarrow.flight as flight
import pyarrow.ipc as pa_ipc
from httpx import AsyncClient, ASGITransport
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from core.container import service_container
from core.dependencies import get_health_service
from settings import Settings
from services.health import HealthService
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from tests.publishers.flight_server import ExampleFlightServer, make_batch

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"
REDIS_IMAGE = "redis/redis-stack-server:latest"


# ── Test Settings ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def test_settings():
    return Settings(status="testing", data_dir="./tests/test_data")


# ── ClickHouse fixtures ────────────────────────────────────────────────────

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
        with pa_ipc.open_file(Path(__file__).parent / "test_data" / "clickhouse_seed_data.ipc") as reader:
            arrow_table = reader.read_all()
        await client.insert_arrow("default.items", arrow_table)
        yield client


# ── Postgres fixtures ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(PG_IMAGE) as pg:
        yield pg


# ── Redis fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer(REDIS_IMAGE) as r:
        yield r


# ── Flight fixtures ────────────────────────────────────────────────────────
#
# Three batches designed to exercise all LSM paths with lsm_flush_rows=2,
# lsm_compaction_runs=2:
#
#   BATCH_1: 2 rows → flush threshold hit → run1 created
#   BATCH_2: 2 rows → flush → run2 created → compaction triggered (2 runs)
#             compaction merges run1+run2: id=1 gets v2 (higher seqno wins)
#   BATCH_3: 1 row → stays in memtable; tombstone beats id=2 in compacted run
#
# Expected GET /data/cache result: total=2, id=1→"v2", id=2 absent, id=3→"v1"

@pytest.fixture(scope="session")
def example_flight_server():
    script = [
        make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")]),
        make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")]),
        make_batch([(2, "beta", "v1", "delete")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.02, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


# ── Override Services ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def override_health_service(test_settings):
    yield HealthService(test_settings)


# ── Async Test Client ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
async def test_client(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    example_flight_server,
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
        flight_host="localhost",
        flight_port=example_flight_server.port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )

    app.dependency_overrides[get_health_service] = lambda: override_health_service
    service_container.register_singleton(HealthService, override_health_service)

    lifespan_ready = asyncio.Event()
    lifespan_done = asyncio.Event()

    async def _run_lifespan():
        async with create_lifespan(test_settings)(app):
            lifespan_ready.set()
            await lifespan_done.wait()

    lifespan_task = asyncio.create_task(_run_lifespan())
    await lifespan_ready.wait()

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
    finally:
        lifespan_done.set()
        await lifespan_task
```

- [ ] **Step 2: Run the full flight test suite**

```bash
pytest tests/test_flight_cache.py tests/test_example_server.py -v
```
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "refactor: update conftest flight fixtures to use publishers/ and new batch design"
```

---

## Task 9: Update `tests/test_flight_cache.py`

Replace the test bodies to match the new batch design with explicit comments
explaining which LSM path each assertion exercises.

**Files:**
- Modify: `tests/test_flight_cache.py`

- [ ] **Step 1: Replace the file**

```python
import asyncio
import time

import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.flight


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_newest_wins_across_compaction(test_client: AsyncClient):
    # BATCH_2 flushes → compaction merges run1+run2; id=1's seqno in run2 > run1
    body = await _poll_cache(test_client, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[1] == "v2"


async def test_tombstone_beats_compacted_run(test_client: AsyncClient):
    # BATCH_3 (delete for id=2) stays in memtable; its seqno > id=2 in compacted run
    body = await _poll_cache(test_client, expected_total=2)
    assert 2 not in {r["id"] for r in body["rows"]}


async def test_unmodified_row_survives(test_client: AsyncClient):
    # id=3 was introduced in BATCH_2, never updated or deleted
    body = await _poll_cache(test_client, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[3] == "v1"


async def test_limit_respected(test_client: AsyncClient):
    await _poll_cache(test_client, expected_total=2)
    body = (await test_client.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
```

- [ ] **Step 2: Run to verify**

```bash
pytest tests/test_flight_cache.py -v
```
Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_flight_cache.py
git commit -m "test: replace flight cache tests with documented batch design"
```

---

## Task 10: Delete Obsolete Files

**Files:** all listed below are deleted.

- [ ] **Step 1: Delete old persistence layer**

```bash
git rm persistence/stream_store/flight/flight_client.py
git rm persistence/stream_store/flight/lsm_store.py
```

Check if `persistence/stream_store/flight/__init__.py` exists and remove if so:
```bash
git rm --ignore-unmatch persistence/stream_store/flight/__init__.py
```

- [ ] **Step 2: Delete old service**

```bash
git rm services/flight_cache.py
```

- [ ] **Step 3: Delete old test helpers and unit tests**

```bash
git rm tests/example_server.py
git rm tests/flight_helpers.py
git rm tests/test_lsm_store.py
git rm tests/test_flight_service.py
git rm tests/test_flight_merge.py
```

- [ ] **Step 4: Run the full test suite to confirm nothing is broken**

```bash
pytest tests/ -v --ignore=tests/test_solace_cache.py --ignore=tests/test_http_ingest.py
```
Expected: all existing tests pass; no import errors.

- [ ] **Step 5: Commit**

```bash
git commit -m "chore: delete obsolete flight_client, lsm_store (old path), flight_cache service, and unit tests"
```

---

## Task 11: `POST /data/ingest` — TDD

Write the test first against the unimplemented endpoint (it exists from Task 6, so
the test will actually pass immediately — but confirm correct behaviour with both
valid and invalid bodies).

**Files:**
- Create: `tests/test_http_ingest.py`

The HTTP ingest test uses its own module-scoped `test_client_http` fixture with an
**empty-script Flight server**, so the LSMStore starts empty and all data comes
from HTTP POST requests.

- [ ] **Step 1: Create `tests/test_http_ingest.py`**

```python
import asyncio
import threading
import time

import pyarrow as pa
import pyarrow.flight as pa_flight
import pyarrow.ipc as pa_ipc
import pytest
from httpx import AsyncClient, ASGITransport

from core.container import service_container
from core.dependencies import get_health_service
from settings import Settings
from services.health import HealthService
from tests.publishers.flight_server import ExampleFlightServer, make_batch

pytestmark = pytest.mark.http_ingest


def _serialize_batch(batch: pa.RecordBatch) -> bytes:
    buf = pa.BufferOutputStream()
    with pa_ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    return buf.getvalue().to_pybytes()


@pytest.fixture(scope="module")
def empty_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    # Empty script: server accepts connections but sends zero batches
    server = ExampleFlightServer(location, [], interval=0.0, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


@pytest.fixture(scope="module")
async def test_client_http(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    empty_flight_server,
):
    from main import app, create_lifespan

    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))

    http_settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="flight",
        flight_host="localhost",
        flight_port=empty_flight_server.port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )

    override_hs = HealthService(http_settings)
    app.dependency_overrides[get_health_service] = lambda: override_hs
    service_container.register_singleton(HealthService, override_hs)

    lifespan_ready = asyncio.Event()
    lifespan_done = asyncio.Event()

    async def _run_lifespan():
        async with create_lifespan(http_settings)(app):
            lifespan_ready.set()
            await lifespan_done.wait()

    lifespan_task = asyncio.create_task(_run_lifespan())
    await lifespan_ready.wait()

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
    finally:
        lifespan_done.set()
        await lifespan_task


async def _poll_for_id(client: AsyncClient, id_: int, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        rows_by_id = {r["id"]: r for r in body["rows"]}
        if id_ in rows_by_id:
            return rows_by_id[id_]
        await asyncio.sleep(0.05)
    raise AssertionError(f"id={id_} never appeared in cache")


async def test_post_ingest_upsert_appears_in_cache(test_client_http: AsyncClient):
    batch = make_batch([(100, "http", "v1", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    row = await _poll_for_id(test_client_http, 100)
    assert row["value"] == "v1"


async def test_post_ingest_newest_wins(test_client_http: AsyncClient):
    batch_v2 = make_batch([(100, "http", "v2", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch_v2),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    row = await _poll_for_id(test_client_http, 100)
    assert row["value"] == "v2"


async def test_post_ingest_tombstone(test_client_http: AsyncClient):
    delete_batch = make_batch([(100, "http", "v2", "delete")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(delete_batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        body = (await test_client_http.get("/data/cache?limit=100")).json()
        if 100 not in {r["id"] for r in body["rows"]}:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("id=100 still present after delete")


async def test_post_ingest_invalid_body_returns_400(test_client_http: AsyncClient):
    res = await test_client_http.post(
        "/data/ingest",
        content=b"not arrow ipc",
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 400
```

- [ ] **Step 2: Run the HTTP ingest tests**

```bash
pytest tests/test_http_ingest.py -v
```
Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_http_ingest.py
git commit -m "test: add HTTP ingest integration tests (POST /data/ingest)"
```

---

## Task 12: `SolacePublisher` Test Helper

**Files:**
- Create: `tests/publishers/solace_publisher.py`

- [ ] **Step 1: Create `tests/publishers/solace_publisher.py`**

```python
import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.resources.topic import Topic


class SolacePublisher:
    def __init__(self, host: str, port: int, vpn: str,
                 username: str, password: str, topic: str) -> None:
        self._topic = topic
        props = {
            "solace.messaging.transport.host": f"tcp://{host}:{port}",
            "solace.messaging.service.vpn-name": vpn,
            "solace.messaging.authentication.scheme.basic.username": username,
            "solace.messaging.authentication.scheme.basic.password": password,
        }
        self._service: MessagingService = (
            MessagingService.builder().from_properties(props).build()
        )
        self._service.connect()
        self._publisher: DirectMessagePublisher = (
            self._service.create_direct_message_publisher_builder().build()
        )
        self._publisher.start()

    def publish_batch(self, batch: pa.RecordBatch) -> None:
        buf = pa.BufferOutputStream()
        with pa_ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
        payload = buf.getvalue().to_pybytes()
        message = self._service.message_builder().build(payload)
        self._publisher.publish(message=message, destination=Topic.of(self._topic))

    def close(self) -> None:
        self._publisher.terminate()
        self._service.disconnect()
```

- [ ] **Step 2: Commit**

```bash
git add tests/publishers/solace_publisher.py
git commit -m "feat: add SolacePublisher test helper for publishing Arrow IPC batches"
```

---

## Task 13: `SolaceBatchConsumer`

Replaces the stub created in Task 6.

**Files:**
- Modify: `ingestion/solace/client.py`

- [ ] **Step 1: Replace `ingestion/solace/client.py`**

```python
import asyncio
import queue
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.resources.topic_subscription import TopicSubscription

from settings import Settings


class _BatchHandler(MessageHandler):
    def __init__(self, q: queue.Queue) -> None:
        self._queue = q

    def on_message(self, message: InboundMessage) -> None:
        payload = message.get_payload_as_bytes()
        try:
            reader = pa_ipc.open_stream(pa.BufferReader(payload))
            for batch in reader:
                self._queue.put(batch)
        except Exception:
            pass  # malformed message silently dropped


class SolaceBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: MessagingService | None = None
        self._receiver: DirectMessageReceiver | None = None
        self._queue: queue.Queue[pa.RecordBatch | None] = queue.Queue()

    async def __aenter__(self) -> "SolaceBatchConsumer":
        self._service = await asyncio.to_thread(self._connect)
        return self

    def _connect(self) -> MessagingService:
        props = {
            "solace.messaging.transport.host":
                f"tcp://{self._settings.solace_host}:{self._settings.solace_port}",
            "solace.messaging.service.vpn-name": self._settings.solace_vpn,
            "solace.messaging.authentication.scheme.basic.username":
                self._settings.solace_username,
            "solace.messaging.authentication.scheme.basic.password":
                self._settings.solace_password,
        }
        svc = MessagingService.builder().from_properties(props).build()
        svc.connect()
        return svc

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        self._receiver = (
            self._service
            .create_direct_message_receiver_builder()
            .with_subscriptions(TopicSubscription.of(self._settings.solace_topic))
            .build()
        )
        self._receiver.start()
        self._receiver.receive_async(_BatchHandler(self._queue))
        while True:
            item = self._queue.get()    # blocks until message or None sentinel
            if item is None:
                break
            yield item

    def close(self) -> None:
        self._queue.put(None)           # unblocks batches() generator
        if self._receiver is not None:
            self._receiver.terminate()
            self._receiver = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
```

- [ ] **Step 2: Verify flight tests still pass (Solace import must not break them)**

```bash
pytest tests/test_flight_cache.py -v
```
Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add ingestion/solace/client.py
git commit -m "feat: implement SolaceBatchConsumer with Arrow IPC deserialization"
```

---

## Task 14: Solace Integration Tests

**Files:**
- Create: `tests/test_solace_cache.py`

All fixtures are defined within this file (module-scoped) to avoid `service_container`
conflicts when the solace stage runs independently from the flight stage.

Run this test stage with: `pytest tests/test_solace_cache.py -v`

- [ ] **Step 1: Create `tests/test_solace_cache.py`**

```python
import asyncio
import time

import pytest
from httpx import AsyncClient, ASGITransport
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from core.container import service_container
from core.dependencies import get_health_service
from settings import Settings
from services.health import HealthService
from tests.publishers.flight_server import make_batch
from tests.publishers.solace_publisher import SolacePublisher

pytestmark = pytest.mark.solace

SOLACE_IMAGE = "solace/solace-pubsub-standard:latest"

# Same batch design as flight tests — proves transport equivalence.
# lsm_flush_rows=2, lsm_compaction_runs=2:
#   BATCH_1: 2 rows → flush → run1
#   BATCH_2: 2 rows → flush → run2 → compaction: id=1 gets v2
#   BATCH_3: 1 row  → memtable tombstone beats id=2 in compacted run
BATCH_1 = make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")])
BATCH_2 = make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")])
BATCH_3 = make_batch([(2, "beta", "v1", "delete")])


@pytest.fixture(scope="module")
def solace_container():
    container = (
        DockerContainer(SOLACE_IMAGE)
        .with_exposed_ports(55555, 8080)
        .with_env("username_admin_globalaccesslevel", "admin")
        .with_env("username_admin_password", "admin")
    )
    with container:
        wait_for_logs(container, "Primary Virtual Router Up", timeout=120)
        yield container


@pytest.fixture(scope="module")
async def test_client_solace(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    solace_container,
):
    from main import app, create_lifespan

    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))
    solace_smf_port = int(solace_container.get_exposed_port(55555))

    solace_settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="solace",
        solace_host="localhost",
        solace_port=solace_smf_port,
        solace_vpn="default",
        solace_username="admin",
        solace_password="admin",
        solace_topic="ingest/batches",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )

    override_hs = HealthService(solace_settings)
    app.dependency_overrides[get_health_service] = lambda: override_hs
    service_container.register_singleton(HealthService, override_hs)

    lifespan_ready = asyncio.Event()
    lifespan_done = asyncio.Event()

    async def _run_lifespan():
        async with create_lifespan(solace_settings)(app):
            lifespan_ready.set()
            await lifespan_done.wait()

    lifespan_task = asyncio.create_task(_run_lifespan())
    await lifespan_ready.wait()

    # Publish test batches AFTER the app has subscribed to the topic
    publisher = SolacePublisher(
        host="localhost",
        port=solace_smf_port,
        vpn="default",
        username="admin",
        password="admin",
        topic="ingest/batches",
    )
    try:
        publisher.publish_batch(BATCH_1)
        publisher.publish_batch(BATCH_2)
        publisher.publish_batch(BATCH_3)
    finally:
        publisher.close()

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
    finally:
        lifespan_done.set()
        await lifespan_task


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_newest_wins_across_compaction(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[1] == "v2"


async def test_tombstone_beats_compacted_run(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert 2 not in {r["id"] for r in body["rows"]}


async def test_unmodified_row_survives(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[3] == "v1"


async def test_limit_respected(test_client_solace: AsyncClient):
    await _poll_cache(test_client_solace, expected_total=2)
    body = (await test_client_solace.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
```

- [ ] **Step 2: Run Solace tests (requires Docker)**

```bash
pytest tests/test_solace_cache.py -v
```
Expected: 4 tests pass (Solace container takes ~60–90 s to be ready).

- [ ] **Step 3: Commit**

```bash
git add tests/test_solace_cache.py
git commit -m "test: add Solace integration tests using real PubSub+ testcontainer"
```

---

## Task 15: Docker-Compose Solace Additions

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add `solace` and `solace-publisher` services; add `INGEST_TRANSPORT` to `app`**

Add after the `redis` service block:

```yaml
  solace:
    image: solace/solace-pubsub-standard:latest
    ports:
      - "55555:55555"
      - "8080:8080"
    environment:
      username_admin_globalaccesslevel: admin
      username_admin_password: admin
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 18
      start_period: 60s

  solace-publisher:
    build: .
    command: ["python", "tests/performance/publishers/solace_publisher.py"]
    environment:
      SOLACE_HOST: solace
      SOLACE_PORT: "55555"
      SOLACE_VPN: default
      SOLACE_USERNAME: admin
      SOLACE_PASSWORD: admin
      SOLACE_TOPIC: "ingest/batches"
      PUBLISH_INTERVAL: "0.1"
    depends_on:
      solace:
        condition: service_healthy
```

Update the `app` service environment block to add `INGEST_TRANSPORT`:

```yaml
  app:
    ...
    environment:
      ...
      INGEST_TRANSPORT: ${INGEST_TRANSPORT:-flight}
      SOLACE_HOST: solace
      SOLACE_PORT: "55555"
      SOLACE_VPN: default
      SOLACE_USERNAME: admin
      SOLACE_PASSWORD: admin
      SOLACE_TOPIC: "ingest/batches"
```

Update `app` `depends_on` to add (conditionally for solace profile):

```yaml
    depends_on:
      db:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
      redis:
        condition: service_healthy
      flight:
        condition: service_healthy
```

Note: keep `flight` as the default `depends_on`; when running with Solace,
use `INGEST_TRANSPORT=solace docker compose up solace solace-publisher app`.

- [ ] **Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add solace and solace-publisher services to docker-compose"
```

---

## Task 16: Performance Test Fixtures

**Files:**
- Create: `tests/performance/data/generate_ingest_batch.py`
- Create: `tests/performance/data/ingest_batch.ipc` (generated)
- Modify: `tests/performance/lib/checks.js`

- [ ] **Step 1: Create `tests/performance/data/generate_ingest_batch.py`**

```python
"""Run once to regenerate ingest_batch.ipc: python generate_ingest_batch.py"""
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("value", pa.string()),
    pa.field("op", pa.string()),
])

# Mixed upsert/delete so the LSM stays exercised under load without growing unboundedly
ROWS = [
    (1, "alpha", "v1", "upsert"),
    (2, "beta",  "v1", "upsert"),
    (3, "gamma", "v1", "upsert"),
    (1, "alpha", "v2", "upsert"),  # newest-wins for id=1
    (2, "beta",  "v1", "delete"),  # tombstone for id=2
]

batch = pa.record_batch({
    "id":    pa.array([r[0] for r in ROWS], pa.int64()),
    "name":  pa.array([r[1] for r in ROWS], pa.string()),
    "value": pa.array([r[2] for r in ROWS], pa.string()),
    "op":    pa.array([r[3] for r in ROWS], pa.string()),
}, schema=SCHEMA)

out = Path(__file__).parent / "ingest_batch.ipc"
with pa_ipc.new_stream(out.open("wb"), SCHEMA) as writer:
    writer.write_batch(batch)

print(f"Written {out} ({out.stat().st_size} bytes)")
```

- [ ] **Step 2: Generate the fixture**

```bash
cd python-webservice-template
python tests/performance/data/generate_ingest_batch.py
```
Expected output: `Written .../ingest_batch.ipc (NNN bytes)`

- [ ] **Step 3: Add `checkStatus202` to `tests/performance/lib/checks.js`**

```javascript
import { check } from 'k6';

export function checkStatus200(res) {
  return check(res, { 'status is 200': (r) => r.status === 200 });
}

export function checkStatus202(res) {
  return check(res, { 'status is 202': (r) => r.status === 202 });
}

export function checkDataRows(res) {
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has rows array': (r) => {
      try { return Array.isArray(JSON.parse(r.body).rows); }
      catch { return false; }
    },
    'has total field': (r) => {
      try { return JSON.parse(r.body).total !== undefined; }
      catch { return false; }
    },
  });
}
```

- [ ] **Step 4: Commit**

```bash
git add tests/performance/data/generate_ingest_batch.py tests/performance/data/ingest_batch.ipc tests/performance/lib/checks.js
git commit -m "feat: add ingest_batch.ipc fixture and checkStatus202 k6 helper"
```

---

## Task 17: k6 Performance Scripts

**Files:**
- Create: `tests/performance/ingest_http.js`
- Create: `tests/performance/solace_cache.js`
- Create: `tests/performance/publishers/solace_publisher.py`

- [ ] **Step 1: Create `tests/performance/ingest_http.js`**

```javascript
import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus202 } from './lib/checks.js';
import { NORMAL_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const BATCH = open('./data/ingest_batch.ipc', 'b');

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: { ...NORMAL_SLO },
};

export default function () {
  const res = http.post(
    `${BASE_URL}/data/ingest`,
    BATCH,
    {
      headers: { 'Content-Type': 'application/vnd.apache.arrow.stream' },
      tags: { endpoint: 'data_ingest' },
    }
  );
  checkStatus202(res);
  sleep(0.1);
}
```

- [ ] **Step 2: Create `tests/performance/solace_cache.js`**

Measures `GET /data/cache` while the Solace publisher floods ingest — mirrors
`flight_cache.js` for the Solace transport:

```javascript
import http from 'k6/http';
import { sleep } from 'k6';
import { checkDataRows } from './lib/checks.js';
import { NORMAL_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  vus: 5,
  duration: '30s',
  thresholds: { ...NORMAL_SLO },
};

export default function () {
  checkDataRows(http.get(`${BASE_URL}/data/cache`, { tags: { endpoint: 'data_cache' } }));
  sleep(1);
}
```

- [ ] **Step 3: Create `tests/performance/publishers/solace_publisher.py`**

Continuous publisher for performance test runs. Run as a docker-compose service
(`solace-publisher`) or standalone:

```python
"""Continuous Solace publisher for performance test runs.

Environment variables:
  SOLACE_HOST, SOLACE_PORT, SOLACE_VPN, SOLACE_USERNAME, SOLACE_PASSWORD,
  SOLACE_TOPIC, PUBLISH_INTERVAL (seconds between publishes, default 0.1)
"""
import os
import time

import pyarrow as pa

from tests.publishers.solace_publisher import SolacePublisher
from tests.publishers.flight_server import make_batch

BATCHES = [
    make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")]),
    make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")]),
    make_batch([(2, "beta", "v1", "delete")]),
]


def main() -> None:
    publisher = SolacePublisher(
        host=os.environ.get("SOLACE_HOST", "localhost"),
        port=int(os.environ.get("SOLACE_PORT", "55555")),
        vpn=os.environ.get("SOLACE_VPN", "default"),
        username=os.environ.get("SOLACE_USERNAME", "admin"),
        password=os.environ.get("SOLACE_PASSWORD", "admin"),
        topic=os.environ.get("SOLACE_TOPIC", "ingest/batches"),
    )
    interval = float(os.environ.get("PUBLISH_INTERVAL", "0.1"))
    try:
        while True:
            for batch in BATCHES:
                publisher.publish_batch(batch)
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Commit**

```bash
git add tests/performance/ingest_http.js tests/performance/solace_cache.js tests/performance/publishers/solace_publisher.py
git commit -m "feat: add k6 HTTP ingest script, Solace cache script, and Solace perf publisher"
```

---

## Test Stages Summary

```bash
# Flight tests (default CI gate)
pytest tests/ -v --ignore=tests/test_solace_cache.py --ignore=tests/test_http_ingest.py

# HTTP ingest tests (separate stage)
pytest tests/test_http_ingest.py -v

# Solace tests (separate stage, requires Docker + Solace image)
pytest tests/test_solace_cache.py -v

# Performance — Flight ingest read-under-load
docker compose up -d && k6 run tests/performance/flight_cache.js

# Performance — HTTP ingest load test
docker compose up -d && k6 run tests/performance/ingest_http.js

# Performance — Solace ingest read-under-load
INGEST_TRANSPORT=solace docker compose up -d solace solace-publisher app
k6 run tests/performance/solace_cache.js
```
