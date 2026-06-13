# Arrow Flight LSM Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Apache Arrow Flight client that continuously consumes pushed record batches into an in-memory Log-Structured Merge (LSM) store, exposed via `GET /data/cache`.

**Architecture:** A fourth persistence store (`persistence/stream_store/flight/`) following the existing client → store → service → router layering. A dedicated ingest thread reads the (blocking) Flight stream and writes to a single-writer/multi-reader LSM with lock-free snapshot reads; queries merge on read via a polars window function offloaded with `asyncio.to_thread`.

**Tech Stack:** FastAPI, pyarrow + pyarrow.flight (already bundled), polars, pytest + threads, k6, docker-compose.

**Reference spec:** `docs/superpowers/specs/2026-06-07-flight-lsm-cache-design.md`

**Conventions to follow (already in the codebase):**
- Clients are async context managers: `__aenter__` returns the live handle, `__aexit__` closes with a `None` guard (see `persistence/cache_store/redis/redis_client.py`).
- Services are registered as singletons in `create_lifespan` in `main.py`, then resolved via `core/dependencies.py` getters + `Annotated` aliases.
- Tests drive REST endpoints via the `test_client` async HTTPX fixture; the real `create_lifespan` runs in tests.
- `pytest-asyncio` is configured so `async def test_*` functions run without decorators.

---

### Task 1: Settings fields

**Files:**
- Modify: `core/settings.py`
- Test: `tests/test_settings.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_settings.py`:

```python
from core.settings import Settings


def test_settings_has_flight_defaults():
    s = Settings()
    assert s.flight_host == "localhost"
    assert s.flight_port == 8815
    assert s.flight_ticket == "items"
    assert s.lsm_flush_rows == 1000
    assert s.lsm_compaction_runs == 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_settings.py -v`
Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'flight_host'`

- [ ] **Step 3: Add the fields**

In `core/settings.py`, add after the `redis_url` line (currently line 26):

```python
    redis_url: str = "redis://localhost:6379/0"

    flight_host: str = "localhost"
    flight_port: int = 8815          # pyarrow Flight default
    flight_ticket: str = "items"
    lsm_flush_rows: int = 1000       # memtable -> run threshold
    lsm_compaction_runs: int = 4     # run count -> compaction threshold
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_settings.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/settings.py tests/test_settings.py
git commit -m "feat: add Flight and LSM settings fields"
```

---

### Task 2: Window-function merge

**Files:**
- Create: `persistence/stream_store/flight/lsm_store.py` (merge helpers only in this task)
- Test: `tests/test_flight_merge.py` (create)

> Note: this codebase has **no `__init__.py` files** (they were removed in commit 9da8a54). Do not create any — imports work via the project root being on `sys.path`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_flight_merge.py`:

```python
import polars as pl

from persistence.stream_store.flight.lsm_store import _merge_to_rows


def _frame(rows):
    return pl.DataFrame(rows)


def test_merge_newest_wins():
    f = _frame([
        {"id": 1, "name": "a", "value": "old", "op": "upsert", "seqno": 0},
        {"id": 1, "name": "a", "value": "new", "op": "upsert", "seqno": 5},
    ])
    rows, total = _merge_to_rows((f,), ["id"], None)
    assert total == 1
    assert rows == [{"id": 1, "name": "a", "value": "new"}]


def test_merge_applies_tombstone():
    f = _frame([
        {"id": 1, "name": "a", "value": "v", "op": "upsert", "seqno": 0},
        {"id": 1, "name": "a", "value": "v", "op": "delete", "seqno": 5},
    ])
    rows, total = _merge_to_rows((f,), ["id"], None)
    assert rows == []
    assert total == 0


def test_merge_respects_limit():
    f = _frame([
        {"id": 1, "name": "a", "value": "x", "op": "upsert", "seqno": 0},
        {"id": 2, "name": "b", "value": "y", "op": "upsert", "seqno": 1},
    ])
    rows, total = _merge_to_rows((f,), ["id"], 1)
    assert total == 2
    assert len(rows) == 1


def test_merge_composite_key_extension():
    f = _frame([
        {"id": 1, "version": 1, "name": "a", "value": "v1", "op": "upsert", "seqno": 0},
        {"id": 1, "version": 2, "name": "a", "value": "v2", "op": "upsert", "seqno": 1},
    ])
    rows, total = _merge_to_rows((f,), ["id", "version"], None)
    assert total == 2  # different composite keys -> both survive


def test_merge_empty():
    rows, total = _merge_to_rows((), ["id"], None)
    assert rows == []
    assert total == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_flight_merge.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'persistence.stream_store'`

- [ ] **Step 3: Implement the merge helpers**

Create `persistence/stream_store/flight/lsm_store.py`:

```python
import polars as pl

ORDER_COLUMN = "seqno"


def _merge_frame(frames: tuple[pl.DataFrame, ...],
                 key_columns: list[str]) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    # Window function: rank rows within each key partition by recency
    # (newest seqno first). key_columns is the single extension point:
    # ["id"] today, ["id", "version"] later, with no other change.
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_flight_merge.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add persistence/stream_store/flight/lsm_store.py tests/test_flight_merge.py
git commit -m "feat: add window-function merge for Flight LSM store"
```

---

### Task 3: LSMStore engine (ingest, flush, compaction, query)

**Files:**
- Modify: `persistence/stream_store/flight/lsm_store.py`
- Create: `tests/flight_helpers.py` (shared Arrow batch builder)
- Test: `tests/test_lsm_store.py` (create)

- [ ] **Step 1: Write the shared batch helper**

Create `tests/flight_helpers.py`:

```python
import pyarrow as pa


def make_batch(rows: list[tuple[int, str, str, str]]) -> pa.RecordBatch:
    """rows: list of (id, name, value, op)."""
    return pa.record_batch({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "name": pa.array([r[1] for r in rows], pa.string()),
        "value": pa.array([r[2] for r in rows], pa.string()),
        "op": pa.array([r[3] for r in rows], pa.string()),
    })
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_lsm_store.py`:

```python
from persistence.stream_store.flight.lsm_store import LSMStore
from tests.flight_helpers import make_batch


def test_flush_creates_run():
    store = LSMStore(flush_rows=2, compaction_runs=10)
    store.ingest(make_batch([(1, "a", "x", "upsert"), (2, "b", "y", "upsert")]))
    assert len(store._runs) == 1
    assert store._memtable == []


def test_query_merges_memtable_and_runs():
    store = LSMStore(flush_rows=2, compaction_runs=10)
    store.ingest(make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert")]))  # flush -> run
    store.ingest(make_batch([(1, "a", "new", "upsert")]))                            # memtable
    rows, total = store.query(10)
    assert total == 2
    assert {"id": 1, "name": "a", "value": "new"} in rows


def test_compaction_reduces_runs():
    store = LSMStore(flush_rows=1, compaction_runs=2)
    store.ingest(make_batch([(1, "a", "v1", "upsert")]))  # flush -> run1
    store.ingest(make_batch([(1, "a", "v2", "upsert")]))  # flush -> run2 -> compact -> 1 run
    assert len(store._runs) == 1
    rows, total = store.query(10)
    assert rows == [{"id": 1, "name": "a", "value": "v2"}]
    assert total == 1


def test_query_empty_store():
    store = LSMStore(flush_rows=10, compaction_runs=10)
    assert store.query(10) == ([], 0)


def test_tombstone_then_query():
    store = LSMStore(flush_rows=10, compaction_runs=10)
    store.ingest(make_batch([(1, "a", "x", "upsert")]))
    store.ingest(make_batch([(1, "a", "x", "delete")]))
    assert store.query(10) == ([], 0)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_lsm_store.py -v`
Expected: FAIL with `ImportError: cannot import name 'LSMStore'`

- [ ] **Step 4: Implement LSMStore**

Append to `persistence/stream_store/flight/lsm_store.py` (keep the existing merge helpers and `ORDER_COLUMN`; add the missing imports at the top of the file and the classes at the bottom).

Add to the top imports (`import polars as pl` is already there from Task 2 — add only these):

```python
from dataclasses import dataclass

import pyarrow as pa
```

Append at the bottom:

```python
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
        # writer-private working set (only the ingest thread touches these):
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
        snap = self._snapshot  # atomic read, no lock
        return _merge_to_rows(snap.runs + snap.memtable, self._key_columns, limit)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_lsm_store.py -v`
Expected: PASS (5 passed)

- [ ] **Step 6: Commit**

```bash
git add persistence/stream_store/flight/lsm_store.py tests/flight_helpers.py tests/test_lsm_store.py
git commit -m "feat: add LSMStore engine with ingest, flush, compaction"
```

---

### Task 4: ExampleFlightServer (reusable dummy server)

**Files:**
- Create: `persistence/stream_store/flight/example_server.py`
- Test: `tests/test_example_server.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_example_server.py`:

```python
import threading

import pyarrow.flight as flight

from persistence.stream_store.flight.example_server import ExampleFlightServer
from tests.flight_helpers import make_batch


def test_example_server_streams_script():
    script = [
        make_batch([(1, "a", "x", "upsert")]),
        make_batch([(2, "b", "y", "upsert")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        client = flight.connect(f"grpc://localhost:{server.port}")
        reader = client.do_get(flight.Ticket(b"items"))
        table = reader.read_all()
        assert table.num_rows == 2
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_example_server.py -v`
Expected: FAIL with `ModuleNotFoundError`/`ImportError` for `example_server`

- [ ] **Step 3: Implement the example server**

Create `persistence/stream_store/flight/example_server.py`:

```python
import os
import time

import pyarrow as pa
import pyarrow.flight as flight


class ExampleFlightServer(flight.FlightServerBase):
    def __init__(self, location, script: list[pa.RecordBatch],
                 interval: float, loop: bool = False) -> None:
        super().__init__(location)
        self._script = script
        self._interval = interval
        self._loop = loop

    def do_get(self, context, ticket):
        schema = self._script[0].schema

        def gen():
            while True:
                for batch in self._script:
                    if self._interval:
                        time.sleep(self._interval)
                    yield batch
                if not self._loop:
                    break

        return flight.GeneratorStream(schema, gen())


def _default_script() -> list[pa.RecordBatch]:
    def batch(rows):
        return pa.record_batch({
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "value": pa.array([r[2] for r in rows], pa.string()),
            "op": pa.array([r[3] for r in rows], pa.string()),
        })

    return [
        batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert"), (3, "c", "z", "upsert")]),
        batch([(1, "a", "new", "upsert")]),
        batch([(2, "b", "y", "delete")]),
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

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_example_server.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add persistence/stream_store/flight/example_server.py tests/test_example_server.py
git commit -m "feat: add reusable ExampleFlightServer for tests and compose"
```

---

### Task 5: FlightCacheClient (connection context manager)

**Files:**
- Create: `persistence/stream_store/flight/flight_client.py`
- Test: `tests/test_flight_client.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_flight_client.py`:

```python
import threading

import pyarrow.flight as flight

from core.settings import Settings
from persistence.stream_store.flight.example_server import ExampleFlightServer
from persistence.stream_store.flight.flight_client import FlightCacheClient
from tests.flight_helpers import make_batch


async def test_flight_client_connects_and_reads():
    script = [make_batch([(1, "a", "x", "upsert")])]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        settings = Settings(flight_host="localhost", flight_port=server.port)
        async with FlightCacheClient(settings) as client:
            reader = client.do_get(flight.Ticket(b"items"))
            assert reader.read_all().num_rows == 1
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_flight_client.py -v`
Expected: FAIL with `ImportError: cannot import name 'FlightCacheClient'`

- [ ] **Step 3: Implement the client**

Create `persistence/stream_store/flight/flight_client.py`:

```python
import asyncio

import pyarrow.flight as flight

from core.settings import Settings


class FlightCacheClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None

    async def __aenter__(self) -> flight.FlightClient:
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_flight_client.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add persistence/stream_store/flight/flight_client.py tests/test_flight_client.py
git commit -m "feat: add FlightCacheClient async context manager"
```

---

### Task 6: FlightCacheService (dedicated ingest thread)

**Files:**
- Create: `services/flight_cache.py`
- Test: `tests/test_flight_service.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_flight_service.py`:

```python
import asyncio
import threading
import time

import pyarrow.flight as flight

from core.settings import Settings
from persistence.stream_store.flight.example_server import ExampleFlightServer
from persistence.stream_store.flight.flight_client import FlightCacheClient
from persistence.stream_store.flight.lsm_store import LSMStore
from services.flight_cache import FlightCacheService
from tests.flight_helpers import make_batch


class _Chunk:
    def __init__(self, data):
        self.data = data


class _FakeReader:
    def __init__(self, chunks):
        self._it = iter(chunks)

    def read_chunk(self):
        return next(self._it)  # raises StopIteration when exhausted


class _FakeClient:
    def __init__(self, reader):
        self._reader = reader

    def do_get(self, ticket):
        return self._reader

    def close(self):
        pass


def test_consume_skips_malformed_and_stops():
    chunks = [_Chunk(make_batch([(1, "a", "x", "upsert")])), _Chunk("not a batch")]
    store = LSMStore(flush_rows=100, compaction_runs=100)
    svc = FlightCacheService(_FakeClient(_FakeReader(chunks)), store, Settings())
    svc._consume_loop()  # runs to completion: good ingested, bad skipped, StopIteration breaks
    rows, total = store.query(10)
    assert total == 1
    assert rows == [{"id": 1, "name": "a", "value": "x"}]


async def test_service_ingests_and_serves():
    script = [
        make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert")]),
        make_batch([(1, "a", "new", "upsert")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        settings = Settings(flight_host="localhost", flight_port=server.port,
                            flight_ticket="items")
        async with FlightCacheClient(settings) as client:
            store = LSMStore(flush_rows=100, compaction_runs=100)
            svc = FlightCacheService(client, store, settings)
            await svc.start()
            deadline = time.monotonic() + 10
            resp = None
            while time.monotonic() < deadline:
                resp = await svc.get_data(10)
                if resp.total == 2:
                    break
                await asyncio.sleep(0.05)
            await svc.stop()
        assert resp.total == 2
        values = {r.id: r.value for r in resp.rows}
        assert values[1] == "new"
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_flight_service.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'services.flight_cache'`

- [ ] **Step 3: Implement the service**

Create `services/flight_cache.py`:

```python
import asyncio
import logging
import threading

import pyarrow.flight as flight

from core.settings import Settings
from persistence.stream_store.flight.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class FlightCacheService:
    def __init__(self, client: flight.FlightClient, store: LSMStore,
                 settings: Settings) -> None:
        self._client = client
        self._store = store
        self._ticket = flight.Ticket(settings.flight_ticket.encode())
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self) -> None:
        reader = self._client.do_get(self._ticket)
        while not self._stop.is_set():
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            except Exception:
                log.exception("flight read failed; stopping ingest")
                break
            try:
                self._store.ingest(chunk.data)
            except Exception:
                log.exception("ingest failed; skipping batch")

    async def stop(self) -> None:
        self._stop.set()
        self._client.close()  # unblocks a pending read_chunk
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join)

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_flight_service.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add services/flight_cache.py tests/test_flight_service.py
git commit -m "feat: add FlightCacheService with dedicated ingest thread"
```

---

### Task 7: Dependency wiring

**Files:**
- Modify: `core/dependencies.py`
- Test: covered by Task 10 (endpoint) — no standalone test for the getter.

- [ ] **Step 1: Add the getter and alias**

In `core/dependencies.py`:

Add to imports (after the `from services.cache import CacheService` line):

```python
from services.flight_cache import FlightCacheService
```

Add after `get_cache_service`:

```python
def get_flight_cache_service() -> FlightCacheService:
    return service_container.get(FlightCacheService)
```

Add after the `CacheServiceDep` alias:

```python
FlightCacheServiceDep = Annotated[FlightCacheService, Depends(get_flight_cache_service)]
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `python -c "import core.dependencies"`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add core/dependencies.py
git commit -m "feat: wire FlightCacheService dependency"
```

---

### Task 8: Router endpoint

**Files:**
- Modify: `routers/data.py`
- Test: deferred to Task 10 (needs the full lifespan + Flight server fixture).

- [ ] **Step 1: Add the endpoint**

In `routers/data.py`:

Change the import line:

```python
from core.dependencies import DataServiceDep
```

to:

```python
from core.dependencies import DataServiceDep, FlightCacheServiceDep
```

Append after the existing `get_data` function:

```python
@router.get("/cache", response_model=DataRowsResponse)
async def get_cached_data(
    flight_cache_service: FlightCacheServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await flight_cache_service.get_data(limit=limit)
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `python -c "import routers.data"`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add routers/data.py
git commit -m "feat: add GET /data/cache endpoint"
```

---

### Task 9: Lifespan wiring

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Add imports**

In `main.py`, add to the imports block:

```python
from persistence.stream_store.flight.flight_client import FlightCacheClient
from persistence.stream_store.flight.lsm_store import LSMStore
from services.flight_cache import FlightCacheService
```

- [ ] **Step 2: Nest Flight in `create_lifespan`**

Replace the innermost ClickHouse block in `create_lifespan` (currently lines 44-50):

```python
                async with ClickHouseClient(settings) as ch_client:
                    if not await ch_client.ping():
                        raise RuntimeError("ClickHouse startup ping failed")
                    service_container.register_singleton(DataService, DataService(ch_client))

                    async with mcp.session_manager.run():
                        yield
```

with:

```python
                async with ClickHouseClient(settings) as ch_client:
                    if not await ch_client.ping():
                        raise RuntimeError("ClickHouse startup ping failed")
                    service_container.register_singleton(DataService, DataService(ch_client))

                    async with FlightCacheClient(settings) as flight_client:
                        store = LSMStore(
                            flush_rows=settings.lsm_flush_rows,
                            compaction_runs=settings.lsm_compaction_runs,
                            key_columns=["id"],
                        )
                        flight_service = FlightCacheService(flight_client, store, settings)
                        await flight_service.start()
                        service_container.register_singleton(FlightCacheService, flight_service)
                        try:
                            async with mcp.session_manager.run():
                                yield
                        finally:
                            await flight_service.stop()
```

- [ ] **Step 3: Verify the module imports cleanly**

Run: `python -c "import main"`
Expected: no output, exit 0

- [ ] **Step 4: Commit**

```bash
git add main.py
git commit -m "feat: wire Flight cache into application lifespan"
```

---

### Task 10: HTTP end-to-end tests (conftest fixture + endpoint tests)

**Files:**
- Modify: `tests/conftest.py`
- Test: `tests/test_flight_cache.py` (create)

- [ ] **Step 1: Add the Flight server fixture and settings to conftest**

In `tests/conftest.py`:

Add to the imports near the top:

```python
import threading

import pyarrow.flight as flight

from persistence.stream_store.flight.example_server import ExampleFlightServer
from tests.flight_helpers import make_batch
```

Add a new fixture after the `redis_container` fixture (the `# ── Redis fixtures ──` block):

```python
# ── Flight fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def example_flight_server():
    script = [
        make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert"), (3, "c", "z", "upsert")]),
        make_batch([(1, "a", "new", "upsert")]),
        make_batch([(2, "b", "y", "delete")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.02, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()
```

In the `test_client` fixture, add `example_flight_server` to the parameter list (after `redis_container`):

```python
@pytest.fixture(scope="session")
async def test_client(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    example_flight_server,
    override_health_service,
):
```

Add Flight settings to the `Settings(...)` constructed inside `test_client` (after the `redis_url=...` line):

```python
        redis_url=f"redis://localhost:{redis_port}/0",
        flight_host="localhost",
        flight_port=example_flight_server.port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_flight_cache.py`:

```python
import asyncio
import time

from httpx import AsyncClient


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_cache_returns_merged_rows(test_client: AsyncClient):
    body = await _poll_cache(test_client, expected_total=2)
    values = {r["id"]: r["value"] for r in body["rows"]}
    assert values[1] == "new"   # newest upsert wins
    assert values[3] == "z"


async def test_cache_applies_tombstone(test_client: AsyncClient):
    body = await _poll_cache(test_client, expected_total=2)
    ids = {r["id"] for r in body["rows"]}
    assert 2 not in ids         # deleted id suppressed by tombstone


async def test_cache_respects_limit(test_client: AsyncClient):
    await _poll_cache(test_client, expected_total=2)
    body = (await test_client.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
```

- [ ] **Step 3: Run test to verify it fails (before conftest wiring would have 404)**

Run: `pytest tests/test_flight_cache.py -v`
Expected: PASS now that Tasks 1–9 are complete and conftest is wired. (If run against an earlier checkout it would 404.)

> If any test fails because data has not yet streamed, the `_poll_cache` timeout (15s) covers the `interval=0.02` × 3-batch script; do not shorten the timeout.

- [ ] **Step 4: Run the full suite to confirm no regressions and coverage holds**

Run: `pytest --cov --cov-report=term-missing`
Expected: all tests pass; coverage ≥ 95%. The only acceptable miss is the `__aexit__` `if self._client is not None` false-branch in `flight_client.py` (branch-coverage quirk shared by all four clients).

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py tests/test_flight_cache.py
git commit -m "test: add Flight cache end-to-end HTTP tests"
```

---

### Task 11: Docker Compose service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the `flight` service**

In `docker-compose.yml`, add a new service after the `redis` service (before `app`):

```yaml
  flight:
    build: .
    command: ["python", "-m", "persistence.stream_store.flight.example_server"]
    environment:
      FLIGHT_BIND_HOST: 0.0.0.0
      FLIGHT_PORT: "8815"
      FLIGHT_INTERVAL: "0.2"
    ports:
      - "8815:8815"
```

- [ ] **Step 2: Add Flight env + dependency to the `app` service**

In the `app` service `environment:` block, add:

```yaml
      FLIGHT_HOST: flight
      FLIGHT_PORT: "8815"
```

In the `app` service `depends_on:` block, add:

```yaml
      flight:
        condition: service_started
```

- [ ] **Step 3: Validate compose config**

Run: `docker compose config`
Expected: prints the merged config with no errors; the `flight` service and `app` `FLIGHT_*` env are present.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Flight example server to docker-compose"
```

---

### Task 12: k6 benchmark

**Files:**
- Create: `performance/flight_cache.js`

- [ ] **Step 1: Write the k6 script**

Create `performance/flight_cache.js`:

```javascript
import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus200, checkDataRows } from './lib/checks.js';
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

- [ ] **Step 2: Validate the script parses**

Run: `k6 inspect performance/flight_cache.js`
Expected: prints the parsed options (vus: 5, duration: 30s) with no syntax error. (If `k6` is not installed locally, this is exercised in CI; skip locally.)

- [ ] **Step 3: Commit**

```bash
git add performance/flight_cache.js
git commit -m "test: add k6 benchmark for /data/cache"
```

---

## Final Verification

After all tasks:

```bash
pytest --cov --cov-report=term-missing
```

Expected: all tests pass; overall coverage ≥ 95%; the new Flight modules are fully covered except the documented `__aexit__` false-branch.
