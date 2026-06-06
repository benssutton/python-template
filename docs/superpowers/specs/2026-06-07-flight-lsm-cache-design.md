# Arrow Flight LSM Cache Design

## Goal

Add an Apache Arrow Flight client that continuously consumes record batches pushed
by a (stateless) Flight server, accumulates them in an in-memory **Log-Structured
Merge (LSM)** store, and exposes the merged result via `GET /data/cache` using the
same pattern as the existing `GET /data` endpoint.

## Background & Constraints

- **Stateless server.** The Flight server is a dumb producer. It emits record
  batches `(id, name, value, op)` where `op ∈ {"upsert", "delete"}`. It has no
  memory of what it sent before and never dedupes — the same `id` may appear in
  many batches over time, and the server cannot say which is newer. **All merge
  semantics live in the client.**
- **Client-owned versioning.** On ingest the client stamps every row with a
  monotonically increasing `seqno`. "Newest wins" per key is resolved by the
  highest `seqno`; a `delete` op winning at the highest `seqno` for a key
  suppresses it (tombstone). This is how a real LSM uses sequence numbers to
  order writes across the memtable and runs.
- **Thread isolation (hard requirement).** The Flight read + LSM write pipeline
  runs on its own dedicated thread. Queries against the cached data must never
  block that ingest thread, and vice versa.
- **Coverage.** Overall test coverage is currently ~95% and must not drop.
- **Extensible key.** Today the merge key is `["id"]`. In practice it will become
  a composite `["id", "version"]`. The merge is built as a polars **window
  function** over a configurable `key_columns` list so this is a one-line change.

## Architecture

The Flight source slots in as a fourth persistence store, parallel to the
existing three (`transaction_store/postgres`, `analytics_store/clickhouse`,
`cache_store/redis`). It follows the identical layered structure:
client → store → service → router.

`pyarrow.flight` ships with the existing `pyarrow` dependency — no new runtime
requirement. Because `pyarrow.flight` is synchronous/blocking (gRPC), the stream
is consumed on a dedicated OS thread, consistent with CLAUDE.md ("synchronous
blocking calls are acceptable outside async context managers").

```
persistence/stream_store/flight/
  flight_client.py     FlightCacheClient — async ctx mgr wrapping pyarrow.flight.FlightClient
  lsm_store.py         LSMStore — in-memory LSM engine (memtable, runs, flush, compaction, merge)
  example_server.py    Reusable dummy Flight server (scripted batches); used by tests AND docker-compose
```

## New Files

| File | Responsibility |
|------|---------------|
| `persistence/stream_store/flight/flight_client.py` | `FlightCacheClient(settings)` async ctx mgr; `__aenter__` connects, returns live `FlightClient`; `__aexit__` closes |
| `persistence/stream_store/flight/lsm_store.py` | `LSMStore` — memtable, immutable runs, `seqno` stamping, flush, compaction, window-function merge-on-read |
| `persistence/stream_store/flight/example_server.py` | `ExampleFlightServer` streaming a scripted batch sequence; `python -m` entrypoint for compose |
| `services/flight_cache.py` | `FlightCacheService` — owns `LSMStore`, runs the background consumer on a dedicated thread (`start`/`stop`), exposes `get_data(limit)` |
| `tests/test_flight_cache.py` | HTTP end-to-end merge/tombstone tests + focused `LSMStore`/consumer unit tests |
| `performance/flight_cache.js` | k6 script hitting `GET /data/cache` using existing `lib/checks` + `lib/thresholds` |

## Modified Files

| File | Change |
|------|--------|
| `routers/data.py` | Add `GET /cache` → `DataRowsResponse` (under existing **Data Service** tag — no new tag metadata) |
| `core/dependencies.py` | Add `get_flight_cache_service()` + `FlightCacheServiceDep` |
| `core/settings.py` | Add `flight_host`, `flight_port`, `flight_ticket`, `lsm_flush_rows`, `lsm_compaction_runs` |
| `main.py` | `create_lifespan`: nest `FlightCacheClient`, build `LSMStore`, start/stop `FlightCacheService` task, register singleton |
| `docker-compose.yml` | Add `flight` service (runs `example_server.py`); app gets `FLIGHT_*` env + `depends_on` |
| `tests/conftest.py` | Add in-process `example_flight_server` thread fixture; add `flight_*` + LSM thresholds to test settings |

**Unchanged:** `schemas/data.py` (reuses `DataRowsResponse` / `DataRowResponse`)
and `requirements.txt` (`pyarrow.flight` ships with existing `pyarrow`).

## Threading & Concurrency Model

The LSM is a **single-writer / multi-reader** structure with **lock-free reads**.

| Context | Thread | Work |
|---------|--------|------|
| Event loop | main | FastAPI request handling; never does CPU-bound merge or blocking Flight reads |
| **Ingest** | one **dedicated** `threading.Thread` | Flight `read_chunk` (blocking) **+** all LSM writes (append, flush, compaction) |
| **Query** | `asyncio.to_thread` worker pool | LSM merge-on-read (polars), offloaded off the event loop |

Why this satisfies the isolation requirement:

- The ingest thread is a **dedicated `threading.Thread`**, *not* drawn from the
  `asyncio.to_thread` default executor, so concurrent query load can never occupy
  it.
- Reads are **lock-free**. The ingest thread is the only writer; it mutates a
  private working set, then publishes an **immutable snapshot** via a single
  atomic reference assignment (GIL-safe). Queries read the current snapshot
  reference and merge from it — they never acquire a lock the writer needs, so a
  query can never block ingest, and ingest never blocks a query.
- polars merge and Arrow conversion **release the GIL** during computation, so
  ingest and queries genuinely run in parallel on separate cores.

## Component Details

### FlightCacheClient

Mirrors `ClickHouseClient` / `PostgresClient` / `RedisClient`.

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

### LSMStore

Synchronous (runs on dedicated threads, not the event loop). The published
snapshot is an immutable dataclass swapped atomically.

```python
from dataclasses import dataclass

import polars as pl
import pyarrow as pa

ORDER_COLUMN = "seqno"


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
        snap = self._snapshot                     # atomic read, no lock
        rows, total = _merge_to_rows(snap.runs + snap.memtable,
                                     self._key_columns, limit)
        return rows, total
```

> Note: `_compact` keeps the merged result as a single immutable run frame
> (carrying `seqno`, `op`, key columns) so later compactions and reads still see
> correct recency and tombstones. The merge helper exposes both a frame-returning
> form (compaction) and a rows form (read); see below.

### Window-Function Merge (extension point)

Used by both compaction and merge-on-read. `key_columns` is the single extension
point — `["id"]` today, `["id", "version"]` later, with no other change.

```python
def _merge_frame(frames: tuple[pl.DataFrame, ...],
                 key_columns: list[str]) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    # Window function: rank rows within each key partition by recency.
    winners = (
        combined
        .with_columns(
            pl.col(ORDER_COLUMN)
              .rank("ordinal", descending=True)
              .over(key_columns)            # <-- the window/partition
              .alias("_rn")
        )
        .filter(pl.col("_rn") == 1)         # newest row per key
        .drop("_rn")
    )
    return winners


def _merge_to_rows(frames, key_columns, limit):
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit is not None:
        live = live.head(limit)
    return live.select(["id", "name", "value"]).to_dicts(), total
```

Compaction calls `_merge_frame` and keeps the winning rows (including tombstones,
so a delete still suppresses an older value that lives in another run until those
runs also compact). Merge-on-read calls `_merge_to_rows`, which additionally
drops tombstones and applies the limit. `seqno` breaks ties for identical keys.

### FlightCacheService

Owns the dedicated ingest thread.

```python
import asyncio
import logging
import threading

import pyarrow.flight as flight
from core.settings import Settings
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class FlightCacheService:
    def __init__(self, client: flight.FlightClient, store, settings: Settings):
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
            except Exception:                         # noqa: BLE001 - log & keep ingesting
                log.exception("flight read failed")
                break
            try:
                self._store.ingest(chunk.data)
            except Exception:                         # noqa: BLE001 - skip malformed batch
                log.exception("ingest failed; skipping batch")

    async def stop(self) -> None:
        self._stop.set()
        self._client.close()                          # unblocks a pending read_chunk
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join)

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )
```

### Router (add to `routers/data.py`)

```python
@router.get("/cache", response_model=DataRowsResponse)
async def get_cached_data(
    flight_cache_service: FlightCacheServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await flight_cache_service.get_data(limit=limit)
```

### Dependencies (add to `core/dependencies.py`)

```python
from services.flight_cache import FlightCacheService

def get_flight_cache_service() -> FlightCacheService:
    return service_container.get(FlightCacheService)

FlightCacheServiceDep = Annotated[FlightCacheService, Depends(get_flight_cache_service)]
```

### Settings (add to `core/settings.py`)

```python
flight_host: str = "localhost"
flight_port: int = 8815          # pyarrow Flight default
flight_ticket: str = "items"
lsm_flush_rows: int = 1000       # memtable -> run threshold
lsm_compaction_runs: int = 4     # run count -> compaction threshold
```

### Lifespan wiring (`create_lifespan`, innermost before MCP)

```python
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

## Error Handling

- **Consumer loop:** a malformed batch is logged and skipped without killing the
  ingest thread; `StopIteration` / stream-end exits the loop cleanly; connection
  errors are logged and end the loop. On shutdown, `stop()` sets the event and
  closes the client to unblock a pending `read_chunk`.
- **Query before any data:** returns `{"rows": [], "total": 0, "limit": N}` from
  an empty snapshot — never errors.

## Example Server (shared by tests and compose)

```python
import time
import pyarrow as pa
import pyarrow.flight as flight


class ExampleFlightServer(flight.FlightServerBase):
    def __init__(self, location, script: list[pa.RecordBatch], interval: float):
        super().__init__(location)
        self._script = script
        self._interval = interval

    def do_get(self, context, ticket):
        def gen():
            for batch in self._script:
                time.sleep(self._interval)
                yield batch
        return flight.GeneratorStream(self._script[0].schema, gen())
```

The scripted batches deliberately exercise the LSM: overlapping `id`s across
batches (to prove newest-wins) and at least one `op="delete"` (tombstone). A
`python -m persistence.stream_store.flight.example_server` entrypoint serves it
continuously for docker-compose.

## Testing

**`conftest.py` additions:**

- `example_flight_server` fixture: builds the script, starts `ExampleFlightServer`
  on a free port in a daemon thread, yields host/port, calls `server.shutdown()`
  on teardown.
- `test_client` `test_settings` gains `flight_host` / `flight_port` /
  `flight_ticket`, plus small `lsm_flush_rows` / `lsm_compaction_runs` so the test
  data actually triggers a flush and a compaction.

**`tests/test_flight_cache.py`:**

HTTP end-to-end (via `test_client`, full Flight → consumer → LSM → endpoint):

1. `test_cache_returns_merged_rows` — poll `GET /data/cache` until `total` reaches
   the expected live-key count (bounded timeout); assert the overlapping `id`
   shows the newest value.
2. `test_cache_applies_tombstone` — assert the deleted `id` is absent.
3. `test_cache_respects_limit` — `?limit=1` returns one row, `total` unchanged.

`LSMStore` unit tests (synchronous, no Flight):

4. `test_flush_creates_run` — ingest past `flush_rows`; assert a run is published.
5. `test_compaction_merges_runs` — ingest past `compaction_runs` flushes; assert
   run count drops and rows are correct.
6. `test_merge_newest_wins_and_tombstone` — direct merge assertions.
7. `test_composite_key_extension` — `LSMStore(key_columns=["id", "version"])`;
   two rows with same `id` but different `version` both survive. Locks the
   extension contract.

Consumer unit test:

8. `test_consume_skips_malformed_and_stops` — drive `_consume_loop` with a fake
   reader yielding one good chunk, one malformed, then `StopIteration`; assert
   good data ingested and the loop exits.

**Coverage:** the only structurally-unhittable line is the
`if self._client is not None` false-branch in `FlightCacheClient.__aexit__` (the
same branch-coverage quirk as the other three clients). Everything else (ingest,
flush, compaction, merge, tombstone, consumer loop incl. error/stop branches,
endpoint, start/stop) is covered by tests 1–8, keeping overall coverage ≥ 95%.

## Benchmark

- `performance/flight_cache.js` — k6 script hitting `GET /data/cache`, reusing
  `lib/checks.js` (`checkStatus200`, `checkDataRows`) and a `lib/thresholds.js`
  SLO preset.
- `docker-compose.yml` — add a `flight` service running `example_server.py`
  (continuous stream); the `app` service gets `FLIGHT_HOST=flight` /
  `FLIGHT_PORT` env and `depends_on: flight`, so the k6 run exercises a live
  ingest + merge path end-to-end.
