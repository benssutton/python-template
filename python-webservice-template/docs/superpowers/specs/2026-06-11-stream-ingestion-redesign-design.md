# Stream Ingestion Redesign — Design Spec

## Goal

Introduce a composable ingestion layer that decouples transport mechanisms (Apache
Flight, Solace PubSub+, HTTP) from the Record Batch store (LSMStore). Add Solace
as a second ingest transport alongside the existing Flight path, expose HTTP as a
third transport, replace current unit tests with real-endpoint integration tests,
and add load/stress performance tests for all three transports.

## Background & Constraints

- **Template goals**: simple, transparent patterns; high testability; separates
  concerns between modules; idiomatic Python.
- **Existing LSMStore**: single-writer / multi-reader, lock-free reads via atomic
  snapshot reference. Concurrency model stays unchanged.
- **One transport per deployment**: Flight or Solace is selected via config.
  Simultaneous dual-transport is out of scope.
- **Arrow IPC on the wire**: Solace messages carry Arrow IPC-serialized
  `RecordBatch` objects (same `id/name/value/op` schema as Flight).
- **No mocks**: all tests exercise real endpoints, real servers, real containers.
- **Coverage**: must stay ≥ 95%.

## Architecture

```
ingestion/                          Transport abstraction layer (NEW)
  base.py                           BatchConsumer Protocol
  flight/client.py                  FlightBatchConsumer
  solace/client.py                  SolaceBatchConsumer

persistence/
  stream_store/
    lsm_store.py                    Promoted from flight/ subfolder (logic unchanged)
  analytics_store/...               Unchanged
  cache_store/...                   Unchanged
  transaction_store/...             Unchanged

services/
  stream_ingest.py                  StreamIngestService — replaces flight_cache.py

routers/
  data.py                           GET /data/cache (unchanged) + POST /data/ingest (new)

tests/
  publishers/
    flight_server.py                Renamed from tests/example_server.py
    solace_publisher.py             New — publishes IPC batches to Solace topic
  test_flight_cache.py              Updated — real-endpoint tests only
  test_solace_cache.py              New — same assertions via Solace transport
  performance/
    ingest_http.js                  New — k6 pumps POST /data/ingest
    solace_cache.js                 New — k6 reads /data/cache under Solace ingest
    publishers/
      solace_publisher.py           New — continuous Solace publisher for perf runs
    data/
      ingest_batch.ipc              New — pre-baked Arrow IPC fixture for k6
```

## Deleted Files

| File | Reason |
|------|--------|
| `persistence/stream_store/flight/flight_client.py` | Replaced by `ingestion/flight/client.py` |
| `persistence/stream_store/flight/lsm_store.py` | Promoted to `persistence/stream_store/lsm_store.py` |
| `services/flight_cache.py` | Replaced by `services/stream_ingest.py` |
| `tests/example_server.py` | Renamed to `tests/publishers/flight_server.py` |
| `tests/flight_helpers.py` | Absorbed into `tests/publishers/flight_server.py` |
| `tests/test_lsm_store.py` | Unit tests with fakes — replaced by real-endpoint tests |
| `tests/test_flight_service.py` | Unit tests with `_FakeReader`/`_FakeClient` — removed |
| `tests/test_flight_merge.py` | Pure merge unit tests — removed |

## Component Details

### `BatchConsumer` Protocol (`ingestion/base.py`)

```python
from typing import Protocol, Iterator
import pyarrow as pa

class BatchConsumer(Protocol):
    def batches(self) -> Iterator[pa.RecordBatch]: ...
    def close(self) -> None: ...
```

Two synchronous methods. `batches()` is a blocking generator that runs on the
dedicated ingest thread. `close()` must be thread-safe and unblock any pending
`batches()` call. No ABC, no inheritance — structural subtyping (duck typing)
only. Adding a new transport means implementing the duck type; nothing else
changes.

### `FlightBatchConsumer` (`ingestion/flight/client.py`)

Refactored from `FlightCacheClient`. Key change: `__aenter__` returns `self`
instead of the raw `flight.FlightClient`. The consume logic moves into `batches()`.

```python
class FlightBatchConsumer:
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

### `SolaceBatchConsumer` (`ingestion/solace/client.py`)

Uses `solace-pubsubplus` SDK. An async receive callback puts batches onto a
`queue.Queue`; `batches()` blocks on `.get()` so it is identical to the Flight
generator from the service's perspective. A `None` sentinel from `close()`
breaks the loop.

```python
class SolaceBatchConsumer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: MessagingService | None = None
        self._receiver = None
        self._queue: queue.Queue[pa.RecordBatch | None] = queue.Queue()

    async def __aenter__(self) -> "SolaceBatchConsumer":
        self._service = await asyncio.to_thread(self._connect)
        return self

    def _connect(self) -> MessagingService:
        props = {
            "solace.messaging.transport.host":
                f"{self._settings.solace_host}:{self._settings.solace_port}",
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
        self._receiver.receive_async(self._on_message)
        while True:
            item = self._queue.get()    # blocks until message or sentinel
            if item is None:
                break
            yield item

    def _on_message(self, message) -> None:
        payload = message.get_payload_as_bytes()
        reader = pa_ipc.open_stream(pa.BufferReader(payload))
        for batch in reader:
            self._queue.put(batch)

    def close(self) -> None:
        self._queue.put(None)           # unblocks batches()
        if self._receiver is not None:
            self._receiver.terminate()
            self._receiver = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
```

### `SolacePublisher` (`tests/publishers/solace_publisher.py`)

Test-time counterpart. Connects to Solace and publishes Arrow IPC batches to the
topic. Used by the pytest fixture and as a standalone script.

```python
class SolacePublisher:
    def publish_batch(self, batch: pa.RecordBatch) -> None:
        buf = pa.BufferOutputStream()
        with pa_ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
        message = self._service.message_builder().build(
            buf.getvalue().to_pybytes()
        )
        self._publisher.publish(message, Topic.of(self._topic))
```

### `StreamIngestService` (`services/stream_ingest.py`)

Replaces `FlightCacheService`. Takes any `BatchConsumer` — thread management
lives here once.

```python
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
        for batch in self._consumer.batches():
            try:
                self._store.ingest(batch)
            except Exception:
                log.exception("ingest failed; skipping batch")

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

### HTTP Ingest Endpoint (add to `routers/data.py`)

```python
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

Content-type: `application/vnd.apache.arrow.stream`. Malformed bodies return 400
explicitly — pyarrow exceptions are not known to FastAPI and would otherwise
surface as 500.

### Dependencies (`core/dependencies.py`)

```python
def get_stream_ingest_service() -> StreamIngestService:
    return service_container.get(StreamIngestService)

StreamIngestServiceDep = Annotated[StreamIngestService, Depends(get_stream_ingest_service)]
```

Both `GET /data/cache` and `POST /data/ingest` use `StreamIngestServiceDep`.

### New Settings (`settings.py`)

```python
ingest_transport: str = "flight"        # "flight" | "solace"

# Solace (only resolved when ingest_transport="solace")
solace_host: str = "localhost"
solace_port: int = 55555
solace_vpn: str = "default"
solace_username: str = "admin"
solace_password: str = "admin"
solace_topic: str = "ingest/batches"
```

### Lifespan (`main.py`)

```python
_CONSUMERS = {
    "flight": FlightBatchConsumer,
    "solace": SolaceBatchConsumer,
}

# inside create_lifespan:
ConsumerClass = _CONSUMERS[settings.ingest_transport]
async with ConsumerClass(settings) as consumer:
    store = LSMStore(
        flush_rows=settings.lsm_flush_rows,
        compaction_runs=settings.lsm_compaction_runs,
    )
    async with StreamIngestService(consumer, store) as svc:
        service_container.register_singleton(StreamIngestService, svc)
        async with mcp.session_manager.run():
            yield
```

## Test Strategy

### Test Batches

The same three batches are used for both Flight and Solace tests, with
`lsm_flush_rows=2` and `lsm_compaction_runs=2`:

```python
# 2 rows — hits flush threshold → run1 created
BATCH_1 = make_batch([
    (1, "alpha", "v1", "upsert"),
    (2, "beta",  "v1", "upsert"),
])

# 2 rows — flush → run2 → compaction triggered (2 runs)
# compaction merges run1+run2: id=1 gets v2 (higher seqno wins)
BATCH_2 = make_batch([
    (1, "alpha", "v2", "upsert"),
    (3, "gamma", "v1", "upsert"),
])

# 1 row — stays in memtable (below flush threshold)
# tombstone has highest seqno for id=2 → beats id=2 in the compacted run
BATCH_3 = make_batch([
    (2, "beta", "v1", "delete"),
])
```

Expected `/data/cache` result: `total=2`, `id=1→"v2"`, `id=2` absent,
`id=3→"v1"`.

This exercises: flush, compaction, newest-wins across compaction, tombstone from
memtable beating a compacted run.

### `tests/test_flight_cache.py` (updated)

Four tests, all polling `GET /data/cache` via the real endpoint:

1. `test_newest_wins_across_compaction` — `id=1` shows `"v2"` not `"v1"`
2. `test_tombstone_beats_compacted_run` — `id=2` absent from results
3. `test_unmodified_row_survives` — `id=3` shows `"v1"`
4. `test_limit_respected` — `?limit=1` returns 1 row, `total=2`

### `tests/test_solace_cache.py` (new)

Identical four assertions via a `test_client_solace` fixture defined **within
`tests/test_solace_cache.py` itself** (not in the top-level `conftest.py`) to
avoid `service_container` registration conflicts when both test files are
collected in the same process. The fixture is module-scoped and:
1. Starts a `solace/solace-pubsub-standard` testcontainer
2. Starts the app with `ingest_transport="solace"` and Solace connection settings
3. Publishes `BATCH_1`, `BATCH_2`, `BATCH_3` via `SolacePublisher`
4. Polls `GET /data/cache` until `total=2`

### Transport Isolation

Flight and Solace tests each need their own app instance (separate
`service_container` registrations). They run as **separate pytest stages** — in
CI and locally:

```bash
pytest tests/test_flight_cache.py   # flight stage
pytest tests/test_solace_cache.py   # solace stage (requires Solace Docker)
```

`pytest.ini` marks: `@pytest.mark.flight` and `@pytest.mark.solace`. The Solace
stage is opt-in locally via `--solace` flag.

## Performance Tests

### Approach (a) — k6 HTTP ingest

`tests/performance/ingest_http.js` pumps a pre-baked Arrow IPC binary
(`data/ingest_batch.ipc`) at `POST /data/ingest`:

```javascript
const BATCH = open('./data/ingest_batch.ipc', 'b');

export default function () {
  const res = http.post(`${BASE_URL}/data/ingest`, BATCH, {
    headers: { 'Content-Type': 'application/vnd.apache.arrow.stream' },
    tags: { endpoint: 'data_ingest' },
  });
  checkStatus202(res);
}
```

`data/ingest_batch.ipc` is generated once by
`tests/performance/data/generate_ingest_batch.py` and committed as a binary
fixture (same pattern as `clickhouse_seed_data.ipc`). The batch contains mixed
`upsert`/`delete` rows so the LSM stays exercised without growing unboundedly.

`lib/checks.js` gains `checkStatus202` alongside the existing `checkStatus200`.

### Approach (b) — Python publisher + k6 read-under-ingest

`tests/performance/publishers/solace_publisher.py` loops forever publishing
batches to Solace with a configurable interval. The Flight equivalent is
`tests/publishers/flight_server.py` with `loop=True` — already the `flight`
docker-compose service.

`tests/performance/solace_cache.js` mirrors `flight_cache.js` — measures
`GET /data/cache` while Solace ingest is running.

### Docker-compose additions

```yaml
solace:
  image: solace/solace-pubsub-standard:latest
  ports: ["55555:55555", "8080:8080"]
  environment:
    username_admin_globalaccesslevel: admin
    username_admin_password: admin
  healthcheck:
    test: ["CMD", "curl", "-sf", "http://localhost:8080"]
    interval: 10s
    retries: 12

solace-publisher:
  build: .
  command: ["python", "tests/performance/publishers/solace_publisher.py"]
  environment:
    SOLACE_HOST: solace
    SOLACE_PORT: "55555"
    SOLACE_TOPIC: "ingest/batches"
  depends_on:
    solace:
      condition: service_healthy
```

`app` gains `INGEST_TRANSPORT: ${INGEST_TRANSPORT:-flight}`. Switching to Solace
for a perf run: `INGEST_TRANSPORT=solace docker compose up`.

## Error Handling

- **`_ingest_loop`**: malformed batch is logged and skipped (same as current);
  `batches()` generator exhaustion exits the loop cleanly.
- **`SolaceBatchConsumer.close()`**: puts `None` sentinel before terminating
  receiver/service, so `batches()` always exits cleanly even if no message
  arrived.
- **`POST /data/ingest`**: malformed Arrow IPC body returns 400 via pyarrow's
  own exception propagation; FastAPI converts to an HTTP error response.
- **Query before any data**: empty snapshot returns `{"rows": [], "total": 0}`.

## Modified Files Summary

| File | Change |
|------|--------|
| `settings.py` | Add `ingest_transport`, Solace settings |
| `main.py` | Config-driven consumer selection, `StreamIngestService` wiring |
| `core/dependencies.py` | Replace `FlightCacheServiceDep` → `StreamIngestServiceDep` |
| `routers/data.py` | Add `POST /data/ingest` |
| `docker-compose.yml` | Add `solace`, `solace-publisher` services; `INGEST_TRANSPORT` on `app` |
| `requirements.txt` | Add `solace-pubsubplus` |
| `tests/conftest.py` | Update flight fixture to use `FlightBatchConsumer`; add Solace module-scoped fixtures |
| `tests/performance/lib/checks.js` | Add `checkStatus202` |
| `pytest.ini` | Register `flight` and `solace` marks |
