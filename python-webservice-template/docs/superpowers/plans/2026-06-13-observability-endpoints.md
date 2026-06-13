# Observability Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `/health/status` stub with Kubernetes-style liveness/readiness probes, a rich JSON status report, and a Prometheus `/metrics` endpoint for Grafana — surfacing dependency health, ingest-transport connection state, data freshness, system resources, and uptime, so a silent Solace/Flight disconnect fails readiness immediately instead of serving stale data.

**Architecture:** Each component reports its own health from signals it owns (services expose `health_check()`; consumers expose event-driven `connection_state()`); `HealthService` only aggregates, resolving services lazily from the per-app `Container`. Metrics use a per-app `CollectorRegistry` (the multi-app test pattern forbids the global registry) refreshed on each scrape. All checks are active/pull — no background pollers — except an opt-in disconnect watchdog.

**Tech Stack:** FastAPI, Pydantic, `psutil`, `prometheus-fastapi-instrumentator` (v8, `prometheus-client`), pyarrow Flight, Solace PubSub+ SDK, pytest + testcontainers.

**Environment note:** This machine has no `python` on PATH. Use the project env explicitly: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe"`. Container-backed tests need Docker Desktop running; the Python Docker SDK can hit a 60s named-pipe timeout when the daemon is busy — if container fixtures time out, free the daemon (`docker compose down`) and retry. Unit-test-only tasks (3, 5, 6, 7) run without Docker.

---

## Spec reference

`docs/superpowers/specs/2026-06-13-observability-endpoints-design.md`

## File structure

**Create:**
- `core/system_metrics.py` — pure `collect_system_snapshot(psutil.Process) -> SystemSnapshot`.
- `services/metrics.py` — `MetricsService`: per-app registry, custom gauges, `instrument(app)`, `refresh(health_service)`.
- `routers/metrics.py` — `GET /metrics` route.
- `observability/prometheus.yml`, `observability/grafana/provisioning/datasources/datasource.yml`, `observability/grafana/provisioning/dashboards/dashboards.yml`, `observability/grafana/dashboards/service-overview.json`.
- `tests/test_ingest_health.py` — consumer/service unit tests (no Docker).
- `tests/test_observability.py` — endpoint integration tests (Docker).

**Modify:**
- `requirements.txt`, `settings.py`, `schemas/health.py`, `ingestion/base.py`, `ingestion/flight/client.py`, `ingestion/solace/client.py`, `services/stream_ingest.py`, `services/config.py`, `services/data.py`, `services/cache.py`, `services/health.py`, `core/container.py`, `routers/health.py`, `main.py`, `tests/publishers/flight_server.py`, `tests/test_health.py`, `pytest.ini`, `docker-compose.yml`.

---

## Task 1: Dependencies and settings

**Files:**
- Modify: `requirements.txt`
- Modify: `settings.py:24-52`

- [ ] **Step 1: Add the two new dependencies to `requirements.txt`** (append under a new section before `# Testing`):

```
# Observability
psutil
prometheus-fastapi-instrumentator
```

- [ ] **Step 2: Install them into the project env**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pip.exe" install psutil prometheus-fastapi-instrumentator`
Expected: both install (psutil + prometheus-fastapi-instrumentator 8.x + prometheus-client).

- [ ] **Step 3: Add settings fields.** In `settings.py`, immediately after the `ingest_transport` line (currently line 44), insert:

```python
    # Observability
    metrics_enabled: bool = True
    health_check_timeout_seconds: float = 2.0                 # per-dependency ping timeout
    ingest_staleness_threshold_seconds: float | None = None   # None = staleness never reported
    ingest_stale_fails_readiness: bool = False                # stale -> 503 only if True
    ingest_max_disconnect_seconds: float | None = 60.0        # non-CONNECTED longer than this -> SIGTERM; None disables
```

- [ ] **Step 4: Verify settings import cleanly**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "from settings import Settings; s=Settings(); print(s.metrics_enabled, s.ingest_max_disconnect_seconds, s.health_check_timeout_seconds)"`
Expected: `True 60.0 2.0`

- [ ] **Step 5: Commit**

```bash
git add requirements.txt settings.py
git commit -m "feat: add observability deps and settings fields"
```

---

## Task 2: Health schemas

**Files:**
- Modify: `schemas/health.py` (currently only `HealthStatusResponse`)

- [ ] **Step 1: Replace the contents of `schemas/health.py` with the full model set** (keep `HealthStatusResponse` — the MCP tool still uses it):

```python
from datetime import datetime

from pydantic import BaseModel


class HealthStatusResponse(BaseModel):
    status: str


class ProbeResult(BaseModel):
    """Result of a single dependency health probe."""
    name: str
    status: str            # "up" | "down"
    latency_ms: float
    error: str | None = None


class IngestHealth(BaseModel):
    transport: str
    connection_state: str  # "connected" | "reconnecting" | "down"
    thread_alive: bool
    last_batch_at: datetime | None = None
    seconds_since_last_batch: float | None = None
    rows_ingested_total: int = 0
    stale: bool = False


class CheckResult(BaseModel):
    """A single entry in the flat /health/ready checks array.

    Dependency checks populate name/status/latency_ms; the ingest check
    additionally populates transport/connection_state/etc. Serialised with
    response_model_exclude_none so each check shows only its relevant fields.
    """
    name: str
    status: str
    latency_ms: float | None = None
    transport: str | None = None
    connection_state: str | None = None
    thread_alive: bool | None = None
    last_batch_at: datetime | None = None
    seconds_since_last_batch: float | None = None
    error: str | None = None


class LivenessResponse(BaseModel):
    status: str = "alive"
    uptime_seconds: float


class ReadinessResponse(BaseModel):
    status: str            # "ready" | "not_ready"
    checks: list[CheckResult]


class ProcessStats(BaseModel):
    cpu_percent: float
    memory_rss_bytes: int
    num_threads: int
    open_files: int


class HostStats(BaseModel):
    cpu_percent: float
    memory_total_bytes: int
    memory_available_bytes: int
    memory_percent: float


class SystemSnapshot(BaseModel):
    process: ProcessStats
    host: HostStats


class AppInfo(BaseModel):
    title: str
    version: str
    status: str


class UptimeInfo(BaseModel):
    process_seconds: float
    system_boot_seconds: float


class RequestInfo(BaseModel):
    last_request_at: datetime | None = None


class DetailedStatusResponse(BaseModel):
    app: AppInfo
    uptime: UptimeInfo
    dependencies: list[ProbeResult]
    ingest: IngestHealth
    requests: RequestInfo
    system: SystemSnapshot
```

- [ ] **Step 2: Verify the schemas import**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "import schemas.health as h; print(h.DetailedStatusResponse.model_fields.keys())"`
Expected: prints `dict_keys(['app', 'uptime', 'dependencies', 'ingest', 'requests', 'system'])`

- [ ] **Step 3: Commit**

```bash
git add schemas/health.py
git commit -m "feat: add observability response schemas"
```

---

## Task 3: System metrics module (unit-testable, no Docker)

**Files:**
- Create: `core/system_metrics.py`
- Test: `tests/test_system_metrics.py`

- [ ] **Step 1: Write the failing test** — create `tests/test_system_metrics.py`:

```python
import psutil

from core.system_metrics import collect_system_snapshot


def test_collect_system_snapshot_returns_plausible_values():
    process = psutil.Process()
    process.cpu_percent()           # prime (first call returns 0.0)
    snapshot = collect_system_snapshot(process)

    assert snapshot.process.memory_rss_bytes > 0
    assert snapshot.process.num_threads >= 1
    assert snapshot.process.open_files >= 0
    assert snapshot.process.cpu_percent >= 0.0

    assert snapshot.host.memory_total_bytes > 0
    assert 0 <= snapshot.host.memory_available_bytes <= snapshot.host.memory_total_bytes
    assert 0.0 <= snapshot.host.memory_percent <= 100.0
    assert snapshot.host.cpu_percent >= 0.0
```

- [ ] **Step 2: Run it to verify it fails**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_system_metrics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'core.system_metrics'`

- [ ] **Step 3: Implement `core/system_metrics.py`:**

```python
import psutil

from schemas.health import HostStats, ProcessStats, SystemSnapshot


def collect_system_snapshot(process: psutil.Process) -> SystemSnapshot:
    """Snapshot of process- and container-visible system resources.

    `cpu_percent()` returns a delta since the previous call, so callers must
    prime it once at startup (the first call always returns 0.0).
    """
    with process.oneshot():
        cpu_percent = process.cpu_percent()
        memory_rss = process.memory_info().rss
        num_threads = process.num_threads()
        try:
            open_files = len(process.open_files())
        except (psutil.AccessDenied, OSError):
            open_files = 0

    vm = psutil.virtual_memory()
    return SystemSnapshot(
        process=ProcessStats(
            cpu_percent=cpu_percent,
            memory_rss_bytes=memory_rss,
            num_threads=num_threads,
            open_files=open_files,
        ),
        host=HostStats(
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_total_bytes=vm.total,
            memory_available_bytes=vm.available,
            memory_percent=vm.percent,
        ),
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_system_metrics.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/system_metrics.py tests/test_system_metrics.py
git commit -m "feat: add psutil system-metrics snapshot"
```

---

## Task 4: ConnectionState enum and protocol

**Files:**
- Modify: `ingestion/base.py`

- [ ] **Step 1: Replace `ingestion/base.py` with the enum + extended protocol:**

```python
from enum import Enum
from typing import Iterator, Protocol

import pyarrow as pa


class ConnectionState(str, Enum):
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DOWN = "down"


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    batches() is a blocking generator; close() must be thread-safe and unblock
    any pending batches() call so the ingest thread can exit cleanly.
    connection_state() is a cheap, cached read (no I/O) reporting the live
    transport connection state.
    """

    def batches(self) -> Iterator[pa.RecordBatch]: ...

    def close(self) -> None: ...

    def connection_state(self) -> ConnectionState: ...
```

- [ ] **Step 2: Verify it imports**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "from ingestion.base import ConnectionState; print(ConnectionState.CONNECTED.value)"`
Expected: `connected`

- [ ] **Step 3: Commit**

```bash
git add ingestion/base.py
git commit -m "feat: add ConnectionState to BatchConsumer protocol"
```

---

## Task 5: Flight consumer connection state + idle server + unit test (no Docker)

**Files:**
- Modify: `ingestion/flight/client.py`
- Modify: `tests/publishers/flight_server.py` (add `IdleFlightServer`)
- Test: `tests/test_ingest_health.py` (new file, first tests)

- [ ] **Step 1: Write the failing test** — create `tests/test_ingest_health.py`:

```python
import threading

import pyarrow.flight as flight
import pytest

from ingestion.base import ConnectionState
from ingestion.flight.client import FlightBatchConsumer
from settings import Settings
from tests.publishers.flight_server import ExampleFlightServer, make_batch


async def test_flight_consumer_unexpected_stream_end_sets_reconnecting():
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(
        location, [make_batch([(1, "a", "v1", "upsert")])], interval=0.0, loop=False
    )
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        consumer = FlightBatchConsumer(
            Settings(flight_host="localhost", flight_port=server.port, flight_ticket="items")
        )
        await consumer.__aenter__()
        assert consumer.connection_state() == ConnectionState.RECONNECTING  # connected, not streaming yet

        # The script has one batch then the stream ends; an unexpected end must
        # raise (so the ingest loop reconnects) and leave state RECONNECTING.
        with pytest.raises(ConnectionError):
            for _ in consumer.batches():
                pass
        assert consumer.connection_state() == ConnectionState.RECONNECTING
        consumer.close()
        assert consumer.connection_state() == ConnectionState.DOWN
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py -v`
Expected: FAIL — `AttributeError: 'FlightBatchConsumer' object has no attribute 'connection_state'`

- [ ] **Step 3: Replace `ingestion/flight/client.py`:**

```python
import asyncio
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight

from ingestion.base import ConnectionState
from settings import Settings


class FlightBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None
        self._state = ConnectionState.DOWN
        self._closing = False

    async def __aenter__(self) -> "FlightBatchConsumer":
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        self._state = ConnectionState.RECONNECTING  # connected to server, not yet streaming
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def connection_state(self) -> ConnectionState:
        return self._state

    def batches(self) -> Iterator[pa.RecordBatch]:
        self._state = ConnectionState.RECONNECTING
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = self._client.do_get(ticket)      # raises if the server is unreachable
        self._state = ConnectionState.CONNECTED
        try:
            for chunk in reader:
                yield chunk.data
        except Exception:
            if self._closing:
                return                              # intentional shutdown — exit cleanly
            self._state = ConnectionState.RECONNECTING
            raise
        if self._closing:
            return
        # Clean end of stream while not closing == an unexpected disconnect.
        self._state = ConnectionState.RECONNECTING
        raise ConnectionError("flight stream ended unexpectedly")

    def close(self) -> None:
        self._closing = True
        self._state = ConnectionState.DOWN
        client, self._client = self._client, None
        if client is not None:
            client.close()
```

- [ ] **Step 4: Add `IdleFlightServer` to `tests/publishers/flight_server.py`** (append after `ExampleFlightServer`, before `_default_script`). It holds the stream open without ever sending — used to test connected-but-idle:

```python
class IdleFlightServer(flight.FlightServerBase):
    """Accepts a do_get and keeps the stream open forever without sending.

    Used to exercise the 'connected but no data' (idle) ingest state.
    """

    def do_get(self, context, ticket):
        def gen():
            while True:
                time.sleep(0.1)
                if False:        # never yields; keeps the stream open
                    yield

        return flight.GeneratorStream(RECORD_SCHEMA, gen())
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ingestion/flight/client.py tests/publishers/flight_server.py tests/test_ingest_health.py
git commit -m "feat: Flight consumer reports connection state; add IdleFlightServer"
```

---

## Task 6: Solace consumer connection state + unit test (no Docker)

**Files:**
- Modify: `ingestion/solace/client.py`
- Test: `tests/test_ingest_health.py` (append)

- [ ] **Step 1: Write the failing test** — append to `tests/test_ingest_health.py`:

```python
from ingestion.solace.client import SolaceBatchConsumer, _StateListener


def test_solace_consumer_state_transitions_via_listeners():
    consumer = SolaceBatchConsumer(Settings())
    assert consumer.connection_state() == ConnectionState.DOWN

    listener = _StateListener(consumer)
    listener.on_reconnecting(None)
    assert consumer.connection_state() == ConnectionState.RECONNECTING
    listener.on_reconnected(None)
    assert consumer.connection_state() == ConnectionState.CONNECTED
    listener.on_service_interrupted(None)
    assert consumer.connection_state() == ConnectionState.DOWN
```

- [ ] **Step 2: Run it to verify it fails**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py::test_solace_consumer_state_transitions_via_listeners -v`
Expected: FAIL — `ImportError: cannot import name '_StateListener'`

- [ ] **Step 3: Replace `ingestion/solace/client.py`** (adds the listener class, a lock-guarded state, and listener registration in `_connect`):

```python
import asyncio
import logging
import queue
import threading
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionAttemptListener,
    ReconnectionListener,
    ServiceEvent,
    ServiceInterruptionListener,
)
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.resources.topic_subscription import TopicSubscription

from ingestion.base import ConnectionState
from settings import Settings

log = logging.getLogger(__name__)


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
            log.warning("Solace: malformed IPC message dropped", exc_info=True)


class _StateListener(
    ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener
):
    """Bridges Solace SDK lifecycle callbacks (fired on SDK threads) to the
    consumer's connection state. The event argument is unused."""

    def __init__(self, consumer: "SolaceBatchConsumer") -> None:
        self._consumer = consumer

    def on_reconnected(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.CONNECTED)

    def on_reconnecting(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.RECONNECTING)

    def on_service_interrupted(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.DOWN)


class SolaceBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: MessagingService | None = None
        self._receiver: DirectMessageReceiver | None = None
        self._queue: queue.Queue[pa.RecordBatch | None] = queue.Queue()
        self._state = ConnectionState.DOWN
        self._state_lock = threading.Lock()

    def _set_state(self, state: ConnectionState) -> None:
        with self._state_lock:
            self._state = state

    def connection_state(self) -> ConnectionState:
        with self._state_lock:
            return self._state

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
        listener = _StateListener(self)
        svc.add_reconnection_listener(listener)
        svc.add_reconnection_attempt_listener(listener)
        svc.add_service_interruption_listener(listener)
        self._set_state(ConnectionState.CONNECTED)
        return svc

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        self._receiver = (
            self._service
            .create_direct_message_receiver_builder()
            .with_subscriptions([TopicSubscription.of(self._settings.solace_topic)])
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
        self._set_state(ConnectionState.DOWN)
        self._queue.put(None)           # unblocks batches() generator
        if self._receiver is not None:
            self._receiver.terminate()
            self._receiver = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py -v`
Expected: PASS (both Flight and Solace unit tests)

- [ ] **Step 5: Commit**

```bash
git add ingestion/solace/client.py tests/test_ingest_health.py
git commit -m "feat: Solace consumer reports connection state via SDK listeners"
```

---

## Task 7: StreamIngestService — freshness, health_check, injectable shutdown, watchdog (no Docker)

**Files:**
- Modify: `services/stream_ingest.py`
- Modify: `main.py:61` (pass `settings` to the constructor)
- Test: `tests/test_ingest_health.py` (append)

- [ ] **Step 1: Write the failing tests** — append to `tests/test_ingest_health.py`:

```python
import asyncio

from persistence.stream_store.lsm_store import LSMStore
from services.stream_ingest import StreamIngestService

# Note: `threading` and `Settings` are already imported earlier in this file
# (Task 5 block); `make_batch` and `ConnectionState` likewise.


class _FakeConsumer:
    """Real (non-mock) BatchConsumer whose state is fixed and whose batches()
    blocks until close(), so the ingest thread stays alive during tests."""

    def __init__(self, state: ConnectionState) -> None:
        self._state = state
        self._closed = threading.Event()

    def batches(self):
        self._closed.wait()
        return
        yield  # make this a generator

    def close(self) -> None:
        self._closed.set()

    def connection_state(self) -> ConnectionState:
        return self._state


async def test_ingest_health_tracks_freshness_after_ingest():
    settings = Settings(ingest_transport="flight", ingest_staleness_threshold_seconds=None)
    svc = StreamIngestService(_FakeConsumer(ConnectionState.CONNECTED), LSMStore(100, 4), settings)

    before = await svc.health_check()
    assert before.connection_state == "connected"
    assert before.last_batch_at is None
    assert before.rows_ingested_total == 0

    await svc.ingest_batch(make_batch([(1, "a", "v1", "upsert")]))
    after = await svc.health_check()
    assert after.rows_ingested_total == 1
    assert after.last_batch_at is not None
    assert after.seconds_since_last_batch is not None


async def test_disconnect_watchdog_invokes_shutdown_hook():
    triggered = asyncio.Event()
    settings = Settings(ingest_transport="flight", ingest_max_disconnect_seconds=0.2)
    svc = StreamIngestService(
        _FakeConsumer(ConnectionState.DOWN), LSMStore(100, 4), settings,
        shutdown_hook=triggered.set,
    )
    async with svc:
        await asyncio.wait_for(triggered.wait(), timeout=3.0)
    assert triggered.is_set()
```

- [ ] **Step 2: Run to verify failure**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py::test_ingest_health_tracks_freshness_after_ingest -v`
Expected: FAIL — `TypeError: StreamIngestService.__init__() takes 3 positional arguments but 4 were given`

- [ ] **Step 3: Replace `services/stream_ingest.py`:**

```python
import asyncio
import logging
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Callable

import pyarrow as pa

from ingestion.base import BatchConsumer, ConnectionState
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse
from schemas.health import IngestHealth
from settings import Settings

log = logging.getLogger(__name__)

_INGEST_BASE_DELAY = 1.0        # seconds before first retry
_INGEST_MAX_DELAY = 60.0        # seconds — retry cap
_INGEST_MAX_FAILURES = 5        # consecutive batches() failures before shutdown
_JOIN_TIMEOUT = 10.0            # seconds to wait for ingest thread on shutdown


def _default_shutdown() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


class StreamIngestService:
    def __init__(
        self,
        consumer: BatchConsumer,
        store: LSMStore,
        settings: Settings,
        shutdown_hook: Callable[[], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._settings = settings
        self._request_shutdown = shutdown_hook or _default_shutdown
        self._thread: threading.Thread | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._last_batch_at: datetime | None = None
        self._rows_total = 0

    async def __aenter__(self) -> "StreamIngestService":
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()
        if self._settings.ingest_max_disconnect_seconds is not None:
            self._watchdog_task = asyncio.create_task(self._disconnect_watchdog())
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT)
            if self._thread.is_alive():
                log.error("ingest thread did not stop within %.0fs; abandoning", _JOIN_TIMEOUT)
            self._thread = None

    def _record_ingest(self, batch: pa.RecordBatch) -> None:
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(timezone.utc)
        self._rows_total += batch.num_rows

    def _ingest_loop(self) -> None:
        consecutive_failures = 0
        delay = _INGEST_BASE_DELAY
        while True:
            try:
                for batch in self._consumer.batches():
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    try:
                        self._record_ingest(batch)
                    except Exception:
                        log.exception("ingest failed; skipping batch")
                return  # batches() returned cleanly — consumer was closed
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "consumer batches() failed (failure %d/%d)",
                    consecutive_failures, _INGEST_MAX_FAILURES,
                )
                if consecutive_failures >= _INGEST_MAX_FAILURES:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        consecutive_failures,
                    )
                    self._request_shutdown()
                    return
                jitter = delay * 0.25 * random.random()
                time.sleep(delay + jitter)
                delay = min(delay * 2, _INGEST_MAX_DELAY)

    async def _disconnect_watchdog(self) -> None:
        threshold = self._settings.ingest_max_disconnect_seconds
        poll = min(threshold / 2, 5.0)
        disconnected_since: float | None = None
        try:
            while True:
                await asyncio.sleep(poll)
                if self._consumer.connection_state() == ConnectionState.CONNECTED:
                    disconnected_since = None
                    continue
                now = asyncio.get_event_loop().time()
                if disconnected_since is None:
                    disconnected_since = now
                elif now - disconnected_since >= threshold:
                    log.critical(
                        "ingest transport not connected for %.0fs; requesting shutdown",
                        threshold,
                    )
                    self._request_shutdown()
                    return
        except asyncio.CancelledError:
            return

    async def health_check(self) -> IngestHealth:
        state = self._consumer.connection_state()
        last = self._last_batch_at
        seconds_since = (
            (datetime.now(timezone.utc) - last).total_seconds() if last is not None else None
        )
        threshold = self._settings.ingest_staleness_threshold_seconds
        stale = bool(
            threshold is not None and seconds_since is not None and seconds_since > threshold
        )
        return IngestHealth(
            transport=self._settings.ingest_transport,
            connection_state=state.value,
            thread_alive=self._thread.is_alive() if self._thread is not None else False,
            last_batch_at=last,
            seconds_since_last_batch=round(seconds_since, 3) if seconds_since is not None else None,
            rows_ingested_total=self._rows_total,
            stale=stale,
        )

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._record_ingest, batch)
```

- [ ] **Step 4: Update the constructor call in `main.py`.** Change line 61 from:

```python
            ingest_svc = await stack.enter_async_context(StreamIngestService(consumer, store))
```
to:
```python
            ingest_svc = await stack.enter_async_context(StreamIngestService(consumer, store, settings))
```

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_ingest_health.py -v`
Expected: PASS (all four unit tests)

- [ ] **Step 6: Commit**

```bash
git add services/stream_ingest.py main.py tests/test_ingest_health.py
git commit -m "feat: ingest freshness tracking, health_check, and disconnect watchdog"
```

---

## Task 8: Per-dependency health_check methods

**Files:**
- Modify: `services/config.py`
- Modify: `services/data.py`
- Modify: `services/cache.py`

- [ ] **Step 1: Add `health_check` to `services/config.py`.** Add imports at the top and the method to `ConfigService`:

```python
import time

import asyncpg

from schemas.config import ConfigEntry
from schemas.health import ProbeResult
```
and append this method to the `ConfigService` class:
```python
    async def health_check(self) -> ProbeResult:
        start = time.perf_counter()
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="postgres", status="up", latency_ms=round(latency_ms, 2))
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="postgres", status="down",
                               latency_ms=round(latency_ms, 2), error=str(exc))
```

- [ ] **Step 2: Add `health_check` to `services/data.py`.** Add the import and method:

```python
import time
```
(add alongside existing imports) and append to the `DataService` class:
```python
    async def health_check(self):
        from schemas.health import ProbeResult
        start = time.perf_counter()
        try:
            ok = await self._client.ping()
            latency_ms = (time.perf_counter() - start) * 1000
            if ok:
                return ProbeResult(name="clickhouse", status="up", latency_ms=round(latency_ms, 2))
            return ProbeResult(name="clickhouse", status="down",
                               latency_ms=round(latency_ms, 2), error="ping returned False")
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="clickhouse", status="down",
                               latency_ms=round(latency_ms, 2), error=str(exc))
```

- [ ] **Step 3: Add `health_check` to `services/cache.py`.** Add the import and method:

```python
import time
```
(add alongside existing imports) and append to the `CacheService` class:
```python
    async def health_check(self):
        from schemas.health import ProbeResult
        start = time.perf_counter()
        try:
            await self._client.ping()
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="redis", status="up", latency_ms=round(latency_ms, 2))
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="redis", status="down",
                               latency_ms=round(latency_ms, 2), error=str(exc))
```

- [ ] **Step 4: Verify all three import cleanly**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "import services.config, services.data, services.cache; print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add services/config.py services/data.py services/cache.py
git commit -m "feat: add per-dependency health_check probes"
```

(These are exercised by the integration tests in Task 14 against real containers.)

---

## Task 9: HealthService aggregator + Container wiring

**Files:**
- Modify: `services/health.py`
- Modify: `core/container.py`

- [ ] **Step 1: Replace `services/health.py`:**

```python
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import psutil

from core.system_metrics import collect_system_snapshot
from schemas.health import (
    AppInfo,
    CheckResult,
    DetailedStatusResponse,
    HealthStatusResponse,
    IngestHealth,
    LivenessResponse,
    ProbeResult,
    ReadinessResponse,
    RequestInfo,
    UptimeInfo,
)
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.stream_ingest import StreamIngestService
from settings import Settings

if TYPE_CHECKING:
    from core.container import Container

log = logging.getLogger(__name__)


class HealthService:
    def __init__(self, settings: Settings, container: "Container") -> None:
        self.settings = settings
        self._container = container
        self._started_at = time.monotonic()
        self._process = psutil.Process()
        self._process.cpu_percent()        # prime process CPU delta
        psutil.cpu_percent(interval=None)  # prime host CPU delta

    # Kept for the MCP get_health_status tool (REST-mirroring, simple string).
    def status(self) -> HealthStatusResponse:
        return HealthStatusResponse(status=self.settings.status)

    def _uptime_seconds(self) -> float:
        return time.monotonic() - self._started_at

    def liveness(self) -> LivenessResponse:
        return LivenessResponse(status="alive", uptime_seconds=round(self._uptime_seconds(), 3))

    async def _probe(self, service_type: type, name: str) -> ProbeResult:
        try:
            service = self._container.get(service_type)
        except ValueError:
            return ProbeResult(name=name, status="down", latency_ms=0.0, error="initializing")
        try:
            return await asyncio.wait_for(
                service.health_check(), timeout=self.settings.health_check_timeout_seconds
            )
        except asyncio.TimeoutError:
            return ProbeResult(
                name=name, status="down",
                latency_ms=round(self.settings.health_check_timeout_seconds * 1000, 2),
                error="timeout",
            )
        except Exception as exc:
            return ProbeResult(name=name, status="down", latency_ms=0.0, error=str(exc))

    async def _gather_dependencies(self) -> list[ProbeResult]:
        return list(await asyncio.gather(
            self._probe(ConfigService, "postgres"),
            self._probe(DataService, "clickhouse"),
            self._probe(CacheService, "redis"),
        ))

    async def _ingest_health(self) -> IngestHealth:
        try:
            service = self._container.get(StreamIngestService)
        except ValueError:
            return IngestHealth(
                transport=self.settings.ingest_transport,
                connection_state="down", thread_alive=False,
            )
        return await service.health_check()

    def _ingest_status(self, ingest: IngestHealth) -> str:
        if ingest.connection_state != "connected":
            return "down"
        if ingest.stale and self.settings.ingest_stale_fails_readiness:
            return "down"
        return "up"

    async def readiness(self) -> ReadinessResponse:
        deps = await self._gather_dependencies()
        ingest = await self._ingest_health()
        ingest_status = self._ingest_status(ingest)

        checks = [
            CheckResult(name=d.name, status=d.status, latency_ms=d.latency_ms, error=d.error)
            for d in deps
        ]
        checks.append(CheckResult(
            name="ingest", status=ingest_status, transport=ingest.transport,
            connection_state=ingest.connection_state, thread_alive=ingest.thread_alive,
            last_batch_at=ingest.last_batch_at,
            seconds_since_last_batch=ingest.seconds_since_last_batch,
        ))

        all_up = all(d.status == "up" for d in deps) and ingest_status == "up"
        return ReadinessResponse(status="ready" if all_up else "not_ready", checks=checks)

    async def detailed_status(self) -> DetailedStatusResponse:
        deps = await self._gather_dependencies()
        ingest = await self._ingest_health()
        snapshot = collect_system_snapshot(self._process)
        return DetailedStatusResponse(
            app=AppInfo(
                title=self.settings.app_title,
                version=self.settings.app_version,
                status=self.settings.status,
            ),
            uptime=UptimeInfo(
                process_seconds=round(self._uptime_seconds(), 3),
                system_boot_seconds=psutil.boot_time(),
            ),
            dependencies=deps,
            ingest=ingest,
            requests=RequestInfo(last_request_at=self._container.last_request_at),
            system=snapshot,
        )
```

- [ ] **Step 2: Update `core/container.py`** to track `last_request_at` and pass `self` to `HealthService`. Replace the file body:

```python
import logging
from datetime import datetime

from settings import Settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    """Per-application singleton registry.

    Each FastAPI app built by main.create_app() owns one Container instance
    (stored on app.state.container), so multiple apps — e.g. isolated test
    apps running in one pytest process — never share or clobber each other's
    services.
    """

    def __init__(self, settings: Settings):
        self._singletons = {}
        self.settings = settings
        self.last_request_at: datetime | None = None
        self.register_singleton(HealthService, HealthService(settings, self))

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        raise ValueError(f"No service registered for type {service_type.__name__}")
```

(The unused `clear()` method is dropped — it is dead code.)

- [ ] **Step 3: Verify import (no circular-import error)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "from core.container import Container; from settings import Settings; c=Container(Settings()); print(c.get.__name__, c.last_request_at)"`
Expected: `get None`

- [ ] **Step 4: Commit**

```bash
git add services/health.py core/container.py
git commit -m "feat: HealthService aggregates liveness/readiness/detailed status"
```

---

## Task 10: Health routers (live/ready/status) + update existing health test

**Files:**
- Modify: `routers/health.py`
- Modify: `tests/test_health.py`

- [ ] **Step 1: Update `tests/test_health.py`** to match the new shapes (this is the spec change for `/health/status`; also covers the new probes):

```python
async def test_liveness_returns_alive(test_client):
    response = await test_client.get("/health/live")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "alive"
    assert body["uptime_seconds"] >= 0.0


async def test_status_reports_app_and_system(test_client):
    """status is overridden to 'testing' in the test fixtures."""
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    body = response.json()
    assert body["app"]["status"] == "testing"
    assert body["uptime"]["process_seconds"] >= 0.0
    assert body["system"]["process"]["memory_rss_bytes"] > 0
    assert isinstance(body["dependencies"], list)
    assert body["ingest"]["transport"] == "flight"


async def test_root_returns_non_empty_json(test_client):
    response = await test_client.get("/")
    assert response.status_code == 200
    assert response.json()
```

- [ ] **Step 2: Replace `routers/health.py`:**

```python
import logging

from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep
from schemas.health import DetailedStatusResponse, LivenessResponse, ReadinessResponse

log = logging.getLogger(__name__)

TAG = "Application Health"
TAG_METADATA = {
    "name": TAG,
    "description": "Liveness, readiness and detailed status endpoints",
}

router = APIRouter(tags=[TAG])


@router.get("/live", response_model=LivenessResponse)
async def get_live(health_service: HealthServiceDep):
    return health_service.liveness()


@router.get("/ready", response_model=ReadinessResponse, response_model_exclude_none=True)
async def get_ready(health_service: HealthServiceDep, response: Response):
    result = await health_service.readiness()
    if result.status != "ready":
        response.status_code = 503
    return result


@router.get("/status", response_model=DetailedStatusResponse)
async def get_status(health_service: HealthServiceDep):
    return await health_service.detailed_status()
```

- [ ] **Step 3: Run the health tests (needs Docker — uses the session `test_client`)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_health.py -v`
Expected: PASS (3 tests). If container startup times out, free the Docker daemon and retry.

- [ ] **Step 4: Commit**

```bash
git add routers/health.py tests/test_health.py
git commit -m "feat: add /health/live and /health/ready; detailed /health/status"
```

---

## Task 11: Request-timestamp middleware

**Files:**
- Modify: `main.py` (inside `create_app`, after `app.state.container = container`)

- [ ] **Step 1: Add the import** at the top of `main.py` (with the other stdlib imports):

```python
from datetime import datetime, timezone
```

- [ ] **Step 2: Register the middleware** in `create_app`, immediately after `app.state.container = container` (currently line 94):

```python
    @app.middleware("http")
    async def _track_last_request(request, call_next):
        request.app.state.container.last_request_at = datetime.now(timezone.utc)
        return await call_next(request)
```

- [ ] **Step 3: Verify the app still builds**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "import main; print(type(main.app).__name__)"`
Expected: `FastAPI`

- [ ] **Step 4: Commit**

```bash
git add main.py
git commit -m "feat: record last-request timestamp via middleware"
```

---

## Task 12: MetricsService, /metrics router, and wiring

**Files:**
- Create: `services/metrics.py`
- Create: `routers/metrics.py`
- Modify: `core/dependencies.py`
- Modify: `main.py` (`create_app`)

- [ ] **Step 1: Create `services/metrics.py`:**

```python
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, Info, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from settings import Settings


class MetricsService:
    """Owns a per-app Prometheus registry (the multi-app test pattern forbids
    the global default registry) and refreshes custom gauges on each scrape."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.registry = CollectorRegistry()
        self._instrumentator = Instrumentator(registry=self.registry)

        self.app_info = Info("app", "Application info", registry=self.registry)
        self.app_info.info({"title": settings.app_title, "version": settings.app_version})

        self.dep_up = Gauge(
            "dependency_up", "1 if dependency reachable else 0", ["name"], registry=self.registry)
        self.dep_latency = Gauge(
            "dependency_check_latency_seconds", "Dependency health-check latency",
            ["name"], registry=self.registry)

        self.ingest_state = Gauge(
            "ingest_connection_state", "Ingest transport connection state (1=active)",
            ["transport", "state"], registry=self.registry)
        self.ingest_secs = Gauge(
            "ingest_seconds_since_last_batch", "Seconds since last ingested batch",
            registry=self.registry)
        self.ingest_rows = Gauge(
            "ingest_rows_ingested", "Total rows ingested", registry=self.registry)

        self.proc_cpu = Gauge("process_cpu_percent", "Process CPU percent", registry=self.registry)
        self.proc_mem = Gauge(
            "process_memory_rss_bytes", "Process resident memory bytes", registry=self.registry)
        self.proc_threads = Gauge(
            "process_num_threads", "Process thread count", registry=self.registry)
        self.proc_fds = Gauge(
            "process_open_files", "Process open file count", registry=self.registry)
        self.proc_uptime = Gauge(
            "process_uptime_seconds", "Process uptime seconds", registry=self.registry)

        self.sys_cpu = Gauge("system_cpu_percent", "Host CPU percent", registry=self.registry)
        self.sys_mem_total = Gauge(
            "system_memory_total_bytes", "Host total memory bytes", registry=self.registry)
        self.sys_mem_avail = Gauge(
            "system_memory_available_bytes", "Host available memory bytes", registry=self.registry)
        self.sys_mem_pct = Gauge(
            "system_memory_used_percent", "Host memory used percent", registry=self.registry)
        self.boot_time = Gauge(
            "system_boot_time_seconds", "Host boot time (unix seconds)", registry=self.registry)

    def instrument(self, app) -> None:
        self._instrumentator.instrument(app)

    async def refresh(self, health_service) -> None:
        status = await health_service.detailed_status()

        for dep in status.dependencies:
            self.dep_up.labels(name=dep.name).set(1.0 if dep.status == "up" else 0.0)
            self.dep_latency.labels(name=dep.name).set(dep.latency_ms / 1000.0)

        ingest = status.ingest
        for state in ("connected", "reconnecting", "down"):
            self.ingest_state.labels(transport=ingest.transport, state=state).set(
                1.0 if ingest.connection_state == state else 0.0)
        self.ingest_secs.set(ingest.seconds_since_last_batch or 0.0)
        self.ingest_rows.set(ingest.rows_ingested_total)

        proc = status.system.process
        host = status.system.host
        self.proc_cpu.set(proc.cpu_percent)
        self.proc_mem.set(proc.memory_rss_bytes)
        self.proc_threads.set(proc.num_threads)
        self.proc_fds.set(proc.open_files)
        self.proc_uptime.set(status.uptime.process_seconds)
        self.sys_cpu.set(host.cpu_percent)
        self.sys_mem_total.set(host.memory_total_bytes)
        self.sys_mem_avail.set(host.memory_available_bytes)
        self.sys_mem_pct.set(host.memory_percent)
        self.boot_time.set(status.uptime.system_boot_seconds)

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
```

- [ ] **Step 2: Add a dependency getter in `core/dependencies.py`.** Add the import and getter + alias (place near the other service getters):

```python
from services.metrics import MetricsService
```
```python
def get_metrics_service(container: ContainerDep) -> MetricsService:
    return container.get(MetricsService)
```
```python
MetricsServiceDep = Annotated[MetricsService, Depends(get_metrics_service)]
```

- [ ] **Step 3: Create `routers/metrics.py`:**

```python
from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep, MetricsServiceDep

router = APIRouter()


@router.get("/metrics")
async def get_metrics(health_service: HealthServiceDep, metrics_service: MetricsServiceDep):
    await metrics_service.refresh(health_service)
    body, content_type = metrics_service.render()
    return Response(content=body, media_type=content_type)
```

- [ ] **Step 4: Wire it into `main.py`.** Add imports near the other router/service imports:

```python
from routers import health, data, config, cache, metrics
from services.metrics import MetricsService
```
Then in `create_app`, after the existing `app.include_router(...)` block and before `app.mount("/mcp", ...)`, add:

```python
    if settings.metrics_enabled:
        metrics_service = MetricsService(settings)
        metrics_service.instrument(app)
        container.register_singleton(MetricsService, metrics_service)
        app.include_router(metrics.router)
```

- [ ] **Step 5: Verify the app builds with metrics enabled**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "from main import create_app; from settings import Settings; app=create_app(Settings()); print([r.path for r in app.routes if getattr(r,'path','')=='/metrics'])"`
Expected: `['/metrics']`

- [ ] **Step 6: Verify two apps can be built in one process (registry isolation)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\python.exe" -c "from main import create_app; from settings import Settings; a=create_app(Settings()); b=create_app(Settings()); print('two apps ok')"`
Expected: `two apps ok` (no "Duplicated timeseries" error — proves the per-app registry works)

- [ ] **Step 7: Commit**

```bash
git add services/metrics.py routers/metrics.py core/dependencies.py main.py
git commit -m "feat: add Prometheus /metrics endpoint with per-app registry"
```

---

## Task 13: Grafana/Prometheus docker-compose profile + dashboard

**Files:**
- Create: `observability/prometheus.yml`
- Create: `observability/grafana/provisioning/datasources/datasource.yml`
- Create: `observability/grafana/provisioning/dashboards/dashboards.yml`
- Create: `observability/grafana/dashboards/service-overview.json`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Create `observability/prometheus.yml`:**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: python-template
    metrics_path: /metrics
    static_configs:
      - targets: ["app:8000"]
```

- [ ] **Step 2: Create `observability/grafana/provisioning/datasources/datasource.yml`:**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 3: Create `observability/grafana/provisioning/dashboards/dashboards.yml`:**

```yaml
apiVersion: 1
providers:
  - name: default
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 4: Create `observability/grafana/dashboards/service-overview.json`:**

```json
{
  "title": "Service Overview",
  "uid": "service-overview",
  "schemaVersion": 39,
  "version": 1,
  "time": {"from": "now-15m", "to": "now"},
  "panels": [
    {
      "id": 1, "type": "timeseries", "title": "Request rate (req/s)",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "targets": [{"expr": "sum(rate(http_requests_total[1m]))", "legendFormat": "requests"}]
    },
    {
      "id": 2, "type": "timeseries", "title": "Request latency p95 (s)",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "targets": [{"expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p95"}]
    },
    {
      "id": 3, "type": "stat", "title": "Dependency up",
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 8},
      "targets": [{"expr": "dependency_up", "legendFormat": "{{name}}"}]
    },
    {
      "id": 4, "type": "stat", "title": "Ingest connection state",
      "gridPos": {"h": 8, "w": 8, "x": 8, "y": 8},
      "targets": [{"expr": "ingest_connection_state", "legendFormat": "{{state}}"}]
    },
    {
      "id": 5, "type": "timeseries", "title": "Seconds since last ingest",
      "gridPos": {"h": 8, "w": 8, "x": 16, "y": 8},
      "targets": [{"expr": "ingest_seconds_since_last_batch", "legendFormat": "staleness"}]
    },
    {
      "id": 6, "type": "timeseries", "title": "Process CPU % / memory bytes",
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16},
      "targets": [
        {"expr": "process_cpu_percent", "legendFormat": "cpu %"},
        {"expr": "process_memory_rss_bytes", "legendFormat": "rss bytes"}
      ]
    }
  ]
}
```

- [ ] **Step 5: Add the `observability` profile services to `docker-compose.yml`.** Insert before the top-level `volumes:` block:

```yaml
  prometheus:
    profiles: [observability]
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      app:
        condition: service_healthy

  grafana:
    profiles: [observability]
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus
```

- [ ] **Step 6: Update the `app` healthcheck** in `docker-compose.yml` to use the lightweight liveness probe. Change the healthcheck `test` line (currently hits `/health/status`) to:

```yaml
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live')\""]
```

- [ ] **Step 7: Validate compose config parses**

Run: `docker compose --profile observability config --quiet; echo "exit: $?"`
Expected: `exit: 0` (no YAML/schema errors). If Docker is unavailable, skip and note it.

- [ ] **Step 8: Commit**

```bash
git add observability docker-compose.yml
git commit -m "feat: add opt-in Prometheus+Grafana observability profile"
```

---

## Task 14: Endpoint integration tests (Docker)

**Files:**
- Create: `tests/test_observability.py`
- Modify: `pytest.ini` (add an `observability` marker)

- [ ] **Step 1: Add the marker** to `pytest.ini` under `markers =`:

```
    observability: tests for the observability endpoints
```

- [ ] **Step 2: Create `tests/test_observability.py`.** It defines a streaming Flight server (loop=True keeps the consumer CONNECTED with fresh batches), plus dedicated fixtures for the disconnect, idle-staleness, and dependency-down cases:

```python
import threading

import pyarrow.flight as pa_flight
import pytest
from httpx import AsyncClient
from testcontainers.redis import RedisContainer

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.flight_server import ExampleFlightServer, IdleFlightServer, make_batch

pytestmark = pytest.mark.observability

REDIS_IMAGE = "redis/redis-stack-server:latest"

_STREAM_SCRIPT = [make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")])]


def _settings(pg, ch, redis_url, flight_port, **overrides) -> Settings:
    return Settings(
        status="testing",
        postgres_url=f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}/{pg.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=int(ch.get_exposed_port(8123)),
        clickhouse_user=ch.username or "default",
        clickhouse_password=ch.password or "",
        clickhouse_database="default",
        redis_url=redis_url,
        ingest_transport="flight",
        flight_host="localhost",
        flight_port=flight_port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
        **overrides,
    )


@pytest.fixture(scope="module")
def streaming_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.05, loop=True)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


async def _poll_ready(client: AsyncClient, expected_code: int, timeout: float = 10.0):
    import time
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await client.get("/health/ready")
        if last.status_code == expected_code:
            return last
        import asyncio
        await asyncio.sleep(0.1)
    return last


async def test_liveness_always_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"


async def test_readiness_all_up_returns_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await _poll_ready(client, 200)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        by_name = {c["name"]: c for c in body["checks"]}
        assert by_name["postgres"]["status"] == "up"
        assert by_name["clickhouse"]["status"] == "up"
        assert by_name["redis"]["status"] == "up"
        assert by_name["ingest"]["status"] == "up"
        assert by_name["ingest"]["connection_state"] == "connected"


async def test_status_structure_and_ingest_freshness(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await _poll_ready(client, 200)
        body = (await client.get("/health/status")).json()
        assert body["app"]["status"] == "testing"
        assert body["uptime"]["process_seconds"] >= 0.0
        assert body["system"]["process"]["memory_rss_bytes"] > 0
        assert body["ingest"]["rows_ingested_total"] > 0
        assert body["ingest"]["last_batch_at"] is not None


async def test_metrics_endpoint_exposes_series(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await client.get("/health/live")          # generate one request metric
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        text = resp.text
        assert "http_request_duration_seconds" in text
        assert "dependency_up" in text
        assert "ingest_connection_state" in text
        assert "ingest_seconds_since_last_batch" in text
        assert "process_memory_rss_bytes" in text


async def test_ingest_disconnect_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container,
):
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.05, loop=True)
    threading.Thread(target=server.serve, daemon=True).start()
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        server.port,
        ingest_max_disconnect_seconds=None,   # don't SIGTERM the test process
    )
    async with lifespan_test_client(settings) as client:
        assert (await _poll_ready(client, 200)).status_code == 200
        server.shutdown()                     # silent disconnect
        resp = await _poll_ready(client, 503)
        assert resp.status_code == 503
        ingest = {c["name"]: c for c in resp.json()["checks"]}["ingest"]
        assert ingest["connection_state"] != "connected"


async def test_idle_ingest_is_stale_but_ready(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container,
):
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = IdleFlightServer(location)         # connected, never sends
    threading.Thread(target=server.serve, daemon=True).start()
    redis_url = f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0"
    try:
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, server.port,
            ingest_staleness_threshold_seconds=0.5,
            ingest_stale_fails_readiness=False,
            ingest_max_disconnect_seconds=None,
        )
        async with lifespan_test_client(settings) as client:
            # Connected but no batches: ready stays 200 even once stale.
            resp = await _poll_ready(client, 200)
            assert resp.status_code == 200
            import asyncio
            await asyncio.sleep(0.7)
            status = (await client.get("/health/status")).json()
            assert status["ingest"]["connection_state"] == "connected"
            assert status["ingest"]["stale"] is True
            assert (await client.get("/health/ready")).status_code == 200
    finally:
        server.shutdown()


async def test_dependency_down_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, streaming_flight_server,
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        redis_url = f"redis://localhost:{int(dedicated_redis.get_exposed_port(6379))}/0"
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_redis.stop()             # kill only this test's redis
            resp = await _poll_ready(client, 503)
            assert resp.status_code == 503
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass
```

- [ ] **Step 3: Run the observability tests (Docker required)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_observability.py -v --tb=short`
Expected: PASS (7 tests). If container startup hits the named-pipe timeout, run `docker compose down` to free the daemon and retry.

- [ ] **Step 4: Commit**

```bash
git add tests/test_observability.py pytest.ini
git commit -m "test: integration tests for observability endpoints"
```

---

## Task 15: Full regression

- [ ] **Step 1: Run the unit-only suites (no Docker)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" tests/test_system_metrics.py tests/test_ingest_health.py tests/test_retry.py -v`
Expected: all PASS.

- [ ] **Step 2: Run the whole suite (Docker required)**

Run: `& "C:\Users\Alexander\miniconda3\envs\p312\Scripts\pytest.exe" -v --tb=short`
Expected: all PASS. Pre-existing container-backed suites (`test_config`, `test_data`, `test_cache`, `test_flight_cache`, `test_http_ingest`, `test_mcp`, `test_health`, `test_observability`) plus the new unit suites.

- [ ] **Step 3: Final commit (only if any fixups were needed)**

```bash
git add -A
git commit -m "chore: observability regression fixups"
```

---

## Self-review notes (for the implementer)

- **`test_data.py` / `test_mcp.py`** still call `DataService`/`HealthService` — unchanged public methods (`get_data`, `status`) are preserved, so they keep passing. The MCP `get_health_status` tool still calls `HealthService.status().status`.
- **`StreamIngestService` constructor** gained a required `settings` arg — the only production caller is `main.py:61` (updated in Task 7); test constructors pass it explicitly.
- **Per-app metrics registry** is the load-bearing detail for the multi-app test process — verified in Task 12 Step 6.
- **Flight stream-end → `ConnectionError`** changes the previous "clean exit" behaviour; intentional (Task 5) so an unexpected end reconnects rather than silently stopping the thread. `close()` sets `_closing` so real shutdown still exits cleanly.
