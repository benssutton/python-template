# Observability Endpoints Design

**Status:** Approved (design phase)
**Date:** 2026-06-13
**Topic:** Best-in-class observability REST endpoints for the Python FastAPI template

---

## Goal

Replace the single `GET /health/status` stub with a best-in-class observability surface: Kubernetes-style liveness/readiness probes, a rich human/debug status report, and a Prometheus `/metrics` endpoint for Grafana — covering dependency health, ingest-transport connection state, data freshness, system resources, uptime, and request activity. A core objective is making **silent ingest disconnects loud**: a dropped Solace (or Flight) connection must surface immediately rather than letting the application serve stale data.

## Background — current state

- One endpoint: `GET /health/status` → `{"status": "running"}`, which just echoes `settings.status`. No real checks.
- `HealthService` is a stub wrapping `settings`.
- Each dependency has a natural probe that is **not** wired to any endpoint: Postgres pool (`SELECT 1`), ClickHouse (`.ping()`), Redis (`.ping()`), and the ingest thread + `LSMStore`.
- The ingest thread (`services/stream_ingest.py`) runs `consumer.batches()` with exception→backoff→SIGTERM-after-N-failures resilience.
- **Silent-failure gap:** `thread.is_alive()` only catches the Flight failure mode (stream ends → thread exits). It misses the Solace silent disconnect — the SDK reconnects in the background, no exception reaches us, the ingest thread blocks forever on `queue.get()`, and the LSM store serves stale data with nothing reporting it.
- No metrics, no system stats, no uptime tracking, no request instrumentation.

## Design decisions (from brainstorming)

1. **Build both** paradigms, cleanly separated: JSON health endpoints **and** a Prometheus `/metrics` endpoint.
2. **Active per-request** dependency health checks (parallel, short timeout). No background polling for the health path.
3. **`prometheus-fastapi-instrumentator`** for request metrics + custom collectors (idiomatic, minimal boilerplate).
4. **Process/container-visible** system metrics via `psutil` — no privileged host access, no `/proc` mounting. "Host" = whatever the container is allowed to see.
5. Ingest health reads **event-driven transport connection state**, not `thread.is_alive()`.
6. `RECONNECTING` **fails readiness immediately** (strict, no grace period).
7. Data staleness is **reported and alertable** but does **not** auto-fail readiness by default (idle topics are legitimate; avoids false 503s).

---

## Section 1: Endpoint surface

Four endpoints under `/health` plus `/metrics`. Liveness/readiness split follows the Kubernetes convention.

### `GET /health/live` — liveness
- **Always 200** if the process is up and the event loop responds. No dependency or transport checks.
- Body: `{"status": "alive", "uptime_seconds": 1234.5}`
- **Purpose:** k8s liveness probe. Failure → orchestrator *restarts* the pod. Must never depend on any external service.

### `GET /health/ready` — readiness
- Pings all dependencies **in parallel** (Postgres `SELECT 1`, ClickHouse `ping`, Redis `ping`), each wrapped in `asyncio.wait_for(timeout=health_check_timeout_seconds)` (default 2s), **plus** the ingest transport's cached connection state (no I/O, no timeout).
- **200** if all critical checks pass, **503** otherwise.
- Body:
```json
{
  "status": "ready",
  "checks": [
    {"name": "postgres",   "status": "up", "latency_ms": 3.1},
    {"name": "clickhouse", "status": "up", "latency_ms": 5.0},
    {"name": "redis",      "status": "up", "latency_ms": 1.2},
    {
      "name": "ingest",
      "status": "up",
      "transport": "solace",
      "connection_state": "connected",
      "thread_alive": true,
      "last_batch_at": "2026-06-12T10:31:02Z",
      "seconds_since_last_batch": 1.4
    }
  ]
}
```
- **Readiness rules:**
  - Any of Postgres/ClickHouse/Redis unreachable or timing out → that check `down` → **503**.
  - Ingest `connection_state == CONNECTED` → ingest `up`.
  - Ingest `connection_state == RECONNECTING` **or** `DOWN` → ingest `down` → **503 immediately** (no grace period). The pod is pulled from the LB until the transport reconnects, then flips back to 200 with no restart.
  - Staleness beyond `ingest_staleness_threshold_seconds` → reported as `stale`, but does **not** fail readiness unless `ingest_stale_fails_readiness` is enabled (default `false`).
- **Purpose:** k8s readiness probe / load-balancer gate. Failure withholds traffic but does *not* restart.

### `GET /health/status` — detailed human/debug view
- Rich JSON "dashboard". **Always 200** (a report, not a gate — usable even when a dependency is down).
```json
{
  "app":    {"title": "...", "version": "1.0.0", "status": "running"},
  "uptime": {"process_seconds": 1234.5, "system_boot_seconds": 980000.0},
  "dependencies": [
    {"name": "postgres",   "status": "up", "latency_ms": 3.1},
    {"name": "clickhouse", "status": "up", "latency_ms": 5.0},
    {"name": "redis",      "status": "up", "latency_ms": 1.2}
  ],
  "ingest": {
    "transport": "solace",
    "connection_state": "connected",
    "thread_alive": true,
    "last_batch_at": "2026-06-12T10:31:02Z",
    "seconds_since_last_batch": 1.4,
    "rows_ingested_total": 482,
    "stale": false
  },
  "requests": {"last_request_at": "2026-06-12T10:33:50Z"},
  "system": {
    "process": {"cpu_percent": 2.1, "memory_rss_bytes": 84213760, "num_threads": 11, "open_files": 23},
    "host":    {"cpu_percent": 14.0, "memory_total_bytes": 8e9, "memory_available_bytes": 3.2e9, "memory_percent": 60.0}
  }
}
```

### `GET /metrics` — Prometheus exposition
- Plain-text Prometheus format. Time-series for CPU/mem/IO, request rate/latency/status, dependency-up, and ingest health (including `ingest_connection_state` and `ingest_seconds_since_last_batch`). Full set in Section 3.

**Cross-cutting:** all timestamps are UTC ISO-8601.

---

## Section 2: Internal architecture & data flow

Guiding principle: **each component reports its own health from signals it owns**; `HealthService` only aggregates.

### 2.1 Transport connection state (the silent-disconnect fix)

**`ingestion/base.py`** — add an enum and extend the protocol:
```python
from enum import Enum

class ConnectionState(str, Enum):
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DOWN = "down"

class BatchConsumer(Protocol):
    def batches(self) -> Iterator[pa.RecordBatch]: ...
    def close(self) -> None: ...
    def connection_state(self) -> ConnectionState: ...   # NEW — cheap, cached, no I/O
```

**Solace (`ingestion/solace/client.py`):** register the SDK's lifecycle listeners in `_connect()`. They fire on Solace's own threads; store `(state, changed_at)` under a `threading.Lock`:
- `ReconnectionAttemptListener` / `ServiceInterruptionListener` → `RECONNECTING` / `DOWN`
- `ReconnectionListener` → `CONNECTED`

This catches the case where the ingest thread is blocked on `queue.get()` and nothing raises — the listener flips state independently of the data path.

**Flight (`ingestion/flight/client.py`):** `CONNECTED` while iterating `do_get`. If the for-loop ends while not intentionally closing, raise `ConnectionError("flight stream ended")` so the existing exception→backoff path re-opens the stream (today it returns cleanly and the thread silently exits — a bug this fixes). `RECONNECTING` during backoff; `DOWN` after `close()`. The consumer carries a `_closing` flag set by `close()` to distinguish intentional shutdown from an unexpected stream end.

### 2.2 Ingest service tracking (`services/stream_ingest.py`)
- On each yielded batch: update `self._last_batch_at = datetime.now(UTC)` and `self._rows_total += batch.num_rows`.
- Existing exception→backoff→SIGTERM-after-`_INGEST_MAX_FAILURES` logic stays unchanged.
- New `async def health_check(self) -> IngestHealth` returns `transport`, `connection_state()`, `thread_alive`, `last_batch_at`, `seconds_since_last_batch`, `rows_ingested_total`, and `stale` (true when `seconds_since_last_batch > ingest_staleness_threshold_seconds`, if configured).
- **Disconnect watchdog** (`ingest_max_disconnect_seconds`, default `60.0`; set `None` to disable): one small asyncio task started in `__aenter__`, wakes periodically; if `connection_state()` has been non-`CONNECTED` continuously longer than the threshold, logs `critical` and `os.kill(getpid(), SIGTERM)`. This is the streaming equivalent of the CLAUDE.md "shut down after repeated failure" rule. Primary recovery is readiness-503 + auto-reconnect within the window; the watchdog is the last resort when the transport never comes back. The 60s default gives auto-reconnect a full minute to recover before the pod shuts down and is restarted by the orchestrator.

### 2.3 Per-dependency health checks (owned by each service)
Each service that holds a client gains `async def health_check(self) -> ProbeResult`:
- `ConfigService` → `SELECT 1` via `pool.acquire()`
- `DataService` → `await client.ping()` (ClickHouse)
- `CacheService` → `await client.ping()` (Redis)

`ProbeResult` carries `name`, `status` (`up`/`down`), `latency_ms`, optional `error`. Probe logic lives next to the client that owns it.

### 2.4 HealthService as aggregator (`services/health.py`)
- Construction changes to `HealthService(settings, container)` (passed `self` from `Container.__init__`); records `process_started_at`; primes a cached `psutil.Process`.
- Resolves dependent services **lazily from the container at request time** (they are registered later in the lifespan, not at container init). If a service is not yet registered (startup race), its probe returns `down: "initializing"`.
- `liveness()` → uptime only; never touches deps.
- `readiness()` → `asyncio.gather` over the three dependency `health_check()`s (each `wait_for`-timeout-wrapped) ∥ the ingest `health_check()` (no timeout — cached read); applies Section 1 rules; returns 200/503.
- `detailed_status()` → everything readiness gathers + system snapshot + `last_request_at`; always 200.

### 2.5 System metrics (`core/system_metrics.py`)
- Pure `collect_system_snapshot(process: psutil.Process) -> SystemSnapshot`. Cross-platform, container-visible (process CPU%/RSS/threads/open-files + host cpu/mem). `cpu_percent()` primed once at startup (first call returns 0.0) so subsequent reads are non-blocking deltas.

### 2.6 Request timestamp
- Minimal `@app.middleware("http")` sets `app.state.last_request_at = datetime.now(UTC)`. Its only job. Request counts/latencies come from the instrumentator (Section 3) — no double-counting.

### 2.7 New settings (`settings.py`)
```python
health_check_timeout_seconds: float = 2.0                 # per-dependency ping timeout
ingest_staleness_threshold_seconds: float | None = None   # None = staleness never reported as stale
ingest_stale_fails_readiness: bool = False                # stale -> 503 only if True
ingest_max_disconnect_seconds: float | None = 60.0        # non-CONNECTED longer than this -> SIGTERM; None = disabled
metrics_enabled: bool = True
```

### 2.8 Thread-safety
- Solace `(state, changed_at)`: written on SDK threads, read on the asyncio thread → guarded by a `threading.Lock`.
- `last_batch_at` / `rows_total`: single-attribute writes on the ingest thread, read on the asyncio thread → atomic under the GIL, no lock.
- `connection_state()` / `health_check()` do no blocking I/O → safe to call inline from the readiness handler.

**Data flow for `/health/ready`:** request → `HealthService.readiness()` → `gather(pg SELECT 1, ch ping, redis ping)` with timeouts ∥ `ingest.health_check()` (cached state) → apply rules → 200/503 JSON.

---

## Section 3: Metrics, Grafana, testing & dependencies

### 3.1 Prometheus metric set
**Auto (instrumentator middleware):** `http_requests_total{method,handler,status}`, `http_request_duration_seconds` (histogram), `http_requests_in_progress`, `http_request_size_bytes`, `http_response_size_bytes`.

**Custom (`services/metrics.py`, refreshed per-scrape):**
- System: `process_cpu_percent`, `process_memory_rss_bytes`, `process_num_threads`, `process_open_files`, `process_uptime_seconds`, `system_cpu_percent`, `system_memory_total_bytes`, `system_memory_available_bytes`, `system_memory_used_percent`, `system_boot_time_seconds`
- Dependencies: `dependency_up{name}` (1/0), `dependency_check_latency_seconds{name}`
- Ingest: `ingest_connection_state{transport,state}` (state-set: exactly one of connected/reconnecting/down = 1), `ingest_seconds_since_last_batch`, `ingest_rows_ingested`
- `app_info{version,title}` (Info metric)

`prometheus_client` defaults (`process_*`, `python_gc_*`) are kept; psutil gauges fill the cross-platform gap with distinct names.

### 3.2 `/metrics` wiring
`Instrumentator().instrument(app)` adds the request-metrics middleware. Instead of its default sync `.expose()`, define an **async `/metrics` route** (`routers/metrics.py`) that:
1. `await metrics_service.refresh()` — pulls a psutil snapshot + runs the same dependency/ingest probes `HealthService` uses, and `.set()`s every custom gauge.
2. returns `generate_latest()` with `CONTENT_TYPE_LATEST`.

One freshness model (pull/active) across `/health/ready` and `/metrics`; no background poller. A `SELECT 1`/`ping` per ~15s scrape is negligible. Documented alternative: gauges reflect the last readiness result (cheaper, staler) — the template uses fresh-on-scrape for clarity.

When `metrics_enabled` is `false`, neither the instrumentator middleware nor the `/metrics` route is registered (the endpoint returns 404). Health endpoints are unaffected.

### 3.3 Grafana integration — opt-in docker-compose profile
Mirror the existing `profiles: [solace]` pattern with `profiles: [observability]`:
- `prometheus` — scrapes `app:<port>/metrics`; config in `observability/prometheus.yml`.
- `grafana` — auto-provisioned Prometheus datasource + starter dashboard `observability/grafana/dashboards/service-overview.json` (request rate, p95 latency, `dependency_up`, `ingest_connection_state`, `ingest_seconds_since_last_batch`, process CPU/mem).

Run with `docker compose --profile observability up`. A raw Prometheus scrape snippet also goes in the docs for non-compose deployments. Opt-in, so the default `docker compose up` is untouched.

### 3.4 Testing strategy (real endpoints, real containers, isolated apps)
`tests/test_observability.py` + `tests/test_ingest_health.py`:
- **`test_liveness_always_200`** — trivial up.
- **`test_readiness_all_up_returns_200`** — all containers + Flight connected; assert `ready`, all four checks `up`.
- **`test_readiness_dependency_down_returns_503`** — dedicated *function-scoped* Redis container; after app start, `redis_container.stop()`, then GET `/health/ready` → 503 with redis `down`. Function-scoped so it never clobbers the shared session container.
- **`test_ingest_disconnect_fails_readiness`** — the core concern. Start the app against `ExampleFlightServer`, then `server.shutdown()`; the Flight stream ends → consumer raises → state `RECONNECTING`; poll `/health/ready` until 503 with ingest `connection_state` ≠ connected.
- **`test_solace_listener_state_transitions`** (unit, no broker) — invoke the registered Solace listener callbacks directly; assert `connection_state()` walks CONNECTED→RECONNECTING→CONNECTED.
- **`test_ingest_staleness_idle_not_disconnected`** — isolated app + *empty* Flight server (connected, zero batches), small `ingest_staleness_threshold_seconds`: `/health/status` shows `stale: true` but `/health/ready` stays **200** (idle ≠ down). Flip `ingest_stale_fails_readiness=True` → 503.
- **`test_status_structure`** — uptime > 0, deps present, `system.process.memory_rss_bytes > 0`; after an HTTP ingest, `last_batch_at` set and `rows_ingested > 0`.
- **`test_metrics_endpoint`** — 200, correct content-type, contains `http_request_duration_seconds`, `dependency_up`, `ingest_connection_state`, `ingest_seconds_since_last_batch`, psutil gauges; `http_requests_total` increments after a call.
- **`test_system_metrics_unit`** — `collect_system_snapshot()` returns plausible values, no container.
- **`test_disconnect_watchdog_triggers_shutdown`** (unit, no broker, no mocks) — a real fake `BatchConsumer` whose `connection_state()` returns `DOWN`; `StreamIngestService` with a tiny `ingest_max_disconnect_seconds`. The test installs a `SIGTERM` handler that sets an `asyncio.Event`, then asserts the event fires within the window (proving the watchdog requested shutdown). Restores the original handler afterward.

### 3.5 New dependencies
- `psutil`
- `prometheus-fastapi-instrumentator` (pulls in `prometheus-client`)

### 3.6 File inventory
**New:** `core/system_metrics.py`, `services/metrics.py`, `routers/metrics.py`, `observability/**` (prometheus + grafana provisioning + dashboard), `tests/test_observability.py`, `tests/test_ingest_health.py`

**Modified:** `ingestion/base.py`, `ingestion/solace/client.py`, `ingestion/flight/client.py`, `services/stream_ingest.py`, `services/config.py`, `services/data.py`, `services/cache.py`, `services/health.py`, `core/container.py`, `routers/health.py`, `schemas/health.py`, `settings.py`, `main.py`, `docker-compose.yml`, `pyproject.toml`/requirements.

---

## Out of scope
- OpenTelemetry/OTLP push pipelines (pull/scrape only).
- Distributed tracing / span propagation.
- True host-hardware introspection from inside the app container (documented `node_exporter` sidecar is the recommended path if needed later).
- Log aggregation (Loki/ELK).
- Alertmanager rules beyond the starter Grafana dashboard.
