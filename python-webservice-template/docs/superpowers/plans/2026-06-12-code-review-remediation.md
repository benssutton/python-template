# Code Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address all 10 findings from the June 2026 code review and add two targeted test improvements (MCP health-status call, LSM empty-state assertion).

**Architecture:** Ten independent tasks, ordered roughly easiest-first so early commits build confidence. Tasks 1–3 are config/docs fixes with no code logic. Tasks 4–7 fix correctness and resilience bugs in the app core. Tasks 8–10 fix test correctness. Every task follows TDD where a test is the gate.

**Tech Stack:** Python 3.12, FastAPI, asyncpg, redis-py async, clickhouse-connect, pyarrow Flight, Solace PubSub+ SDK, pytest + pytest-asyncio (auto mode, session loop), testcontainers, httpx AsyncClient.

---

## File Map

| File | Change |
|------|--------|
| `.env.example` | Replace with variables for this service |
| `docker-compose.yml` | Remove flight from app `depends_on`; add `profiles` to flight/solace services |
| `main.py` | Remove redundant `get_settings()` in `__main__`; add explanatory comment |
| `persistence/stream_store/lsm_store.py` | Drop internal columns dynamically; fix `limit=0` guard |
| `persistence/cache_store/redis/redis_client.py` | Add startup `ping()` via retry utility |
| `persistence/transaction_store/postgres/postgres_client.py` | Wrap pool creation with retry utility |
| `core/retry.py` | **New** — `connect_with_backoff()` async utility |
| `services/stream_ingest.py` | Retry loop with exponential backoff; SIGTERM after N failures; `join()` timeout |
| `tests/test_retry.py` | **New** — unit tests for retry utility |
| `tests/test_mcp.py` | Fix JSON-RPC method to `tools/call`; add `get_health_status` content assertion |
| `tests/test_config.py` | Remove order dependency; use unique keys per test |
| `tests/test_http_ingest.py` | Add empty-state test; fix `_poll_for_id` to wait for expected value |

---

## Task 1: Fix `.env.example`

**Files:**
- Modify: `.env.example`

The current file contains variables from an unrelated planning/GIS project. No code changes required — just replace the file content.

- [ ] **Step 1: Replace `.env.example`**

```
# Copy to .env and customise. .env is gitignored.

# ── Postgres ──────────────────────────────────────────────────────────────────
# DSN for asyncpg. docker-compose defaults: user/password/appdb on localhost:5432.
POSTGRES_URL=postgresql://user:password@localhost:5432/appdb

# ── ClickHouse ─────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default

# ── Redis ──────────────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0

# ── Apache Arrow Flight (default ingest transport) ─────────────────────────────
FLIGHT_HOST=localhost
FLIGHT_PORT=8815
FLIGHT_TICKET=items

# ── Ingest transport selector ──────────────────────────────────────────────────
# "flight" (default) or "solace"
INGEST_TRANSPORT=flight

# ── Solace PubSub+ (only used when INGEST_TRANSPORT=solace) ───────────────────
SOLACE_HOST=localhost
SOLACE_PORT=55555
SOLACE_VPN=default
SOLACE_USERNAME=admin
SOLACE_PASSWORD=admin
SOLACE_TOPIC=ingest/batches

# ── Server ─────────────────────────────────────────────────────────────────────
# Default: HTTPS on port 443 with certs/key.pem + certs/cert.pem.
# For plain HTTP during local dev override both:
# SERVER_PORT=8000
# Unset SSL_KEYFILE and SSL_CERTFILE (or point them at valid certs).
SERVER_HOST=0.0.0.0
SERVER_PORT=443
SSL_KEYFILE=./certs/key.pem
SSL_CERTFILE=./certs/cert.pem
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "fix: replace .env.example with correct variables for this service"
```

---

## Task 2: Fix `docker-compose.yml` — remove unconditional flight dependency

**Files:**
- Modify: `docker-compose.yml`

The `app` service declares `depends_on: flight: condition: service_healthy` unconditionally. Running Solace mode (`docker compose up db clickhouse redis solace solace-publisher app`) fails because Compose can't satisfy the flight healthcheck for a service that isn't included. Fix: assign `profiles` to the transport-specific services so they are opt-in, and remove the flight healthcheck gate from `app` (the app's own retry layer handles transport readiness).

- [ ] **Step 1: Update `docker-compose.yml`**

Apply the following changes to the file (show the full `app.depends_on` block and the `flight`/`solace`/`solace-publisher` service headers):

The `app` service `depends_on` block becomes:
```yaml
    depends_on:
      db:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
      redis:
        condition: service_healthy
      # Transport services (flight / solace) are not listed here.
      # The ingest layer retries the transport connection so Compose does not
      # need to gate app startup on them.
      # Flight mode:  docker compose --profile flight up
      # Solace mode:  docker compose --profile solace up
```

The `flight` service gets `profiles: [flight]`:
```yaml
  flight:
    profiles: [flight]
    build: .
    command: ["python", "tests/publishers/flight_server.py"]
    ...
```

The `solace` service gets `profiles: [solace]`:
```yaml
  solace:
    profiles: [solace]
    image: solace/solace-pubsub-standard:latest
    ...
```

The `solace-publisher` service gets `profiles: [solace]`:
```yaml
  solace-publisher:
    profiles: [solace]
    build: .
    ...
```

Delete the now-stale comment on the `app` service that references the old selective `docker compose up` invocation.

- [ ] **Step 2: Verify flight mode works**

```bash
docker compose --profile flight config --services
```
Expected output includes: `db`, `clickhouse`, `redis`, `flight`, `app`.

- [ ] **Step 3: Verify solace mode services are correct**

```bash
docker compose --profile solace config --services
```
Expected output includes: `db`, `clickhouse`, `redis`, `solace`, `solace-publisher`, `app`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "fix: use compose profiles for transport services; remove unconditional flight dependency from app"
```

---

## Task 3: Clean up `main.py` import-time side-effect

**Files:**
- Modify: `main.py`

Two issues:
1. `app = create_app(get_settings())` executes at module import time. If a required env var is ever added to `Settings`, `import main` during test collection raises `ValidationError` before any test runs. A comment makes the risk explicit and documents the `--factory` alternative.
2. `settings = get_settings()` in `__main__` is a redundant call — `get_settings()` is `@lru_cache` so it returns the same instance, but a reader can't know that without checking the function. Use `app` directly (already constructed above) or derive settings once.

- [ ] **Step 1: Update `main.py` bottom section**

Replace the final block (lines 116–129) with:

```python
# Module-level app for `uvicorn main:app`.
# All Settings fields have defaults so this is safe to execute at import time.
# If you later add a field with no default, switch to the factory pattern:
#   uvicorn main:create_app --factory
app = create_app(get_settings())


if __name__ == "__main__":
    import uvicorn
    log.info("Starting the application from main.py")
    settings = get_settings()   # same cached instance as `app` above
    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        ssl_keyfile=settings.ssl_keyfile,
        ssl_certfile=settings.ssl_certfile,
    )
```

The only material change is adding the comment block. The redundant `settings = get_settings()` call is intentionally kept and annotated so the lru_cache relationship is explicit to readers.

- [ ] **Step 2: Commit**

```bash
git add main.py
git commit -m "docs: annotate module-level app creation and lru_cache relationship in main.py"
```

---

## Task 4: Fix `lsm_store.py` — dynamic column projection and `limit=0`

**Files:**
- Modify: `persistence/stream_store/lsm_store.py` (lines 31–41)

Two bugs in `_merge_to_rows`:
1. `.select(["id", "name", "value"])` is hardcoded. Adding a column to the schema and to `key_columns` will not appear in query results — silently wrong. Fix: drop the internal LSM columns (`seqno`, `op`) instead of selecting business columns explicitly.
2. `if limit is not None` passes `limit=0`, calling `live.head(0)` which returns zero rows while `total` reports the real count. Fix: `if limit` (treats both `None` and `0` as "no limit").

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_http_ingest.py` (after imports, before existing tests):

```python
async def test_lsm_query_limit_zero_returns_all_rows():
    """limit=0 must not silently return an empty result."""
    from persistence.stream_store.lsm_store import LSMStore
    import pyarrow as pa
    from tests.publishers.flight_server import make_batch

    store = LSMStore(flush_rows=100, compaction_runs=4)
    store.ingest(make_batch([(1, "a", "v1", "upsert"), (2, "b", "v2", "upsert")]))
    rows, total = store.query(limit=0)
    assert total == 2
    assert len(rows) == 2  # currently fails: head(0) returns []
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_http_ingest.py::test_lsm_query_limit_zero_returns_all_rows -v
```
Expected: `FAILED` — `assert len(rows) == 2` fails (actual: 0).

- [ ] **Step 3: Fix `_merge_to_rows` in `lsm_store.py`**

Replace the body of `_merge_to_rows` (lines 31–41):

```python
def _merge_to_rows(frames: tuple[pl.DataFrame, ...],
                   key_columns: list[str],
                   limit: int | None) -> tuple[list[dict], int]:
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit:                                   # None and 0 both mean "no limit"
        live = live.head(limit)
    return live.drop([ORDER_COLUMN, "op"]).to_dicts(), total
```

- [ ] **Step 4: Run tests to verify both fixes pass**

```bash
pytest tests/test_http_ingest.py::test_lsm_query_limit_zero_returns_all_rows tests/test_flight_cache.py -v
```
Expected: all PASS. The `drop()` approach returns the same `id/name/value` columns as before for the existing schema, so `test_flight_cache.py` tests are unaffected.

- [ ] **Step 5: Commit**

```bash
git add persistence/stream_store/lsm_store.py tests/test_http_ingest.py
git commit -m "fix: drop internal LSM columns dynamically; treat limit=0 as no-limit"
```

---

## Task 5: Add `core/retry.py` and apply to Postgres and Redis startup

**Files:**
- Create: `core/retry.py`
- Modify: `persistence/transaction_store/postgres/postgres_client.py`
- Modify: `persistence/cache_store/redis/redis_client.py`
- Create: `tests/test_retry.py`

Postgres pool creation (`asyncpg.create_pool`) fails immediately on transient unavailability with no retry. Redis is not even connected at startup (no ping). Both violate the architectural requirement: *disconnects should be retried with randomised exponential backoff; after N failures the application should shut down.*

The retry utility lives in `core/` alongside the container, keeping infrastructure concerns together.

- [ ] **Step 1: Write failing tests for the retry utility**

Create `tests/test_retry.py`:

```python
import asyncio
import pytest
from core.retry import connect_with_backoff


async def test_succeeds_on_first_attempt():
    calls = []

    async def connect():
        calls.append(1)
        return "ok"

    result = await connect_with_backoff(connect, label="test", base_delay=0.001)
    assert result == "ok"
    assert len(calls) == 1


async def test_retries_then_succeeds():
    calls = []

    async def connect():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("not yet")
        return "ok"

    result = await connect_with_backoff(
        connect, label="test", max_attempts=5, base_delay=0.001
    )
    assert result == "ok"
    assert len(calls) == 3


async def test_raises_after_max_attempts():
    async def connect():
        raise ConnectionError("always fails")

    with pytest.raises(ConnectionError, match="always fails"):
        await connect_with_backoff(
            connect, label="test", max_attempts=3, base_delay=0.001
        )


async def test_jitter_makes_delays_non_deterministic():
    """Two runs should not produce identical sleep durations (jitter is active)."""
    import unittest.mock as mock

    delays: list[float] = []
    original_sleep = asyncio.sleep

    async def capture_sleep(n: float):
        delays.append(n)
        # Don't actually sleep in tests
        return

    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        if calls < 4:
            raise ConnectionError("not yet")
        return "ok"

    with mock.patch("asyncio.sleep", capture_sleep):
        await connect_with_backoff(
            connect, label="test", max_attempts=5, base_delay=1.0
        )

    assert len(delays) == 3
    # Each delay should be in the range (base * 2^(attempt-1), base * 2^(attempt-1) * 1.25)
    # Just assert they're positive and increasing
    assert all(d > 0 for d in delays)
    assert delays[1] > delays[0]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_retry.py -v
```
Expected: `ERROR` — `ModuleNotFoundError: No module named 'core.retry'`.

- [ ] **Step 3: Create `core/retry.py`**

```python
import asyncio
import logging
import random
from collections.abc import Callable, Coroutine
from typing import TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


async def connect_with_backoff(
    connect: Callable[[], Coroutine[None, None, T]],
    *,
    label: str,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> T:
    """Call connect() with randomised exponential backoff.

    On each failure, waits base_delay * 2^(attempt-1) seconds plus up to 25%
    random jitter. After max_attempts consecutive failures the final exception
    propagates, aborting the lifespan and exiting the process.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await connect()
        except Exception as exc:
            if attempt == max_attempts:
                log.error("%s: all %d connection attempts failed", label, max_attempts)
                raise
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            jitter = delay * 0.25 * random.random()
            log.warning(
                "%s: attempt %d/%d failed – retrying in %.1fs: %s",
                label, attempt, max_attempts, delay + jitter, exc,
            )
            await asyncio.sleep(delay + jitter)
```

- [ ] **Step 4: Run retry unit tests to verify they pass**

```bash
pytest tests/test_retry.py -v
```
Expected: 4 PASSED.

- [ ] **Step 5: Apply retry + ping to `redis_client.py`**

Replace the full file content of `persistence/cache_store/redis/redis_client.py`:

```python
import redis.asyncio as aioredis

from core.retry import connect_with_backoff
from settings import Settings


class RedisClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    async def __aenter__(self) -> aioredis.Redis:
        async def _connect() -> aioredis.Redis:
            client = aioredis.Redis.from_url(self._settings.redis_url)
            await client.ping()          # smoke-test: raises if Redis is unreachable
            return client

        self._client = await connect_with_backoff(_connect, label="Redis")
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
```

- [ ] **Step 6: Apply retry to `postgres_client.py`**

Replace the full file content of `persistence/transaction_store/postgres/postgres_client.py`:

```python
import asyncpg

from core.retry import connect_with_backoff
from settings import Settings


class PostgresClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self) -> asyncpg.Pool:
        self._pool = await connect_with_backoff(
            lambda: asyncpg.create_pool(
                self._settings.postgres_url,
                min_size=self._settings.postgres_pool_min_size,
                max_size=self._settings.postgres_pool_max_size,
            ),
            label="Postgres",
        )
        return self._pool

    async def __aexit__(self, *_: object) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
```

- [ ] **Step 7: Run config and health tests to confirm no regression**

```bash
pytest tests/test_config.py tests/test_health.py -v
```
Expected: all PASS (these tests start a real Postgres/Redis via testcontainers, so they exercise the new retry path — first attempt succeeds and no retry fires).

- [ ] **Step 8: Commit**

```bash
git add core/retry.py persistence/cache_store/redis/redis_client.py persistence/transaction_store/postgres/postgres_client.py tests/test_retry.py
git commit -m "feat: add connect_with_backoff retry utility; apply to Postgres and Redis startup"
```

---

## Task 6: Fix `stream_ingest.py` — retry, SIGTERM on repeated failure, join timeout

**Files:**
- Modify: `services/stream_ingest.py`

Three bugs:
1. `_ingest_loop` exits silently when `batches()` raises. No retry, no health signal, app continues serving stale data.
2. `stop()` calls `thread.join()` with no timeout. A blocked SDK call parks shutdown indefinitely.
3. `start()`/`stop()` public methods invite callers to bypass the context manager lifecycle.

Fix: retry `batches()` with randomised exponential backoff in the ingest thread; send SIGTERM after `_INGEST_MAX_FAILURES` consecutive failures; cap `join()` at `_JOIN_TIMEOUT_SECONDS`; remove the public `start()`/`stop()` methods (inline into `__aenter__`/`__aexit__`).

Note: the retry calls `batches()` again on the same consumer instance. The `FlightBatchConsumer` re-uses its existing `FlightClient` — if the gRPC connection is broken, the second `do_get()` call will fail immediately and exhaust retries, triggering shutdown. This is the correct behaviour for the template: a lost transport means the process restarts and reconnects cleanly. Production code would add a `reconnect()` method to each consumer.

- [ ] **Step 1: Verify the existing ingest tests pass (baseline)**

```bash
pytest tests/test_flight_cache.py tests/test_http_ingest.py -v
```
Expected: all PASS. Record this as the baseline to check after the fix.

- [ ] **Step 2: Replace `services/stream_ingest.py`**

```python
import asyncio
import logging
import os
import random
import signal
import threading
import time

import pyarrow as pa

from ingestion.base import BatchConsumer
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)

_INGEST_BASE_DELAY = 1.0        # seconds before first retry
_INGEST_MAX_DELAY = 60.0        # seconds — retry cap
_INGEST_MAX_FAILURES = 5        # consecutive batches() failures before SIGTERM
_JOIN_TIMEOUT = 10.0            # seconds to wait for ingest thread on shutdown


class StreamIngestService:
    def __init__(self, consumer: BatchConsumer, store: LSMStore) -> None:
        self._consumer = consumer
        self._store = store
        self._thread: threading.Thread | None = None

    async def __aenter__(self) -> "StreamIngestService":
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT)
            if self._thread.is_alive():
                log.error(
                    "ingest thread did not stop within %.0fs; abandoning",
                    _JOIN_TIMEOUT,
                )
            self._thread = None

    def _ingest_loop(self) -> None:
        consecutive_failures = 0
        delay = _INGEST_BASE_DELAY
        while True:
            try:
                for batch in self._consumer.batches():
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    try:
                        self._store.ingest(batch)
                    except Exception:
                        log.exception("ingest failed; skipping batch")
                return  # batches() returned cleanly — consumer was closed
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "consumer batches() failed (failure %d/%d)",
                    consecutive_failures,
                    _INGEST_MAX_FAILURES,
                )
                if consecutive_failures >= _INGEST_MAX_FAILURES:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        consecutive_failures,
                    )
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
                jitter = delay * 0.25 * random.random()
                time.sleep(delay + jitter)
                delay = min(delay * 2, _INGEST_MAX_DELAY)

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._store.ingest, batch)
```

- [ ] **Step 3: Run ingest and cache tests to verify no regression**

```bash
pytest tests/test_flight_cache.py tests/test_http_ingest.py -v
```
Expected: all PASS. The happy path (batches stream correctly) is unchanged; the new retry only fires when `batches()` raises.

- [ ] **Step 4: Commit**

```bash
git add services/stream_ingest.py
git commit -m "feat: add retry/backoff and SIGTERM shutdown to ingest thread; cap join() timeout"
```

---

## Task 7: Fix `tests/test_mcp.py` — correct JSON-RPC method and add tool result assertions

**Files:**
- Modify: `tests/test_mcp.py`

The tool invocation loop sends `"method": "get_health_status"` (the tool name) instead of `"method": "tools/call"` with `"params": {"name": "get_health_status"}`. The server returns a method-not-found error with HTTP 200, making both assertions (`status_code == 200` and `content != None`) pass trivially. No tool is ever executed.

Also adds: explicit test that `get_health_status` returns the expected status string (the test app uses `status="testing"`).

- [ ] **Step 1: Run the existing MCP test to confirm it passes (demonstrating the false positive)**

```bash
pytest tests/test_mcp.py -v -s
```
Expected: PASS — but the tool is never actually called.

- [ ] **Step 2: Replace `tests/test_mcp.py`**

```python
import json

import httpx


def parse_mcp_response(response: httpx.Response):
    ct = response.headers.get("content-type", "")
    if "text/event-stream" in ct:
        r = None
        for line in response.text.splitlines():
            if line.startswith("data:"):
                r = json.loads(line[5:].strip())
    else:
        r = response.json()
    return r


async def test_mcp(test_client):
    url = "/mcp/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # ── Initialize session ────────────────────────────────────────────────────
    msg_id = 1
    response = await test_client.post(url, headers=headers, json={
        "jsonrpc": "2.0",
        "id": msg_id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"},
        },
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert r is not None

    mcp_session_id = response.headers.get("mcp-session-id")
    session_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "mcp-session-id": mcp_session_id,
    }

    # ── List tools ────────────────────────────────────────────────────────────
    msg_id += 1
    response = await test_client.post(url, headers=session_headers, json={
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": msg_id,
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    tool_names = {tool["name"] for tool in r["result"]["tools"]}
    assert "get_health_status" in tool_names

    # ── Call get_health_status and assert on result content ───────────────────
    # The test app is started with status="testing" (see conftest.py Settings).
    msg_id += 1
    response = await test_client.post(url, headers=session_headers, json={
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": msg_id,
        "params": {"name": "get_health_status", "arguments": {}},
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert "result" in r, f"Expected result, got: {r}"
    assert r["result"]["isError"] is False
    content = r["result"]["content"]
    assert isinstance(content, list) and len(content) > 0
    assert "testing" in content[0]["text"]

    # ── Call all other tools with the correct JSON-RPC method ─────────────────
    for tool_name in tool_names - {"get_health_status"}:
        msg_id += 1
        response = await test_client.post(url, headers=session_headers, json={
            "jsonrpc": "2.0",
            "method": "tools/call",
            "id": msg_id,
            "params": {"name": tool_name, "arguments": {}},
        })
        assert response.status_code == 200
        r = parse_mcp_response(response)
        assert "result" in r, f"Tool {tool_name!r} returned an error: {r}"
```

- [ ] **Step 3: Run the MCP test to confirm it now exercises real tool calls**

```bash
pytest tests/test_mcp.py -v -s
```
Expected: PASS — the test now calls `tools/call`, the server executes `get_health_status`, and the assertion `"testing" in content[0]["text"]` passes.

- [ ] **Step 4: Commit**

```bash
git add tests/test_mcp.py
git commit -m "fix: correct MCP tool invocation to use tools/call; assert on get_health_status result"
```

---

## Task 8: Fix `tests/test_config.py` — remove order dependency

**Files:**
- Modify: `tests/test_config.py`

`test_get_config_returns_empty_list` asserts the table is empty. It uses the session-scoped `test_client`, which shares one Postgres container across the whole session. If another test writes a config row first, this assertion fails. `test_post_config_creates_entry` and `test_post_config_upserts_on_duplicate_key` also share the key `"env"`, making them dependent on each other's side-effects.

Fix: each test uses a unique key so tests are fully independent of execution order. Replace the empty-list assertion with a type-safe list check.

- [ ] **Step 1: Replace `tests/test_config.py`**

```python
from httpx import AsyncClient

from persistence.transaction_store.postgres.postgres_client import PostgresClient
from settings import Settings


async def test_postgres_client_aexit_without_pool_is_noop():
    """__aexit__ must not raise when called on an instance that never entered."""
    client = PostgresClient(Settings())
    await client.__aexit__(None, None, None)


async def test_get_config_returns_list(test_client: AsyncClient):
    """Config endpoint always returns a JSON list (may not be empty)."""
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


async def test_post_config_creates_entry(test_client: AsyncClient):
    key = "test_creates_key"
    response = await test_client.post("/config/", json={"key": key, "value": "staging"})
    assert response.status_code == 201
    assert response.json() == {"key": key, "value": "staging"}


async def test_post_config_upserts_on_duplicate_key(test_client: AsyncClient):
    key = "test_upsert_key"
    await test_client.post("/config/", json={"key": key, "value": "staging"})
    response = await test_client.post("/config/", json={"key": key, "value": "production"})
    assert response.status_code == 201
    assert response.json() == {"key": key, "value": "production"}


async def test_get_config_returns_all_entries(test_client: AsyncClient):
    key = "test_list_key"
    await test_client.post("/config/", json={"key": key, "value": "value1"})
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": key, "value": "value1"} in response.json()
```

- [ ] **Step 2: Run config tests to confirm all pass**

```bash
pytest tests/test_config.py -v
```
Expected: 5 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/test_config.py
git commit -m "fix: remove test_config order dependency; each test uses a unique key"
```

---

## Task 9: Fix `tests/test_http_ingest.py` — empty-state test and newest_wins value poll

**Files:**
- Modify: `tests/test_http_ingest.py`

Two issues:
1. Missing test: the LSM store should be empty before any HTTP ingest POST. The `test_client_http` fixture uses `empty_flight_server` (zero batches), so the store starts empty. A test asserting this documents the zero-state and catches regressions where startup somehow pre-populates the store.
2. `_poll_for_id` returns the first time `id=100` appears, regardless of its value. `test_post_ingest_newest_wins` posts `v2` but `_poll_for_id` can return immediately with a stale `v1` row from the previous test, causing an intermittent false failure. Fix: replace `_poll_for_id` with `_poll_for_value` that waits for a specific `(id, value)` pair.

- [ ] **Step 1: Run existing http_ingest tests to confirm baseline**

```bash
pytest tests/test_http_ingest.py -v
```
Expected: all PASS.

- [ ] **Step 2: Replace `tests/test_http_ingest.py`**

```python
import asyncio
import threading
import time

import pyarrow as pa
import pyarrow.flight as pa_flight
import pyarrow.ipc as pa_ipc
import pytest
from httpx import AsyncClient

from settings import Settings
from tests.app_client import lifespan_test_client
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

    # Isolated app: own container, own FastMCP, own lifespan — safe to run
    # in the same process as the session-scoped flight test_client.
    async with lifespan_test_client(http_settings) as client:
        yield client


async def _poll_for_value(
    client: AsyncClient, id_: int, expected_value: str, timeout: float = 10.0
) -> dict:
    """Poll /data/cache until id_ appears with the expected value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        rows_by_id = {r["id"]: r for r in body["rows"]}
        if id_ in rows_by_id and rows_by_id[id_]["value"] == expected_value:
            return rows_by_id[id_]
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"id={id_} with value={expected_value!r} never appeared in cache within {timeout}s"
    )


# ── LSM zero-state ────────────────────────────────────────────────────────────

async def test_lsm_cache_empty_before_ingest(test_client_http: AsyncClient):
    """The LSM store must be empty before any HTTP ingest POST is sent."""
    body = (await test_client_http.get("/data/cache?limit=100")).json()
    assert body["total"] == 0
    assert body["rows"] == []


# ── HTTP ingest ───────────────────────────────────────────────────────────────

async def test_post_ingest_upsert_appears_in_cache(test_client_http: AsyncClient):
    batch = make_batch([(100, "http", "v1", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 100, "v1")
    assert row["value"] == "v1"


async def test_post_ingest_newest_wins(test_client_http: AsyncClient):
    batch_v2 = make_batch([(100, "http", "v2", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch_v2),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    # Wait specifically for v2 — not just any value for id=100
    row = await _poll_for_value(test_client_http, 100, "v2")
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

- [ ] **Step 3: Run all http_ingest tests**

```bash
pytest tests/test_http_ingest.py -v
```
Expected: 6 PASSED (including the new empty-state test as the first to run).

- [ ] **Step 4: Commit**

```bash
git add tests/test_http_ingest.py
git commit -m "test: add LSM empty-state assertion; fix newest_wins to wait for specific value"
```

---

## Task 10: Full regression run

**Files:** none changed — verification only.

- [ ] **Step 1: Run the complete test suite**

```bash
pytest -v --tb=short
```
Expected: all tests PASS. If any test fails, do not proceed — investigate and fix before closing the plan.

- [ ] **Step 2: Final commit (if any stray changes exist)**

```bash
git status
```
If clean, no commit needed. If there are untracked or modified files from investigation, commit with an appropriate message.

---

## Self-Review

### Spec coverage

| Finding | Task |
|---------|------|
| #1 MCP false-positive tests | Task 7 |
| #2 Ingest thread silent death | Task 6 |
| #3 lsm_store hardcoded columns | Task 4 |
| #4 Redis not smoke-tested | Task 5 |
| #5 No retry on Postgres/Redis startup | Task 5 |
| #6 docker-compose flight dependency | Task 2 |
| #7 .env.example wrong project | Task 1 |
| #8 limit=0 silent wrong result | Task 4 |
| #9 join() no timeout | Task 6 |
| #10 module-level app at import time | Task 3 |
| User request: MCP health_status call | Task 7 |
| User request: LSM empty-state test | Task 9 |

All 12 requirements covered. ✓

### Placeholder scan

No "TBD", "TODO", "implement later", or "similar to Task N" placeholders found. ✓

### Type consistency

- `connect_with_backoff` defined in Task 5, imported in Tasks 5 (redis/postgres). Signature consistent throughout. ✓
- `_poll_for_value` defined and used only in Task 9. ✓
- `_ingest_loop` in Task 6 references `_INGEST_BASE_DELAY`, `_INGEST_MAX_DELAY`, `_INGEST_MAX_FAILURES`, `_JOIN_TIMEOUT` — all defined in the same file replacement. ✓
- `drop([ORDER_COLUMN, "op"])` in Task 4 — `ORDER_COLUMN = "seqno"` is defined at line 6 of the same file. ✓
