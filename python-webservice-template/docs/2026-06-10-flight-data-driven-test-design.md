# Flight Data-Driven Integration Test — Design (ON HOLD)

> Status: **design recorded, implementation deferred.** The Flight server/client
> approach needs more thinking before we build this (decision: 2026-06-10).
> Captured here so the design isn't lost.

## Goal

Replace the single hard-coded Flight integration scenario with a **data-driven**
suite: a comprehensive on-disk dataset drives multiple stream scenarios through
the real FastAPI app, asserting on the final LSM/stream-store state exposed at
`GET /data/cache`. Adding a scenario should require **no Python changes** — just
drop a new directory of fixtures.

## Current state (the problem)

- `tests/conftest.py` `example_flight_server` is **session-scoped** with a
  hard-coded 3-batch Python script. The whole session can exercise exactly one
  Flight scenario.
- `tests/test_flight_cache.py` asserts against those literal values.
- The app consumes the Flight stream **once** at lifespan startup into an
  in-process `LSMStore`, so each scenario needs its own server **and** its own
  app-lifespan run.

## Proposed design

### Fixture layout (driver data on disk)

```
tests/test_data/flight_scenarios/
  01_newest_wins/   stream.arrows   manifest.json
  02_tombstone/     stream.arrows   manifest.json
  03_compaction/    stream.arrows   manifest.json
```

- `stream.arrows` — Arrow IPC **stream** format (NOT file format). Stream format
  preserves record-batch boundaries, which is essential: flush / compaction /
  tombstone behaviour depends on how rows are chunked across streamed batches.
  Generate per batch with `pa.ipc.new_stream(sink, schema).write_batch(b)`.
- `manifest.json` — expected end state + the LSM knobs the scenario needs:

```json
{
  "lsm_flush_rows": 2,
  "lsm_compaction_runs": 2,
  "expected_total": 2,
  "expected_rows": [
    {"id": 1, "name": "a", "value": "new"},
    {"id": 3, "name": "c", "value": "z"}
  ]
}
```

### Factory fixture (reuses session containers; server + lifespan per scenario)

```python
def _read_stream(path: Path) -> list[pa.RecordBatch]:
    with pa_ipc.open_stream(path) as reader:
        return list(reader)

@pytest.fixture
def flight_scenario_client(postgres_container, clickhouse_container,
                           test_clickhouse_client, redis_container):
    @asynccontextmanager
    async def _make(batches, flush_rows, compaction_runs):
        from main import app, create_lifespan
        location = flight.Location.for_grpc_tcp("localhost", 0)
        server = ExampleFlightServer(location, batches, interval=0.0, loop=False)
        threading.Thread(target=server.serve, daemon=True).start()
        try:
            settings = Settings(
                status="testing",
                # ...postgres_url / clickhouse_* / redis_url from session containers...
                flight_host="localhost", flight_port=server.port, flight_ticket="items",
                lsm_flush_rows=flush_rows, lsm_compaction_runs=compaction_runs,
            )
            async with _lifespan_running(app, create_lifespan(settings)):
                async with AsyncClient(transport=ASGITransport(app=app),
                                       base_url="http://localhost:8000") as client:
                    yield client
        finally:
            server.shutdown()
    return _make
```

### The test (the loop is automatic via parametrize)

```python
SCENARIOS = sorted((Path(__file__).parent / "test_data" / "flight_scenarios").iterdir())

@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda p: p.name)
async def test_flight_scenario(scenario, flight_scenario_client):
    manifest = json.loads((scenario / "manifest.json").read_text())
    batches = _read_stream(scenario / "stream.arrows")
    async with flight_scenario_client(batches,
                                      manifest["lsm_flush_rows"],
                                      manifest["lsm_compaction_runs"]) as client:
        body = await _poll_cache(client, expected_total=manifest["expected_total"])
    assert body["rows"] == manifest["expected_rows"]
```

## Prerequisite refactors

Factor two things out of `conftest.py` so the existing `test_client` fixture and
the new factory share them:

1. `_lifespan_running(app, lifespan)` — the anyio dedicated-task block currently
   inlined at `conftest.py` (mcp cancel-scope same-task requirement).
2. The "build `Settings` from the session containers" helper.

## Open questions / why it's on hold

- **Fidelity vs cost:** full-app-per-scenario re-runs the app lifespan each time.
  A lighter variant drives `FlightCacheService` + `LSMStore` directly (no ASGI
  app) — faster, but skips the `/data/cache` route. Recommendation when resumed:
  full-app for headline scenarios (newest-wins, tombstone, compaction) + keep the
  fast pure-function `test_flight_merge.py` for exhaustive merge edge cases.
- The shared module-level `app` / `service_container` singletons are re-used
  across scenarios. Sequential pytest execution makes this safe, but parallel
  (`pytest-xdist`) would clash — needs thought before adopting xdist.
- Decide how to generate/commit the `.arrows` fixtures (a small generator script
  vs. the existing `generate_test_data.ipynb` pattern).
