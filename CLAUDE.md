# Python Template

A Python FastAPI service with MCP endpoints and Rust extensions, ready for Claude.

This application is intended as an illustration and re-usable template of best practices when creating a REST-first webservice using FastAPI. See the following sections in this document for details of those best practices.

## Architecture
```
main.py                         FastAPI app entry point; lifespan manages DB connections
settings.py                     Pydantic BaseSettings config; env vars override defaults
  certs/                        Self-signed SSL certificates & certificate generation script
  core/                         DI container and dependency getters
  docs/                         Folder for Claude to store specs and plans.  Not used by the application.
  mcp_routers/                  MCP tools, resources and prompts
  persistence/
    analytics_store/
      clickhouse/               ClickHouse via clickhouse-connect async client
    cache_store/
      redis/                    Redis via redis-py async client
    stream_store/
      flight/                   Apache arrow flight client and LSM store of record batches.
    transaction_store/          
      postgres/                 Postgres via asyncpg connection pool

  routers/                      REST endpoints (health, data, config)
  schemas/                      Pydantic request/response models
  scripts/                      SQL DDL for both Postgres and ClickHouse (run at startup / via docker-compose)
  services/                     All business logic (health, data, config)
  tests/                        Pytest integration tests + k6 performance tests
    data/                       Binary test fixtures (items.arrow)
    example_server.py           Reusable dummy Flight server used by tests and docker-compose
    performance/                k6 performance test scripts
      lib/                      Shared k6 check helpers and SLO threshold presets
      data/                     k6 test data (rows_params.json, clickhouse-seed.sql)
```

## Stack
- FastAPI + Pydantic
- asyncpg — Postgres transaction store (direct, no ORM)
- clickhouse-connect[async] — ClickHouse analytics store
- polars / pyarrow — data shaping and Arrow IPC fixtures
- pytest + testcontainers — integration tests against real DB containers
- k6 — performance tests (smoke, load, stress)
- Docker Compose -- full local stack (Postgres, ClickHouse, app)
- GitLab CI -- pytest and k6 as quality gates

## Key Patterns

**Dependency Injection**
- Custom `Container` in `core/container.py` holds singletons; `core/dependencies.py` provides getter functions and `Annotated` type aliases for FastAPI routes.
- The FastAPI `app` object must never be imported outside of test fixtures.
- Singletons that depend on external connections (e.g. `DataService`) are registered in the lifespan *after* health checks pass, not at container initialisation time.

**Async**
- All I/O is async. Synchronous blocking calls (e.g. `Path.read_text()`) are acceptable outside of async context managers.

**Config**
- `settings.py` uses Pydantic `BaseSettings` -- env vars override defaults, `.env` is auto-loaded.

**Persistence -- Postgres**
- `PostgresClient` in `persistence/transaction_store/postgres/postgres_client.py` mirrors `ClickHouseClient`: async context manager whose `__aenter__` returns a live `asyncpg.Pool`, `__aexit__` closes it.
- In `main.py` the lifespan wraps startup in `async with PostgresClient(settings) as pg_pool:`. `ConfigService` is registered as a singleton holding the pool.
- Schema is in `scripts/postgres-init.sql` (DDL only, `CREATE TABLE IF NOT EXISTS`). The lifespan runs it at startup -- idempotent, no migration tooling needed.
- Services acquire connections per-operation via `async with pool.acquire() as conn:`. No session injection into routes.

**Persistence -- ClickHouse**
- `ClickHouseClient` in `persistence/analytics_store/clickhouse/clickhouse_client.py` is an async context manager class. Use `async with ClickHouseClient(settings) as client:` -- `__aenter__` returns the raw `AsyncClient`, `__aexit__` closes it. No module-level global state.
- In `main.py` the lifespan wraps startup in `async with ClickHouseClient(settings) as ch_client:` so the connection is guaranteed to close on shutdown whether cleanly or via exception.

**Routers**
- Routers implement minimal business logic and call service methods.
- Each router file exports `TAG` and `TAG_METADATA` constants (name + description). `main.py` assembles `openapi_tags` from these exports -- tag metadata is co-located with the router that owns it, not in `Settings`.

**MCP**
- MCP tools, resources and prompts implement minimal logic and call service methods.

**Testing**
- Tests invoke REST endpoints via an async HTTPX test client.
- Application behaviour is overridden via FastAPI dependency injection (no monkeypatching).
- Each test session starts fresh testcontainer instances for Postgres and ClickHouse; containers are torn down at session end.
- Postgres schema is created from `scripts/postgres-init.sql` via the `postgres_pool` fixture.
- ClickHouse schema is created from `scripts/clickhouse-init.sql` (single source of truth shared with docker-compose). Seed data is loaded from `tests/data/items.arrow` via `client.insert_arrow()`.

**Performance Tests**
- `tests/performance/lib/checks.js` -- shared k6 check helpers (`checkStatus200`, `checkDataCount`, `checkDataRows`).
- `tests/performance/lib/thresholds.js` -- named SLO presets (`STRICT_SLO`, `NORMAL_SLO`, `RELAXED_SLO`) spread into `options.thresholds`.
- Three scripts: `smoke.js` (1 VU/30 s, hard CI gate), `load.js` (ramping-vus + constant-vus, hard gate), `stress.js` (ramping-arrival-rate, soft gate).
- k6 is run via a `tests/performance/Dockerfile` image built in CI -- avoids docker:dind volume-mount issues.
- The docker-compose project is named `python-template` so the network is always `python-template_default`.

**SQL Management**
- `scripts/postgres-init.sql` -- DDL only (CREATE TABLE IF NOT EXISTS). Run by the app lifespan at startup and by pytest via `postgres_pool` fixture.
- `scripts/clickhouse-init.sql` -- DDL only (CREATE TABLE). Used by both docker-compose and pytest.
- `tests/performance/data/clickhouse-seed.sql` -- DML only (INSERT). Used by docker-compose for performance test data.
- docker-compose mounts the ClickHouse scripts as `01-schema.sql` and `02-seed.sql` so ClickHouse runs them in order.

## Database Investigation

When investigating a Postgres-related issue, always start a fresh container via `testcontainers` by running the relevant pytest test:

```bash
pytest tests/test_config.py -v -s
```

When investigating a ClickHouse-related issue, run:

```bash
pytest tests/test_data.py -v -s
```

Never connect to any container a developer may have running locally. Never assume an existing container is safe to query or modify. Do not reuse containers between investigations -- each pytest session starts a clean, isolated container that is destroyed when the session ends.
