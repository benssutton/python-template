# Python Template

A Python FastAPI service with MCP endpoints and Rust extensions, ready for Claude.

This application is intended as an illustration and re-usable template of best practices when creating a REST-first webservice using FastAPI. See the following sections in this document for details of those best practices.

## Architecture
```
main.py                         FastAPI app entry point; lifespan manages DB connections
  core/                         DI container, dependency getters, Pydantic BaseSettings config
  routers/                      REST endpoints (health, data, config)
  schemas/                      Pydantic request/response models
  services/                     All business logic (health, data, config)
  mcp_routers/                  MCP tools, resources and prompts
  persistence/
    transaction_store/          Postgres via SQLAlchemy async + Alembic
      postgres/                 Engine, session factory, declarative base
      models/                   SQLAlchemy ORM models
    analytics_store/
      clickhouse/               ClickHouse via clickhouse-connect async client
  alembic/                      Schema migrations (async env.py)
  performance/                  k6 performance test scripts
    lib/                        Shared k6 check helpers and SLO threshold presets
    data/                       k6 test data (rows_params.json, clickhouse-seed.sql)
  scripts/                      Shared SQL DDL and utility scripts
  tests/                        Pytest integration tests
    data/                       Binary test fixtures (items.arrow)
```

## Stack
- FastAPI + Pydantic
- SQLAlchemy (async) + Alembic — Postgres transaction store
- clickhouse-connect[async] — ClickHouse analytics store
- polars / pyarrow — data shaping and Arrow IPC fixtures
- pytest + testcontainers — integration tests against real DB containers
- k6 — performance tests (smoke, load, stress)
- Docker Compose — full local stack (Postgres, ClickHouse, app)
- GitLab CI — pytest and k6 as quality gates

## Key Patterns

**Dependency Injection**
- Custom `Container` in `core/container.py` holds singletons; `core/dependencies.py` provides getter functions and `Annotated` type aliases for FastAPI routes.
- The FastAPI `app` object must never be imported outside of test fixtures.
- Singletons that depend on external connections (e.g. `DataService`) are registered in the lifespan *after* health checks pass, not at container initialisation time.

**Async**
- All I/O is async. Synchronous blocking calls (e.g. `Path.read_text()`) are acceptable outside of async context managers.

**Config**
- `core/settings.py` uses Pydantic `BaseSettings` — env vars override defaults, `.env` is auto-loaded.

**Persistence — Postgres**
- SQLAlchemy async engine and session factory live in `persistence/transaction_store/postgres/`.
- Sessions are injected via `TransactionSessionDep`; commit/rollback is managed in the dependency, not in services.
- Schema changes go through Alembic migrations in `alembic/versions/`.

**Persistence — ClickHouse**
- `ClickHouseClient` in `persistence/analytics_store/clickhouse/clickhouse_client.py` is an async context manager class. Use `async with ClickHouseClient(settings) as client:` — `__aenter__` returns the raw `AsyncClient`, `__aexit__` closes it. No module-level global state.
- In `main.py` the lifespan wraps startup in `async with ClickHouseClient(settings) as ch_client:` so the connection is guaranteed to close on shutdown whether cleanly or via exception.

**Routers**
- Routers implement minimal business logic and call service methods.
- Each router file exports `TAG` and `TAG_METADATA` constants (name + description). `main.py` assembles `openapi_tags` from these exports — tag metadata is co-located with the router that owns it, not in `Settings`.

**MCP**
- MCP tools, resources and prompts implement minimal logic and call service methods.

**Testing**
- Tests invoke REST endpoints via an async HTTPX test client.
- Application behaviour is overridden via FastAPI dependency injection (no monkeypatching).
- Each test session starts fresh testcontainer instances for Postgres and ClickHouse; containers are torn down at session end.
- ClickHouse schema is created from `scripts/clickhouse-init.sql` (single source of truth shared with docker-compose). Seed data is loaded from `tests/data/items.arrow` via `client.insert_arrow()`.

**Performance Tests**
- `performance/lib/checks.js` — shared k6 check helpers (`checkStatus200`, `checkDataCount`, `checkDataRows`).
- `performance/lib/thresholds.js` — named SLO presets (`STRICT_SLO`, `NORMAL_SLO`, `RELAXED_SLO`) spread into `options.thresholds`.
- Three scripts: `smoke.js` (1 VU/30 s, hard CI gate), `load.js` (ramping-vus + constant-vus, hard gate), `stress.js` (ramping-arrival-rate, soft gate).
- k6 is run via a `performance/Dockerfile` image built in CI — avoids docker:dind volume-mount issues.
- The docker-compose project is named `python-template` so the network is always `python-template_default`.

**SQL Management**
- `scripts/clickhouse-init.sql` — DDL only (CREATE TABLE). Used by both docker-compose and pytest.
- `performance/data/clickhouse-seed.sql` — DML only (INSERT). Used by docker-compose for performance test data.
- docker-compose mounts both as `01-schema.sql` and `02-seed.sql` so ClickHouse runs them in order.

## Database Investigation

When investigating a Postgres-related issue, always start a fresh container via `testcontainers` by running the relevant pytest test:

```bash
pytest tests/test_config.py -v -s
```

When investigating a ClickHouse-related issue, run:

```bash
pytest tests/test_data.py -v -s
```

Never connect to any container a developer may have running locally. Never assume an existing container is safe to query or modify. Do not reuse containers between investigations — each pytest session starts a clean, isolated container that is destroyed when the session ends.
