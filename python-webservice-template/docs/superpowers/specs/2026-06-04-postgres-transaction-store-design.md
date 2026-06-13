# Postgres Transaction Store Design

**Date:** 2026-06-04  
**Status:** Approved

## Context

The application currently has no persistent storage — service state lives in memory or flat files. This design adds a Postgres-backed transaction store to provide durable key-value configuration storage, illustrated via `POST /config` and `GET /config` endpoints.

The design uses naming that anticipates future parallel stores: an analytic store (Clickhouse) and a distributed cache (Redis). The `db/transaction_store/` folder owns transactional data backed by Postgres today; each future store follows the same role-folder / technology-subfolder pattern without touching existing code.

---

## Folder & File Structure

```
db/
  transaction_store/
    models/
      __init__.py
      config.py               # Configuration ORM model
    postgres/
      __init__.py
      postgres_engine.py      # async engine, session factory
      postgres_base.py        # SQLAlchemy DeclarativeBase

alembic/
  env.py                      # async-aware Alembic environment
  script.py.mako
  versions/
    0001_create_configuration_table.py
alembic.ini

services/
  config.py                   # ConfigService

schemas/
  config.py                   # ConfigSetRequest, ConfigEntry

routers/
  config.py                   # POST /config, GET /config

.env                          # local credentials (gitignored)
.env.example                  # committed template of required vars

CLAUDE.md                     # updated with DB investigation instructions
```

Future stores slot in alongside `transaction_store/` without modifying it:

```
db/
  transaction_store/          # Postgres (this design)
  analytic_store/             # Clickhouse (future)
    clickhouse/
  cache/                      # Redis (future)
    redis/
```

---

## Credentials & Environment Configuration

A single `DATABASE_URL` env var is used across all environments. No code changes between environments — only the value of the env var differs.

**`core/settings.py`** gains one field:
```python
database_url: str = "postgresql+asyncpg://user:password@localhost:5432/appdb"
```

**`.env`** (gitignored, local dev only):
```
DATABASE_URL=postgresql+asyncpg://appuser:apppassword@localhost:5432/appdb
```

**`.env.example`** (committed):
```
DATABASE_URL=postgresql+asyncpg://<user>:<password>@<host>:<port>/<dbname>
```

CI/CD and UAT/prod inject `DATABASE_URL` as a runtime environment variable. Pydantic `BaseSettings` reads it transparently.

**`docker-compose.yml`** (project root, local dev, defines the Postgres container):
```yaml
services:
  db:
    image: postgres:18
    environment:
      POSTGRES_USER: appuser
      POSTGRES_PASSWORD: apppassword
      POSTGRES_DB: appdb
    ports:
      - "5432:5432"
```

---

## DB Infrastructure

### `db/transaction_store/postgres/postgres_engine.py`

Creates the single async engine and session factory for the process lifetime.

```python
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,   # silently recycles stale connections
    pool_size=5,
    max_overflow=10,
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
```

`pool_pre_ping=True` ensures connections from the pool are validated before use. `expire_on_commit=False` prevents SQLAlchemy from expiring ORM attributes after a commit, which would trigger lazy-load errors in async contexts.

### `db/transaction_store/postgres/postgres_base.py`

```python
class PostgresBase(DeclarativeBase):
    pass
```

All transaction store ORM models inherit from `PostgresBase`. Alembic's `env.py` imports `PostgresBase.metadata` to detect schema changes.

### `core/dependencies.py` additions

```python
async def get_transaction_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
        await session.commit()

TransactionSessionDep = Annotated[AsyncSession, Depends(get_transaction_session)]
```

Services and routers depend on `TransactionSessionDep`. Nothing outside `db/transaction_store/postgres/` imports the engine or session factory directly.

---

## ORM Model

### `db/transaction_store/models/config.py`

```python
class Configuration(PostgresBase):
    __tablename__ = "configuration"

    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str] = mapped_column(nullable=False)
```

`key` is the primary key — enforces uniqueness and makes upsert via `session.merge()` unambiguous.

---

## Alembic Migrations

Alembic manages all schema changes. No `create_all()` calls in application code.

**`alembic.ini`** — points `sqlalchemy.url` at `%(DATABASE_URL)s` (reads from environment at migration time).

**`alembic/env.py`** — configured for async:
- Imports `PostgresBase.metadata` as `target_metadata`
- Uses `run_async_migrations()` with `AsyncEngine`
- Reads `DATABASE_URL` from environment

**First migration** — `0001_create_configuration_table.py`:
```python
def upgrade():
    op.create_table(
        "configuration",
        sa.Column("key", sa.String(), primary_key=True),
        sa.Column("value", sa.String(), nullable=False),
    )

def downgrade():
    op.drop_table("configuration")
```

Migrations run with `alembic upgrade head` before starting the application. In CI this is a pipeline step before the test run.

---

## Eager Startup / Fail-Fast

In `main.py`'s `lifespan` context manager, a `SELECT 1` is issued before the application begins serving traffic. If Postgres is unreachable the app raises immediately rather than serving requests that will fail.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))
    async with mcp.session_manager.run():
        yield
```

---

## Schema

### `schemas/config.py`

```python
class ConfigSetRequest(BaseModel):
    key: str
    value: str

class ConfigEntry(BaseModel):
    key: str
    value: str
    model_config = ConfigDict(from_attributes=True)
```

`from_attributes=True` allows `ConfigEntry.model_validate(orm_row)` — no manual field mapping.

---

## Service

### `services/config.py`

`ConfigService` is instantiated per-request with a session injected by the router. It has no knowledge of Postgres specifically — it uses SQLAlchemy's `AsyncSession` interface.

```python
class ConfigService:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_all(self) -> list[ConfigEntry]:
        result = await self._session.execute(select(Configuration))
        return [ConfigEntry.model_validate(row) for row in result.scalars()]

    async def set(self, key: str, value: str) -> ConfigEntry:
        entry = await self._session.merge(Configuration(key=key, value=value))
        return ConfigEntry.model_validate(entry)
```

`session.merge()` performs an upsert: inserts if the key is new, updates if it already exists.

---

## Router

### `routers/config.py`

```python
TAG = "Configuration"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for managing key-value configuration settings",
}

router = APIRouter(tags=[TAG])

@router.post("/", response_model=ConfigEntry, status_code=201)
async def set_config(body: ConfigSetRequest, session: TransactionSessionDep):
    return await ConfigService(session).set(body.key, body.value)

@router.get("/", response_model=list[ConfigEntry])
async def get_config(session: TransactionSessionDep):
    return await ConfigService(session).get_all()
```

Registered in `main.py`:
```python
from routers import health, data, config
app.include_router(config.router, prefix="/config")
# openapi_tags updated to include config.TAG_METADATA
```

---

## Testing

### Approach

Tests use `testcontainers-python` (`testcontainers[postgresql]`) to start a real Postgres Docker container for the test session. This container is completely isolated from any developer-running container — it binds to a random free port and is destroyed when the session ends.

The full stack is exercised: HTTP → router → `ConfigService` → SQLAlchemy → asyncpg → Postgres. No mocking of the DB layer.

### `tests/conftest.py` additions

```python
@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:18") as pg:
        yield pg

@pytest.fixture(scope="session")
async def transaction_engine(postgres_container):
    url = postgres_container.get_connection_url(driver="asyncpg")
    engine = create_async_engine(url, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(PostgresBase.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture(scope="session")
async def transaction_session(transaction_engine):
    async with AsyncSession(transaction_engine) as session:
        yield session

@pytest.fixture(scope="session")
async def test_client(transaction_session, test_settings, override_health_service, override_data_service):
    app.dependency_overrides[get_transaction_session] = lambda: transaction_session
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
```

The existing `test_client` fixture in `conftest.py` is updated in-place (not duplicated) to add the `get_transaction_session` override alongside the existing health and data service overrides.

Seeding data in a test is done directly via `transaction_session`. Use `flush()` (not `commit()`) to make data visible within the shared session without permanently committing it:
```python
async def test_get_config_returns_seeded_data(transaction_session, test_client):
    transaction_session.add(Configuration(key="env", value="test"))
    await transaction_session.flush()
    response = await test_client.get("/config/")
    assert response.status_code == 200
    assert {"key": "env", "value": "test"} in response.json()
```

**Test isolation:** Because `transaction_session` is session-scoped, data flushed by one test is visible to subsequent tests within the session. Tests that depend on a clean state (e.g. "returns empty list") must either run first or explicitly delete their seed data after the assertion. For this illustrative template, test ordering is acceptable. The production pattern for strict isolation is a function-scoped session with rollback after each test, which can be introduced when the test suite grows.

### `tests/test_config.py`

Covers:
- `GET /config` returns empty list when no entries exist (must run before seeding tests)
- `POST /config` creates a new entry (201, body matches)
- `POST /config` with an existing key updates the value (upsert)
- `GET /config` returns all previously created entries

### Sufficiency

Endpoint tests against a real containerised Postgres are sufficient for end-to-end coverage. The query, schema, connection, and serialisation are all exercised in a single test pass. Unit-testing `ConfigService` in isolation would add no meaningful coverage here.

---

## CLAUDE.md Instructions

The following section is added to `CLAUDE.md`:

```markdown
## Database Investigation

When investigating a database-related issue, always start a fresh Postgres container
via `testcontainers` (run `pytest` with the relevant test) rather than connecting to
any container a developer may have running. Never assume an existing container is safe
to query or modify. Do not reuse containers between investigations.
```

---

## Extensibility Notes

When adding a second store in future:

| Store | Role folder | Technology subfolder | Session/client type |
|---|---|---|---|
| Postgres | `db/transaction_store/` | `postgres/` | `AsyncSession` (SQLAlchemy) |
| Clickhouse | `db/analytic_store/` | `clickhouse/` | `AsyncClient` (clickhouse-connect) |
| Redis | `db/cache/` | `redis/` | `Redis` (redis-py async) |

Each store adds its own dependency getter in `core/dependencies.py`. Services declare only what they need — `ConfigService` depends on `TransactionSessionDep`, a future `AnalyticsService` would depend on its own session type.

---

## Verification

1. `docker compose up -d` — start local Postgres
2. `alembic upgrade head` — apply migrations
3. `uvicorn main:app --reload` — app starts; startup `SELECT 1` confirms DB connection
4. Visit `/docs` — `Configuration` tag appears with POST and GET endpoints
5. `POST /config` with `{"key": "env", "value": "prod"}` → 201
6. `GET /config` → `[{"key": "env", "value": "prod"}]`
7. `pytest tests/test_config.py` — all tests pass against isolated testcontainer
