# ClickHouse Client Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the module-level ClickHouse singleton with an async context manager class, reuse that class in tests, consolidate init SQL into `scripts/`, and seed test data from a committed Arrow IPC file.

**Architecture:** `ClickHouseClient(settings)` is an async context manager whose `__aenter__` creates and returns the raw `AsyncClient` and whose `__aexit__` closes it — no module-level global. `main.py` wraps its lifespan in `async with ClickHouseClient(settings) as ch_client:`. Tests instantiate the same class pointed at a testcontainer, read the CREATE TABLE DDL from `scripts/clickhouse-init.sql`, and seed via `client.insert_arrow()` from a committed Arrow IPC file. Docker Compose mounts the schema SQL and a separate seed SQL so the performance stack gets data without Python.

**Tech Stack:** Python `clickhouse-connect[async]`, `pyarrow`, pytest-asyncio, testcontainers

---

## File Structure

**Modified:**
- `persistence/analytics_store/clickhouse/clickhouse_client.py` — replace two functions + module global with `ClickHouseClient` async context manager class
- `main.py` — update lifespan to `async with ClickHouseClient(settings) as ch_client:`
- `tests/conftest.py` — use `ClickHouseClient`, read DDL from file, seed via Arrow
- `docker-compose.yml` — replace single clickhouse volume mount with two (schema + seed)
- `requirements.txt` — add `pyarrow`

**Created:**
- `scripts/clickhouse-init.sql` — DDL only (`CREATE TABLE IF NOT EXISTS default.items ...`)
- `performance/data/clickhouse-seed.sql` — DML only (`INSERT INTO default.items ...`)
- `tests/data/items.arrow` — Arrow IPC file with 3 seed rows (id, name, value)

**Deleted:**
- `performance/data/clickhouse-init.sql` — replaced by the two files above

---

### Task 1: ClickHouseClient async context manager class

**Files:**
- Modify: `persistence/analytics_store/clickhouse/clickhouse_client.py`
- Modify: `main.py`

- [ ] **Step 1: Replace `clickhouse_client.py` with the class**

Replace the entire file with:

```python
from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from core.settings import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        self._client = await clickhouse_connect.get_async_client(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            username=self._settings.clickhouse_user,
            password=self._settings.clickhouse_password,
            database=self._settings.clickhouse_database,
        )
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
```

- [ ] **Step 2: Update `main.py`**

Replace the import at line 10:
```python
# before
from persistence.analytics_store.clickhouse.clickhouse_client import create_client, close_client
# after
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
```

Replace the entire `lifespan` function (lines 19–39):
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with ClickHouseClient(settings) as ch_client:
        service_container.register_singleton(DataService, DataService(ch_client))
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        try:
            ok = await ch_client.ping()
            if not ok:
                raise RuntimeError("ClickHouse startup ping failed")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError("ClickHouse startup ping failed") from exc

        async with mcp.session_manager.run():
            yield
```

The `async with ClickHouseClient(settings) as ch_client:` block replaces the explicit `create_client` / `finally: close_client()` pattern. `__aexit__` is called automatically when the lifespan exits (shutdown), whether cleanly or via exception.

- [ ] **Step 3: Run tests**

```bash
pytest tests/ -v
```

Expected: 10 passed, 1 pre-existing error (`tests/test_mcp.py::test_mcp` — unrelated to this change).

- [ ] **Step 4: Commit**

```bash
git add persistence/analytics_store/clickhouse/clickhouse_client.py main.py
git commit -m "refactor: replace ClickHouse create/close functions with ClickHouseClient context manager"
```

---

### Task 2: Reuse ClickHouseClient in test fixture

**Files:**
- Modify: `tests/conftest.py`

- [ ] **Step 1: Update imports in `tests/conftest.py`**

Remove the line:
```python
import clickhouse_connect
```

Add this import (with other project imports):
```python
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
```

- [ ] **Step 2: Replace the `test_clickhouse_client` fixture**

Replace lines 51–64 (the `test_clickhouse_client` fixture) with:

```python
@pytest.fixture(scope="session")
async def test_clickhouse_client(clickhouse_container):
    http_port = int(clickhouse_container.get_exposed_port(8123))
    ch_settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=http_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database=clickhouse_container.dbname or "default",
    )
    async with ClickHouseClient(ch_settings) as client:
        await client.command(_CREATE_ITEMS)
        await client.insert("items", _SEED_ITEMS, column_names=["id", "name", "value"])
        yield client
```

The `_CREATE_ITEMS` and `_SEED_ITEMS` constants remain unchanged for now — they are removed in Tasks 3 and 4.

- [ ] **Step 3: Run tests**

```bash
pytest tests/ -v
```

Expected: 10 passed, 1 pre-existing error.

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py
git commit -m "refactor: use ClickHouseClient in test fixture instead of raw clickhouse_connect"
```

---

### Task 3: Consolidate SQL — DDL to `scripts/`, DML to `performance/data/`

**Files:**
- Create: `scripts/clickhouse-init.sql`
- Create: `performance/data/clickhouse-seed.sql`
- Modify: `docker-compose.yml`
- Modify: `tests/conftest.py`
- Delete: `performance/data/clickhouse-init.sql`

- [ ] **Step 1: Create `scripts/clickhouse-init.sql` (DDL only)**

```sql
CREATE TABLE IF NOT EXISTS default.items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;
```

- [ ] **Step 2: Create `performance/data/clickhouse-seed.sql` (DML only)**

```sql
INSERT INTO default.items VALUES (1, 'alpha', 'a'), (2, 'beta', 'b'), (3, 'gamma', 'c');
```

- [ ] **Step 3: Update `docker-compose.yml` volume mounts**

In the `clickhouse` service, replace:
```yaml
    volumes:
      - ./performance/data/clickhouse-init.sql:/docker-entrypoint-initdb.d/init.sql
```
with:
```yaml
    volumes:
      - ./scripts/clickhouse-init.sql:/docker-entrypoint-initdb.d/01-schema.sql
      - ./performance/data/clickhouse-seed.sql:/docker-entrypoint-initdb.d/02-seed.sql
```

ClickHouse runs init scripts in lexicographic order, so `01-schema.sql` runs before `02-seed.sql`.

- [ ] **Step 4: Update `tests/conftest.py`**

Add `from pathlib import Path` to the stdlib imports at the top.

Remove the `_CREATE_ITEMS` constant (lines 22–28):
```python
_CREATE_ITEMS = """
CREATE TABLE IF NOT EXISTS items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id
"""
```

In the `test_clickhouse_client` fixture, replace `await client.command(_CREATE_ITEMS)` with:
```python
_DDL = (Path(__file__).parent.parent / "scripts" / "clickhouse-init.sql").read_text()
await client.command(_DDL)
```

`Path(__file__).parent` is `tests/`, so `.parent.parent` is the project root.

- [ ] **Step 5: Delete the old combined init file**

```bash
git rm performance/data/clickhouse-init.sql
```

- [ ] **Step 6: Run tests**

```bash
pytest tests/ -v
```

Expected: 10 passed, 1 pre-existing error.

- [ ] **Step 7: Commit**

```bash
git add scripts/clickhouse-init.sql performance/data/clickhouse-seed.sql docker-compose.yml tests/conftest.py
git commit -m "refactor: split clickhouse init SQL into DDL (scripts/) and DML (performance/data/)"
```

---

### Task 4: Seed test data from Arrow IPC file

**Files:**
- Modify: `requirements.txt`
- Create: `tests/data/items.arrow`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Add `pyarrow` to `requirements.txt`**

In `requirements.txt`, add `pyarrow` under the `# Data` section:
```
# Data
polars
pyarrow
rapidfuzz
```

Install it:
```bash
pip install pyarrow
```

- [ ] **Step 2: Create `tests/data/items.arrow`**

Run this one-liner to generate the Arrow IPC file:

```bash
python -c "
import pyarrow as pa, pyarrow.ipc as pa_ipc, os
os.makedirs('tests/data', exist_ok=True)
t = pa.table({'id': pa.array([1,2,3], type=pa.uint64()), 'name': pa.array(['alpha','beta','gamma']), 'value': pa.array(['a','b','c'])})
with pa_ipc.new_file('tests/data/items.arrow', t.schema) as w: w.write_table(t)
print('Written', t.num_rows, 'rows to tests/data/items.arrow')
"
```

Expected output: `Written 3 rows to tests/data/items.arrow`

The schema matches the ClickHouse table: `id UInt64`, `name String`, `value String`.

- [ ] **Step 3: Update `tests/conftest.py`**

Add `import pyarrow.ipc as pa_ipc` to the imports at the top.

Remove the `_SEED_ITEMS` constant (line 30):
```python
_SEED_ITEMS = [[1, "alpha", "a"], [2, "beta", "b"], [3, "gamma", "c"]]
```

In the `test_clickhouse_client` fixture, replace:
```python
await client.insert("items", _SEED_ITEMS, column_names=["id", "name", "value"])
```
with:
```python
_arrow_path = Path(__file__).parent / "data" / "items.arrow"
with pa_ipc.open_file(_arrow_path) as reader:
    arrow_table = reader.read_all()
await client.insert_arrow("items", arrow_table)
```

`Path(__file__).parent` is `tests/`, so this resolves to `tests/data/items.arrow`.

- [ ] **Step 4: Run tests**

```bash
pytest tests/ -v
```

Expected: 10 passed, 1 pre-existing error.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt tests/data/items.arrow tests/conftest.py
git commit -m "feat: seed ClickHouse test data from Arrow IPC file"
```
