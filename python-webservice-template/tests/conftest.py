import threading
from pathlib import Path

import pytest
import pyarrow.flight as flight
import pyarrow.ipc as pa_ipc
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from settings import Settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from tests.app_client import lifespan_test_client
from tests.publishers.flight_server import ExampleFlightServer, make_batch

PG_IMAGE = "postgres:18"
CH_IMAGE = "clickhouse/clickhouse-server:latest"
REDIS_IMAGE = "redis/redis-stack-server:latest"


# ── ClickHouse fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def clickhouse_container():
    with ClickHouseContainer(CH_IMAGE, port=8123) as ch:
        yield ch


@pytest.fixture(scope="session")
async def test_clickhouse_client(clickhouse_container):
    http_port = int(clickhouse_container.get_exposed_port(8123))
    ch_settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=http_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
    )
    schema_sql = (Path(__file__).parent.parent / "scripts" / "clickhouse-init.sql").read_text()
    async with ClickHouseClient(ch_settings) as client:
        await client.command(schema_sql)
        with pa_ipc.open_file(Path(__file__).parent / "test_data" / "clickhouse_seed_data.ipc") as reader:
            arrow_table = reader.read_all()
        await client.insert_arrow("default.items", arrow_table)
        yield client


# ── Postgres fixtures ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(PG_IMAGE) as pg:
        yield pg


# ── Redis fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer(REDIS_IMAGE) as r:
        yield r


# ── Flight fixtures ────────────────────────────────────────────────────────
#
# Three batches designed to exercise all LSM paths with lsm_flush_rows=2,
# lsm_compaction_runs=2:
#
#   BATCH_1: 2 rows → flush threshold hit → run1 created
#   BATCH_2: 2 rows → flush → run2 created → compaction triggered (2 runs)
#             compaction merges run1+run2: id=1 gets v2 (higher seqno wins)
#   BATCH_3: 1 row → stays in memtable; tombstone beats id=2 in compacted run
#
# Expected GET /data/cache result: total=2, id=1→"v2", id=2 absent, id=3→"v1"

@pytest.fixture(scope="session")
def example_flight_server():
    script = [
        make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")]),
        make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")]),
        make_batch([(2, "beta", "v1", "delete")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.02, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


# ── Async Test Client ──────────────────────────────────────────────────────
#
# Each client fixture builds its OWN isolated app via main.create_app — its
# own DI container, FastMCP instance, and lifespan — so multiple client
# fixtures (flight / HTTP-ingest / Solace) can coexist in one pytest process
# without sharing state.

@pytest.fixture(scope="session")
async def test_client(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    example_flight_server,
):
    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))

    flight_settings = Settings(
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
        flight_port=example_flight_server.port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )

    async with lifespan_test_client(flight_settings) as client:
        yield client
