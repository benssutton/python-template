import asyncio
import time

import pytest
from httpx import AsyncClient, ASGITransport
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from core.container import service_container
from core.dependencies import get_health_service
from settings import Settings
from services.health import HealthService
from tests.publishers.flight_server import make_batch
from tests.publishers.solace_publisher import SolacePublisher

pytestmark = pytest.mark.solace

SOLACE_IMAGE = "solace/solace-pubsub-standard:latest"

# Same batch design as flight tests — proves transport equivalence.
# lsm_flush_rows=2, lsm_compaction_runs=2:
#   BATCH_1: 2 rows → flush → run1
#   BATCH_2: 2 rows → flush → run2 → compaction: id=1 gets v2
#   BATCH_3: 1 row  → memtable tombstone beats id=2 in compacted run
BATCH_1 = make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")])
BATCH_2 = make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")])
BATCH_3 = make_batch([(2, "beta", "v1", "delete")])


@pytest.fixture(scope="module")
def solace_container():
    container = (
        DockerContainer(SOLACE_IMAGE)
        .with_exposed_ports(55555, 8080)
        .with_env("username_admin_globalaccesslevel", "admin")
        .with_env("username_admin_password", "admin")
    )
    with container:
        wait_for_logs(container, "Primary Virtual Router Up", timeout=120)
        yield container


@pytest.fixture(scope="module")
async def test_client_solace(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    solace_container,
):
    from main import app, create_lifespan

    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))
    solace_smf_port = int(solace_container.get_exposed_port(55555))

    solace_settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="solace",
        solace_host="localhost",
        solace_port=solace_smf_port,
        solace_vpn="default",
        solace_username="admin",
        solace_password="admin",
        solace_topic="ingest/batches",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )

    override_hs = HealthService(solace_settings)
    app.dependency_overrides[get_health_service] = lambda: override_hs
    service_container.register_singleton(HealthService, override_hs)

    lifespan_ready = asyncio.Event()
    lifespan_done = asyncio.Event()

    async def _run_lifespan():
        async with create_lifespan(solace_settings)(app):
            lifespan_ready.set()
            await lifespan_done.wait()

    lifespan_task = asyncio.create_task(_run_lifespan())
    await lifespan_ready.wait()

    # Publish test batches AFTER the app has subscribed to the topic
    publisher = SolacePublisher(
        host="localhost",
        port=solace_smf_port,
        vpn="default",
        username="admin",
        password="admin",
        topic="ingest/batches",
    )
    try:
        publisher.publish_batch(BATCH_1)
        publisher.publish_batch(BATCH_2)
        publisher.publish_batch(BATCH_3)
    finally:
        publisher.close()

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
    finally:
        lifespan_done.set()
        await lifespan_task


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_newest_wins_across_compaction(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[1] == "v2"


async def test_tombstone_beats_compacted_run(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert 2 not in {r["id"] for r in body["rows"]}


async def test_unmodified_row_survives(test_client_solace: AsyncClient):
    body = await _poll_cache(test_client_solace, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[3] == "v1"


async def test_limit_respected(test_client_solace: AsyncClient):
    await _poll_cache(test_client_solace, expected_total=2)
    body = (await test_client_solace.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
