import asyncio
import time
import urllib.error
import urllib.request

import pytest
from httpx import AsyncClient, ASGITransport
from testcontainers.core.container import DockerContainer

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
    # Solace PubSub+ runs a Power-On-Self-Test (POST) at startup and shuts
    # down if hard requirements aren't met. Two are relevant here:
    #   - nofile hard limit must be >= 1048576 (the old 2448:6592 values from
    #     pre-10.x docs cause a POST ERROR and immediate shutdown).
    #   - memory: the broker needs ~1 GiB even at the minimum scaling tier.
    #     system_scaling_maxconnectioncount=100 selects that minimum tier.
    # The host Docker VM must have enough RAM (see this test's docstring /
    # GETTING_STARTED.md): a ~2 GiB WSL2 VM will OOM-kill the broker.
    container = (
        DockerContainer(SOLACE_IMAGE)
        .with_exposed_ports(55555, 8080, 5550)
        .with_env("username_admin_globalaccesslevel", "admin")
        .with_env("username_admin_password", "admin")
        .with_env("system_scaling_maxconnectioncount", "100")
        .with_kwargs(
            shm_size="1g",
            ulimits=[
                {"Name": "core", "Soft": -1, "Hard": -1},
                {"Name": "nofile", "Soft": 1048576, "Hard": 1048576},
            ],
        )
    )
    with container:
        # Readiness is NOT logged to the container's stdout (the broker writes
        # "Primary Virtual Router is now active" to its internal system.log, not
        # stdout), so wait_for_logs can never match. Poll the guaranteed-messaging
        # health-check port instead — it returns 200 once the broker is active.
        _wait_for_solace_ready(container, timeout=180)
        yield container


def _wait_for_solace_ready(container: DockerContainer, timeout: float) -> None:
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(5550))
    url = f"http://{host}:{port}/health-check/guaranteed-active"
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    return
        except urllib.error.HTTPError as exc:
            last_err = exc
            if exc.code == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc  # broker not accepting connections yet
        time.sleep(2)
    raise TimeoutError(f"Solace health-check not ready within {timeout}s: {last_err}")


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

    publisher = SolacePublisher(
        host="localhost",
        port=solace_smf_port,
        vpn="default",
        username="admin",
        password="admin",
        topic="ingest/batches",
    )

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            # Solace Direct messaging has NO retroactive delivery: a batch
            # published before the app's subscription is active on the broker is
            # silently dropped. lifespan_ready fires when the ingest thread is
            # spawned, which races the consumer's receiver.start(). Re-publish
            # the same ordered batches until the merged result appears — this is
            # idempotent (newest-wins always converges to total=2).
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                publisher.publish_batch(BATCH_1)
                publisher.publish_batch(BATCH_2)
                publisher.publish_batch(BATCH_3)
                await asyncio.sleep(1)
                body = (await client.get("/data/cache?limit=100")).json()
                if body["total"] == 2:
                    break
            publisher.close()
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
