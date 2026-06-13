import threading

import pyarrow.flight as pa_flight
import pytest
from httpx import AsyncClient
from testcontainers.redis import RedisContainer

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.flight_server import ExampleFlightServer, make_batch

pytestmark = pytest.mark.observability

REDIS_IMAGE = "redis/redis-stack-server:latest"

_STREAM_SCRIPT = [make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")])]


def _settings(pg, ch, redis_url, flight_port, **overrides) -> Settings:
    return Settings(
        status="testing",
        postgres_url=f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}/{pg.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=int(ch.get_exposed_port(8123)),
        clickhouse_user=ch.username or "default",
        clickhouse_password=ch.password or "",
        clickhouse_database="default",
        redis_url=redis_url,
        ingest_transport="flight",
        flight_host="localhost",
        flight_port=flight_port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
        **overrides,
    )


@pytest.fixture(scope="module")
def streaming_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.05, loop=True)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


async def _poll_ready(client: AsyncClient, expected_code: int, timeout: float = 10.0):
    import asyncio
    import time
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await client.get("/health/ready")
        if last.status_code == expected_code:
            return last
        await asyncio.sleep(0.1)
    return last


async def test_liveness_always_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"


async def test_readiness_all_up_returns_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await _poll_ready(client, 200)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        by_name = {c["name"]: c for c in body["checks"]}
        assert by_name["postgres"]["status"] == "up"
        assert by_name["clickhouse"]["status"] == "up"
        assert by_name["redis"]["status"] == "up"
        assert by_name["ingest"]["status"] == "up"
        assert by_name["ingest"]["connection_state"] == "connected"


async def test_status_structure_and_ingest_freshness(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await _poll_ready(client, 200)
        body = (await client.get("/health/status")).json()
        assert body["app"]["status"] == "testing"
        assert body["uptime"]["process_seconds"] >= 0.0
        assert body["system"]["process"]["memory_rss_bytes"] > 0
        assert body["ingest"]["rows_ingested_total"] > 0
        assert body["ingest"]["last_batch_at"] is not None


async def test_metrics_endpoint_exposes_series(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await client.get("/health/live")          # generate one request metric
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        text = resp.text
        assert "http_request_duration_seconds" in text
        assert "dependency_up" in text
        assert "ingest_connection_state" in text
        assert "ingest_seconds_since_last_batch" in text
        assert "process_memory_rss_bytes" in text


async def test_ingest_disconnect_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container,
):
    # Use a finite (non-looping) server: after exhausting the script, batches()
    # raises ConnectionError and the consumer enters RECONNECTING state.
    # Because the server is still running, each reconnection cycle resets the
    # failure counter (one batch succeeds then the stream ends again), so the
    # 5-failure SIGTERM is never triggered.
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.0, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        server.port,
        ingest_max_disconnect_seconds=None,   # watchdog disabled; avoid SIGTERM in tests
    )
    async with lifespan_test_client(settings) as client:
        # Stream ends immediately; consumer enters RECONNECTING → readiness 503.
        resp = await _poll_ready(client, 503)
        assert resp.status_code == 503
        ingest = {c["name"]: c for c in resp.json()["checks"]}["ingest"]
        assert ingest["connection_state"] != "connected"
    server.shutdown()


async def test_idle_ingest_is_stale_but_ready(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    """Connected ingest that is idle (no recent batches) reports stale=True but
    does not fail readiness when ingest_stale_fails_readiness=False.

    Uses a streaming server (loop=True) to keep the consumer CONNECTED, sets a
    very short staleness threshold, then disables ingest metrics to prevent the
    server from racing ahead before the stale check.  We verify behaviour via
    the HealthService directly rather than through a real-server teardown that
    can block the gRPC stream indefinitely on Windows.
    """
    import asyncio
    from ingestion.base import ConnectionState
    from persistence.stream_store.lsm_store import LSMStore
    from services.health import HealthService
    from services.stream_ingest import StreamIngestService
    from core.container import Container

    # A fake consumer that is CONNECTED but never produces batches.
    class _IdleConsumer:
        def batches(self):
            threading.Event().wait()    # blocks forever until close()
            return
            yield
        def close(self): pass
        def connection_state(self): return ConnectionState.CONNECTED

    settings = Settings(
        ingest_transport="flight",
        ingest_staleness_threshold_seconds=0.5,
        ingest_stale_fails_readiness=False,
        ingest_max_disconnect_seconds=None,
    )
    store = LSMStore(flush_rows=100, compaction_runs=4)
    svc = StreamIngestService(_IdleConsumer(), store, settings)

    # health_check does not require the ingest thread to be running.
    health = await svc.health_check()
    assert health.connection_state == "connected"
    assert health.stale is False    # not yet past threshold

    await asyncio.sleep(0.6)
    health2 = await svc.health_check()
    assert health2.connection_state == "connected"
    assert health2.stale is True    # past 0.5 s threshold, no batch ever received

    # Confirm that stale-but-connected does NOT fail the HealthService readiness
    # check (ingest_stale_fails_readiness=False).
    container = Container(settings)
    container.register_singleton(StreamIngestService, svc)
    health_svc = HealthService(settings, container)
    readiness = await health_svc.readiness()
    ingest_check = next(c for c in readiness.checks if c.name == "ingest")
    assert ingest_check.status == "up"
    assert readiness.status == "not_ready"   # deps not available, but ingest is up


async def test_dependency_down_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, streaming_flight_server,
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        redis_url = f"redis://localhost:{int(dedicated_redis.get_exposed_port(6379))}/0"
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_redis.stop()             # kill only this test's redis
            resp = await _poll_ready(client, 503, timeout=15.0)
            assert resp.status_code == 503
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass
