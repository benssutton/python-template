import threading

import pyarrow.flight as flight
import pytest

from ingestion.base import ConnectionState
from ingestion.flight.client import FlightBatchConsumer
from ingestion.solace.client import SolaceBatchConsumer, _StateListener
from settings import Settings
from tests.publishers.flight_server import ExampleFlightServer, make_batch


async def test_flight_consumer_unexpected_stream_end_sets_reconnecting():
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(
        location, [make_batch([(1, "a", "v1", "upsert")])], interval=0.0, loop=False
    )
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        consumer = FlightBatchConsumer(
            Settings(flight_host="localhost", flight_port=server.port, flight_ticket="items")
        )
        await consumer.__aenter__()
        assert consumer.connection_state() == ConnectionState.RECONNECTING  # connected, not streaming yet

        # The script has one batch then the stream ends; an unexpected end must
        # raise (so the ingest loop reconnects) and leave state RECONNECTING.
        with pytest.raises(ConnectionError):
            for _ in consumer.batches():
                pass
        assert consumer.connection_state() == ConnectionState.RECONNECTING
        consumer.close()
        assert consumer.connection_state() == ConnectionState.DOWN
    finally:
        server.shutdown()


def test_solace_consumer_state_transitions_via_listeners():
    consumer = SolaceBatchConsumer(Settings())
    assert consumer.connection_state() == ConnectionState.DOWN

    listener = _StateListener(consumer)
    listener.on_reconnecting(None)
    assert consumer.connection_state() == ConnectionState.RECONNECTING
    listener.on_reconnected(None)
    assert consumer.connection_state() == ConnectionState.CONNECTED
    listener.on_service_interrupted(None)
    assert consumer.connection_state() == ConnectionState.DOWN


import asyncio

from persistence.stream_store.lsm_store import LSMStore
from services.stream_ingest import StreamIngestService

# Note: `threading` and `Settings` are already imported earlier in this file
# (Task 5 block); `make_batch` and `ConnectionState` likewise.


class _FakeConsumer:
    """Real (non-mock) BatchConsumer whose state is fixed and whose batches()
    blocks until close(), so the ingest thread stays alive during tests."""

    def __init__(self, state: ConnectionState) -> None:
        self._state = state
        self._closed = threading.Event()

    def batches(self):
        self._closed.wait()
        return
        yield  # make this a generator

    def close(self) -> None:
        self._closed.set()

    def connection_state(self) -> ConnectionState:
        return self._state


async def test_ingest_health_tracks_freshness_after_ingest():
    settings = Settings(ingest_transport="flight", ingest_staleness_threshold_seconds=None)
    svc = StreamIngestService(_FakeConsumer(ConnectionState.CONNECTED), LSMStore(100, 4), settings)

    before = await svc.health_check()
    assert before.connection_state == "connected"
    assert before.last_batch_at is None
    assert before.rows_ingested_total == 0

    await svc.ingest_batch(make_batch([(1, "a", "v1", "upsert")]))
    after = await svc.health_check()
    assert after.rows_ingested_total == 1
    assert after.last_batch_at is not None
    assert after.seconds_since_last_batch is not None


async def test_disconnect_watchdog_invokes_shutdown_hook():
    triggered = asyncio.Event()
    settings = Settings(ingest_transport="flight", ingest_max_disconnect_seconds=0.2)
    svc = StreamIngestService(
        _FakeConsumer(ConnectionState.DOWN), LSMStore(100, 4), settings,
        shutdown_hook=triggered.set,
    )
    async with svc:
        await asyncio.wait_for(triggered.wait(), timeout=3.0)
    assert triggered.is_set()
