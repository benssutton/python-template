import threading

import pyarrow.flight as flight
import pytest

from ingestion.base import ConnectionState
from ingestion.flight.client import FlightBatchConsumer
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
