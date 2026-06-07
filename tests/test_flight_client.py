import threading

import pyarrow.flight as flight

from core.settings import Settings
from persistence.stream_store.flight.example_server import ExampleFlightServer
from persistence.stream_store.flight.flight_client import FlightCacheClient
from tests.flight_helpers import make_batch


async def test_flight_client_connects_and_reads():
    script = [make_batch([(1, "a", "x", "upsert")])]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    try:
        settings = Settings(flight_host="localhost", flight_port=server.port)
        async with FlightCacheClient(settings) as client:
            reader = client.do_get(flight.Ticket(b"items"))
            assert reader.read_all().num_rows == 1
    finally:
        server.shutdown()
