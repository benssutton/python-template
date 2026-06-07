import threading

import pytest
import pyarrow.flight as flight

from persistence.stream_store.flight.example_server import ExampleFlightServer
from tests.flight_helpers import make_batch


def test_example_server_streams_script():
    script = [
        make_batch([(1, "a", "x", "upsert")]),
        make_batch([(2, "b", "y", "upsert")]),
    ]
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, script, interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    client = None
    try:
        client = flight.connect(f"grpc://localhost:{server.port}")
        reader = client.do_get(flight.Ticket(b"items"))
        table = reader.read_all()
        assert table.num_rows == 2
    finally:
        if client is not None:
            client.close()
        server.shutdown()


def test_empty_script_rejected():
    location = flight.Location.for_grpc_tcp("localhost", 0)
    with pytest.raises(ValueError):
        ExampleFlightServer(location, [], interval=0.0)
