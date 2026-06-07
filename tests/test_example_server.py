import threading

import pytest
import pyarrow.flight as flight

from example_server import ExampleFlightServer
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


def test_default_script_exercises_lsm_edge_cases():
    from example_server import _default_script

    script = _default_script()
    assert len(script) == 3
    assert script[0].num_rows == 3                       # initial upserts
    assert script[2].column("op").to_pylist() == ["delete"]  # tombstone batch


def test_main_uses_env_and_serves(monkeypatch):
    import example_server as es

    served = {}

    def fake_serve(self):
        served["port"] = self.port

    monkeypatch.setattr(es.ExampleFlightServer, "serve", fake_serve)
    monkeypatch.setenv("FLIGHT_BIND_HOST", "localhost")
    monkeypatch.setenv("FLIGHT_PORT", "0")
    monkeypatch.setenv("FLIGHT_INTERVAL", "0.0")

    es.main()  # builds location, constructs the server (loop=True), calls our fake serve

    assert "port" in served
