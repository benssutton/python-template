import threading

import pyarrow.flight as flight

from tests.publishers.flight_server import ExampleFlightServer, make_batch, _default_script


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


def test_empty_script_serves_zero_batches():
    location = flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, [], interval=0.0)
    threading.Thread(target=server.serve, daemon=True).start()
    client = None
    try:
        client = flight.connect(f"grpc://localhost:{server.port}")
        reader = client.do_get(flight.Ticket(b"items"))
        table = reader.read_all()
        assert table.num_rows == 0
    finally:
        if client is not None:
            client.close()
        server.shutdown()


def test_default_script_exercises_lsm_edge_cases():
    script = _default_script()
    assert len(script) == 3
    assert script[0].num_rows == 3
    assert script[2].column("op").to_pylist() == ["delete"]


def test_main_uses_env_and_serves(monkeypatch):
    import tests.publishers.flight_server as fs

    served = {}

    def fake_serve(self):
        served["port"] = self.port

    monkeypatch.setattr(fs.ExampleFlightServer, "serve", fake_serve)
    monkeypatch.setenv("FLIGHT_BIND_HOST", "localhost")
    monkeypatch.setenv("FLIGHT_PORT", "0")
    monkeypatch.setenv("FLIGHT_INTERVAL", "0.0")

    fs.main()

    assert "port" in served
