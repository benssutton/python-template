import os
import time

import pyarrow as pa
import pyarrow.flight as flight


RECORD_SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("value", pa.string()),
    pa.field("op", pa.string()),
])


def make_batch(rows: list[tuple[int, str, str, str]]) -> pa.RecordBatch:
    """rows: list of (id, name, value, op)."""
    return pa.record_batch({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "name": pa.array([r[1] for r in rows], pa.string()),
        "value": pa.array([r[2] for r in rows], pa.string()),
        "op": pa.array([r[3] for r in rows], pa.string()),
    })


class ExampleFlightServer(flight.FlightServerBase):
    def __init__(self, location, script: list[pa.RecordBatch],
                 interval: float, loop: bool = False) -> None:
        super().__init__(location)
        self._script = script
        self._interval = interval
        self._loop = loop
        # Use provided schema or fall back to module constant for empty scripts
        self._schema = script[0].schema if script else RECORD_SCHEMA

    def do_get(self, context, ticket):
        def gen():
            while True:
                for batch in self._script:
                    if self._interval:
                        time.sleep(self._interval)
                    yield batch
                if not self._loop:
                    break

        return flight.GeneratorStream(self._schema, gen())


class IdleFlightServer(flight.FlightServerBase):
    """Accepts a do_get and keeps the stream open forever without sending.

    Used to exercise the 'connected but no data' (idle) ingest state.
    """

    def do_get(self, context, ticket):
        def gen():
            while True:
                time.sleep(0.1)
                if False:        # never yields; keeps the stream open
                    yield

        return flight.GeneratorStream(RECORD_SCHEMA, gen())


def _default_script() -> list[pa.RecordBatch]:
    return [
        make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert"), (3, "c", "z", "upsert")]),
        make_batch([(1, "a", "new", "upsert")]),
        make_batch([(2, "b", "y", "delete")]),
    ]


def main() -> None:
    host = os.environ.get("FLIGHT_BIND_HOST", "0.0.0.0")
    port = int(os.environ.get("FLIGHT_PORT", "8815"))
    interval = float(os.environ.get("FLIGHT_INTERVAL", "0.2"))
    location = flight.Location.for_grpc_tcp(host, port)
    server = ExampleFlightServer(location, _default_script(), interval=interval, loop=True)
    server.serve()


if __name__ == "__main__":
    main()
