import asyncio
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight

from ingestion.base import ConnectionState
from settings import Settings


class FlightBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None
        self._reader: flight.FlightStreamReader | None = None
        self._state = ConnectionState.DOWN
        self._closing = False

    async def __aenter__(self) -> "FlightBatchConsumer":
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        self._state = ConnectionState.RECONNECTING  # connected to server, not yet streaming
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def connection_state(self) -> ConnectionState:
        return self._state

    def batches(self) -> Iterator[pa.RecordBatch]:
        # close() may have run between reconnect attempts — exit cleanly so the
        # ingest loop terminates instead of dereferencing a closed client.
        if self._closing or self._client is None:
            return
        self._state = ConnectionState.RECONNECTING
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = self._client.do_get(ticket)      # raises if the server is unreachable
        self._reader = reader                     # tracked so close() can cancel it
        self._state = ConnectionState.CONNECTED
        try:
            for chunk in reader:
                yield chunk.data
        except Exception:
            if self._closing:
                return                              # intentional shutdown — exit cleanly
            self._state = ConnectionState.RECONNECTING
            raise
        finally:
            self._reader = None
        if self._closing:
            return
        # Clean end of stream while not closing == an unexpected disconnect.
        self._state = ConnectionState.RECONNECTING
        raise ConnectionError("flight stream ended unexpectedly")

    def close(self) -> None:
        self._closing = True
        self._state = ConnectionState.DOWN
        # Cancel the in-flight do_get first: on Windows, client.close() alone does
        # not unblock a thread iterating the stream reader, so the ingest thread
        # would be abandoned while still holding the gRPC stream open (which in
        # turn blocks the server's shutdown). Cancelling the reader aborts that
        # read promptly so the ingest loop exits.
        reader, self._reader = self._reader, None
        if reader is not None:
            try:
                reader.cancel()
            except Exception:
                pass
        client, self._client = self._client, None
        if client is not None:
            client.close()
