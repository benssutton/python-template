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
        self._state = ConnectionState.RECONNECTING
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = self._client.do_get(ticket)      # raises if the server is unreachable
        self._state = ConnectionState.CONNECTED
        try:
            for chunk in reader:
                yield chunk.data
        except Exception:
            if self._closing:
                return                              # intentional shutdown — exit cleanly
            self._state = ConnectionState.RECONNECTING
            raise
        if self._closing:
            return
        # Clean end of stream while not closing == an unexpected disconnect.
        self._state = ConnectionState.RECONNECTING
        raise ConnectionError("flight stream ended unexpectedly")

    def close(self) -> None:
        self._closing = True
        self._state = ConnectionState.DOWN
        client, self._client = self._client, None
        if client is not None:
            client.close()
