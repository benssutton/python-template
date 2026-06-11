import asyncio
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight

from settings import Settings


class FlightBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None

    async def __aenter__(self) -> "FlightBatchConsumer":
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = self._client.do_get(ticket)
        for chunk in reader:
            yield chunk.data

    def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            client.close()
