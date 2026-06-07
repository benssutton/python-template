import asyncio

import pyarrow.flight as flight

from core.settings import Settings


class FlightCacheClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None

    async def __aenter__(self) -> flight.FlightClient:
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None
