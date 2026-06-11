import asyncio
import logging
import threading

import pyarrow.flight as flight

from settings import Settings
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class FlightCacheService:
    def __init__(self, client: flight.FlightClient, store: LSMStore,
                 settings: Settings) -> None:
        self._client = client
        self._store = store
        self._ticket = flight.Ticket(settings.flight_ticket.encode())
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    async def __aenter__(self) -> "FlightCacheService":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self) -> None:
        reader = self._client.do_get(self._ticket)
        while not self._stop.is_set():
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            except Exception:
                log.exception("flight read failed; stopping ingest")
                break
            try:
                self._store.ingest(chunk.data)
            except Exception:
                log.exception("ingest failed; skipping batch")

    async def stop(self) -> None:
        if self._thread is None:  # start() was never called — nothing to stop
            return
        self._stop.set()
        self._client.close()  # unblocks a pending read_chunk
        await asyncio.to_thread(self._thread.join)

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )
