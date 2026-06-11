import asyncio
import logging
import threading

import pyarrow as pa

from ingestion.base import BatchConsumer
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class StreamIngestService:
    def __init__(self, consumer: BatchConsumer, store: LSMStore) -> None:
        self._consumer = consumer
        self._store = store
        self._thread: threading.Thread | None = None

    async def __aenter__(self) -> "StreamIngestService":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()

    def _ingest_loop(self) -> None:
        try:
            for batch in self._consumer.batches():
                try:
                    self._store.ingest(batch)
                except Exception:
                    log.exception("ingest failed; skipping batch")
        except Exception:
            log.exception("consumer batches() failed; stopping ingest")

    async def stop(self) -> None:
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join)
            self._thread = None

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._store.ingest, batch)
