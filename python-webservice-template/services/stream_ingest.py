import asyncio
import logging
import os
import random
import signal
import threading
import time

import pyarrow as pa

from ingestion.base import BatchConsumer
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)

_INGEST_BASE_DELAY = 1.0        # seconds before first retry
_INGEST_MAX_DELAY = 60.0        # seconds — retry cap
_INGEST_MAX_FAILURES = 5        # consecutive batches() failures before SIGTERM
_JOIN_TIMEOUT = 10.0            # seconds to wait for ingest thread on shutdown


class StreamIngestService:
    def __init__(self, consumer: BatchConsumer, store: LSMStore) -> None:
        self._consumer = consumer
        self._store = store
        self._thread: threading.Thread | None = None

    async def __aenter__(self) -> "StreamIngestService":
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT)
            if self._thread.is_alive():
                log.error(
                    "ingest thread did not stop within %.0fs; abandoning",
                    _JOIN_TIMEOUT,
                )
            self._thread = None

    def _ingest_loop(self) -> None:
        consecutive_failures = 0
        delay = _INGEST_BASE_DELAY
        while True:
            try:
                for batch in self._consumer.batches():
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    try:
                        self._store.ingest(batch)
                    except Exception:
                        log.exception("ingest failed; skipping batch")
                return  # batches() returned cleanly — consumer was closed
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "consumer batches() failed (failure %d/%d)",
                    consecutive_failures,
                    _INGEST_MAX_FAILURES,
                )
                if consecutive_failures >= _INGEST_MAX_FAILURES:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        consecutive_failures,
                    )
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
                jitter = delay * 0.25 * random.random()
                time.sleep(delay + jitter)
                delay = min(delay * 2, _INGEST_MAX_DELAY)

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._store.ingest, batch)
