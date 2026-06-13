import asyncio
import logging
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Callable

import pyarrow as pa

from ingestion.base import BatchConsumer, ConnectionState
from persistence.stream_store.lsm_store import LSMStore
from schemas.data import DataRowResponse, DataRowsResponse
from schemas.health import IngestHealth
from settings import Settings

log = logging.getLogger(__name__)

_INGEST_BASE_DELAY = 1.0        # seconds before first retry
_INGEST_MAX_DELAY = 60.0        # seconds — retry cap
_INGEST_MAX_FAILURES = 5        # consecutive batches() failures before shutdown
_JOIN_TIMEOUT = 10.0            # seconds to wait for ingest thread on shutdown


def _default_shutdown() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


class StreamIngestService:
    def __init__(
        self,
        consumer: BatchConsumer,
        store: LSMStore,
        settings: Settings,
        shutdown_hook: Callable[[], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._settings = settings
        self._request_shutdown = shutdown_hook or _default_shutdown
        self._thread: threading.Thread | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._last_batch_at: datetime | None = None
        self._started_at: datetime = datetime.now(timezone.utc)
        self._rows_total = 0

    async def __aenter__(self) -> "StreamIngestService":
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()
        if self._settings.ingest_max_disconnect_seconds is not None:
            self._watchdog_task = asyncio.create_task(self._disconnect_watchdog())
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT)
            if self._thread.is_alive():
                log.error("ingest thread did not stop within %.0fs; abandoning", _JOIN_TIMEOUT)
            self._thread = None

    def _record_ingest(self, batch: pa.RecordBatch) -> None:
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(timezone.utc)
        self._rows_total += batch.num_rows

    def _ingest_loop(self) -> None:
        consecutive_failures = 0
        delay = _INGEST_BASE_DELAY
        # ingest_max_disconnect_seconds=None disables *all* automatic shutdown:
        # the watchdog (see __aenter__) and this consecutive-failure trigger.
        shutdown_on_failure = self._settings.ingest_max_disconnect_seconds is not None
        while True:
            try:
                for batch in self._consumer.batches():
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    try:
                        self._record_ingest(batch)
                    except Exception:
                        log.exception("ingest failed; skipping batch")
                return  # batches() returned cleanly — consumer was closed
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "consumer batches() failed (failure %d/%d)",
                    consecutive_failures, _INGEST_MAX_FAILURES,
                )
                if shutdown_on_failure and consecutive_failures >= _INGEST_MAX_FAILURES:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        consecutive_failures,
                    )
                    self._request_shutdown()
                    return
                jitter = delay * 0.25 * random.random()
                time.sleep(delay + jitter)
                delay = min(delay * 2, _INGEST_MAX_DELAY)

    async def _disconnect_watchdog(self) -> None:
        threshold = self._settings.ingest_max_disconnect_seconds
        poll = min(threshold / 2, 5.0)
        disconnected_since: float | None = None
        try:
            while True:
                await asyncio.sleep(poll)
                if self._consumer.connection_state() == ConnectionState.CONNECTED:
                    disconnected_since = None
                    continue
                now = asyncio.get_event_loop().time()
                if disconnected_since is None:
                    disconnected_since = now
                elif now - disconnected_since >= threshold:
                    log.critical(
                        "ingest transport not connected for %.0fs; requesting shutdown",
                        threshold,
                    )
                    self._request_shutdown()
                    return
        except asyncio.CancelledError:
            return

    async def health_check(self) -> IngestHealth:
        state = self._consumer.connection_state()
        last = self._last_batch_at
        now = datetime.now(timezone.utc)
        seconds_since = (now - last).total_seconds() if last is not None else None
        threshold = self._settings.ingest_staleness_threshold_seconds
        # Stale when explicitly configured and either:
        # - a batch was received but too long ago, or
        # - no batch has ever been received (use time-since-start as proxy).
        if threshold is not None:
            elapsed = seconds_since if seconds_since is not None else (now - self._started_at).total_seconds()
            stale = elapsed > threshold
        else:
            stale = False
        return IngestHealth(
            transport=self._settings.ingest_transport,
            connection_state=state.value,
            thread_alive=self._thread.is_alive() if self._thread is not None else False,
            last_batch_at=last,
            seconds_since_last_batch=round(seconds_since, 3) if seconds_since is not None else None,
            rows_ingested_total=self._rows_total,
            stale=stale,
        )

    async def get_data(self, limit: int) -> DataRowsResponse:
        rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        await asyncio.to_thread(self._record_ingest, batch)
