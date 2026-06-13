from enum import Enum
from typing import Iterator, Protocol

import pyarrow as pa


class ConnectionState(str, Enum):
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DOWN = "down"


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    batches() is a blocking generator; close() must be thread-safe and unblock
    any pending batches() call so the ingest thread can exit cleanly.
    connection_state() is a cheap, cached read (no I/O) reporting the live
    transport connection state.
    """

    def batches(self) -> Iterator[pa.RecordBatch]: ...

    def close(self) -> None: ...

    def connection_state(self) -> ConnectionState: ...
