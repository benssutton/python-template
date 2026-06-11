from typing import Iterator, Protocol

import pyarrow as pa


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    batches() is a blocking generator; close() must be thread-safe and unblock
    any pending batches() call so the ingest thread can exit cleanly.
    """

    def batches(self) -> Iterator[pa.RecordBatch]: ...

    def close(self) -> None: ...
