# Stub — replaced in Task 13 with full SolaceBatchConsumer implementation
from typing import Iterator
import asyncio
import pyarrow as pa
from settings import Settings


class SolaceBatchConsumer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def __aenter__(self) -> "SolaceBatchConsumer":
        return self

    async def __aexit__(self, *_: object) -> None:
        pass

    def batches(self) -> Iterator[pa.RecordBatch]:
        return iter([])

    def close(self) -> None:
        pass
