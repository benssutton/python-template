from dataclasses import dataclass

import pyarrow as pa
import polars as pl

ORDER_COLUMN = "seqno"


def _merge_frame(frames: tuple[pl.DataFrame, ...],
                 key_columns: list[str]) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    # Window function: rank rows within each key partition by recency
    # (newest seqno first). key_columns is the single extension point:
    # ["id"] today, ["id", "version"] later, with no other change.
    winners = (
        combined
        .with_columns(
            pl.col(ORDER_COLUMN)
            .rank("ordinal", descending=True)
            .over(key_columns)
            .alias("_rn")
        )
        .filter(pl.col("_rn") == 1)
        .drop("_rn")
    )
    return winners


def _merge_to_rows(frames: tuple[pl.DataFrame, ...],
                   key_columns: list[str],
                   limit: int | None) -> tuple[list[dict], int]:
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit:
        live = live.head(limit)
    return live.drop([ORDER_COLUMN, "op"]).to_dicts(), total


@dataclass(frozen=True)
class _Snapshot:
    runs: tuple[pl.DataFrame, ...]
    memtable: tuple[pl.DataFrame, ...]


class LSMStore:
    def __init__(self, flush_rows: int, compaction_runs: int,
                 key_columns: list[str] | None = None) -> None:
        self._flush_rows = flush_rows
        self._compaction_runs = compaction_runs
        self._key_columns = key_columns or ["id"]
        self._seqno = 0
        # writer-private working set (only the ingest thread touches these):
        self._memtable: list[pl.DataFrame] = []
        self._memtable_rows = 0
        self._runs: list[pl.DataFrame] = []
        self._snapshot = _Snapshot(runs=(), memtable=())

    def ingest(self, batch: pa.RecordBatch) -> None:
        frame = pl.from_arrow(batch)
        n = frame.height
        frame = frame.with_columns(
            pl.Series(ORDER_COLUMN, range(self._seqno, self._seqno + n))
        )
        self._seqno += n
        self._memtable.append(frame)
        self._memtable_rows += n
        if self._memtable_rows >= self._flush_rows:
            self._flush()
            if len(self._runs) >= self._compaction_runs:
                self._compact()
        self._publish()

    def _flush(self) -> None:
        if not self._memtable:
            return
        self._runs.append(pl.concat(self._memtable, how="vertical"))
        self._memtable = []
        self._memtable_rows = 0

    def _compact(self) -> None:
        merged = _merge_frame(tuple(self._runs), self._key_columns)
        self._runs = [merged] if merged is not None else []

    def _publish(self) -> None:
        self._snapshot = _Snapshot(runs=tuple(self._runs),
                                   memtable=tuple(self._memtable))

    def query(self, limit: int) -> tuple[list[dict], int]:
        snap = self._snapshot  # atomic read, no lock
        return _merge_to_rows(snap.runs + snap.memtable, self._key_columns, limit)
