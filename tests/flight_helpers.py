import pyarrow as pa


def make_batch(rows: list[tuple[int, str, str, str]]) -> pa.RecordBatch:
    """rows: list of (id, name, value, op)."""
    return pa.record_batch({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "name": pa.array([r[1] for r in rows], pa.string()),
        "value": pa.array([r[2] for r in rows], pa.string()),
        "op": pa.array([r[3] for r in rows], pa.string()),
    })
