import polars as pl

from persistence.stream_store.flight.lsm_store import _merge_to_rows


def _frame(rows):
    return pl.DataFrame(rows)


def test_merge_newest_wins():
    f = _frame([
        {"id": 1, "name": "a", "value": "old", "op": "upsert", "seqno": 0},
        {"id": 1, "name": "a", "value": "new", "op": "upsert", "seqno": 5},
    ])
    rows, total = _merge_to_rows((f,), ["id"], None)
    assert total == 1
    assert rows == [{"id": 1, "name": "a", "value": "new"}]


def test_merge_applies_tombstone():
    f = _frame([
        {"id": 1, "name": "a", "value": "v", "op": "upsert", "seqno": 0},
        {"id": 1, "name": "a", "value": "v", "op": "delete", "seqno": 5},
    ])
    rows, total = _merge_to_rows((f,), ["id"], None)
    assert rows == []
    assert total == 0


def test_merge_respects_limit():
    f = _frame([
        {"id": 1, "name": "a", "value": "x", "op": "upsert", "seqno": 0},
        {"id": 2, "name": "b", "value": "y", "op": "upsert", "seqno": 1},
    ])
    rows, total = _merge_to_rows((f,), ["id"], 1)
    assert total == 2
    assert len(rows) == 1


def test_merge_composite_key_extension():
    f = _frame([
        {"id": 1, "version": 1, "name": "a", "value": "v1", "op": "upsert", "seqno": 0},
        {"id": 1, "version": 2, "name": "a", "value": "v2", "op": "upsert", "seqno": 1},
    ])
    rows, total = _merge_to_rows((f,), ["id", "version"], None)
    assert total == 2  # different composite keys -> both survive


def test_merge_empty():
    rows, total = _merge_to_rows((), ["id"], None)
    assert rows == []
    assert total == 0
