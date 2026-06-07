# import pyarrow as pa

# from persistence.stream_store.flight.lsm_store import LSMStore
# from tests.flight_helpers import make_batch


# def test_flush_creates_run():
#     store = LSMStore(flush_rows=2, compaction_runs=10)
#     store.ingest(make_batch([(1, "a", "x", "upsert"), (2, "b", "y", "upsert")]))
#     assert len(store._runs) == 1
#     assert store._memtable == []
#     rows, total = store.query(10)
#     assert total == 2
#     assert {"id": 1, "name": "a", "value": "x"} in rows
#     assert {"id": 2, "name": "b", "value": "y"} in rows


# def test_query_merges_memtable_and_runs():
#     store = LSMStore(flush_rows=2, compaction_runs=10)
#     store.ingest(make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert")]))  # flush -> run
#     store.ingest(make_batch([(1, "a", "new", "upsert")]))                            # memtable
#     rows, total = store.query(10)
#     assert total == 2
#     assert {"id": 1, "name": "a", "value": "new"} in rows
#     assert {"id": 1, "name": "a", "value": "old"} not in rows


# def test_compaction_reduces_runs():
#     store = LSMStore(flush_rows=1, compaction_runs=2)
#     store.ingest(make_batch([(1, "a", "v1", "upsert")]))  # flush -> run1
#     store.ingest(make_batch([(1, "a", "v2", "upsert")]))  # flush -> run2 -> compact -> 1 run
#     assert len(store._runs) == 1
#     rows, total = store.query(10)
#     assert rows == [{"id": 1, "name": "a", "value": "v2"}]
#     assert total == 1


# def test_query_empty_store():
#     store = LSMStore(flush_rows=10, compaction_runs=10)
#     assert store.query(10) == ([], 0)


# def test_tombstone_then_query():
#     store = LSMStore(flush_rows=10, compaction_runs=10)
#     store.ingest(make_batch([(1, "a", "x", "upsert")]))
#     store.ingest(make_batch([(1, "a", "x", "delete")]))
#     assert store.query(10) == ([], 0)


# def test_tombstone_after_flush_suppresses_earlier_run():
#     store = LSMStore(flush_rows=1, compaction_runs=10)
#     store.ingest(make_batch([(1, "a", "x", "upsert")]))   # flush -> run1 (upsert)
#     store.ingest(make_batch([(1, "a", "x", "delete")]))   # flush -> run2 (tombstone)
#     assert store.query(10) == ([], 0)


# def test_store_uses_composite_key_columns():
#     store = LSMStore(flush_rows=10, compaction_runs=10, key_columns=["id", "version"])
#     batch = pa.record_batch({
#         "id": pa.array([1, 1], pa.int64()),
#         "version": pa.array([1, 2], pa.int64()),
#         "name": pa.array(["a", "a"], pa.string()),
#         "value": pa.array(["v1", "v2"], pa.string()),
#         "op": pa.array(["upsert", "upsert"], pa.string()),
#     })
#     store.ingest(batch)
#     rows, total = store.query(10)
#     assert total == 2  # both versions survive because the key includes version
