# import asyncio
# import threading
# import time

# import pyarrow.flight as flight

# from settings import Settings
# from example_server import ExampleFlightServer
# from persistence.stream_store.flight.flight_client import FlightCacheClient
# from persistence.stream_store.flight.lsm_store import LSMStore
# from services.flight_cache import FlightCacheService
# from tests.flight_helpers import make_batch


# class _Chunk:
#     def __init__(self, data):
#         self.data = data


# class _FakeReader:
#     def __init__(self, chunks):
#         self._it = iter(chunks)

#     def read_chunk(self):
#         return next(self._it)  # raises StopIteration when exhausted


# class _FakeClient:
#     def __init__(self, reader):
#         self._reader = reader

#     def do_get(self, ticket):
#         return self._reader

#     def close(self):
#         pass


# def test_consume_skips_malformed_and_stops():
#     chunks = [_Chunk(make_batch([(1, "a", "x", "upsert")])), _Chunk("not a batch")]
#     store = LSMStore(flush_rows=100, compaction_runs=100)
#     svc = FlightCacheService(_FakeClient(_FakeReader(chunks)), store, Settings())
#     svc._consume_loop()  # runs to completion: good ingested, bad skipped, StopIteration breaks
#     rows, total = store.query(10)
#     assert total == 1
#     assert rows == [{"id": 1, "name": "a", "value": "x"}]


# async def test_service_ingests_and_serves():
#     script = [
#         make_batch([(1, "a", "old", "upsert"), (2, "b", "y", "upsert")]),
#         make_batch([(1, "a", "new", "upsert")]),
#     ]
#     location = flight.Location.for_grpc_tcp("localhost", 0)
#     server = ExampleFlightServer(location, script, interval=0.0)
#     threading.Thread(target=server.serve, daemon=True).start()
#     try:
#         settings = Settings(flight_host="localhost", flight_port=server.port,
#                             flight_ticket="items")
#         async with FlightCacheClient(settings) as client:
#             store = LSMStore(flush_rows=100, compaction_runs=100)
#             svc = FlightCacheService(client, store, settings)
#             await svc.start()
#             deadline = time.monotonic() + 10
#             resp = None
#             while time.monotonic() < deadline:
#                 resp = await svc.get_data(10)
#                 if resp.total == 2:
#                     break
#                 await asyncio.sleep(0.05)
#             await svc.stop()
#             assert not svc._thread.is_alive()
#         assert resp.total == 2
#         values = {r.id: r.value for r in resp.rows}
#         assert values[1] == "new"
#     finally:
#         server.shutdown()


# async def test_stop_without_start_is_noop():
#     store = LSMStore(flush_rows=100, compaction_runs=100)
#     svc = FlightCacheService(_FakeClient(_FakeReader([])), store, Settings())
#     await svc.stop()  # must not raise


# def test_consume_breaks_on_read_error():
#     class _RaisingReader:
#         def read_chunk(self):
#             raise RuntimeError("boom")

#     class _RaisingClient:
#         def do_get(self, ticket):
#             return _RaisingReader()

#         def close(self):
#             pass

#     store = LSMStore(flush_rows=100, compaction_runs=100)
#     svc = FlightCacheService(_RaisingClient(), store, Settings())
#     svc._consume_loop()  # must log the error and break, not raise
#     assert store.query(10) == ([], 0)
