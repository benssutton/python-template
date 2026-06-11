import asyncio
import time

import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.flight


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_newest_wins_across_compaction(test_client: AsyncClient):
    # BATCH_2 flushes → compaction merges run1+run2; id=1's seqno in run2 > run1
    body = await _poll_cache(test_client, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[1] == "v2"


async def test_tombstone_beats_compacted_run(test_client: AsyncClient):
    # BATCH_3 (delete for id=2) stays in memtable; its seqno > id=2 in compacted run
    body = await _poll_cache(test_client, expected_total=2)
    assert 2 not in {r["id"] for r in body["rows"]}


async def test_unmodified_row_survives(test_client: AsyncClient):
    # id=3 was introduced in BATCH_2, never updated or deleted
    body = await _poll_cache(test_client, expected_total=2)
    assert {r["id"]: r["value"] for r in body["rows"]}[3] == "v1"


async def test_limit_respected(test_client: AsyncClient):
    await _poll_cache(test_client, expected_total=2)
    body = (await test_client.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
