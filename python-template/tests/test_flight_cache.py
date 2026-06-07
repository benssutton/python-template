import asyncio
import time

from httpx import AsyncClient


async def _poll_cache(client: AsyncClient, expected_total: int, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        if body["total"] == expected_total:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"cache never reached total={expected_total}")


async def test_cache_returns_merged_rows(test_client: AsyncClient):
    body = await _poll_cache(test_client, expected_total=2)
    values = {r["id"]: r["value"] for r in body["rows"]}
    assert values[1] == "new"   # newest upsert wins
    assert values[3] == "z"


async def test_cache_applies_tombstone(test_client: AsyncClient):
    body = await _poll_cache(test_client, expected_total=2)
    ids = {r["id"] for r in body["rows"]}
    assert 2 not in ids         # deleted id suppressed by tombstone


async def test_cache_respects_limit(test_client: AsyncClient):
    await _poll_cache(test_client, expected_total=2)
    body = (await test_client.get("/data/cache?limit=1")).json()
    assert len(body["rows"]) == 1
    assert body["total"] == 2
