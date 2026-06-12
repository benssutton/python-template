import asyncio
import pytest
from core.retry import connect_with_backoff


async def test_succeeds_on_first_attempt():
    calls = []

    async def connect():
        calls.append(1)
        return "ok"

    result = await connect_with_backoff(connect, label="test", base_delay=0.001)
    assert result == "ok"
    assert len(calls) == 1


async def test_retries_then_succeeds():
    calls = []

    async def connect():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("not yet")
        return "ok"

    result = await connect_with_backoff(
        connect, label="test", max_attempts=5, base_delay=0.001
    )
    assert result == "ok"
    assert len(calls) == 3


async def test_raises_after_max_attempts():
    async def connect():
        raise ConnectionError("always fails")

    with pytest.raises(ConnectionError, match="always fails"):
        await connect_with_backoff(
            connect, label="test", max_attempts=3, base_delay=0.001
        )


async def test_delays_are_positive_and_increasing():
    """Verify jitter produces positive, increasing delays."""
    delays: list[float] = []
    original_sleep = asyncio.sleep

    async def capture_sleep(n: float):
        delays.append(n)

    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        if calls < 4:
            raise ConnectionError("not yet")
        return "ok"

    import unittest.mock as mock
    with mock.patch("asyncio.sleep", capture_sleep):
        await connect_with_backoff(
            connect, label="test", max_attempts=5, base_delay=1.0
        )

    assert len(delays) == 3
    assert all(d > 0 for d in delays)
    assert delays[1] > delays[0]
