"""Shared helper for running an isolated app + lifespan in async tests."""
import asyncio
from contextlib import asynccontextmanager

from httpx import AsyncClient, ASGITransport

from settings import Settings


@asynccontextmanager
async def lifespan_test_client(settings: Settings):
    """Build an isolated app via main.create_app, run its lifespan, yield a client.

    The lifespan runs in a dedicated task so that anyio cancel scopes (created
    by mcp.session_manager.run()) are entered and exited in the same task —
    anyio forbids cancel scopes crossing task boundaries, which happens when
    pytest-asyncio runs fixture setup and teardown as separate asyncio tasks.

    Startup failures fail fast: readiness is awaited *alongside* the lifespan
    task, so an exception during startup is re-raised immediately instead of
    deadlocking on a readiness event that will never be set.
    """
    from main import create_app

    app = create_app(settings)

    lifespan_ready = asyncio.Event()
    lifespan_done = asyncio.Event()

    async def _run_lifespan():
        async with app.router.lifespan_context(app):
            lifespan_ready.set()
            await lifespan_done.wait()

    lifespan_task = asyncio.create_task(_run_lifespan())
    ready_task = asyncio.create_task(lifespan_ready.wait())
    done, _ = await asyncio.wait(
        {lifespan_task, ready_task}, return_when=asyncio.FIRST_COMPLETED
    )
    if lifespan_task in done:
        ready_task.cancel()
        lifespan_task.result()  # re-raises the startup exception
        raise RuntimeError("app lifespan exited before signalling readiness")

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000") as client:
            yield client
    finally:
        lifespan_done.set()
        await lifespan_task
