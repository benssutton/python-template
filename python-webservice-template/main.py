import logging
from contextlib import asynccontextmanager, AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import Container
from settings import get_settings, Settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.cache_store.redis.redis_client import RedisClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from persistence.stream_store.lsm_store import LSMStore
from ingestion.flight.client import FlightBatchConsumer
from ingestion.solace.client import SolaceBatchConsumer
from routers import health, data, config, cache, metrics
from mcp_routers import tools
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.metrics import MetricsService
from services.stream_ingest import StreamIngestService

log = logging.getLogger(__name__)

logging.getLogger("asyncio").addFilter(
    lambda r: not (r.exc_info and isinstance(r.exc_info[1], ConnectionResetError))
)

_CONSUMERS = {
    "flight": FlightBatchConsumer,
    "solace": SolaceBatchConsumer,
}


def create_lifespan(settings: Settings, mcp: FastMCP):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        container: Container = app.state.container
        async with AsyncExitStack() as stack:
            pg_pool = await stack.enter_async_context(PostgresClient(settings))
            schema_sql = (Path(__file__).parent / "scripts" / "postgres-init.sql").read_text()
            async with pg_pool.acquire() as conn:
                await conn.execute(schema_sql)
            container.register_singleton(ConfigService, ConfigService(pg_pool))

            redis_client = await stack.enter_async_context(RedisClient(settings))
            container.register_singleton(CacheService, CacheService(redis_client))

            ch_client = await stack.enter_async_context(ClickHouseClient(settings))
            if not await ch_client.ping():
                raise RuntimeError("ClickHouse startup ping failed")
            container.register_singleton(DataService, DataService(ch_client))

            ConsumerClass = _CONSUMERS[settings.ingest_transport]
            consumer = await stack.enter_async_context(ConsumerClass(settings))
            store = LSMStore(
                flush_rows=settings.lsm_flush_rows,
                compaction_runs=settings.lsm_compaction_runs,
                key_columns=settings.lsm_key_columns,
            )
            ingest_svc = await stack.enter_async_context(StreamIngestService(consumer, store, settings))
            container.register_singleton(StreamIngestService, ingest_svc)

            await stack.enter_async_context(mcp.session_manager.run())
            yield
    return lifespan


def create_app(settings: Settings) -> FastAPI:
    """Build a fully isolated application instance.

    Everything stateful — the DI container, the FastMCP server (whose
    session manager can only run once per instance), and the lifespan — is
    created fresh per call, so multiple apps can coexist in one process
    (e.g. test apps with different transports running in the same pytest
    session).
    """
    container = Container(settings)

    mcp = FastMCP(
        name=settings.mcp_name,
        streamable_http_path="/",
        instructions=settings.mcp_instructions,
    )
    tools.register(mcp, container)

    app = FastAPI(
        title=settings.app_title,
        version=settings.app_version,
        description=settings.app_description,
        openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA, cache.TAG_METADATA],
        lifespan=create_lifespan(settings, mcp),
    )
    app.state.container = container

    @app.middleware("http")
    async def _track_last_request(request, call_next):
        request.app.state.container.last_request_at = datetime.now(timezone.utc)
        return await call_next(request)

    app.include_router(health.router, prefix="/health")
    app.include_router(data.router, prefix="/data")
    app.include_router(config.router, prefix="/config")
    app.include_router(cache.router, prefix="/cache")

    if settings.metrics_enabled:
        metrics_service = MetricsService(settings)
        metrics_service.instrument(app)
        container.register_singleton(MetricsService, metrics_service)
        app.include_router(metrics.router)

    app.mount("/mcp", mcp.streamable_http_app())

    @app.get("/", tags=["API Root Page"])
    async def get_root():
        return {
            "title": settings.app_title,
            "version": settings.app_version,
            "description": settings.app_description,
            "docs": "/docs",
            "MCP": "/mcp"
        }

    return app


# Module-level app for `uvicorn main:app`.
# All Settings fields have defaults so this is safe to execute at import time.
# If you later add a field with no default, switch to the factory pattern:
#   uvicorn main:create_app --factory
app = create_app(get_settings())


if __name__ == "__main__":
    import uvicorn
    log.info("Starting the application from main.py")
    settings = get_settings()   # same cached instance as `app` above
    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        ssl_keyfile=settings.ssl_keyfile,
        ssl_certfile=settings.ssl_certfile,
    )
