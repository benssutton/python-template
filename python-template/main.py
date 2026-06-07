import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import service_container
from settings import get_settings, Settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.cache_store.redis.redis_client import RedisClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from routers import health, data, config, cache
from mcp_routers import tools
from persistence.stream_store.flight.flight_client import FlightCacheClient
from persistence.stream_store.flight.lsm_store import LSMStore
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.flight_cache import FlightCacheService

log = logging.getLogger(__name__)

logging.getLogger("asyncio").addFilter(
    lambda r: not (r.exc_info and isinstance(r.exc_info[1], ConnectionResetError))
)

settings = get_settings()

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)

tools.register(mcp)


def create_lifespan(settings: Settings):
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async with PostgresClient(settings) as pg_pool:
            schema_sql = (Path(__file__).parent / "scripts" / "postgres-init.sql").read_text()
            async with pg_pool.acquire() as conn:
                await conn.execute(schema_sql)
            service_container.register_singleton(ConfigService, ConfigService(pg_pool))

            async with RedisClient(settings) as redis_client:
                service_container.register_singleton(CacheService, CacheService(redis_client))

                async with ClickHouseClient(settings) as ch_client:
                    if not await ch_client.ping():
                        raise RuntimeError("ClickHouse startup ping failed")
                    service_container.register_singleton(DataService, DataService(ch_client))

                    async with FlightCacheClient(settings) as flight_client:
                        store = LSMStore(
                            flush_rows=settings.lsm_flush_rows,
                            compaction_runs=settings.lsm_compaction_runs,
                            key_columns=["id"],
                        )
                        flight_service = FlightCacheService(flight_client, store, settings)
                        await flight_service.start()
                        service_container.register_singleton(FlightCacheService, flight_service)
                        try:
                            async with mcp.session_manager.run():
                                yield
                        finally:
                            await flight_service.stop()
    return lifespan


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA, cache.TAG_METADATA],
    lifespan=create_lifespan(settings),
)

app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")
app.include_router(cache.router, prefix="/cache")

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


if __name__ == "__main__":
    log.info("Starting the application from main.py")
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=443,
        ssl_keyfile="./certs/key.pem",
        ssl_certfile="./certs/cert.pem",
    )
