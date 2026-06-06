import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.container import service_container
from core.settings import get_settings
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from routers import health, data, config
from mcp_routers import tools
from services.config import ConfigService
from services.data import DataService

log = logging.getLogger(__name__)

settings = get_settings()

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)

tools.register(mcp)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with PostgresClient(settings) as pg_pool:
        schema_sql = Path("scripts/postgres-init.sql").read_text()
        async with pg_pool.acquire() as conn:
            await conn.execute(schema_sql)
        service_container.register_singleton(ConfigService, ConfigService(pg_pool))

        async with ClickHouseClient(settings) as ch_client:
            if not await ch_client.ping():
                raise RuntimeError("ClickHouse startup ping failed")
            service_container.register_singleton(DataService, DataService(ch_client))

            async with mcp.session_manager.run():
                yield


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA],
    lifespan=lifespan,
)

app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")

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
