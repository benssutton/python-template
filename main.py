import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from sqlalchemy import text

from core.container import service_container
from core.settings import Settings
from persistence.analytics_store.clickhouse.clickhouse_client import create_client, close_client
from persistence.transaction_store.postgres.postgres_engine import engine
from services.data import DataService

log = logging.getLogger(__name__)

settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ch_client = await create_client(settings)
    service_container.register_singleton(DataService, DataService(ch_client))
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        try:
            ok = await ch_client.ping()
            if not ok:
                raise RuntimeError("ClickHouse startup ping failed")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError("ClickHouse startup ping failed") from exc

        async with mcp.session_manager.run():
            yield
    finally:
        await close_client()


from routers import health, data, config

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=[health.TAG_METADATA, data.TAG_METADATA, config.TAG_METADATA],
    lifespan=lifespan,
)

# REST Routers
app.include_router(health.router, prefix="/health")
app.include_router(data.router, prefix="/data")
app.include_router(config.router, prefix="/config")

# Root Endpoint

@app.get("/", tags=["API Root Page"])
async def get_root():
    return {
        "title": settings.app_title,
        "version": settings.app_version,
        "description": settings.app_description,
        "docs": "/docs",
    }

# MCP
from mcp_routers import tools

mcp = FastMCP(
    name="python-template",
    streamable_http_path="/",
    instructions="Tools for this template application.",
)
tools.register(mcp)

app.mount("/mcp", mcp.streamable_http_app())


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
