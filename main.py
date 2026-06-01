import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from core.settings import Settings


log = logging.getLogger(__name__)

settings = Settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp.session_manager.run():
        yield

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=settings.app_description,
    openapi_tags=settings.open_api_tags,
    lifespan=lifespan
)

from routers import health
app.include_router(health.router, prefix="/health")

from mcp_routers import tools

mcp = FastMCP(
        name="python-template",
        streamable_http_path="/",
        instructions=(
            "Tools for this template application."
        ),
    )

tools.register(mcp)

app.mount("/mcp", 
          mcp.streamable_http_app())

if __name__ == "__main__":
    log.info("Starting the application from main.py")
    import uvicorn
    uvicorn.run(app)
