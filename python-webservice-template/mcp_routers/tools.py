from mcp.server.fastmcp import FastMCP

from core.container import Container
from services.health import HealthService


def register(mcp: FastMCP, container: Container) -> None:
    # MCP tools run outside FastAPI's request DI, so the owning app's
    # container is passed in explicitly and captured by the tool closures.
    @mcp.tool()
    def get_health_status() -> str:
        """Returns the current application health status."""
        return container.get(HealthService).status().status
