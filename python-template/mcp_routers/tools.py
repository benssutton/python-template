from mcp.server.fastmcp import FastMCP

from core.dependencies import get_health_service


def register(mcp: FastMCP) -> None:
    @mcp.tool()
    def get_health_status() -> str:
        """Returns the current application health status."""
        return get_health_service().status().status
