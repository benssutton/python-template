from mcp.server.fastmcp import FastMCP

from core.dependencies import get_health_service, get_data_service


def register(mcp: FastMCP) -> None:
    @mcp.tool()
    def get_health_status() -> str:
        """Returns the current application health status."""
        return get_health_service().status().status

    @mcp.tool()
    async def get_data_count() -> int:
        """Returns the total number of items in the data store."""
        result = await get_data_service().get_count()
        return result.count
