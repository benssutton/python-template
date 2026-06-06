from mcp.server.fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    raise NotImplementedError(
        "Register your MCP resources in mcp_routers/resources.py. "
        "See https://gofastmcp.com/servers/resources for examples."
    )
