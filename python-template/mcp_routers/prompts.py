from mcp.server.fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    raise NotImplementedError(
        "Register your MCP prompts in mcp_routers/prompts.py. "
        "See https://gofastmcp.com/servers/prompts for examples."
    )
