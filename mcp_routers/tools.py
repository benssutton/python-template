import logging

from core.dependencies import get_health_service, get_data_service

from mcp.server.fastmcp import FastMCP

log = logging.getLogger(__name__)

def register(mcp: FastMCP):
    
    @mcp.tool()
    def get_health_service_tool():
        return get_health_service

    @mcp.tool()
    def get_data_service_tool():
        return get_data_service

