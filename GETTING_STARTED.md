## Integrating with Claude Desktop

Start the FastAPI app — /mcp is mounted at http://localhost:8000/mcp.
Add to Claude Desktop's claude_desktop_config.json:

  "mcpServers": {
    "python-template": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8000/mcp"]
    }
  },

Restart Claude Desktop. The health tool should appear.
