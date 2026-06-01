import json

import pytest


async def test_health_status(test_client):
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    assert response.json() == "testing"

async def test_mcp(test_client):
    HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # GET requires a pre-existing session; initialize with POST first.
    response = await test_client.post(
        "/mcp/",
        headers=HEADERS,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"},
            },
        },
    )
    assert response.status_code == 200

    ct = response.headers.get("content-type", "")
    if "text/event-stream" in ct:
        r = None
        for line in response.text.splitlines():
            if line.startswith("data:"):
                r = json.loads(line[5:].strip())
    else:
        r = response.json()

    assert r is not None

