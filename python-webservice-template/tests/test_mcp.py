import json

import httpx


def parse_mcp_response(response: httpx.Response):
    ct = response.headers.get("content-type", "")
    if "text/event-stream" in ct:
        r = None
        for line in response.text.splitlines():
            if line.startswith("data:"):
                r = json.loads(line[5:].strip())
    else:
        r = response.json()
    return r


async def test_mcp(test_client):

    url = "/mcp/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # ── Initialize session ────────────────────────────────────────────────────
    msg_id = 1
    response = await test_client.post(url, headers=headers, json={
        "jsonrpc": "2.0",
        "id": msg_id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"},
        },
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert r is not None

    mcp_session_id = response.headers.get("mcp-session-id")
    session_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "mcp-session-id": mcp_session_id,
    }

    # ── List tools ────────────────────────────────────────────────────────────
    msg_id += 1
    response = await test_client.post(url, headers=session_headers, json={
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": msg_id,
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    tool_names = {tool["name"] for tool in r["result"]["tools"]}
    assert "get_health_status" in tool_names

    # ── Call get_health_status and assert on result content ───────────────────
    # The test app is started with status="testing" (see conftest.py Settings).
    msg_id += 1
    response = await test_client.post(url, headers=session_headers, json={
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": msg_id,
        "params": {"name": "get_health_status", "arguments": {}},
    })
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert "result" in r, f"Expected result, got: {r}"
    assert r["result"]["isError"] is False
    content = r["result"]["content"]
    assert isinstance(content, list) and len(content) > 0
    assert "testing" in content[0]["text"]

    # ── Call all other tools with the correct JSON-RPC method ─────────────────
    for tool_name in tool_names - {"get_health_status"}:
        msg_id += 1
        response = await test_client.post(url, headers=session_headers, json={
            "jsonrpc": "2.0",
            "method": "tools/call",
            "id": msg_id,
            "params": {"name": tool_name, "arguments": {}},
        })
        assert response.status_code == 200
        r = parse_mcp_response(response)
        assert "result" in r, f"Tool {tool_name!r} returned an error: {r}"
