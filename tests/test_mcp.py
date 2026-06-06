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

    # GET requires a pre-existing session; initialize with POST first.
    msg_id = 1
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    initialise_request = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"},
            }
        }
    response = await test_client.post(
        url,
        headers=headers,
        json=initialise_request
    )
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert r is not None

    mcp_session_id = response.headers.get("mcp-session-id")

    # GET tools
    msg_id+=1
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "mcp-session-id": mcp_session_id
    }
    tool_list_request = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": msg_id
    }
    response = await test_client.post(
        url,
        headers=headers,
        json=tool_list_request
    )
    assert response.status_code==200
    r = parse_mcp_response(response)
    tools = set([tool["name"] for tool in r["result"]["tools"]])
    assert "get_health_status" in tools
    assert "get_data_count" in tools

    # Iterative call tools
    for tool_name in tools:
        msg_id+=1
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "mcp-session-id": mcp_session_id
        }
        tool_list_request = {
                "jsonrpc": "2.0",
                "method": tool_name,
                "id": msg_id
        }
        response = await test_client.post(
            url,
            headers=headers,
            json=tool_list_request
        )

        assert response != None


