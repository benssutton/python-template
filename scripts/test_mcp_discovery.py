import json
import sys
from typing import Any

import httpx

DEFAULT_URL = "http://localhost:8000/mcp"

HEADERS = {
    "Content-Type": "application/json",
    # Tell the server we accept both plain JSON and SSE upgrades.
    "Accept": "application/json, text/event-stream",
}

def _parse_response(resp: httpx.Response) -> dict[str, Any]:
    """Extract JSON-RPC payload from either a plain JSON or SSE response."""
    ct = resp.headers.get("content-type", "")
    if "text/event-stream" in ct:
        # FastMCP may return SSE even for simple requests.
        # The first `data:` line contains the JSON-RPC response.
        for line in resp.text.splitlines():
            if line.startswith("data:"):
                return json.loads(line[5:].strip())
        raise ValueError("SSE response contained no data: line")
    return resp.json()

def _post(
    client: httpx.Client,
    url: str,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    msg_id: int | None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """POST a JSON-RPC 2.0 request and return the parsed response dict."""
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if msg_id is not None:
        payload["id"] = msg_id
    if params:
        payload["params"] = params

    hdrs = dict(HEADERS)
    if session_id:
        hdrs["mcp-session-id"] = session_id

    resp = client.post(url, json=payload, headers=hdrs)
    resp.raise_for_status()
    return _parse_response(resp)

def _notify(
    client: httpx.Client,
    url: str,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    session_id: str | None = None,
) -> None:
    """Send a JSON-RPC notification (no id, response ignored)."""
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if params:
        payload["params"] = params

    hdrs = dict(HEADERS)
    if session_id:
        hdrs["mcp-session-id"] = session_id

    client.post(url, json=payload, headers=hdrs)  # response intentionally ignored


def step_initialize(
    client: httpx.Client, url: str, verbose: bool = True
) -> tuple[dict, str | None]:
    """Send initialize, print server info, return (result, session_id)."""
    print("=" * 60)
    print("STEP 1 — initialize")
    print("=" * 60)

    resp = client.post(
        url,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mcp-smoke-test", "version": "1.0.0"},
            },
        },
        headers=HEADERS,
    )
    resp.raise_for_status()

    # Capture session ID from response headers before parsing body.
    session_id: str | None = resp.headers.get("mcp-session-id")

    data = _parse_response(resp)
    if verbose:
        print(json.dumps(data, indent=2))

    if "error" in data:
        print(f"  ERROR: {data['error']}")
        sys.exit(1)

    result = data.get("result", {})
    info = result.get("serverInfo", {})
    caps = result.get("capabilities", {})

    print(f"  Protocol version : {result.get('protocolVersion', '—')}")
    print(f"  Server name      : {info.get('name', '—')}")
    print(f"  Server version   : {info.get('version', '—')}")
    print(f"  Capabilities     : {', '.join(caps.keys()) or '(none)'}")
    if session_id:
        print(f"  Session ID       : {session_id}")
    print()

    return result, session_id

def step_list_tools(
    client: httpx.Client, url: str, session_id: str | None, verbose: bool
) -> None:
    print("=" * 60)
    print("STEP 2 — tools/list")
    print("=" * 60)

    data = _post(client, url, "tools/list", msg_id=2, session_id=session_id)
    if verbose:
        print(json.dumps(data, indent=2))

    if "error" in data:
        print(f"  ERROR: {data['error']}")
        return

    tools: list[dict] = data.get("result", {}).get("tools", [])
    print(f"  {len(tools)} tool(s) registered:\n")
    for i, t in enumerate(tools, 1):
        name = t.get("name", "?")
        desc = (t.get("description") or "").strip()
        first_line = desc.split("\n")[0][:100]
        schema = t.get("inputSchema", {})
        props = list(schema.get("properties", {}).keys())
        required = schema.get("required", [])
        print(f"  {i:2}. {name}")
        if first_line:
            print(f"      {first_line}")
        if props:
            params_str = ", ".join(
                f"{p}*" if p in required else p for p in props
            )
            print(f"      params: {params_str}  (* = required)")
    print()

def step_list_resources(
    client: httpx.Client, url: str, session_id: str | None, verbose: bool
) -> None:
    print("=" * 60)
    print("STEP 3 — resources/list")
    print("=" * 60)

    data = _post(client, url, "resources/list", msg_id=3, session_id=session_id)
    if verbose:
        print(json.dumps(data, indent=2))

    if "error" in data:
        print(f"  ERROR: {data['error']}")
        return

    result = data.get("result", {})
    resources: list[dict] = result.get("resources", [])
    templates: list[dict] = result.get("resourceTemplates", [])

    combined = resources + templates
    print(f"  {len(combined)} resource(s) / template(s):\n")
    for r in combined:
        uri = r.get("uri") or r.get("uriTemplate") or "?"
        name = r.get("name", "")
        mime = r.get("mimeType", "")
        desc = (r.get("description") or "").strip()[:80]
        tag = "(template)" if "uriTemplate" in r else ""
        print(f"  • {uri}  {tag}")
        if name:
            print(f"    name: {name}  mime: {mime or '—'}")
        if desc:
            print(f"    {desc}")
    print()

def step_list_resource_templates(
    client: httpx.Client, url: str, session_id: str | None, verbose: bool
) -> None:
    print("=" * 60)
    print("STEP 3b — resources/templates/list")
    print("=" * 60)

    data = _post(client, url, "resources/templates/list", msg_id=5, session_id=session_id)
    if verbose:
        print(json.dumps(data, indent=2))

    if "error" in data:
        print(f"  ERROR: {data['error']}")
        return

    templates: list[dict] = data.get("result", {}).get("resourceTemplates", [])
    if templates:
        print(f"  {len(templates)} URI template(s):\n")
        for t in templates:
            uri = t.get("uriTemplate") or t.get("uri") or "?"
            name = t.get("name", "")
            mime = t.get("mimeType", "")
            desc = (t.get("description") or "").strip()[:80]
            print(f"  • {uri}")
            if name:
                print(f"    name: {name}  mime: {mime or '—'}")
            if desc:
                print(f"    {desc}")
    else:
        print("  (no URI templates registered)")
    print()

def step_list_prompts(
    client: httpx.Client, url: str, session_id: str | None, verbose: bool
) -> None:
    print("=" * 60)
    print("STEP 4 — prompts/list")
    print("=" * 60)

    data = _post(client, url, "prompts/list", msg_id=4, session_id=session_id)
    if verbose:
        print(json.dumps(data, indent=2))

    if "error" in data:
        print(f"  ERROR: {data['error']}")
        return

    prompts: list[dict] = data.get("result", {}).get("prompts", [])
    if prompts:
        print(f"  {len(prompts)} prompt(s):\n")
        for p in prompts:
            print(f"  • {p.get('name', '?')} — {p.get('description', '')[:80]}")
    else:
        print("  (no prompts registered)")
    print()


def main() -> None:

    try:
        with httpx.Client(timeout=10.0, follow_redirects=True) as client:

            init_result, session_id = step_initialize(client, DEFAULT_URL, True)

            # Send the required `initialized` notification before any other call.
            _notify(client, DEFAULT_URL, "notifications/initialized", session_id=session_id)
            step_list_tools(client, DEFAULT_URL, session_id, True)
            step_list_resources(client, DEFAULT_URL, session_id, True)
            step_list_resource_templates(client, DEFAULT_URL, session_id, True)
            step_list_prompts(client, DEFAULT_URL, session_id, True)

    except httpx.ConnectError:
        sys.exit(
            f"\nCould not connect to {DEFAULT_URL}\n"
            "Is the FastAPI app running?  Try: python main.py"
        )
    except httpx.HTTPStatusError as exc:
        sys.exit(f"\nHTTP {exc.response.status_code}: {exc.response.text[:200]}")

    print("Smoke-test complete.")

if __name__ == "__main__":
    main()
