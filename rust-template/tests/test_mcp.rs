use axum_test::TestServer;
use rust_template::build_app;

mod common;

fn parse_mcp_response(body: &[u8], content_type: &str) -> serde_json::Value {
    if content_type.contains("text/event-stream") {
        let text = std::str::from_utf8(body).unwrap();
        text.lines()
            .filter(|l| l.starts_with("data:"))
            .map(|l| l["data:".len()..].trim())
            .filter(|data| !data.is_empty())
            .find_map(|data| serde_json::from_str(data).ok())
            .expect("no valid JSON data: line found in SSE response")
    } else {
        serde_json::from_slice(body).unwrap()
    }
}

#[tokio::test]
async fn test_mcp_tool_list() {
    let app = build_app(common::test_state());
    let server = TestServer::new(app).unwrap();

    // Step 1: initialize session
    let init_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"}
        }
    });

    let init_response = server
        .post("/mcp")
        .add_header(
            axum::http::HeaderName::from_static("content-type"),
            axum::http::HeaderValue::from_static("application/json"),
        )
        .add_header(
            axum::http::HeaderName::from_static("accept"),
            axum::http::HeaderValue::from_static("application/json, text/event-stream"),
        )
        .json(&init_body)
        .await;

    assert_eq!(init_response.status_code(), axum::http::StatusCode::OK);

    let session_id = init_response
        .headers()
        .get("mcp-session-id")
        .expect("mcp-session-id header missing on initialize response")
        .to_str()
        .unwrap()
        .to_string();

    let ct = init_response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let init_bytes = init_response.as_bytes().to_vec();
    let init_result = parse_mcp_response(&init_bytes, &ct);
    assert!(init_result.get("result").is_some(), "initialize result missing");

    // Step 2: list tools
    let list_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": 2
    });

    let list_response = server
        .post("/mcp")
        .add_header(
            axum::http::HeaderName::from_static("content-type"),
            axum::http::HeaderValue::from_static("application/json"),
        )
        .add_header(
            axum::http::HeaderName::from_static("accept"),
            axum::http::HeaderValue::from_static("application/json, text/event-stream"),
        )
        .add_header(
            axum::http::HeaderName::from_static("mcp-session-id"),
            axum::http::HeaderValue::from_str(&session_id).unwrap(),
        )
        .json(&list_body)
        .await;

    assert_eq!(list_response.status_code(), axum::http::StatusCode::OK);

    let ct2 = list_response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let list_bytes = list_response.as_bytes().to_vec();
    let list_result = parse_mcp_response(&list_bytes, &ct2);

    let tool_names: Vec<&str> = list_result["result"]["tools"]
        .as_array()
        .expect("tools array missing")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    assert!(
        tool_names.contains(&"get_health_service_tool"),
        "get_health_service_tool not in tool list: {tool_names:?}"
    );
    assert!(
        tool_names.contains(&"get_data_service_tool"),
        "get_data_service_tool not in tool list: {tool_names:?}"
    );
}
