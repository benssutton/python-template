pub mod tools;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};

use crate::state::AppState;
use tools::McpHandler;

pub fn build_router(state: AppState) -> Router<AppState> {
    let service: StreamableHttpService<McpHandler, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(McpHandler::new(state.clone())),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default().disable_allowed_hosts(),
        );

    Router::new().nest_service("/", service)
}
