use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::ServerInfo;

use crate::state::AppState;

#[derive(Clone)]
pub struct McpHandler {
    pub state: AppState,
    tool_router: ToolRouter<Self>,
}

impl McpHandler {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router(router = tool_router)]
impl McpHandler {
    /// Returns the health service status string.
    #[tool(description = "Get the health status of the service")]
    async fn get_health_service_tool(&self) -> String {
        self.state.health_service.get_status()
    }

    /// Returns the shape of the data as a JSON string.
    #[tool(description = "Get the shape of the data (rows and columns)")]
    async fn get_data_service_tool(&self) -> String {
        let shape = self.state.data_service.get_shape();
        serde_json::to_string(&shape).unwrap_or_else(|e| format!("error: {e}"))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default()
    }
}
