use axum::{routing::get, Router};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use crate::{
    mcp,
    openapi::ApiDoc,
    routers::{data, health},
    state::AppState,
};

pub fn build_app(state: AppState) -> Router {
    let mcp_router = mcp::build_router(state.clone());
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .route("/health/status", get(health::get_status))
        .route("/data/shape",    get(data::get_shape))
        .nest("/mcp", mcp_router)
        .with_state(state)
}
