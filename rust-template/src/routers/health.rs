use axum::{extract::State, Json};
use crate::state::AppState;

#[utoipa::path(
    get,
    path = "/health/status",
    tag = "Health",
    responses(
        (status = 200, description = "Current service status string", body = String)
    )
)]
pub async fn get_status(State(state): State<AppState>) -> Json<String> {
    Json(state.health_service.get_status())
}
