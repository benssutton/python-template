pub mod tools;

use axum::Router;
use crate::state::AppState;

pub fn build_router(_state: AppState) -> Router<AppState> {
    Router::new()
}
