use axum::{extract::State, Json};
use crate::{state::AppState, schemas::data::DataShapeResponse};

#[utoipa::path(
    get,
    path = "/data/shape",
    tag = "Data Service",
    responses(
        (status = 200, description = "Row and column count of the dataset", body = DataShapeResponse)
    )
)]
pub async fn get_shape(State(state): State<AppState>) -> Json<DataShapeResponse> {
    Json(state.data_service.get_shape())
}
