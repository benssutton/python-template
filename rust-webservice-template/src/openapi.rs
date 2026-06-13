use utoipa::OpenApi;
use crate::{
    routers::{health, data},
    schemas::data::DataShapeResponse,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        health::get_status,
        data::get_shape,
    ),
    components(schemas(DataShapeResponse)),
    tags(
        (name = "Health",       description = "Endpoints for checking service health and status"),
        (name = "Data Service", description = "Endpoints for querying the dataset"),
    ),
    info(
        title       = "Rust Template Service",
        version     = "1.0.0",
        description = "A Rust service with REST and MCP endpoints, ready for Claude",
    )
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn test_openapi_doc_serialises() {
        let doc = ApiDoc::openapi();
        let json = doc.to_pretty_json().unwrap();
        assert!(json.contains("/health/status"));
        assert!(json.contains("/data/shape"));
        assert!(json.contains("DataShapeResponse"));
    }
}
