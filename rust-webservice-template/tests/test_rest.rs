use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rust_template::build_app;
use tower::ServiceExt;

mod common;

#[tokio::test]
async fn test_health_status() {
    let app = build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/health/status").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value, "testing");
}

#[tokio::test]
async fn test_get_shape() {
    let app = build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/data/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let shape: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(shape["height"], 3);
    assert_eq!(shape["width"],  2);
}

#[tokio::test]
async fn test_swagger_ui_is_served() {
    let app = build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // Swagger UI redirects or serves HTML — either 200 or 303 is acceptable
    assert!(
        response.status() == StatusCode::OK || response.status() == StatusCode::SEE_OTHER,
        "expected 200 or 303, got {}", response.status()
    );
}

#[tokio::test]
async fn test_openapi_json_is_served() {
    let app = build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/api-doc/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(doc["paths"].get("/health/status").is_some());
    assert!(doc["paths"].get("/data/shape").is_some());
}
