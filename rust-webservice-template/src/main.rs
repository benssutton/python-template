use rust_template::{app::build_app, config::Settings, state::AppState};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = envy::from_env::<Settings>().unwrap_or_default();
    tracing::info!(
        app_title   = %settings.app_title,
        app_version = %settings.app_version,
        status      = %settings.status,
        data_dir    = %settings.data_dir,
        "starting rust-template"
    );

    let state    = AppState::new(&settings);
    let app      = build_app(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    tracing::info!(addr = %listener.local_addr().unwrap(), "listening");
    axum::serve(listener, app).await.unwrap();
}
