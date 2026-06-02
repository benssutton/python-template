use rust_template::{config::Settings, state::AppState};

pub fn test_state() -> AppState {
    let settings = Settings {
        status:   "testing".to_string(),
        data_dir: "./tests/test_data".to_string(),
        ..Settings::default()
    };
    AppState::new(&settings)
}
