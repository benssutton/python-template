use std::sync::Arc;
use crate::{
    config::Settings,
    services::{
        health::{HealthServiceTrait, HealthService},
        data::{DataServiceTrait, DataService},
    },
};

#[derive(Clone)]
pub struct AppState {
    pub health_service: Arc<dyn HealthServiceTrait>,
    pub data_service:   Arc<dyn DataServiceTrait>,
}

impl AppState {
    pub fn new(settings: &Settings) -> Self {
        Self {
            health_service: Arc::new(HealthService::new(settings.clone())),
            data_service:   Arc::new(DataService::new(settings.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_wires_services() {
        let state = AppState::new(&Settings::default());
        assert_eq!(state.health_service.get_status(), "running");
    }
}
