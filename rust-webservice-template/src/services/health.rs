use crate::config::Settings;

pub trait HealthServiceTrait: Send + Sync {
    fn get_status(&self) -> String;
}

pub struct HealthService {
    settings: Settings,
}

impl HealthService {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }
}

impl HealthServiceTrait for HealthService {
    fn get_status(&self) -> String {
        self.settings.status.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_returns_status_from_settings() {
        let settings = Settings {
            status: "testing".to_string(),
            ..Settings::default()
        };
        let svc = HealthService::new(settings);
        assert_eq!(svc.get_status(), "testing");
    }

    #[test]
    fn test_returns_default_status() {
        let svc = HealthService::new(Settings::default());
        assert_eq!(svc.get_status(), "running");
    }
}
