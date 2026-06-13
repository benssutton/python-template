use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Settings {
    pub app_title:   String,
    pub app_version: String,
    pub status:      String,
    pub data_dir:    String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            app_title:   "Rust Template Service".to_string(),
            app_version: "1.0.0".to_string(),
            status:      "running".to_string(),
            data_dir:    "./data".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let s = Settings::default();
        assert_eq!(s.status,  "running");
        assert_eq!(s.data_dir, "./data");
    }

    #[test]
    fn test_struct_update_syntax() {
        let s = Settings {
            status:   "testing".to_string(),
            data_dir: "./tests/test_data".to_string(),
            ..Settings::default()
        };
        assert_eq!(s.status,    "testing");
        assert_eq!(s.data_dir,  "./tests/test_data");
        assert_eq!(s.app_title, "Rust Template Service");
    }
}
