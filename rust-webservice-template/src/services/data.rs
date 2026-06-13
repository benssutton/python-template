use std::fs::File;
use polars::prelude::*;
use crate::config::Settings;
use crate::schemas::data::DataShapeResponse;

pub trait DataServiceTrait: Send + Sync {
    fn get_shape(&self) -> DataShapeResponse;
}

pub struct DataService {
    settings: Settings,
}

impl DataService {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }
}

impl DataServiceTrait for DataService {
    fn get_shape(&self) -> DataShapeResponse {
        let path = format!("{}/data.ipc_stream", self.settings.data_dir);
        let file = File::open(&path)
            .unwrap_or_else(|_| panic!("data file not found: {path}"));
        let df = IpcStreamReader::new(file)
            .finish()
            .expect("failed to read IPC stream");
        DataShapeResponse {
            height: df.height() as u64,
            width:  df.width()  as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_shape_reads_test_data() {
        let settings = Settings {
            data_dir: "./tests/test_data".to_string(),
            ..Settings::default()
        };
        let svc = DataService::new(settings);
        let shape = svc.get_shape();
        assert_eq!(shape.height, 3);
        assert_eq!(shape.width,  2);
    }
}
