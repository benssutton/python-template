use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DataShapeResponse {
    pub height: u64,
    pub width:  u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialises_to_json() {
        let r = DataShapeResponse { height: 3, width: 2 };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"{"height":3,"width":2}"#);
    }
}
