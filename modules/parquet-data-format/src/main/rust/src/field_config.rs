/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct FieldConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_type: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,
}

impl FieldConfig {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn is_empty(&self) -> bool {
        self.compression_type.is_none() && self.compression_level.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_field_config_default() {
        let config = FieldConfig::default();
        assert!(config.is_empty());
    }
    
    #[test]
    fn test_field_config_serialization() {
        let config = FieldConfig {
            compression_type: Some("SNAPPY".to_string()),
            compression_level: Some(1),
        };
        
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("SNAPPY"));
        assert!(json.contains("\"compressionLevel\":1"));
    }
    
    #[test]
    fn test_field_config_deserialization() {
        let json = r#"{"compressionType":"ZSTD","compressionLevel":9}"#;
        let config: FieldConfig = serde_json::from_str(json).unwrap();
        
        assert_eq!(config.compression_type, Some("ZSTD".to_string()));
        assert_eq!(config.compression_level, Some(9));
    }
}
