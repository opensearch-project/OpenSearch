/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::field_config::FieldConfig;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct WriterConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_row_limit: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dict_size_bytes: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_group_size_bytes: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_configs: Option<HashMap<String, FieldConfig>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_settings: Option<HashMap<String, serde_json::Value>>,
}

impl WriterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn get_compression_type(&self) -> &str {
        self.compression_type.as_deref().unwrap_or("ZSTD")
    }

    pub fn get_compression_level(&self) -> i32 {
        self.compression_level.unwrap_or(2)
    }

    pub fn get_page_size(&self) -> usize {
        self.page_size.unwrap_or(1024 * 1024)
    }

    pub fn get_page_row_limit(&self) -> usize {
        self.page_row_limit.unwrap_or(20000)
    }

    pub fn get_dict_size_bytes(&self) -> usize {
        self.dict_size_bytes.unwrap_or(2 * 1024 * 1024)
    }

    pub fn get_row_group_size_bytes(&self) -> usize {
        self.row_group_size_bytes.unwrap_or(128 * 1024 * 1024)
    }

    pub fn get_field_config(&self, field_name: &str) -> Option<&FieldConfig> {
        self.field_configs.as_ref()?.get(field_name)
    }

    pub fn has_field_configs(&self) -> bool {
        self.field_configs.as_ref().map_or(false, |configs| !configs.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_config_default() {
        let config = WriterConfig::default();
        assert_eq!(config.get_compression_type(), "ZSTD");
        assert_eq!(config.get_compression_level(), 2);
        assert_eq!(config.get_page_row_limit(), 20000);
        assert_eq!(config.get_dict_size_bytes(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_writer_config_from_json() {
        let json = r#"{"compressionType":"SNAPPY","compressionLevel":1}"#;
        let config = WriterConfig::from_json(json).unwrap();

        assert_eq!(config.compression_type, Some("SNAPPY".to_string()));
        assert_eq!(config.compression_level, Some(1));
    }

    #[test]
    fn test_writer_config_with_field_configs() {
        let json = r#"{
            "compressionType": "ZSTD",
            "fieldConfigs": {
                "timestamp": {
                    "compressionType": "SNAPPY"
                }
            }
        }"#;

        let config = WriterConfig::from_json(json).unwrap();
        assert!(config.has_field_configs());

        let field_config = config.get_field_config("timestamp").unwrap();
        assert_eq!(field_config.compression_type, Some("SNAPPY".to_string()));
    }

    #[test]
    fn test_writer_config_serialization() {
        let mut config = WriterConfig::default();
        config.compression_type = Some("ZSTD".to_string());
        config.compression_level = Some(2);

        let json = config.to_json().unwrap();
        assert!(json.contains("compressionType"));
        assert!(json.contains("ZSTD"));
    }
}
