/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

#[derive(Debug, Clone, Default)]
pub struct FieldConfig {
    pub compression_type: Option<String>,
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
    fn test_field_config_construction() {
        let config = FieldConfig {
            compression_type: Some("SNAPPY".to_string()),
            compression_level: Some(1),
        };
        assert_eq!(config.compression_type, Some("SNAPPY".to_string()));
        assert_eq!(config.compression_level, Some(1));
        assert!(!config.is_empty());
    }
}
