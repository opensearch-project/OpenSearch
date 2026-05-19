/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::collections::HashMap;

use crate::field_config::FieldConfig;

#[derive(Debug, Clone, Default)]
pub struct NativeSettings {
    pub index_name: Option<String>,
    pub compression_level: Option<i32>,
    pub compression_type: Option<String>,
    pub page_size_bytes: Option<usize>,
    pub page_row_limit: Option<usize>,
    pub dict_size_bytes: Option<usize>,
    pub field_configs: Option<HashMap<String, FieldConfig>>,
    pub custom_settings: Option<HashMap<String, String>>,
    pub bloom_filter_enabled: Option<bool>,
    pub bloom_filter_fpp: Option<f64>,
    pub bloom_filter_ndv: Option<u64>,
    pub sort_columns: Vec<String>,
    pub reverse_sorts: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub sort_in_memory_threshold_bytes: Option<u64>,
    pub sort_batch_size: Option<usize>,
    pub merge_batch_size: Option<usize>,
    pub row_group_max_rows: Option<usize>,
    pub merge_rayon_threads: Option<usize>,
    pub merge_io_threads: Option<usize>,
}

impl NativeSettings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_compression_type(&self) -> &str {
        self.compression_type.as_deref().unwrap_or("LZ4_RAW")
    }

    pub fn get_compression_level(&self) -> i32 {
        self.compression_level.unwrap_or(2)
    }

    pub fn get_page_size_bytes(&self) -> usize {
        self.page_size_bytes.unwrap_or(1024 * 1024)
    }

    pub fn get_page_row_limit(&self) -> usize {
        self.page_row_limit.unwrap_or(20000)
    }

    pub fn get_dict_size_bytes(&self) -> usize {
        self.dict_size_bytes.unwrap_or(2 * 1024 * 1024)
    }

    pub fn get_bloom_filter_enabled(&self) -> bool {
        self.bloom_filter_enabled.unwrap_or(true)
    }

    pub fn get_bloom_filter_fpp(&self) -> f64 {
        self.bloom_filter_fpp.unwrap_or(0.1)
    }

    pub fn get_bloom_filter_ndv(&self) -> u64 {
        self.bloom_filter_ndv.unwrap_or(100_000)
    }

    pub fn get_field_config(&self, field_name: &str) -> Option<&FieldConfig> {
        self.field_configs.as_ref()?.get(field_name)
    }

    pub fn has_field_configs(&self) -> bool {
        self.field_configs.as_ref().map_or(false, |configs| !configs.is_empty())
    }

    pub fn get_sort_in_memory_threshold_bytes(&self) -> u64 {
        self.sort_in_memory_threshold_bytes.unwrap_or(32 * 1024 * 1024)
    }

    pub fn get_sort_batch_size(&self) -> usize {
        self.sort_batch_size.unwrap_or(8192)
    }

    pub fn get_merge_batch_size(&self) -> usize {
        self.merge_batch_size.unwrap_or(100_000)
    }

    pub fn get_row_group_max_rows(&self) -> usize {
        self.row_group_max_rows.unwrap_or(1_000_000)
    }

    pub fn get_merge_rayon_threads(&self) -> Option<usize> {
        self.merge_rayon_threads
    }

    pub fn get_merge_io_threads(&self) -> Option<usize> {
        self.merge_io_threads
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = NativeSettings::default();
        assert_eq!(config.get_compression_type(), "LZ4_RAW");
        assert_eq!(config.get_compression_level(), 2);
        assert_eq!(config.get_page_row_limit(), 20000);
        assert_eq!(config.get_dict_size_bytes(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_struct_construction() {
        let config = NativeSettings {
            compression_type: Some("SNAPPY".to_string()),
            compression_level: Some(1),
            ..Default::default()
        };
        assert_eq!(config.get_compression_type(), "SNAPPY");
        assert_eq!(config.get_compression_level(), 1);
    }

    #[test]
    fn test_field_configs() {
        use crate::field_config::FieldConfig;
        use std::collections::HashMap;

        let mut field_configs = HashMap::new();
        field_configs.insert("timestamp".to_string(), FieldConfig {
            compression_type: Some("SNAPPY".to_string()),
            compression_level: None,
        });
        let config = NativeSettings {
            compression_type: Some("ZSTD".to_string()),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        assert!(config.has_field_configs());
        let fc = config.get_field_config("timestamp").unwrap();
        assert_eq!(fc.compression_type, Some("SNAPPY".to_string()));
    }
}
