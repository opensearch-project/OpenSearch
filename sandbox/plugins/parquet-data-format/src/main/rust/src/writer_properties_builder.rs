/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use parquet::basic::{Compression, ZstdLevel, GzipLevel, BrotliLevel};
use parquet::file::metadata::{FileMetaData, KeyValue};
use parquet::file::properties::WriterProperties;

use crate::native_settings::NativeSettings;

/// Parquet file-level metadata key for the writer generation.
pub const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

/// Parquet file-level metadata key for the opensearch-defined parquet format version.
pub const FORMAT_VERSION_KEY: &str = "opensearch.format_version";

/// Current parquet format version produced by this writer. Plugin-defined namespace —
/// NOT comparable to Lucene or other format versions. Must stay in sync with the Java
/// constant `ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION`.
pub const FORMAT_VERSION: &str = "1.0.0.0";

/// Reads the writer generation from a Parquet file's key-value metadata.
/// Returns the generation value, or falls back to `file_index` if not present.
pub fn read_writer_generation(metadata: &FileMetaData, file_index: usize) -> i64 {
    metadata
        .key_value_metadata()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == WRITER_GENERATION_KEY)
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| v.parse::<i64>().ok())
        })
        .unwrap_or(file_index as i64)
}

/// Reads the opensearch format version from a Parquet file's key-value metadata.
/// Returns the stamped version string, or an empty string if the metadata is absent
/// (pre-versioning / legacy file). Callers should treat empty-string as "unknown"
/// rather than rejecting the file; compatibility decisions are plugin-owned.
pub fn read_format_version(metadata: &FileMetaData) -> String {
    metadata
        .key_value_metadata()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == FORMAT_VERSION_KEY)
                .and_then(|kv| kv.value.clone())
        })
        .unwrap_or_default()
}

/// Builder for converting NativeSettings into Parquet WriterProperties.
///
/// This struct follows the Single Responsibility Principle by focusing
/// solely on the conversion logic from configuration to Parquet properties.
///
/// # Design Principles
///
/// - **Single Responsibility**: Only handles WriterProperties construction
/// - **Open/Closed**: Can be extended with new compression types without modification
/// - **Dependency Inversion**: Depends on NativeSettings abstraction
pub struct WriterPropertiesBuilder;

impl WriterPropertiesBuilder {
    /// Builds WriterProperties from a NativeSettings.
    ///
    /// This method applies both index-level and field-level configurations
    /// to create a complete WriterProperties instance for Parquet writing.
    ///
    /// # Arguments
    ///
    /// * `config` - The native settings to convert
    ///
    /// # Returns
    ///
    /// A fully configured WriterProperties instance
    pub fn build(config: &NativeSettings) -> WriterProperties {
        Self::build_with_generation(config, None)
    }

    /// Builds WriterProperties with an optional writer generation stored as key-value metadata.
    pub fn build_with_generation(config: &NativeSettings, writer_generation: Option<i64>) -> WriterProperties {
        let mut builder = WriterProperties::builder();

        // Apply compression settings
        builder = Self::apply_compression_settings(builder, config);

        // Apply page settings
        builder = Self::apply_page_settings(builder, config);

        // Apply row group settings
        builder = Self::apply_row_group_settings(builder, config);

        // Apply dictionary settings
        builder = Self::apply_dictionary_settings(builder, config);

        // Apply bloom filter settings
        builder = Self::apply_bloom_filter_settings(builder, config);

        // Apply field-level configurations
        builder = Self::apply_field_configs(builder, config);

        // Build the full KV metadata vector. Format version is always stamped; writer
        // generation is stamped only when provided.
        let mut kv_metadata = vec![
            KeyValue::new(FORMAT_VERSION_KEY.to_string(), Some(FORMAT_VERSION.to_string())),
        ];
        if let Some(gen) = writer_generation {
            kv_metadata.push(KeyValue::new(
                WRITER_GENERATION_KEY.to_string(),
                Some(gen.to_string()),
            ));
        }
        builder = builder.set_key_value_metadata(Some(kv_metadata));

        builder.build()
    }

    /// Applies compression settings to the builder.
    fn apply_compression_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        let compression = Self::parse_compression_type(
            config.get_compression_type(),
            config.get_compression_level()
        );
        builder = builder.set_compression(compression);
        builder
    }

    /// Applies page size and row limit settings.
    fn apply_page_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder = builder.set_data_page_size_limit(config.get_page_size_bytes());
        builder = builder.set_data_page_row_count_limit(config.get_page_row_limit());
        builder
    }

    /// Applies row group row count limit.
    /// In parquet-rs 57.x, `set_max_row_group_size` is a row count limit (not bytes).
    fn apply_row_group_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder
            .set_max_row_group_size(config.get_row_group_max_rows())
    }

    /// Applies dictionary encoding settings.
    fn apply_dictionary_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder = builder.set_dictionary_page_size_limit(config.get_dict_size_bytes());
        builder
    }

    /// Applies bloom filter settings.
    fn apply_bloom_filter_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        if config.get_bloom_filter_enabled() {
            builder = builder.set_bloom_filter_enabled(true);
            builder = builder.set_bloom_filter_fpp(config.get_bloom_filter_fpp());
            builder = builder.set_bloom_filter_ndv(config.get_bloom_filter_ndv());
        }
        builder
    }

    /// Applies field-level configurations.
    fn apply_field_configs(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        if let Some(field_configs) = &config.field_configs {
            for (field_name, field_config) in field_configs {
                if let Some(compression_type) = &field_config.compression_type {
                    let compression = Self::parse_compression_type(
                        compression_type,
                        field_config.compression_level.unwrap_or(3)
                    );
                    builder = builder.set_column_compression(field_name.clone().into(), compression);
                }
            }
        }
        builder
    }

    /// Parses compression type string to Parquet Compression enum.
    ///
    /// # Arguments
    ///
    /// * `compression_type` - String representation of compression type
    /// * `level` - Compression level (algorithm-dependent)
    ///
    /// # Returns
    ///
    /// Appropriate Compression enum variant
    fn parse_compression_type(compression_type: &str, level: i32) -> Compression {
        match compression_type.to_uppercase().as_str() {
            "ZSTD" => Compression::ZSTD(
                ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default())
            ),
            "SNAPPY" => Compression::SNAPPY,
            "GZIP" => Compression::GZIP(
                GzipLevel::try_new(level as u32).unwrap_or_default()
            ),
            "LZ4" => Compression::LZ4,
            "BROTLI" => Compression::BROTLI(
                BrotliLevel::try_new(level as u32).unwrap_or_default()
            ),
            "LZ4_RAW" => Compression::LZ4_RAW,
            "UNCOMPRESSED" => Compression::UNCOMPRESSED,
            _ => Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_settings::NativeSettings;

    #[test]
    fn test_build_with_compression() {
        let config = NativeSettings {
            compression_type: Some("ZSTD".to_string()),
            compression_level: Some(5),
            ..Default::default()
        };

        let props = WriterPropertiesBuilder::build(&config);
        assert_ne!(props.compression(&parquet::schema::types::ColumnPath::from("test")), Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_parse_compression_types() {
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("ZSTD", 3),
            Compression::ZSTD(_)
        ));

        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("SNAPPY", 0),
            Compression::SNAPPY
        ));

        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("GZIP", 6),
            Compression::GZIP(_)
        ));
    }

    #[test]
    fn test_bloom_filter_disabled_propagates() {
        let config = NativeSettings {
            bloom_filter_enabled: Some(false),
            ..Default::default()
        };
        let props = WriterPropertiesBuilder::build(&config);
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
        // When bloom filter is disabled, no bloom filter properties should be set
        assert!(
            props.bloom_filter_properties(&col_path).is_none(),
            "bloom_filter_properties should be None when disabled, got: {:?}",
            props.bloom_filter_properties(&col_path)
        );
    }

    #[test]
    fn test_bloom_filter_enabled_propagates() {
        let config = NativeSettings {
            bloom_filter_enabled: Some(true),
            bloom_filter_fpp: Some(0.1),
            bloom_filter_ndv: Some(100_000),
            ..Default::default()
        };
        let props = WriterPropertiesBuilder::build(&config);
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
        let bf_props = props.bloom_filter_properties(&col_path);
        assert!(bf_props.is_some(), "bloom_filter_properties should be Some when enabled");
    }

    #[test]
    fn test_bloom_filter_default_is_true() {
        let config = NativeSettings::default();
        assert_eq!(config.get_bloom_filter_enabled(), true);
        let props = WriterPropertiesBuilder::build(&config);
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
        let bf_props = props.bloom_filter_properties(&col_path);
        assert!(bf_props.is_some(), "Default should have bloom filter enabled");
    }

    #[test]
    fn test_build_stamps_format_version() {
        let config = NativeSettings::default();
        let props = WriterPropertiesBuilder::build(&config);
        let kv = props.key_value_metadata().expect("KV metadata missing");
        let found = kv.iter().find(|k| k.key == FORMAT_VERSION_KEY);
        let entry = found.expect("format_version KV entry missing");
        assert_eq!(entry.value.as_deref(), Some(FORMAT_VERSION));
    }

    #[test]
    fn test_build_with_generation_stamps_both() {
        let config = NativeSettings::default();
        let props = WriterPropertiesBuilder::build_with_generation(&config, Some(42));
        let kv = props.key_value_metadata().expect("KV metadata missing");
        let has_format = kv.iter().any(|k| k.key == FORMAT_VERSION_KEY && k.value.as_deref() == Some(FORMAT_VERSION));
        let has_gen = kv.iter().any(|k| k.key == WRITER_GENERATION_KEY && k.value.as_deref() == Some("42"));
        assert!(has_format, "format_version stamp missing");
        assert!(has_gen, "writer_generation stamp missing");
    }
}
