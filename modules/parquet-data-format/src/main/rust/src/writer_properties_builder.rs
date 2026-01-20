/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use parquet::basic::{Compression, ZstdLevel, GzipLevel, BrotliLevel};
use parquet::file::properties::WriterProperties;

use crate::writer_config::WriterConfig;

/// Builder for converting WriterConfig into Parquet WriterProperties.
///
/// This struct follows the Single Responsibility Principle by focusing
/// solely on the conversion logic from configuration to Parquet properties.
///
/// # Design Principles
///
/// - **Single Responsibility**: Only handles WriterProperties construction
/// - **Open/Closed**: Can be extended with new compression types without modification
/// - **Dependency Inversion**: Depends on WriterConfig abstraction
pub struct WriterPropertiesBuilder;

impl WriterPropertiesBuilder {
    /// Builds WriterProperties from a WriterConfig.
    ///
    /// This method applies both index-level and field-level configurations
    /// to create a complete WriterProperties instance for Parquet writing.
    ///
    /// # Arguments
    ///
    /// * `config` - The writer configuration to convert
    ///
    /// # Returns
    ///
    /// A fully configured WriterProperties instance
    pub fn build(config: &WriterConfig) -> WriterProperties {
        let mut builder = WriterProperties::builder();

        // Apply compression settings
        builder = Self::apply_compression_settings(builder, config);

        // Apply page settings
        builder = Self::apply_page_settings(builder, config);

        // Apply dictionary settings
        builder = Self::apply_dictionary_settings(builder, config);

        // Apply field-level configurations
        builder = Self::apply_field_configs(builder, config);

        builder.build()
    }

    /// Applies compression settings to the builder.
    fn apply_compression_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &WriterConfig
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
        config: &WriterConfig
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        if let Some(page_size) = config.page_size {
            builder = builder.set_data_page_size_limit(page_size);
        }

        if let Some(page_row_limit) = config.page_row_limit {
            builder = builder.set_data_page_row_count_limit(page_row_limit);
        }

        builder
    }

    /// Applies dictionary encoding settings.
    fn apply_dictionary_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &WriterConfig
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        if let Some(dict_size) = config.dict_size_bytes {
            builder = builder.set_dictionary_page_size_limit(dict_size);
        }
        builder
    }

    /// Applies field-level configurations.
    fn apply_field_configs(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &WriterConfig
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
    use crate::writer_config::WriterConfig;

    #[test]
    fn test_build_with_compression() {
        let config = WriterConfig {
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
}
