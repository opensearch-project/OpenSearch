/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use parquet::basic::{Compression, Encoding, ZstdLevel, GzipLevel, BrotliLevel};
use parquet::file::metadata::{FileMetaData, KeyValue};
use parquet::file::properties::WriterProperties;

use arrow::datatypes::Schema as ArrowSchema;

use crate::native_settings::NativeSettings;

/// Parquet file-level metadata key for the writer generation.
pub const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

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

/// Maps an Arrow DataType to a lowercase string key used for cluster-level type config lookups.
fn arrow_type_key(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType::*;
    match dt {
        Int8 => "int8".into(),
        Int16 => "int16".into(),
        Int32 => "int32".into(),
        Int64 => "int64".into(),
        UInt8 => "uint8".into(),
        UInt16 => "uint16".into(),
        UInt32 => "uint32".into(),
        UInt64 => "uint64".into(),
        Float32 => "float32".into(),
        Float64 => "float64".into(),
        Boolean => "boolean".into(),
        Utf8 | LargeUtf8 => "utf8".into(),
        Binary | LargeBinary => "binary".into(),
        Date32 | Date64 => "date".into(),
        Timestamp(_, _) => "timestamp".into(),
        _ => format!("{:?}", dt).to_lowercase(),
    }
}

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
    pub fn build(config: &NativeSettings, schema: &ArrowSchema) -> Result<WriterProperties, String> {
        Self::build_with_generation(config, None, schema)
    }

    /// Builds WriterProperties with an optional writer generation stored as key-value metadata.
    pub fn build_with_generation(config: &NativeSettings, writer_generation: Option<i64>, schema: &ArrowSchema) -> Result<WriterProperties, String> {
        let mut builder = WriterProperties::builder();

        // Apply compression settings
        builder = Self::apply_compression_settings(builder, config)?;

        // Apply page settings
        builder = Self::apply_page_settings(builder, config);

        // Apply row group settings
        builder = Self::apply_row_group_settings(builder, config);

        // Apply dictionary settings
        builder = Self::apply_dictionary_settings(builder, config);

        // Apply bloom filter settings
        builder = Self::apply_bloom_filter_settings(builder, config);

        // Apply field-level configurations
        builder = Self::apply_field_configs(builder, config, schema)?;

        // Store writer generation in file-level key-value metadata
        if let Some(gen) = writer_generation {
            builder = builder.set_key_value_metadata(Some(vec![
                KeyValue::new(WRITER_GENERATION_KEY.to_string(), Some(gen.to_string())),
            ]));
        }

        Ok(builder.build())
    }

    /// Applies compression settings to the builder.
    fn apply_compression_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> Result<parquet::file::properties::WriterPropertiesBuilder, String> {
        let compression = Self::parse_compression_type(
            config.get_compression_type(),
            config.get_compression_level()
        )?;
        Ok(builder.set_compression(compression))
    }

    /// Applies page size and row limit settings.
    fn apply_page_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder
            .set_data_page_size_limit(config.get_page_size_bytes())
            .set_data_page_row_count_limit(config.get_page_row_limit())
    }

    /// Applies row group row count limit.
    /// In parquet-rs 57.x, `set_max_row_group_size` is a row count limit (not bytes).
    fn apply_row_group_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder.set_max_row_group_size(config.get_row_group_max_rows())
    }

    /// Applies dictionary encoding settings.
    fn apply_dictionary_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder.set_dictionary_page_size_limit(config.get_dict_size_bytes())
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
        config: &NativeSettings,
        schema: &ArrowSchema,
    ) -> Result<parquet::file::properties::WriterPropertiesBuilder, String> {
        let type_map: std::collections::HashMap<&str, (&arrow::datatypes::DataType, String)> =
            schema.fields().iter()
                .map(|f| (f.name().as_str(), (f.data_type(), arrow_type_key(f.data_type()))))
                .collect();

        let mut field_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        if let Some(fc) = &config.field_configs {
            for name in fc.keys() { field_names.insert(name.as_str()); }
        }
        for f in schema.fields() { field_names.insert(f.name().as_str()); }

        for field_name in field_names {
            let index_cfg = config.field_configs.as_ref().and_then(|m| m.get(field_name));

            let (arrow_type, type_key) = match type_map.get(field_name) {
                Some(v) => (v.0, Some(v.1.as_str())),
                None => return Err(format!(
                    "Field '{}' in field_configs does not exist in schema", field_name
                )),
            };

            // Encoding: parse once, validate, apply. Skip if nothing set.
            let encoding: Option<Encoding> = if let Some(enc) = index_cfg.and_then(|fc| fc.encoding_type.as_deref()) {
                let parsed = Self::parse_encoding_type(enc)?;
                if !Self::is_encoding_valid_for_type(parsed, arrow_type) {
                    return Err(format!(
                        "Encoding '{}' is not supported for field '{}' of type '{:?}'",
                        enc, field_name, arrow_type
                    ));
                }
                Some(parsed)
            } else if let Some(enc) = type_key.and_then(|t| config.type_encoding_configs.as_ref()?.get(t).map(|s| s.as_str())) {
                let parsed = Self::parse_encoding_type(enc)?;
                if !Self::is_encoding_valid_for_type(parsed, arrow_type) {
                    return Err(format!(
                        "Cluster-level encoding '{}' is not supported for type '{:?}' (field '{}')",
                        enc, arrow_type, field_name
                    ));
                }
                Some(parsed)
            } else {
                None
            };

            if let Some(enc) = encoding {
                if matches!(enc, Encoding::DELTA_BINARY_PACKED | Encoding::DELTA_BYTE_ARRAY
                    | Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::BYTE_STREAM_SPLIT
                    | Encoding::RLE) {
                    builder = builder.set_column_dictionary_enabled(
                        field_name.to_string().into(), false
                    );
                    builder = builder.set_column_encoding(field_name.to_string().into(), enc);
                } else if matches!(enc, Encoding::RLE_DICTIONARY) {
                    // RLE_DICTIONARY means use dictionary encoding - just ensure it's enabled
                    builder = builder.set_column_dictionary_enabled(
                        field_name.to_string().into(), true
                    );
                } else {
                    builder = builder.set_column_encoding(field_name.to_string().into(), enc);
                }
            }

            // Compression: parse once, apply. No type restriction.
            let level = index_cfg.and_then(|fc| fc.compression_level).unwrap_or(3);
            let compression: Option<Compression> = if let Some(comp) = index_cfg.and_then(|fc| fc.compression_type.as_deref()) {
                Some(Self::parse_compression_type(comp, level)?)
            } else if let Some(comp) = type_key.and_then(|t| config.type_compression_configs.as_ref()?.get(t).map(|s| s.as_str())) {
                Some(Self::parse_compression_type(comp, level)?)
            } else {
                None
            };

            if let Some(comp) = compression {
                builder = builder.set_column_compression(field_name.to_string().into(), comp);
            }
        }
        Ok(builder)
    }

    fn is_encoding_valid_for_type(encoding: Encoding, dt: &arrow::datatypes::DataType) -> bool {
        use arrow::datatypes::DataType::*;
        match encoding {
            Encoding::PLAIN => true,
            Encoding::RLE_DICTIONARY => !matches!(dt, Boolean),
            Encoding::RLE => matches!(dt, Boolean),
            Encoding::DELTA_BINARY_PACKED => matches!(dt,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
            ),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => matches!(dt,
                Utf8 | LargeUtf8 | Binary | LargeBinary
            ),
            Encoding::DELTA_BYTE_ARRAY => matches!(dt,
                Utf8 | LargeUtf8 | Binary | LargeBinary | FixedSizeBinary(_)
            ),
            Encoding::BYTE_STREAM_SPLIT => matches!(dt,
                Float32 | Float64 | Int32 | Int64 | UInt32 | UInt64 | FixedSizeBinary(_)
            ),
            _ => true,
        }
    }

    /// Parses encoding type string to Parquet Encoding enum.
    fn parse_encoding_type(encoding_type: &str) -> Result<Encoding, String> {
        match encoding_type.to_uppercase().as_str() {
            "PLAIN" => Ok(Encoding::PLAIN),
            "RLE" => Ok(Encoding::RLE),
            "RLE_DICTIONARY" | "DICTIONARY" => Ok(Encoding::RLE_DICTIONARY),
            "DELTA_BINARY_PACKED" | "DELTA" => Ok(Encoding::DELTA_BINARY_PACKED),
            "DELTA_LENGTH_BYTE_ARRAY" => Ok(Encoding::DELTA_LENGTH_BYTE_ARRAY),
            "DELTA_BYTE_ARRAY" => Ok(Encoding::DELTA_BYTE_ARRAY),
            "BYTE_STREAM_SPLIT" => Ok(Encoding::BYTE_STREAM_SPLIT),
            other => Err(format!("Unknown encoding type: '{}'", other)),
        }
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
    fn parse_compression_type(compression_type: &str, level: i32) -> Result<Compression, String> {
        match compression_type.to_uppercase().as_str() {
            "ZSTD" => Ok(Compression::ZSTD(
                ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default())
            )),
            "SNAPPY" => Ok(Compression::SNAPPY),
            "GZIP" => Ok(Compression::GZIP(
                GzipLevel::try_new(level as u32).unwrap_or_default()
            )),
            "BROTLI" => Ok(Compression::BROTLI(
                BrotliLevel::try_new(level as u32).unwrap_or_default()
            )),
            "LZ4_RAW" => Ok(Compression::LZ4_RAW),
            "UNCOMPRESSED" => Ok(Compression::UNCOMPRESSED),
            other => Err(format!("Unknown compression type: '{}'", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_settings::NativeSettings;
    use crate::field_config::FieldConfig;
    use std::collections::HashMap;
    use arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};

    fn empty_schema() -> ArrowSchema {
        ArrowSchema::new(Vec::<Field>::new())
    }

    fn schema_with(fields: Vec<(&str, ArrowDataType)>) -> ArrowSchema {
        ArrowSchema::new(fields.into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect::<Vec<Field>>())
    }

    #[test]
    fn test_build_with_compression() {
        let config = NativeSettings {
            compression_type: Some("ZSTD".to_string()),
            compression_level: Some(5),
            ..Default::default()
        };
        let props = WriterPropertiesBuilder::build(&config, &empty_schema()).unwrap();
        assert_ne!(props.compression(&parquet::schema::types::ColumnPath::from("test")), Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_parse_compression_types() {
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("ZSTD", 3).unwrap(), Compression::ZSTD(_)));
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("SNAPPY", 0).unwrap(), Compression::SNAPPY));
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("GZIP", 6).unwrap(), Compression::GZIP(_)));
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("LZ4_RAW", 0).unwrap(), Compression::LZ4_RAW));
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("BROTLI", 3).unwrap(), Compression::BROTLI(_)));
        assert!(matches!(WriterPropertiesBuilder::parse_compression_type("UNCOMPRESSED", 0).unwrap(), Compression::UNCOMPRESSED));
        // LZ4 (framed) is deprecated per Parquet spec - use LZ4_RAW instead
        assert!(WriterPropertiesBuilder::parse_compression_type("LZ4", 0).is_err());
        assert!(WriterPropertiesBuilder::parse_compression_type("SNPPY", 0).is_err());
        assert!(WriterPropertiesBuilder::parse_compression_type("unknown", 0).is_err());
    }

    #[test]
    fn test_parse_encoding_types() {
        assert_eq!(WriterPropertiesBuilder::parse_encoding_type("PLAIN").unwrap(), Encoding::PLAIN);
        assert_eq!(WriterPropertiesBuilder::parse_encoding_type("DELTA_BINARY_PACKED").unwrap(), Encoding::DELTA_BINARY_PACKED);
        assert_eq!(WriterPropertiesBuilder::parse_encoding_type("DELTA").unwrap(), Encoding::DELTA_BINARY_PACKED);
        assert_eq!(WriterPropertiesBuilder::parse_encoding_type("RLE_DICTIONARY").unwrap(), Encoding::RLE_DICTIONARY);
        assert_eq!(WriterPropertiesBuilder::parse_encoding_type("DICTIONARY").unwrap(), Encoding::RLE_DICTIONARY);
        assert!(WriterPropertiesBuilder::parse_encoding_type("DELTA_BINRY_PACKED").is_err());
        assert!(WriterPropertiesBuilder::parse_encoding_type("unknown").is_err());
    }

    #[test]
    fn test_field_encoding_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert("row_id".to_string(), FieldConfig {
            encoding_type: Some("DELTA_BINARY_PACKED".to_string()),
            ..Default::default()
        });
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("row_id", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("row_id");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_BINARY_PACKED));
    }

    #[test]
    fn test_field_level_compression_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert("timestamp".to_string(), FieldConfig {
            compression_type: Some("SNAPPY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("timestamp", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("timestamp");
        assert!(matches!(props.compression(&col_path), Compression::SNAPPY),
            "expected SNAPPY but got {:?}", props.compression(&col_path));
    }

    #[test]
    fn test_unknown_field_in_field_configs_returns_error() {
        let mut field_configs = HashMap::new();
        field_configs.insert("nonexistent".to_string(), FieldConfig {
            encoding_type: Some("PLAIN".to_string()),
            ..Default::default()
        });
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("id", ArrowDataType::Int32)]);
        let result = WriterPropertiesBuilder::build(&config, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("nonexistent"));
    }

    #[test]
    fn test_invalid_encoding_for_type_returns_error() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("DELTA_BINARY_PACKED".to_string()), // invalid for utf8
            ..Default::default()
        });
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let result = WriterPropertiesBuilder::build(&config, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("DELTA_BINARY_PACKED"));
    }

    #[test]
    fn test_invalid_cluster_encoding_for_type_returns_error() {
        let mut type_encoding = HashMap::new();
        type_encoding.insert("utf8".to_string(), "DELTA_BINARY_PACKED".to_string()); // invalid for utf8
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let result = WriterPropertiesBuilder::build(&config, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("DELTA_BINARY_PACKED"));
    }

    #[test]
    fn test_valid_encoding_for_type_succeeds() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("DELTA_BYTE_ARRAY".to_string()), // valid for utf8
            ..Default::default()
        });
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_ok());
    }

    #[test]
    fn test_rle_valid_only_for_boolean() {
        let mut field_configs = HashMap::new();
        field_configs.insert("flag".to_string(), FieldConfig {
            encoding_type: Some("RLE".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs.clone()), ..Default::default() };

        // valid for boolean
        let schema = schema_with(vec![("flag", ArrowDataType::Boolean)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_ok());

        // invalid for int32
        field_configs.insert("flag".to_string(), FieldConfig {
            encoding_type: Some("RLE".to_string()),
            ..Default::default()
        });
        let config2 = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema2 = schema_with(vec![("flag", ArrowDataType::Int32)]);
        assert!(WriterPropertiesBuilder::build(&config2, &schema2).is_err());
    }

    #[test]
    fn test_rle_disables_dictionary_for_boolean() {
        let mut field_configs = HashMap::new();
        field_configs.insert("flag".to_string(), FieldConfig {
            encoding_type: Some("RLE".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("flag", ArrowDataType::Boolean)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("flag");
        assert_eq!(props.encoding(&col_path), Some(Encoding::RLE));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_rle_dictionary_enables_dictionary_not_column_encoding() {
        // RLE_DICTIONARY should enable dictionary encoding, not set a column encoding
        // (setting it as column encoding causes parquet-rs error)
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("RLE_DICTIONARY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        // Should NOT set a column-level encoding (that would cause the fallback error)
        assert_eq!(props.encoding(&col_path), None);
        // Dictionary should be enabled
        assert!(props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_dictionary_alias_enables_dictionary() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("DICTIONARY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), None);
        assert!(props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_rle_dictionary_invalid_for_boolean() {
        let mut field_configs = HashMap::new();
        field_configs.insert("flag".to_string(), FieldConfig {
            encoding_type: Some("RLE_DICTIONARY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("flag", ArrowDataType::Boolean)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_err());
    }

    #[test]
    fn test_plain_encoding_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("PLAIN".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), Some(Encoding::PLAIN));
    }

    #[test]
    fn test_delta_binary_packed_disables_dictionary() {
        let mut field_configs = HashMap::new();
        field_configs.insert("age".to_string(), FieldConfig {
            encoding_type: Some("DELTA_BINARY_PACKED".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("age", ArrowDataType::Int32)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("age");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_BINARY_PACKED));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_delta_byte_array_disables_dictionary() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("DELTA_BYTE_ARRAY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_delta_length_byte_array_on_string() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("DELTA_LENGTH_BYTE_ARRAY".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_LENGTH_BYTE_ARRAY));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_byte_stream_split_on_float() {
        let mut field_configs = HashMap::new();
        field_configs.insert("score".to_string(), FieldConfig {
            encoding_type: Some("BYTE_STREAM_SPLIT".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("score", ArrowDataType::Float64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("score");
        assert_eq!(props.encoding(&col_path), Some(Encoding::BYTE_STREAM_SPLIT));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_byte_stream_split_invalid_for_string() {
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            encoding_type: Some("BYTE_STREAM_SPLIT".to_string()),
            ..Default::default()
        });
        let config = NativeSettings { field_configs: Some(field_configs), ..Default::default() };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_err());
    }

    #[test]
    fn test_type_level_encoding_applied() {
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int64".to_string(), "DELTA_BINARY_PACKED".to_string());
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            ..Default::default()
        };
        let schema = schema_with(vec![("age", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("age");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_BINARY_PACKED));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_field_encoding_overrides_type_encoding() {
        // Field-level config should take precedence over type-level config
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int64".to_string(), "DELTA_BINARY_PACKED".to_string());
        let mut field_configs = HashMap::new();
        field_configs.insert("age".to_string(), FieldConfig {
            encoding_type: Some("PLAIN".to_string()),
            ..Default::default()
        });
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("age", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("age");
        assert_eq!(props.encoding(&col_path), Some(Encoding::PLAIN));
    }

    #[test]
    fn test_type_level_compression_applied() {
        let mut type_compression = HashMap::new();
        type_compression.insert("utf8".to_string(), "SNAPPY".to_string());
        let config = NativeSettings {
            type_compression_configs: Some(type_compression),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert!(matches!(props.compression(&col_path), Compression::SNAPPY));
    }

    #[test]
    fn test_field_compression_overrides_type_compression() {
        let mut type_compression = HashMap::new();
        type_compression.insert("utf8".to_string(), "SNAPPY".to_string());
        let mut field_configs = HashMap::new();
        field_configs.insert("name".to_string(), FieldConfig {
            compression_type: Some("ZSTD".to_string()),
            ..Default::default()
        });
        let config = NativeSettings {
            type_compression_configs: Some(type_compression),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert!(matches!(props.compression(&col_path), Compression::ZSTD(_)));
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
}
