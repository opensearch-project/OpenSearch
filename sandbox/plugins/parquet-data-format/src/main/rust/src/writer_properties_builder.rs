/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::metadata::{FileMetaData, KeyValue};
use parquet::file::properties::WriterProperties;

use arrow::datatypes::Schema as ArrowSchema;

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
    pub fn build(
        config: &NativeSettings,
        schema: &ArrowSchema,
    ) -> Result<WriterProperties, String> {
        Self::build_with_generation(config, None, schema)
    }

    /// Builds WriterProperties with an optional writer generation stored as key-value metadata.
    pub fn build_with_generation(
        config: &NativeSettings,
        writer_generation: Option<i64>,
        schema: &ArrowSchema,
    ) -> Result<WriterProperties, String> {
        let mut builder = WriterProperties::builder();

        // Apply compression settings
        builder = Self::apply_compression_settings(builder, config)?;

        // Apply page settings
        builder = Self::apply_page_settings(builder, config);

        // Apply row group settings
        builder = Self::apply_row_group_settings(builder, config);

        // Apply dictionary settings
        builder = Self::apply_dictionary_settings(builder, config);

        // Apply field-level configurations
        builder = Self::apply_field_configs(builder, config, schema)?;

        // Build the full KV metadata vector. Format version is always stamped; writer
        // generation is stamped only when provided.
        let mut kv_metadata = vec![KeyValue::new(
            FORMAT_VERSION_KEY.to_string(),
            Some(FORMAT_VERSION.to_string()),
        )];
        if let Some(gen) = writer_generation {
            kv_metadata.push(KeyValue::new(
                WRITER_GENERATION_KEY.to_string(),
                Some(gen.to_string()),
            ));
        }
        builder = builder.set_key_value_metadata(Some(kv_metadata));

        Ok(builder.build())
    }

    /// Applies compression settings to the builder.
    fn apply_compression_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> Result<parquet::file::properties::WriterPropertiesBuilder, String> {
        let compression = Self::parse_compression_type(
            config.get_compression_type(),
            config.get_compression_level(),
        )?;
        Ok(builder.set_compression(compression))
    }

    /// Applies page size and row limit settings.
    fn apply_page_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder
            .set_data_page_size_limit(config.get_page_size_bytes())
            .set_data_page_row_count_limit(config.get_page_row_limit())
    }

    /// Applies row group row count and byte size limits.
    fn apply_row_group_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder
            .set_max_row_group_row_count(Some(config.get_row_group_max_rows()))
            .set_max_row_group_bytes(Some(config.get_row_group_max_bytes()))
    }

    /// Applies dictionary encoding settings.
    fn apply_dictionary_settings(
        builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder.set_dictionary_page_size_limit(config.get_dict_size_bytes())
    }

    /// Applies bloom filter settings.
    /// Applies field-level configurations.
    fn apply_field_configs(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
        schema: &ArrowSchema,
    ) -> Result<parquet::file::properties::WriterPropertiesBuilder, String> {
        let type_map: std::collections::HashMap<&str, (&arrow::datatypes::DataType, String)> =
            schema
                .fields()
                .iter()
                .map(|f| {
                    (
                        f.name().as_str(),
                        (f.data_type(), arrow_type_key(f.data_type())),
                    )
                })
                .collect();

        let mut field_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        if let Some(fc) = &config.field_configs {
            for name in fc.keys() {
                field_names.insert(name.as_str());
            }
        }
        for f in schema.fields() {
            field_names.insert(f.name().as_str());
        }

        for field_name in field_names {
            let index_cfg = config
                .field_configs
                .as_ref()
                .and_then(|m| m.get(field_name));

            let (arrow_type, type_key) = match type_map.get(field_name) {
                Some(v) => (v.0, Some(v.1.as_str())),
                None => {
                    return Err(format!(
                        "Field '{}' in field_configs does not exist in schema",
                        field_name
                    ))
                }
            };

            // Encoding: parse once, validate, apply. Skip if nothing set.
            let encoding: Option<Encoding> =
                if let Some(enc) = index_cfg.and_then(|fc| fc.encoding_type.as_deref()) {
                    let parsed = Self::parse_encoding_type(enc)?;
                    if !Self::is_encoding_valid_for_type(parsed, arrow_type) {
                        return Err(format!(
                            "Encoding '{}' is not supported for field '{}' of type '{:?}'",
                            enc, field_name, arrow_type
                        ));
                    }
                    Some(parsed)
                } else if let Some(enc) = type_key.and_then(|t| {
                    config
                        .type_encoding_configs
                        .as_ref()?
                        .get(t)
                        .map(|s| s.as_str())
                }) {
                    let parsed = Self::parse_encoding_type(enc)?;
                    if !Self::is_encoding_valid_for_type(parsed, arrow_type) {
                        return Err(format!(
                        "Cluster-level encoding '{}' is not supported for type '{:?}' (field '{}')",
                        enc, arrow_type, field_name
                    ));
                    }
                    Some(parsed)
                } else {
                    // Metadata field defaults by name, then type-based defaults
                    match field_name {
                        "__row_id__" | "_seq_no" | "_primary_term" | "_version" => {
                            Some(Encoding::DELTA_BINARY_PACKED)
                        }
                        "_id" => Some(Encoding::PLAIN),
                        _ => match type_key {
                            Some("timestamp") => Some(Encoding::DELTA_BINARY_PACKED),
                            _ => None,
                        },
                    }
                };

            if let Some(enc) = encoding {
                if matches!(
                    enc,
                    Encoding::DELTA_BINARY_PACKED
                        | Encoding::DELTA_BYTE_ARRAY
                        | Encoding::DELTA_LENGTH_BYTE_ARRAY
                        | Encoding::BYTE_STREAM_SPLIT
                        | Encoding::RLE
                ) {
                    builder =
                        builder.set_column_dictionary_enabled(field_name.to_string().into(), false);
                    builder = builder.set_column_encoding(field_name.to_string().into(), enc);
                } else if matches!(enc, Encoding::RLE_DICTIONARY) {
                    // RLE_DICTIONARY means use dictionary encoding - just ensure it's enabled
                    builder =
                        builder.set_column_dictionary_enabled(field_name.to_string().into(), true);
                } else {
                    // PLAIN: explicitly disable dictionary so the writer uses plain encoding
                    builder =
                        builder.set_column_dictionary_enabled(field_name.to_string().into(), false);
                    builder = builder.set_column_encoding(field_name.to_string().into(), enc);
                }
            }

            // Compression: parse once, apply. No type restriction.
            let level = index_cfg.and_then(|fc| fc.compression_level).unwrap_or(3);
            let compression: Option<Compression> =
                if let Some(comp) = index_cfg.and_then(|fc| fc.compression_type.as_deref()) {
                    Some(Self::parse_compression_type(comp, level)?)
                } else if let Some(comp) = type_key.and_then(|t| {
                    config
                        .type_compression_configs
                        .as_ref()?
                        .get(t)
                        .map(|s| s.as_str())
                }) {
                    Some(Self::parse_compression_type(comp, level)?)
                } else {
                    // Metadata field defaults by name, then type-based defaults
                    match field_name {
                        "_primary_term" | "_version" => Some(Compression::UNCOMPRESSED),
                        "_id" => Some(Self::parse_compression_type("LZ4_RAW", 0)?),
                        _ => match type_key {
                            Some("utf8") | Some("binary") => {
                                Some(Self::parse_compression_type("ZSTD", 3)?)
                            }
                            _ => None,
                        },
                    }
                };

            if let Some(comp) = compression {
                builder = builder.set_column_compression(field_name.to_string().into(), comp);
            }

            // Bloom filter: field-level > type-level > global. Applied per-column.
            let bf_enabled = index_cfg
                .and_then(|fc| fc.bloom_filter_enabled)
                .or_else(|| {
                    type_key
                        .and_then(|t| config.type_bloom_filter_enabled.as_ref()?.get(t).copied())
                })
                .unwrap_or(config.get_bloom_filter_enabled());
            if bf_enabled {
                builder =
                    builder.set_column_bloom_filter_enabled(field_name.to_string().into(), true);
                let bf_fpp = index_cfg
                    .and_then(|fc| fc.bloom_filter_fpp)
                    .or_else(|| {
                        type_key
                            .and_then(|t| config.type_bloom_filter_fpp.as_ref()?.get(t).copied())
                    })
                    .unwrap_or(config.get_bloom_filter_fpp());
                builder =
                    builder.set_column_bloom_filter_fpp(field_name.to_string().into(), bf_fpp);
                let bf_ndv = index_cfg
                    .and_then(|fc| fc.bloom_filter_ndv)
                    .or_else(|| {
                        type_key
                            .and_then(|t| config.type_bloom_filter_ndv.as_ref()?.get(t).copied())
                    })
                    .unwrap_or(config.get_bloom_filter_ndv());
                builder =
                    builder.set_column_bloom_filter_ndv(field_name.to_string().into(), bf_ndv);
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
            Encoding::DELTA_BINARY_PACKED => matches!(
                dt,
                Int8 | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Timestamp(_, _)
                    | Date32
                    | Date64
                    | Time32(_)
                    | Time64(_)
                    | Duration(_)
            ),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                matches!(dt, Utf8 | LargeUtf8 | Binary | LargeBinary)
            }
            Encoding::DELTA_BYTE_ARRAY => matches!(
                dt,
                Utf8 | LargeUtf8 | Binary | LargeBinary | FixedSizeBinary(_)
            ),
            Encoding::BYTE_STREAM_SPLIT => matches!(
                dt,
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
                ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()),
            )),
            "SNAPPY" => Ok(Compression::SNAPPY),
            "GZIP" => Ok(Compression::GZIP(
                GzipLevel::try_new(level as u32).unwrap_or_default(),
            )),
            "BROTLI" => Ok(Compression::BROTLI(
                BrotliLevel::try_new(level as u32).unwrap_or_default(),
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
    use crate::field_config::FieldConfig;
    use crate::native_settings::NativeSettings;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use std::collections::HashMap;

    fn empty_schema() -> ArrowSchema {
        ArrowSchema::new(Vec::<Field>::new())
    }

    fn schema_with(fields: Vec<(&str, ArrowDataType)>) -> ArrowSchema {
        ArrowSchema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<Field>>(),
        )
    }

    #[test]
    fn test_build_with_compression() {
        let config = NativeSettings {
            compression_type: Some("ZSTD".to_string()),
            compression_level: Some(5),
            ..Default::default()
        };
        let props = WriterPropertiesBuilder::build(&config, &empty_schema()).unwrap();
        assert_ne!(
            props.compression(&parquet::schema::types::ColumnPath::from("test")),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn test_parse_compression_types() {
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("ZSTD", 3).unwrap(),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("SNAPPY", 0).unwrap(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("GZIP", 6).unwrap(),
            Compression::GZIP(_)
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("LZ4_RAW", 0).unwrap(),
            Compression::LZ4_RAW
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("BROTLI", 3).unwrap(),
            Compression::BROTLI(_)
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("UNCOMPRESSED", 0).unwrap(),
            Compression::UNCOMPRESSED
        ));
        // LZ4 (framed) is deprecated per Parquet spec - use LZ4_RAW instead
        assert!(WriterPropertiesBuilder::parse_compression_type("LZ4", 0).is_err());
        assert!(WriterPropertiesBuilder::parse_compression_type("SNPPY", 0).is_err());
        assert!(WriterPropertiesBuilder::parse_compression_type("unknown", 0).is_err());
    }

    #[test]
    fn test_parse_encoding_types() {
        assert_eq!(
            WriterPropertiesBuilder::parse_encoding_type("PLAIN").unwrap(),
            Encoding::PLAIN
        );
        assert_eq!(
            WriterPropertiesBuilder::parse_encoding_type("DELTA_BINARY_PACKED").unwrap(),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            WriterPropertiesBuilder::parse_encoding_type("DELTA").unwrap(),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            WriterPropertiesBuilder::parse_encoding_type("RLE_DICTIONARY").unwrap(),
            Encoding::RLE_DICTIONARY
        );
        assert_eq!(
            WriterPropertiesBuilder::parse_encoding_type("DICTIONARY").unwrap(),
            Encoding::RLE_DICTIONARY
        );
        assert!(WriterPropertiesBuilder::parse_encoding_type("DELTA_BINRY_PACKED").is_err());
        assert!(WriterPropertiesBuilder::parse_encoding_type("unknown").is_err());
    }

    #[test]
    fn test_field_encoding_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "row_id".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_BINARY_PACKED".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("row_id", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("row_id");
        assert_eq!(
            props.encoding(&col_path),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
    }

    #[test]
    fn test_field_level_compression_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "timestamp".to_string(),
            FieldConfig {
                compression_type: Some("SNAPPY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("timestamp", ArrowDataType::Int64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("timestamp");
        assert!(
            matches!(props.compression(&col_path), Compression::SNAPPY),
            "expected SNAPPY but got {:?}",
            props.compression(&col_path)
        );
    }

    #[test]
    fn test_unknown_field_in_field_configs_returns_error() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "nonexistent".to_string(),
            FieldConfig {
                encoding_type: Some("PLAIN".to_string()),
                ..Default::default()
            },
        );
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
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_BINARY_PACKED".to_string()), // invalid for utf8
                ..Default::default()
            },
        );
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
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_BYTE_ARRAY".to_string()), // valid for utf8
                ..Default::default()
            },
        );
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
        field_configs.insert(
            "flag".to_string(),
            FieldConfig {
                encoding_type: Some("RLE".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs.clone()),
            ..Default::default()
        };

        // valid for boolean
        let schema = schema_with(vec![("flag", ArrowDataType::Boolean)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_ok());

        // invalid for int32
        field_configs.insert(
            "flag".to_string(),
            FieldConfig {
                encoding_type: Some("RLE".to_string()),
                ..Default::default()
            },
        );
        let config2 = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema2 = schema_with(vec![("flag", ArrowDataType::Int32)]);
        assert!(WriterPropertiesBuilder::build(&config2, &schema2).is_err());
    }

    #[test]
    fn test_rle_disables_dictionary_for_boolean() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "flag".to_string(),
            FieldConfig {
                encoding_type: Some("RLE".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
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
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("RLE_DICTIONARY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
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
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("DICTIONARY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), None);
        assert!(props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_rle_dictionary_invalid_for_boolean() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "flag".to_string(),
            FieldConfig {
                encoding_type: Some("RLE_DICTIONARY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("flag", ArrowDataType::Boolean)]);
        assert!(WriterPropertiesBuilder::build(&config, &schema).is_err());
    }

    #[test]
    fn test_plain_encoding_applied() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("PLAIN".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), Some(Encoding::PLAIN));
        assert!(
            !props.dictionary_enabled(&col_path),
            "PLAIN encoding should explicitly disable dictionary"
        );
    }

    #[test]
    fn test_delta_binary_packed_disables_dictionary() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "age".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_BINARY_PACKED".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("age", ArrowDataType::Int32)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("age");
        assert_eq!(
            props.encoding(&col_path),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_delta_byte_array_disables_dictionary() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_BYTE_ARRAY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(props.encoding(&col_path), Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_delta_length_byte_array_on_string() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("DELTA_LENGTH_BYTE_ARRAY".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("name");
        assert_eq!(
            props.encoding(&col_path),
            Some(Encoding::DELTA_LENGTH_BYTE_ARRAY)
        );
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_byte_stream_split_on_float() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "score".to_string(),
            FieldConfig {
                encoding_type: Some("BYTE_STREAM_SPLIT".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("score", ArrowDataType::Float64)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("score");
        assert_eq!(props.encoding(&col_path), Some(Encoding::BYTE_STREAM_SPLIT));
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_byte_stream_split_invalid_for_string() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                encoding_type: Some("BYTE_STREAM_SPLIT".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            field_configs: Some(field_configs),
            ..Default::default()
        };
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
        assert_eq!(
            props.encoding(&col_path),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert!(!props.dictionary_enabled(&col_path));
    }

    #[test]
    fn test_field_encoding_overrides_type_encoding() {
        // Field-level config should take precedence over type-level config
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int64".to_string(), "DELTA_BINARY_PACKED".to_string());
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "age".to_string(),
            FieldConfig {
                encoding_type: Some("PLAIN".to_string()),
                ..Default::default()
            },
        );
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
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                compression_type: Some("ZSTD".to_string()),
                ..Default::default()
            },
        );
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
        let schema = schema_with(vec![("test_col", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
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
        let schema = schema_with(vec![("test_col", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
        let bf_props = props.bloom_filter_properties(&col_path);
        assert!(
            bf_props.is_some(),
            "bloom_filter_properties should be Some when enabled"
        );
    }

    #[test]
    fn test_bloom_filter_default_is_false() {
        let config = NativeSettings::default();
        assert_eq!(config.get_bloom_filter_enabled(), false);
        let schema = schema_with(vec![("test_col", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("test_col");
        let bf_props = props.bloom_filter_properties(&col_path);
        assert!(
            bf_props.is_none(),
            "Default should have bloom filter disabled"
        );
    }

    #[test]
    fn test_column_bloom_filter_disabled_overrides_global() {
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                bloom_filter_enabled: Some(false),
                ..Default::default()
            },
        );
        field_configs.insert(
            "value".to_string(),
            FieldConfig {
                bloom_filter_enabled: Some(true),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            bloom_filter_enabled: Some(true),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("value", ArrowDataType::Int32),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        let value_path = parquet::schema::types::ColumnPath::from("value");

        let name_bf = props.bloom_filter_properties(&name_path);
        assert!(
            name_bf.is_none(),
            "name should have no bloom filter, got: {:?}",
            name_bf
        );

        let value_bf = props.bloom_filter_properties(&value_path);
        assert!(value_bf.is_some(), "value should have bloom filter");
    }

    #[test]
    fn test_type_level_bloom_filter_overrides_global() {
        let mut type_bf_enabled = HashMap::new();
        type_bf_enabled.insert("utf8".to_string(), true);
        let config = NativeSettings {
            bloom_filter_enabled: Some(false),
            type_bloom_filter_enabled: Some(type_bf_enabled),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("age", ArrowDataType::Int32),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        let age_path = parquet::schema::types::ColumnPath::from("age");

        assert!(
            props.bloom_filter_properties(&name_path).is_some(),
            "utf8 type-level enabled should override global disabled"
        );
        assert!(
            props.bloom_filter_properties(&age_path).is_none(),
            "int32 has no type-level setting, should use global disabled"
        );
    }

    #[test]
    fn test_field_bloom_filter_overrides_type_level() {
        let mut type_bf_enabled = HashMap::new();
        type_bf_enabled.insert("utf8".to_string(), true);
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                bloom_filter_enabled: Some(false),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            bloom_filter_enabled: Some(false),
            type_bloom_filter_enabled: Some(type_bf_enabled),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("name", ArrowDataType::Utf8)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        assert!(
            props.bloom_filter_properties(&name_path).is_none(),
            "field-level disabled should override type-level enabled"
        );
    }

    #[test]
    fn test_bloom_filter_fpp_ndv_3tier_fallback() {
        // Type-level sets fpp/ndv for utf8, field-level overrides fpp for "name"
        let mut type_bf_enabled = HashMap::new();
        type_bf_enabled.insert("utf8".to_string(), true);
        type_bf_enabled.insert("int32".to_string(), true);
        let mut type_bf_fpp = HashMap::new();
        type_bf_fpp.insert("utf8".to_string(), 0.05);
        let mut type_bf_ndv = HashMap::new();
        type_bf_ndv.insert("utf8".to_string(), 50_000u64);

        let mut field_configs = HashMap::new();
        field_configs.insert(
            "name".to_string(),
            FieldConfig {
                bloom_filter_enabled: Some(true),
                bloom_filter_fpp: Some(0.01),
                bloom_filter_ndv: Some(200_000),
                ..Default::default()
            },
        );
        field_configs.insert(
            "title".to_string(),
            FieldConfig {
                bloom_filter_enabled: Some(true),
                ..Default::default()
            },
        );

        let config = NativeSettings {
            bloom_filter_enabled: Some(true),
            bloom_filter_fpp: Some(0.1),
            bloom_filter_ndv: Some(100_000),
            type_bloom_filter_enabled: Some(type_bf_enabled),
            type_bloom_filter_fpp: Some(type_bf_fpp),
            type_bloom_filter_ndv: Some(type_bf_ndv),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("title", ArrowDataType::Utf8),
            ("age", ArrowDataType::Int32),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        // "name": field-level fpp=0.01, ndv=200000 (overrides type utf8 fpp=0.05, ndv=50000)
        let name_bf = props
            .bloom_filter_properties(&parquet::schema::types::ColumnPath::from("name"))
            .unwrap();
        assert!(
            (name_bf.fpp() - 0.01).abs() < 1e-9,
            "name fpp should be 0.01, got {}",
            name_bf.fpp()
        );
        assert_eq!(name_bf.ndv(), 200_000, "name ndv should be 200000");

        // "title": field-level enabled but no fpp/ndv -> falls to type utf8 fpp=0.05, ndv=50000
        let title_bf = props
            .bloom_filter_properties(&parquet::schema::types::ColumnPath::from("title"))
            .unwrap();
        assert!(
            (title_bf.fpp() - 0.05).abs() < 1e-9,
            "title fpp should be 0.05, got {}",
            title_bf.fpp()
        );
        assert_eq!(title_bf.ndv(), 50_000, "title ndv should be 50000");

        // "age": int32 type-level enabled but no fpp/ndv -> falls to global fpp=0.1, ndv=100000
        let age_bf = props
            .bloom_filter_properties(&parquet::schema::types::ColumnPath::from("age"))
            .unwrap();
        assert!(
            (age_bf.fpp() - 0.1).abs() < 1e-9,
            "age fpp should be 0.1, got {}",
            age_bf.fpp()
        );
        assert_eq!(age_bf.ndv(), 100_000, "age ndv should be 100000");
    }

    #[test]
    fn test_type_level_encoding_only_applies_to_matching_type() {
        // int64 encoding should NOT apply to utf8 columns
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int64".to_string(), "DELTA_BINARY_PACKED".to_string());
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("age", ArrowDataType::Int64),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        let age_path = parquet::schema::types::ColumnPath::from("age");

        // "name" (utf8) should have no explicit encoding (uses default)
        assert_eq!(props.encoding(&name_path), None);
        // "age" (int64) should have DELTA_BINARY_PACKED
        assert_eq!(
            props.encoding(&age_path),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
    }

    #[test]
    fn test_type_level_compression_only_applies_to_matching_type() {
        let mut type_compression = HashMap::new();
        type_compression.insert("utf8".to_string(), "SNAPPY".to_string());
        let config = NativeSettings {
            type_compression_configs: Some(type_compression),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("age", ArrowDataType::Int32),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        let age_path = parquet::schema::types::ColumnPath::from("age");

        assert!(matches!(props.compression(&name_path), Compression::SNAPPY));
        // age (int32) should use global default (LZ4_RAW from build())
        assert!(!matches!(props.compression(&age_path), Compression::SNAPPY));
    }

    #[test]
    fn test_multiple_type_level_encodings_for_different_types() {
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int32".to_string(), "DELTA_BINARY_PACKED".to_string());
        type_encoding.insert("utf8".to_string(), "DELTA_BYTE_ARRAY".to_string());
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("name", ArrowDataType::Utf8),
            ("age", ArrowDataType::Int32),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let name_path = parquet::schema::types::ColumnPath::from("name");
        let age_path = parquet::schema::types::ColumnPath::from("age");

        assert_eq!(props.encoding(&name_path), Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(!props.dictionary_enabled(&name_path));
        assert_eq!(
            props.encoding(&age_path),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert!(!props.dictionary_enabled(&age_path));
    }

    #[test]
    fn test_field_encoding_plain_overrides_type_disables_dictionary() {
        // Field sets PLAIN, type sets DELTA_BINARY_PACKED — field wins and dictionary is disabled
        let mut type_encoding = HashMap::new();
        type_encoding.insert("int32".to_string(), "DELTA_BINARY_PACKED".to_string());
        let mut field_configs = HashMap::new();
        field_configs.insert(
            "age".to_string(),
            FieldConfig {
                encoding_type: Some("PLAIN".to_string()),
                ..Default::default()
            },
        );
        let config = NativeSettings {
            type_encoding_configs: Some(type_encoding),
            field_configs: Some(field_configs),
            ..Default::default()
        };
        let schema = schema_with(vec![("age", ArrowDataType::Int32)]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();
        let col_path = parquet::schema::types::ColumnPath::from("age");
        assert_eq!(props.encoding(&col_path), Some(Encoding::PLAIN));
        assert!(
            !props.dictionary_enabled(&col_path),
            "PLAIN should disable dictionary even when overriding type-level"
        );
    }

    #[test]
    fn test_type_level_compression_overrides_global() {
        let mut type_compression = HashMap::new();
        type_compression.insert("int32".to_string(), "ZSTD".to_string());
        let config = NativeSettings {
            compression_type: Some("SNAPPY".to_string()),
            type_compression_configs: Some(type_compression),
            ..Default::default()
        };
        let schema = schema_with(vec![
            ("age", ArrowDataType::Int32),
            ("name", ArrowDataType::Utf8),
        ]);
        let props = WriterPropertiesBuilder::build(&config, &schema).unwrap();

        let age_path = parquet::schema::types::ColumnPath::from("age");
        let name_path = parquet::schema::types::ColumnPath::from("name");

        assert!(
            matches!(props.compression(&age_path), Compression::ZSTD(_)),
            "int32 type-level ZSTD should override global SNAPPY"
        );
        assert!(
            matches!(props.compression(&name_path), Compression::ZSTD(_)),
            "utf8 has type-level set to ZSTD"
        );
    }

    #[test]
    fn test_build_stamps_format_version() {
        let config = NativeSettings::default();
        let schema = ArrowSchema::new(Vec::<Field>::new());
        let props = WriterPropertiesBuilder::build(&config, &schema).expect("build failed");
        let kv = props.key_value_metadata().expect("KV metadata missing");
        let found = kv.iter().find(|k| k.key == FORMAT_VERSION_KEY);
        let entry = found.expect("format_version KV entry missing");
        assert_eq!(entry.value.as_deref(), Some(FORMAT_VERSION));
    }

    #[test]
    fn test_build_with_generation_stamps_both() {
        let config = NativeSettings::default();
        let schema = ArrowSchema::new(Vec::<Field>::new());
        let props = WriterPropertiesBuilder::build_with_generation(&config, Some(42), &schema)
            .expect("build failed");
        let kv = props.key_value_metadata().expect("KV metadata missing");
        let has_format = kv
            .iter()
            .any(|k| k.key == FORMAT_VERSION_KEY && k.value.as_deref() == Some(FORMAT_VERSION));
        let has_gen = kv
            .iter()
            .any(|k| k.key == WRITER_GENERATION_KEY && k.value.as_deref() == Some("42"));
        assert!(has_format, "format_version stamp missing");
        assert!(has_gen, "writer_generation stamp missing");
    }

    // ---- W1 (DF55 / arrow-59): production write data-page-layout coverage ----
    //
    // arrow-59 (#9972) byte-bounds oversized data pages for wide variable-width
    // values; production sets `data_page_size_limit = 1 MiB` + `page_row_limit =
    // 20_000` + row-group limits (`native_settings.rs`). Prior to these tests NO
    // coverage asserted the WRITTEN page/row-group layout, so an arrow page-sizing
    // change could silently reshape output files. A single generic probe writes a
    // batch through the REAL `WriterPropertiesBuilder` and reads the OffsetIndex
    // back; each scenario test below pins a different layout regime (byte-driven /
    // row-driven / row-group split) so a future shift is caught (W1 / arrow-rs#9972).

    #[cfg(test)]
    struct ProbedLayout {
        row_groups: usize,
        total_rows: i64,
        pages_rg0_col0: usize,
        max_compressed_page_size: i32,
    }

    /// Write `batch` through the production `WriterPropertiesBuilder` for `config`
    /// and probe the resulting parquet page/row-group layout via the OffsetIndex.
    #[cfg(test)]
    fn probe_production_write_layout(
        config: &NativeSettings,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ProbedLayout {
        use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
        use parquet::arrow::ArrowWriter;

        let schema = batch.schema();
        let props = WriterPropertiesBuilder::build(config, &schema).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }

        // Round-trip through a temp file (File: ChunkReader), page index enabled.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w1.parquet");
        std::fs::write(&path, &buf).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let md = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
            .unwrap();
        let meta = md.metadata();

        let total_rows: i64 = (0..meta.num_row_groups())
            .map(|i| meta.row_group(i).num_rows())
            .sum();
        let oi = meta
            .offset_index()
            .expect("OffsetIndex present (production statistics default = Page)");
        let pages_rg0_col0 = oi[0][0].page_locations().len();
        let mut max_compressed_page_size = 0i32;
        for rg in 0..meta.num_row_groups() {
            for col in 0..oi[rg].len() {
                for p in oi[rg][col].page_locations() {
                    max_compressed_page_size = max_compressed_page_size.max(p.compressed_page_size);
                }
            }
        }
        ProbedLayout {
            row_groups: meta.num_row_groups(),
            total_rows,
            pages_rg0_col0,
            max_compressed_page_size,
        }
    }

    #[cfg(test)]
    fn utf8_batch(name: &str, values: Vec<String>) -> arrow::record_batch::RecordBatch {
        use arrow::array::StringArray;
        use std::sync::Arc;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            name,
            ArrowDataType::Utf8,
            false,
        )]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values(values))],
        )
        .unwrap()
    }

    /// Byte-driven page splits: ~8 MiB of wide ~1 KiB values, under the 20_000
    /// page-row limit so the 1 MiB BYTE limit governs (exercises arrow-59 #9972).
    #[test]
    fn w1_byte_limit_splits_wide_pages() {
        const ROWS: usize = 8192;
        let batch = utf8_batch(
            "body",
            (0..ROWS)
                .map(|i| format!("{i:08}-{}", "x".repeat(1016)))
                .collect(),
        );
        let l = probe_production_write_layout(&NativeSettings::new(), &batch);
        eprintln!(
            "[W1 byte] rgs={} rows={} pages={} max_page={}B",
            l.row_groups, l.total_rows, l.pages_rg0_col0, l.max_compressed_page_size
        );
        assert_eq!(l.row_groups, 1, "8192 rows < 1M RG limit → one row group");
        assert_eq!(l.total_rows, ROWS as i64, "row count round-trips");
        assert!(
            l.pages_rg0_col0 > 1,
            "wide column must split into >1 data page"
        );
        assert!(
            (4..=16).contains(&l.pages_rg0_col0),
            "byte-driven page count {} outside pinned arrow-59 range 4..=16 (W1 / arrow-rs#9972); re-characterize",
            l.pages_rg0_col0
        );
        assert!(
            l.max_compressed_page_size < 2 * 1024 * 1024,
            "a data page ({}B) wildly exceeds the 1 MiB limit — byte-bounding regressed (W1 / arrow-rs#9972)",
            l.max_compressed_page_size
        );
    }

    /// Row-driven page splits: 45_000 tiny values — too small to hit the 1 MiB byte
    /// limit, so the 20_000 `page_row_limit` governs → ceil(45000/20000)=3 pages.
    #[test]
    fn w1_row_limit_splits_narrow_pages() {
        const ROWS: usize = 45_000;
        let batch = utf8_batch("k", (0..ROWS).map(|i| format!("{i:06}")).collect());
        let l = probe_production_write_layout(&NativeSettings::new(), &batch);
        eprintln!(
            "[W1 row] rgs={} rows={} pages={} max_page={}B",
            l.row_groups, l.total_rows, l.pages_rg0_col0, l.max_compressed_page_size
        );
        assert_eq!(l.row_groups, 1, "45k rows < 1M RG limit → one row group");
        assert_eq!(l.total_rows, ROWS as i64);
        assert_eq!(
            l.pages_rg0_col0, 3,
            "20_000 page-row limit → ceil(45000/20000)=3 data pages, got {}",
            l.pages_rg0_col0
        );
    }

    /// Row-group split: a small `row_group_max_rows` forces multiple row groups.
    #[test]
    fn w1_row_group_max_rows_splits_row_groups() {
        const ROWS: usize = 25_000;
        let config = NativeSettings {
            row_group_max_rows: Some(10_000),
            ..Default::default()
        };
        let batch = utf8_batch("k", (0..ROWS).map(|i| format!("{i:06}")).collect());
        let l = probe_production_write_layout(&config, &batch);
        eprintln!("[W1 rg] rgs={} rows={}", l.row_groups, l.total_rows);
        assert_eq!(l.total_rows, ROWS as i64);
        assert_eq!(
            l.row_groups, 3,
            "row_group_max_rows=10_000 over 25_000 rows → 3 row groups, got {}",
            l.row_groups
        );
    }

    /// Field-type matrix cell: a mixed Utf8 + Int64 batch writes cleanly through the
    /// production props and every column gets an OffsetIndex (no per-type surprise).
    #[test]
    fn w1_mixed_field_types_layout_sane() {
        use arrow::array::{Int64Array, StringArray};
        use std::sync::Arc;
        const ROWS: usize = 5_000;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("s", ArrowDataType::Utf8, false),
            Field::new("n", ArrowDataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    (0..ROWS).map(|i| format!("v{i}")),
                )),
                Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            ],
        )
        .unwrap();
        let l = probe_production_write_layout(&NativeSettings::new(), &batch);
        eprintln!(
            "[W1 mixed] rgs={} rows={} pages_c0={}",
            l.row_groups, l.total_rows, l.pages_rg0_col0
        );
        assert_eq!(l.row_groups, 1);
        assert_eq!(
            l.total_rows, ROWS as i64,
            "mixed-type row count round-trips"
        );
        assert!(
            l.pages_rg0_col0 >= 1,
            "col0 must have an OffsetIndex page entry"
        );
    }
}
