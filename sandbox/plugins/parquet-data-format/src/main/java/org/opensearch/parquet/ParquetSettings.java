/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Settings for Parquet data format.
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 50000;

    /** Data page size limit in bytes (default 1MB). */
    public static final Setting<ByteSizeValue> PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Maximum number of rows per data page (default 20000). */
    public static final Setting<Integer> PAGE_ROW_LIMIT = Setting.intSetting(
        "index.parquet.page_row_limit",
        20000,
        1,
        Setting.Property.IndexScope
    );

    /** Dictionary page size limit in bytes (default 2MB). */
    public static final Setting<ByteSizeValue> DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Compression codec for Parquet files, e.g. ZSTD, SNAPPY, LZ4_RAW (default LZ4_RAW). */
    public static final Setting<String> COMPRESSION_TYPE = Setting.simpleString(
        "index.parquet.compression_type",
        "LZ4_RAW",
        Setting.Property.IndexScope
    );

    /** Compression level for the chosen codec (default 2, range 1–9). */
    public static final Setting<Integer> COMPRESSION_LEVEL = Setting.intSetting(
        "index.parquet.compression_level",
        2,
        1,
        9,
        Setting.Property.IndexScope
    );

    /** Whether bloom filters are enabled for Parquet columns (default true). */
    public static final Setting<Boolean> BLOOM_FILTER_ENABLED = Setting.boolSetting(
        "index.parquet.bloom_filter_enabled",
        true,
        Setting.Property.IndexScope
    );

    /** Bloom filter false positive probability (default 0.1). */
    public static final Setting<Double> BLOOM_FILTER_FPP = Setting.doubleSetting(
        "index.parquet.bloom_filter_fpp",
        0.1,
        0.0,
        1.0,
        Setting.Property.IndexScope
    );

    /** Bloom filter number of distinct values hint (default 100000). */
    public static final Setting<Long> BLOOM_FILTER_NDV = Setting.longSetting(
        "index.parquet.bloom_filter_ndv",
        100_000L,
        1L,
        Setting.Property.IndexScope
    );

    /** Maximum native memory allocation for Arrow buffers, as a percentage of non-heap memory (default 10%). */
    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope
    );

    /** Maximum rows per VectorSchemaRoot before rotation is triggered (default 50000). */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "parquet.max_rows_per_vsr",
        DEFAULT_MAX_ROWS_PER_VSR,
        1,
        Setting.Property.NodeScope
    );

    /** File size threshold for in-memory sort vs streaming merge sort (default 32MB). */
    public static final Setting<ByteSizeValue> SORT_IN_MEMORY_THRESHOLD = Setting.byteSizeSetting(
        "index.parquet.sort_in_memory_threshold",
        new ByteSizeValue(32, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Batch size for streaming merge sort (default 8192 rows). */
    public static final Setting<Integer> SORT_BATCH_SIZE = Setting.intSetting(
        "index.parquet.sort_batch_size",
        8192,
        1,
        Setting.Property.IndexScope
    );

    /** Maximum number of rows per row group (default 1000000). */
    public static final Setting<Integer> ROW_GROUP_MAX_ROWS = Setting.intSetting(
        "index.parquet.row_group_max_rows",
        1_000_000,
        1,
        Setting.Property.IndexScope
    );

    /** Batch size for reading records during merge (default 100000 rows). */
    public static final Setting<Integer> MERGE_BATCH_SIZE = Setting.intSetting(
        "index.parquet.merge_batch_size",
        100_000,
        1,
        Setting.Property.IndexScope
    );

    /** Number of Rayon threads for parallel column encoding during merge (default num_cores/8, min 1). */
    public static final Setting<Integer> MERGE_RAYON_THREADS = Setting.intSetting(
        "parquet.merge_rayon_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 8),
        1,
        Setting.Property.NodeScope
    );

    /** Number of Tokio IO threads for async disk writes during merge (default num_cores/8, min 1). */
    public static final Setting<Integer> MERGE_IO_THREADS = Setting.intSetting(
        "parquet.merge_io_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 8),
        1,
        Setting.Property.NodeScope
    );

    static final Set<String> VALID_ENCODINGS = Set.of(
        "PLAIN",
        "RLE",
        "RLE_DICTIONARY",
        "DICTIONARY",
        "DELTA_BINARY_PACKED",
        "DELTA",
        "DELTA_LENGTH_BYTE_ARRAY",
        "DELTA_BYTE_ARRAY",
        "BYTE_STREAM_SPLIT"
    );

    static final Set<String> VALID_COMPRESSIONS = Set.of("ZSTD", "SNAPPY", "GZIP", "BROTLI", "LZ4_RAW", "UNCOMPRESSED");

    /**
     * Group setting for per-field configuration (encoding and compression).
     * Usage: index.parquet.field.{field_name}.encoding=DELTA_BINARY_PACKED
     *        index.parquet.field.{field_name}.compression=SNAPPY
     * Supported encoding values: PLAIN, RLE, RLE_DICTIONARY, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY,
     *                            DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT
     */
    public static final Setting<Settings> FIELD_SETTINGS = Setting.groupSetting("index.parquet.field.", s -> {
        for (String key : s.keySet()) {
            if (key.endsWith(".encoding")) {
                String value = s.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_ENCODINGS.contains(value)) {
                    throw new IllegalArgumentException("Invalid encoding '" + s.get(key) + "'. Valid values: " + VALID_ENCODINGS);
                }
            } else if (key.endsWith(".compression")) {
                String value = s.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_COMPRESSIONS.contains(value)) {
                    throw new IllegalArgumentException("Invalid compression '" + s.get(key) + "'. Valid values: " + VALID_COMPRESSIONS);
                }
            }
        }
    }, Setting.Property.IndexScope, Setting.Property.Dynamic);

    /**
     * Group setting for per-type encoding configuration (cluster-level fallback).
     * Usage: parquet.type_encoding.{arrow_type}.encoding=DELTA_BINARY_PACKED
     * e.g. parquet.type_encoding.int64.encoding=DELTA_BINARY_PACKED
     */
    public static final Setting<Settings> TYPE_ENCODING_SETTINGS = Setting.groupSetting(
        "parquet.type_encoding.",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Group setting for per-type compression configuration (cluster-level fallback).
     * Usage: parquet.type_compression.{arrow_type}.compression=SNAPPY
     * e.g. parquet.type_compression.utf8.compression=SNAPPY
     */
    public static final Setting<Settings> TYPE_COMPRESSION_SETTINGS = Setting.groupSetting(
        "parquet.type_compression.",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Validates that field-level encodings are compatible with their Arrow types in the schema.
     * Should be called when mappings are available (i.e., documentMapper is not null).
     * Skips fields not present in the schema (e.g., dynamic fields not yet mapped).
     */
    public static void validateEncodingTypeCompatibility(Map<String, String> fieldEncodings, Schema schema) {
        Map<String, ArrowType> arrowTypes = new java.util.HashMap<>();
        for (Field field : schema.getFields()) {
            arrowTypes.put(field.getName(), field.getType());
        }
        for (Map.Entry<String, String> entry : fieldEncodings.entrySet()) {
            String fieldName = entry.getKey();
            String encoding = entry.getValue().toUpperCase(java.util.Locale.ROOT);
            ArrowType arrowType = arrowTypes.get(fieldName);
            if (arrowType == null) {
                throw new IllegalArgumentException(
                    "Field '" + fieldName + "' configured in index.parquet.field settings does not exist in mappings"
                );
            }
            if (!isEncodingValidForArrowType(encoding, arrowType)) {
                throw new IllegalArgumentException(
                    "Encoding '" + encoding + "' is not compatible with field '" + fieldName + "' of type '" + arrowType + "'"
                );
            }
        }
    }

    private static boolean isEncodingValidForArrowType(String encoding, ArrowType arrowType) {
        boolean isInt = arrowType instanceof ArrowType.Int;
        boolean isFloat = arrowType instanceof ArrowType.FloatingPoint;
        boolean isBool = arrowType instanceof ArrowType.Bool;
        boolean isUtf8 = arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8;
        boolean isBinary = arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary;
        boolean isFixedBinary = arrowType instanceof ArrowType.FixedSizeBinary;
        boolean isStringLike = isUtf8 || isBinary;
        switch (encoding) {
            case "PLAIN":
                return true;
            case "RLE_DICTIONARY":
            case "DICTIONARY":
                return !isBool;
            case "RLE":
                return isBool;
            case "DELTA_BINARY_PACKED":
            case "DELTA":
                return isInt;
            case "DELTA_LENGTH_BYTE_ARRAY":
                return isStringLike;
            case "DELTA_BYTE_ARRAY":
                return isStringLike || isFixedBinary;
            case "BYTE_STREAM_SPLIT":
                return isInt || isFloat || isFixedBinary;
            default:
                return true;
        }
    }

    /**
     * Extracts per-field encoding map from index settings.
     * Reads all keys under "index.parquet.field.{field_name}.encoding".
     */
    public static java.util.Map<String, String> getFieldEncodings(Settings settings) {
        Settings fieldSettings = FIELD_SETTINGS.get(settings);
        java.util.Map<String, String> result = new java.util.HashMap<>();
        for (String key : fieldSettings.keySet()) {
            if (key.endsWith(".encoding")) {
                String fieldName = key.substring(0, key.length() - ".encoding".length());
                String value = fieldSettings.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_ENCODINGS.contains(value)) {
                    throw new IllegalArgumentException(
                        "Invalid encoding '" + fieldSettings.get(key) + "' for field '" + fieldName + "'. Valid values: " + VALID_ENCODINGS
                    );
                }
                result.put(fieldName, value);
            }
        }
        return result;
    }

    /**
     * Extracts per-field compression map from index settings.
     * Reads all keys under "index.parquet.field.{field_name}.compression".
     */
    public static java.util.Map<String, String> getFieldCompressions(Settings settings) {
        Settings fieldSettings = FIELD_SETTINGS.get(settings);
        java.util.Map<String, String> result = new java.util.HashMap<>();
        for (String key : fieldSettings.keySet()) {
            if (key.endsWith(".compression")) {
                String fieldName = key.substring(0, key.length() - ".compression".length());
                String value = fieldSettings.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_COMPRESSIONS.contains(value)) {
                    throw new IllegalArgumentException(
                        "Invalid compression '"
                            + fieldSettings.get(key)
                            + "' for field '"
                            + fieldName
                            + "'. Valid values: "
                            + VALID_COMPRESSIONS
                    );
                }
                result.put(fieldName, value);
            }
        }
        return result;
    }

    /**
     * Extracts per-type encoding map from node settings.
     * Reads all keys under "parquet.type_encoding.{arrow_type}.encoding".
     */
    public static java.util.Map<String, String> getTypeEncodings(Settings nodeSettings) {
        Settings typeSettings = TYPE_ENCODING_SETTINGS.get(nodeSettings);
        java.util.Map<String, String> result = new java.util.HashMap<>();
        for (String key : typeSettings.keySet()) {
            if (key.endsWith(".encoding")) {
                String typeName = key.substring(0, key.length() - ".encoding".length());
                String value = typeSettings.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_ENCODINGS.contains(value)) {
                    throw new IllegalArgumentException(
                        "Invalid encoding '" + typeSettings.get(key) + "' for type '" + typeName + "'. Valid values: " + VALID_ENCODINGS
                    );
                }
                result.put(typeName, value);
            }
        }
        return result;
    }

    /**
     * Extracts per-type compression map from node settings.
     * Reads all keys under "parquet.type_compression.{arrow_type}.compression".
     */
    public static java.util.Map<String, String> getTypeCompressions(Settings nodeSettings) {
        Settings typeSettings = TYPE_COMPRESSION_SETTINGS.get(nodeSettings);
        java.util.Map<String, String> result = new java.util.HashMap<>();
        for (String key : typeSettings.keySet()) {
            if (key.endsWith(".compression")) {
                String typeName = key.substring(0, key.length() - ".compression".length());
                String value = typeSettings.get(key).toUpperCase(java.util.Locale.ROOT);
                if (!VALID_COMPRESSIONS.contains(value)) {
                    throw new IllegalArgumentException(
                        "Invalid compression '"
                            + typeSettings.get(key)
                            + "' for type '"
                            + typeName
                            + "'. Valid values: "
                            + VALID_COMPRESSIONS
                    );
                }
                result.put(typeName, value);
            }
        }
        return result;
    }

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(
            PAGE_SIZE_BYTES,
            PAGE_ROW_LIMIT,
            DICT_SIZE_BYTES,
            COMPRESSION_TYPE,
            COMPRESSION_LEVEL,
            BLOOM_FILTER_ENABLED,
            BLOOM_FILTER_FPP,
            BLOOM_FILTER_NDV,
            MAX_NATIVE_ALLOCATION,
            MAX_ROWS_PER_VSR,
            SORT_IN_MEMORY_THRESHOLD,
            SORT_BATCH_SIZE,
            ROW_GROUP_MAX_ROWS,
            MERGE_BATCH_SIZE,
            MERGE_RAYON_THREADS,
            MERGE_IO_THREADS,
            FIELD_SETTINGS,
            TYPE_ENCODING_SETTINGS,
            TYPE_COMPRESSION_SETTINGS
        );
    }
}
