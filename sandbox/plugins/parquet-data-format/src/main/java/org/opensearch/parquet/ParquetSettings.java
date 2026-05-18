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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

    /** Whether bloom filters are enabled for Parquet columns (default false). */
    public static final Setting<Boolean> BLOOM_FILTER_ENABLED = Setting.boolSetting(
        "index.parquet.bloom_filter_enabled",
        false,
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

    /** Maximum byte size per row group (default 128MB). */
    public static final Setting<ByteSizeValue> ROW_GROUP_MAX_BYTES = Setting.byteSizeSetting(
        "index.parquet.row_group_max_bytes",
        new ByteSizeValue(128, ByteSizeUnit.MB),
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

    static final Set<String> VALID_ARROW_TYPES = Set.of(
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "boolean",
        "utf8",
        "binary",
        "date",
        "timestamp"
    );

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
                String value = s.get(key).toUpperCase(Locale.ROOT);
                if (VALID_ENCODINGS.contains(value) == false) {
                    throw new IllegalArgumentException("Invalid encoding '" + s.get(key) + "'. Valid values: " + VALID_ENCODINGS);
                }
            } else if (key.endsWith(".compression")) {
                String value = s.get(key).toUpperCase(Locale.ROOT);
                if (VALID_COMPRESSIONS.contains(value) == false) {
                    throw new IllegalArgumentException("Invalid compression '" + s.get(key) + "'. Valid values: " + VALID_COMPRESSIONS);
                }
            } else if (key.endsWith(".bloom_filter_enabled")) {
                String value = s.get(key).toLowerCase(Locale.ROOT);
                if ("true".equals(value) == false && "false".equals(value) == false) {
                    throw new IllegalArgumentException("Invalid bloom_filter_enabled '" + s.get(key) + "'. Valid values: [true, false]");
                }
            } else if (key.endsWith(".bloom_filter_fpp")) {
                try {
                    double fpp = Double.parseDouble(s.get(key));
                    if (fpp <= 0.0 || fpp >= 1.0) {
                        throw new IllegalArgumentException(
                            "Invalid bloom_filter_fpp '" + s.get(key) + "'. Must be between 0.0 and 1.0 (exclusive)"
                        );
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid bloom_filter_fpp '" + s.get(key) + "'. Must be a number");
                }
            } else if (key.endsWith(".bloom_filter_ndv")) {
                try {
                    long ndv = Long.parseLong(s.get(key));
                    if (ndv < 1) {
                        throw new IllegalArgumentException("Invalid bloom_filter_ndv '" + s.get(key) + "'. Must be >= 1");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid bloom_filter_ndv '" + s.get(key) + "'. Must be a number");
                }
            }
        }
    }, Setting.Property.IndexScope, Setting.Property.Dynamic);

    /**
     * Group setting for per-type encoding configuration (cluster-level fallback).
     * Usage: parquet.type_encoding.{arrow_type}.encoding=DELTA_BINARY_PACKED
     * e.g. parquet.type_encoding.int64.encoding=DELTA_BINARY_PACKED
     */
    public static final Setting<Settings> TYPE_ENCODING_SETTINGS = Setting.groupSetting("parquet.type_encoding.", s -> {
        for (String key : s.keySet()) {
            if (key.endsWith(".encoding")) {
                String typeName = key.substring(0, key.length() - ".encoding".length());
                if (VALID_ARROW_TYPES.contains(typeName) == false) {
                    throw new IllegalArgumentException("Invalid arrow type '" + typeName + "'. Valid values: " + VALID_ARROW_TYPES);
                }
                String value = s.get(key).toUpperCase(Locale.ROOT);
                if (VALID_ENCODINGS.contains(value) == false) {
                    throw new IllegalArgumentException(
                        "Invalid encoding '" + s.get(key) + "' for type '" + typeName + "'. Valid values: " + VALID_ENCODINGS
                    );
                }
            }
        }
    }, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Group setting for per-type compression configuration (cluster-level fallback).
     * Usage: parquet.type_compression.{arrow_type}.compression=SNAPPY
     * e.g. parquet.type_compression.utf8.compression=SNAPPY
     */
    public static final Setting<Settings> TYPE_COMPRESSION_SETTINGS = Setting.groupSetting("parquet.type_compression.", s -> {
        for (String key : s.keySet()) {
            if (key.endsWith(".compression")) {
                String typeName = key.substring(0, key.length() - ".compression".length());
                if (VALID_ARROW_TYPES.contains(typeName) == false) {
                    throw new IllegalArgumentException("Invalid arrow type '" + typeName + "'. Valid values: " + VALID_ARROW_TYPES);
                }
                String value = s.get(key).toUpperCase(Locale.ROOT);
                if (VALID_COMPRESSIONS.contains(value) == false) {
                    throw new IllegalArgumentException(
                        "Invalid compression '" + s.get(key) + "' for type '" + typeName + "'. Valid values: " + VALID_COMPRESSIONS
                    );
                }
            }
        }
    }, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Group setting for per-type bloom filter configuration (cluster-level fallback).
     * Usage: parquet.type_bloom_filter.{arrow_type}.enabled=true
     *        parquet.type_bloom_filter.{arrow_type}.fpp=0.01
     *        parquet.type_bloom_filter.{arrow_type}.ndv=50000
     */
    public static final Setting<Settings> TYPE_BLOOM_FILTER_SETTINGS = Setting.groupSetting("parquet.type_bloom_filter.", s -> {
        for (String key : s.keySet()) {
            String typeName = null;
            if (key.endsWith(".enabled")) {
                typeName = key.substring(0, key.length() - ".enabled".length());
                String val = s.get(key).toLowerCase(Locale.ROOT);
                if ("true".equals(val) == false && "false".equals(val) == false) {
                    throw new IllegalArgumentException(
                        "Invalid bloom_filter enabled value '" + s.get(key) + "' for type '" + typeName + "'. Must be true or false"
                    );
                }
            } else if (key.endsWith(".fpp")) {
                typeName = key.substring(0, key.length() - ".fpp".length());
                try {
                    double fpp = Double.parseDouble(s.get(key));
                    if (fpp <= 0.0 || fpp >= 1.0) {
                        throw new IllegalArgumentException(
                            "bloom_filter fpp for type '" + typeName + "' must be between 0.0 and 1.0 exclusive, got: " + fpp
                        );
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid bloom_filter fpp value '" + s.get(key) + "' for type '" + typeName + "'");
                }
            } else if (key.endsWith(".ndv")) {
                typeName = key.substring(0, key.length() - ".ndv".length());
                try {
                    long ndv = Long.parseLong(s.get(key));
                    if (ndv < 1) {
                        throw new IllegalArgumentException("bloom_filter ndv for type '" + typeName + "' must be >= 1, got: " + ndv);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid bloom_filter ndv value '" + s.get(key) + "' for type '" + typeName + "'");
                }
            }
            if (typeName != null && VALID_ARROW_TYPES.contains(typeName) == false) {
                throw new IllegalArgumentException("Invalid arrow type '" + typeName + "'. Valid values: " + VALID_ARROW_TYPES);
            }
        }
    }, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Validates that field-level encodings are compatible with their Arrow types in the schema.
     * Should be called when mappings are available (i.e., documentMapper is not null).
     * Skips fields not present in the schema (e.g., dynamic fields not yet mapped).
     */
    public static void validateEncodingTypeCompatibility(Map<String, String> fieldEncodings, Schema schema) {
        Map<String, ArrowType> arrowTypes = new HashMap<>();
        for (Field field : schema.getFields()) {
            arrowTypes.put(field.getName(), field.getType());
        }
        for (Map.Entry<String, String> entry : fieldEncodings.entrySet()) {
            String fieldName = entry.getKey();
            String encoding = entry.getValue().toUpperCase(Locale.ROOT);
            ArrowType arrowType = arrowTypes.get(fieldName);
            if (arrowType == null) {
                throw new IllegalArgumentException(
                    "Field '" + fieldName + "' configured in index.parquet.field settings does not exist in mappings"
                );
            }
            if (isEncodingValidForArrowType(encoding, arrowType) == false) {
                throw new IllegalArgumentException(
                    "Encoding '" + encoding + "' is not compatible with field '" + fieldName + "' of type '" + arrowType + "'"
                );
            }
        }
    }

    /**
     * Maps each encoding to the set of ArrowType classes it supports.
     * PLAIN and unknown encodings are valid for all types (handled separately).
     */
    private static final Map<String, Set<Class<? extends ArrowType>>> ENCODING_TO_VALID_TYPES = Map.of(
        "RLE",
        Set.of(ArrowType.Bool.class),
        "DELTA_BINARY_PACKED",
        Set.of(ArrowType.Int.class),
        "DELTA",
        Set.of(ArrowType.Int.class),
        "DELTA_LENGTH_BYTE_ARRAY",
        Set.of(ArrowType.Utf8.class, ArrowType.LargeUtf8.class, ArrowType.Binary.class, ArrowType.LargeBinary.class),
        "DELTA_BYTE_ARRAY",
        Set.of(
            ArrowType.Utf8.class,
            ArrowType.LargeUtf8.class,
            ArrowType.Binary.class,
            ArrowType.LargeBinary.class,
            ArrowType.FixedSizeBinary.class
        ),
        "BYTE_STREAM_SPLIT",
        Set.of(ArrowType.Int.class, ArrowType.FloatingPoint.class, ArrowType.FixedSizeBinary.class)
    );

    /** Set of ArrowType classes that do NOT support dictionary encoding. */
    private static final Set<Class<? extends ArrowType>> DICTIONARY_INCOMPATIBLE_TYPES = Set.of(ArrowType.Bool.class);

    private static boolean isEncodingValidForArrowType(String encoding, ArrowType arrowType) {
        if ("PLAIN".equals(encoding)) {
            return true;
        }
        if ("RLE_DICTIONARY".equals(encoding) || "DICTIONARY".equals(encoding)) {
            return DICTIONARY_INCOMPATIBLE_TYPES.contains(arrowType.getClass()) == false;
        }
        Set<Class<? extends ArrowType>> validTypes = ENCODING_TO_VALID_TYPES.get(encoding);
        if (validTypes == null) {
            return true;
        }
        return validTypes.contains(arrowType.getClass());
    }

    /**
     * Extracts a config map from group settings by matching keys with a given suffix,
     * validating the extracted name against an optional set of valid names,
     * and validating the value against a set of valid values.
     */
    private static Map<String, String> extractConfigMap(
        Settings groupSettings,
        String keySuffix,
        Set<String> validNames,
        String nameLabel,
        Set<String> validValues,
        String valueLabel
    ) {
        Map<String, String> result = new HashMap<>();
        for (String key : groupSettings.keySet()) {
            if (key.endsWith(keySuffix)) {
                String name = key.substring(0, key.length() - keySuffix.length());
                if (validNames != null && validNames.contains(name) == false) {
                    throw new IllegalArgumentException("Invalid " + nameLabel + " '" + name + "'. Valid values: " + validNames);
                }
                String value = groupSettings.get(key).toUpperCase(Locale.ROOT);
                if (validValues.contains(value) == false) {
                    throw new IllegalArgumentException(
                        "Invalid "
                            + valueLabel
                            + " '"
                            + groupSettings.get(key)
                            + "' for "
                            + nameLabel
                            + " '"
                            + name
                            + "'. Valid values: "
                            + validValues
                    );
                }
                result.put(name, value);
            }
        }
        return result;
    }

    /**
     * Extracts a typed config map from group settings by matching keys with a given suffix
     * and converting the raw string value using the provided parser.
     */
    private static <T> Map<String, T> extractConfigMap(
        Settings groupSettings,
        String keySuffix,
        java.util.function.Function<String, T> valueParser
    ) {
        Map<String, T> result = new HashMap<>();
        for (String key : groupSettings.keySet()) {
            if (key.endsWith(keySuffix)) {
                String name = key.substring(0, key.length() - keySuffix.length());
                result.put(name, valueParser.apply(groupSettings.get(key)));
            }
        }
        return result;
    }

    public static Map<String, String> getFieldEncodings(Settings settings) {
        return extractConfigMap(FIELD_SETTINGS.get(settings), ".encoding", null, "field", VALID_ENCODINGS, "encoding");
    }

    public static Map<String, String> getFieldCompressions(Settings settings) {
        return extractConfigMap(FIELD_SETTINGS.get(settings), ".compression", null, "field", VALID_COMPRESSIONS, "compression");
    }

    public static Map<String, Boolean> getFieldBloomFilterEnabled(Settings settings) {
        return extractConfigMap(FIELD_SETTINGS.get(settings), ".bloom_filter_enabled", Boolean::parseBoolean);
    }

    public static Map<String, Double> getFieldBloomFilterFpp(Settings settings) {
        return extractConfigMap(FIELD_SETTINGS.get(settings), ".bloom_filter_fpp", Double::parseDouble);
    }

    public static Map<String, Long> getFieldBloomFilterNdv(Settings settings) {
        return extractConfigMap(FIELD_SETTINGS.get(settings), ".bloom_filter_ndv", Long::parseLong);
    }

    /**
     * Extracts per-type encoding map from node settings.
     * Reads all keys under "parquet.type_encoding.{arrow_type}.encoding".
     */
    public static Map<String, String> getTypeEncodings(Settings nodeSettings) {
        return extractConfigMap(
            TYPE_ENCODING_SETTINGS.get(nodeSettings),
            ".encoding",
            VALID_ARROW_TYPES,
            "arrow type",
            VALID_ENCODINGS,
            "encoding"
        );
    }

    /**
     * Extracts per-type compression map from node settings.
     * Reads all keys under "parquet.type_compression.{arrow_type}.compression".
     */
    public static Map<String, String> getTypeCompressions(Settings nodeSettings) {
        return extractConfigMap(
            TYPE_COMPRESSION_SETTINGS.get(nodeSettings),
            ".compression",
            VALID_ARROW_TYPES,
            "arrow type",
            VALID_COMPRESSIONS,
            "compression"
        );
    }

    public static Map<String, Boolean> getTypeBloomFilterEnabled(Settings nodeSettings) {
        return extractConfigMap(TYPE_BLOOM_FILTER_SETTINGS.get(nodeSettings), ".enabled", Boolean::parseBoolean);
    }

    public static Map<String, Double> getTypeBloomFilterFpp(Settings nodeSettings) {
        return extractConfigMap(TYPE_BLOOM_FILTER_SETTINGS.get(nodeSettings), ".fpp", Double::parseDouble);
    }

    public static Map<String, Long> getTypeBloomFilterNdv(Settings nodeSettings) {
        return extractConfigMap(TYPE_BLOOM_FILTER_SETTINGS.get(nodeSettings), ".ndv", Long::parseLong);
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
            ROW_GROUP_MAX_BYTES,
            MERGE_BATCH_SIZE,
            MERGE_RAYON_THREADS,
            MERGE_IO_THREADS,
            FIELD_SETTINGS,
            TYPE_ENCODING_SETTINGS,
            TYPE_COMPRESSION_SETTINGS,
            TYPE_BLOOM_FILTER_SETTINGS
        );
    }
}
