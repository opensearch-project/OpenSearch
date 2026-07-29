/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Settings for Parquet data format.
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String POOL_WRITE = "write";
    public static final String POOL_MERGE = "merge";

    /**
     * Anchor used to scale the batch/sort defaults with machine memory. The tuned base values
     * (VSR rows, merge rows, and sort-threshold MiB) were measured on a 64 GiB machine; defaults
     * scale linearly with total RAM relative to this anchor.
     */
    static final long BATCH_SIZE_ANCHOR_RAM_BYTES = 64L * 1024 * 1024 * 1024;

    /** Fixed bounds for the RAM-scaled {@code parquet.max_rows_per_vsr} default (user-set values are unbounded). */
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 65_536;
    static final int MAX_ROWS_PER_VSR_FLOOR = 8_192;
    static final int MAX_ROWS_PER_VSR_CEIL = 131_072;

    /** Tuned base (64 GiB anchor) and fixed bounds for the RAM-scaled {@code index.parquet.merge_batch_size} default. */
    public static final int DEFAULT_MERGE_BATCH_SIZE = 100_000;
    static final int MERGE_BATCH_SIZE_FLOOR = 12_500;
    static final int MERGE_BATCH_SIZE_CEIL = 100_000;

    /**
     * Tuned base (64 GiB anchor) and fixed bounds, in MiB, for the RAM-scaled
     * {@code index.parquet.sort_in_memory_threshold} default. Governs the sorted-write chunk size;
     * each chunk flush transiently reserves ~2× this on the write pool.
     */
    static final int SORT_IN_MEMORY_THRESHOLD_BASE_MB = 32;
    static final int SORT_IN_MEMORY_THRESHOLD_FLOOR_MB = 16;
    static final int SORT_IN_MEMORY_THRESHOLD_CEIL_MB = 64;

    /**
     * RAM-scaled defaults, computed once at class load (total RAM is constant per node).
     * Any failure reading RAM falls back to the tuned base value (see {@link #safeTotalRamBytes()}).
     */
    private static final int MAX_ROWS_PER_VSR_DEFAULT = deriveBatchSizeDefault(
        safeTotalRamBytes(),
        DEFAULT_MAX_ROWS_PER_VSR,
        MAX_ROWS_PER_VSR_FLOOR,
        MAX_ROWS_PER_VSR_CEIL
    );
    private static final int MERGE_BATCH_SIZE_DEFAULT = deriveBatchSizeDefault(
        safeTotalRamBytes(),
        DEFAULT_MERGE_BATCH_SIZE,
        MERGE_BATCH_SIZE_FLOOR,
        MERGE_BATCH_SIZE_CEIL
    );
    private static final ByteSizeValue SORT_IN_MEMORY_THRESHOLD_DEFAULT = new ByteSizeValue(
        deriveBatchSizeDefault(
            safeTotalRamBytes(),
            SORT_IN_MEMORY_THRESHOLD_BASE_MB,
            SORT_IN_MEMORY_THRESHOLD_FLOOR_MB,
            SORT_IN_MEMORY_THRESHOLD_CEIL_MB
        ),
        ByteSizeUnit.MB
    );

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

    /**
     * Maximum rows per VectorSchemaRoot before rotation is triggered. Default scales with total RAM
     * (linearly, anchored at 64 GiB → {@value #DEFAULT_MAX_ROWS_PER_VSR}), clamped to
     * [{@value #MAX_ROWS_PER_VSR_FLOOR}, {@value #MAX_ROWS_PER_VSR_CEIL}].
     */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "parquet.max_rows_per_vsr",
        MAX_ROWS_PER_VSR_DEFAULT,
        1,
        Setting.Property.NodeScope
    );

    /** File size threshold for in-memory sort vs streaming merge sort. Default scales with total RAM
     *  (anchor 32 MiB @ 64 GiB), clamped to [16 MiB, 64 MiB]. */
    public static final Setting<ByteSizeValue> SORT_IN_MEMORY_THRESHOLD = Setting.byteSizeSetting(
        "index.parquet.sort_in_memory_threshold",
        SORT_IN_MEMORY_THRESHOLD_DEFAULT,
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

    /**
     * Batch size for reading records during merge. Default scales with total RAM
     * (linearly, anchored at 64 GiB → {@value #DEFAULT_MERGE_BATCH_SIZE}), clamped to
     * [{@value #MERGE_BATCH_SIZE_FLOOR}, {@value #MERGE_BATCH_SIZE_CEIL}].
     */
    public static final Setting<Integer> MERGE_BATCH_SIZE = Setting.intSetting(
        "index.parquet.merge_batch_size",
        MERGE_BATCH_SIZE_DEFAULT,
        1,
        Setting.Property.IndexScope
    );

    /** Number of Rayon threads for parallel column encoding during merge (default num_cores/2, min 1). */
    public static final Setting<Integer> MERGE_RAYON_THREADS = Setting.intSetting(
        "parquet.merge_rayon_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
        1,
        Setting.Property.NodeScope
    );

    /** Number of Tokio IO threads for async disk writes during merge (default num_cores/2, min 1). */
    public static final Setting<Integer> MERGE_IO_THREADS = Setting.intSetting(
        "parquet.merge_io_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
        1,
        Setting.Property.NodeScope
    );

    /**
     * Minimum number of variable-width (string/binary) non-sort columns required to activate
     * deferred data loading during merge. Below this threshold, all columns are decoded eagerly
     * (original behavior). Set to 0 to always defer; set very high to disable deferral.
     */
    public static final Setting<Integer> MERGE_DEFERRED_COLUMN_THRESHOLD = Setting.intSetting(
        "index.parquet.merge_deferred_column_threshold",
        0,
        0,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the native write pool. Default is 2% of budget on warm nodes, 4% otherwise. */
    public static final Setting<Long> WRITE_POOL_MIN = new Setting<>(
        "parquet.native.pool.write.min",
        s -> derivePoolMinDefault(s, DiscoveryNode.isWarmNode(s) ? 2 : 4),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [parquet.native.pool.write.min] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the native write pool can burst to. Default is 4% of budget on warm nodes, 8% otherwise. */
    public static final Setting<Long> WRITE_POOL_MAX = new Setting<>(
        "parquet.native.pool.write.max",
        s -> derivePoolMaxDefault(s, DiscoveryNode.isWarmNode(s) ? 4 : 8),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [parquet.native.pool.write.max] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the native merge pool. Default is half of merge max (1% of budget). */
    public static final Setting<Long> MERGE_POOL_MIN = new Setting<>(
        "parquet.native.pool.merge.min",
        s -> derivePoolMinDefault(s, 1),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [parquet.native.pool.merge.min] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the native merge pool can burst to. Default is 3% of node.native_memory.limit. */
    public static final Setting<Long> MERGE_POOL_MAX = new Setting<>(
        "parquet.native.pool.merge.max",
        s -> derivePoolMaxDefault(s, 3),
        s -> {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new IllegalArgumentException("Setting [parquet.native.pool.merge.max] must be >= 0, got " + v);
            }
            return v;
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Computes the default for a pool max as a percentage of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}.
     * Falls back to {@link Long#MAX_VALUE} when AC is unconfigured.
     */
    static String derivePoolMaxDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return Long.toString(Long.MAX_VALUE);
        }
        long pool = Math.max(0L, nativeLimit.getBytes() * percent / 100);
        return Long.toString(pool);
    }

    /**
     * Computes the default for a pool min as a percentage of
     * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}.
     * Returns 0 when AC is unconfigured (unlike max which returns Long.MAX_VALUE).
     */
    static String derivePoolMinDefault(Settings settings, int percent) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return "0";
        }
        return Long.toString(Math.max(0L, nativeLimit.getBytes() * percent / 100));
    }

    /**
     * Scales a batch-size default linearly with total physical RAM, anchored at the 64 GiB benchmark
     * machine ({@link #BATCH_SIZE_ANCHOR_RAM_BYTES}), and clamps the result to [{@code floor}, {@code ceil}].
     * The base value is the default tuned on the anchor machine, and {@code factor = totalRamBytes / anchor}.
     * If RAM is unavailable ({@code totalRamBytes <= 0}), falls back to the tuned default {@code base}
     * with no scaling/clamping applied.
     *
     * @param totalRamBytes total physical RAM in bytes (e.g. {@code OsProbe.getTotalPhysicalMemorySize()})
     * @param base          tuned default at the 64 GiB anchor (also the fallback value)
     * @param floor         fixed lower bound
     * @param ceil          fixed upper bound
     */
    static int deriveBatchSizeDefault(long totalRamBytes, int base, int floor, int ceil) {
        if (totalRamBytes <= 0) {
            // Memory unavailable — fall back to the tuned default, no calculation.
            return base;
        }
        double factor = (double) totalRamBytes / BATCH_SIZE_ANCHOR_RAM_BYTES;
        long scaled = Math.round((double) base * factor);
        return (int) Math.max(floor, Math.min(ceil, scaled));
    }

    /**
     * Returns total physical RAM in bytes, or {@code -1} if it cannot be read for any reason.
     * A negative result drives {@link #deriveBatchSizeDefault} to the tuned base default, ensuring
     * settings always resolve to a sane value (and class initialization never fails) even if the
     * OS probe throws.
     */
    private static long safeTotalRamBytes() {
        try {
            return OsProbe.getInstance().getTotalPhysicalMemorySize();
        } catch (Exception e) {
            return -1L;
        }
    }

    public static final Set<String> VALID_ENCODINGS = Set.of(
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

    public static final Set<String> VALID_COMPRESSIONS = Set.of("ZSTD", "SNAPPY", "GZIP", "BROTLI", "LZ4_RAW", "UNCOMPRESSED");

    public static final Set<String> VALID_ARROW_TYPES = Set.of(
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
     * Maps each encoding to the set of ArrowType classes it supports.
     * PLAIN and unknown encodings are valid for all types (handled separately).
     */
    private static final Map<String, Set<Class<? extends ArrowType>>> ENCODING_TO_VALID_TYPES = Map.of(
        "RLE",
        Set.of(ArrowType.Bool.class),
        "DELTA_BINARY_PACKED",
        Set.of(ArrowType.Int.class, ArrowType.Timestamp.class, ArrowType.Date.class, ArrowType.Time.class, ArrowType.Duration.class),
        "DELTA",
        Set.of(ArrowType.Int.class, ArrowType.Timestamp.class, ArrowType.Date.class, ArrowType.Time.class, ArrowType.Duration.class),
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

    /** Maps arrow type name strings to representative ArrowType instances for compatibility checks. */
    public static final Map<String, ArrowType> ARROW_TYPE_NAME_TO_INSTANCE = Map.ofEntries(
        Map.entry("int8", new ArrowType.Int(8, true)),
        Map.entry("int16", new ArrowType.Int(16, true)),
        Map.entry("int32", new ArrowType.Int(32, true)),
        Map.entry("int64", new ArrowType.Int(64, true)),
        Map.entry("uint8", new ArrowType.Int(8, false)),
        Map.entry("uint16", new ArrowType.Int(16, false)),
        Map.entry("uint32", new ArrowType.Int(32, false)),
        Map.entry("uint64", new ArrowType.Int(64, false)),
        Map.entry("float32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        Map.entry("float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Map.entry("boolean", new ArrowType.Bool()),
        Map.entry("utf8", new ArrowType.Utf8()),
        Map.entry("binary", new ArrowType.Binary()),
        Map.entry("date", new ArrowType.Date(DateUnit.DAY)),
        Map.entry("timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
    );

    public static boolean isEncodingValidForArrowType(String encoding, ArrowType arrowType) {
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

    // Field-level encoding configuration (parallel arrays)
    public static final Setting<List<String>> ENCODING_FIELD_SETTING = Setting.listSetting(
        "index.parquet.encoding.field",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<List<String>> ENCODING_VALUE_SETTING = Setting.listSetting(
        "index.parquet.encoding.value",
        Collections.emptyList(),
        ParquetSettings::validateEncoding,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    // Field-level compression configuration (parallel arrays)
    public static final Setting<List<String>> COMPRESSION_FIELD_SETTING = Setting.listSetting(
        "index.parquet.compression.field",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<List<String>> COMPRESSION_VALUE_SETTING = Setting.listSetting(
        "index.parquet.compression.value",
        Collections.emptyList(),
        ParquetSettings::validateCompression,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    // Field-level bloom filter enabled configuration (parallel arrays)
    public static final Setting<List<String>> BLOOM_FILTER_ENABLED_FIELD_SETTING = Setting.listSetting(
        "index.parquet.bloom_filter_enabled.field",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<List<Boolean>> BLOOM_FILTER_ENABLED_VALUE_SETTING = Setting.listSetting(
        "index.parquet.bloom_filter_enabled.value",
        Collections.emptyList(),
        ParquetSettings::validateBoolean,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

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
                ArrowType arrowType = ARROW_TYPE_NAME_TO_INSTANCE.get(typeName);
                if (arrowType != null && isEncodingValidForArrowType(value, arrowType) == false) {
                    throw new IllegalArgumentException("Encoding '" + value + "' is not compatible with arrow type '" + typeName + "'");
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

    private static String validateEncoding(String encoding) {
        String normalized = encoding.toUpperCase(Locale.ROOT);
        if (!VALID_ENCODINGS.contains(normalized)) {
            throw new IllegalArgumentException("Invalid encoding: " + encoding + ". Valid values: " + VALID_ENCODINGS);
        }
        return normalized;
    }

    private static String validateCompression(String compression) {
        String normalized = compression.toUpperCase(Locale.ROOT);
        if (!VALID_COMPRESSIONS.contains(normalized)) {
            throw new IllegalArgumentException("Invalid compression: " + compression + ". Valid values: " + VALID_COMPRESSIONS);
        }
        return normalized;
    }

    private static Boolean validateBoolean(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        if ("true".equals(normalized)) {
            return true;
        } else if ("false".equals(normalized)) {
            return false;
        } else {
            throw new IllegalArgumentException("Invalid boolean value: " + value + ". Must be true or false");
        }
    }

    /**
     * Builds a field configuration map from parallel arrays.
     */
    private static <V> Map<String, V> buildFieldMap(List<String> fields, List<V> values, String configType) {
        if (fields.isEmpty() && values.isEmpty()) {
            return Collections.emptyMap();
        }

        if (fields.size() != values.size()) {
            throw new IllegalArgumentException(
                "index.parquet."
                    + configType
                    + ".field and index.parquet."
                    + configType
                    + ".value must have the same size. "
                    + "Fields: "
                    + fields
                    + ", Values: "
                    + values
            );
        }

        Map<String, V> result = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            if (result.containsKey(fields.get(i))) {
                throw new IllegalArgumentException("Duplicate field '" + fields.get(i) + "' in index.parquet." + configType + ".field");
            }
            result.put(fields.get(i), values.get(i));
        }
        return result;
    }

    public static Map<String, String> getFieldEncodings(Settings settings) {
        return buildFieldMap(ENCODING_FIELD_SETTING.get(settings), ENCODING_VALUE_SETTING.get(settings), "encoding");
    }

    public static Map<String, String> getFieldCompressions(Settings settings) {
        return buildFieldMap(COMPRESSION_FIELD_SETTING.get(settings), COMPRESSION_VALUE_SETTING.get(settings), "compression");
    }

    public static Map<String, Boolean> getFieldBloomFilterEnabled(Settings settings) {
        return buildFieldMap(
            BLOOM_FILTER_ENABLED_FIELD_SETTING.get(settings),
            BLOOM_FILTER_ENABLED_VALUE_SETTING.get(settings),
            "bloom_filter_enabled"
        );
    }

    public static Map<String, Double> getFieldBloomFilterFpp(Settings settings) {
        Map<String, Double> result = new HashMap<>();
        for (String key : settings.keySet()) {
            if (key.startsWith("index.parquet.field.") && key.endsWith(".bloom_filter_fpp")) {
                String fieldName = key.substring("index.parquet.field.".length(), key.length() - ".bloom_filter_fpp".length());
                result.put(fieldName, settings.getAsDouble(key, null));
            }
        }
        return result;
    }

    public static Map<String, Long> getFieldBloomFilterNdv(Settings settings) {
        Map<String, Long> result = new HashMap<>();
        for (String key : settings.keySet()) {
            if (key.startsWith("index.parquet.field.") && key.endsWith(".bloom_filter_ndv")) {
                String fieldName = key.substring("index.parquet.field.".length(), key.length() - ".bloom_filter_ndv".length());
                result.put(fieldName, settings.getAsLong(key, null));
            }
        }
        return result;
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

    /**
     * Extracts a config map from group settings by matching keys with a given suffix,
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
    private static <T> Map<String, T> extractConfigMap(Settings groupSettings, String keySuffix, Function<String, T> valueParser) {
        Map<String, T> result = new HashMap<>();
        for (String key : groupSettings.keySet()) {
            if (key.endsWith(keySuffix)) {
                String name = key.substring(0, key.length() - keySuffix.length());
                result.put(name, valueParser.apply(groupSettings.get(key)));
            }
        }
        return result;
    }

    /**
     * Validates that field-level configurations are compatible with their Arrow types in the schema.
     */
    public static void validateFieldConfigurations(
        Map<String, String> fieldEncodings,
        Map<String, String> fieldCompressions,
        Map<String, Boolean> fieldBloomFilterEnabled,
        Schema schema
    ) {
        Map<String, ArrowType> arrowTypes = new HashMap<>();
        for (Field field : schema.getFields()) {
            arrowTypes.put(field.getName(), field.getType());
        }

        // Validate encoding configurations
        for (Map.Entry<String, String> entry : fieldEncodings.entrySet()) {
            String fieldName = entry.getKey();
            String encoding = entry.getValue();
            ArrowType arrowType = arrowTypes.get(fieldName);
            if (arrowType == null) {
                throw new IllegalArgumentException("Field '" + fieldName + "' in encoding configuration does not exist in mappings");
            }
            if (!isEncodingValidForArrowType(encoding, arrowType)) {
                throw new IllegalArgumentException(
                    "Encoding '" + encoding + "' is not compatible with field '" + fieldName + "' of type '" + arrowType + "'"
                );
            }
        }

        // Validate compression field existence
        for (String fieldName : fieldCompressions.keySet()) {
            if (!arrowTypes.containsKey(fieldName)) {
                throw new IllegalArgumentException("Field '" + fieldName + "' in compression configuration does not exist in mappings");
            }
        }

        // Validate bloom filter field existence
        for (String fieldName : fieldBloomFilterEnabled.keySet()) {
            if (!arrowTypes.containsKey(fieldName)) {
                throw new IllegalArgumentException(
                    "Field '" + fieldName + "' in bloom_filter_enabled configuration does not exist in mappings"
                );
            }
        }
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
            MAX_ROWS_PER_VSR,
            SORT_IN_MEMORY_THRESHOLD,
            ROW_GROUP_MAX_ROWS,
            ROW_GROUP_MAX_BYTES,
            MERGE_BATCH_SIZE,
            MERGE_RAYON_THREADS,
            MERGE_IO_THREADS,
            MERGE_DEFERRED_COLUMN_THRESHOLD,
            WRITE_POOL_MIN,
            WRITE_POOL_MAX,
            MERGE_POOL_MIN,
            MERGE_POOL_MAX,
            ENCODING_FIELD_SETTING,
            ENCODING_VALUE_SETTING,
            COMPRESSION_FIELD_SETTING,
            COMPRESSION_VALUE_SETTING,
            BLOOM_FILTER_ENABLED_FIELD_SETTING,
            BLOOM_FILTER_ENABLED_VALUE_SETTING,
            TYPE_ENCODING_SETTINGS,
            TYPE_COMPRESSION_SETTINGS,
            TYPE_BLOOM_FILTER_SETTINGS
        );
    }
}
