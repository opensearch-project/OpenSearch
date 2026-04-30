/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;

/**
 * Settings for Parquet data format.
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 50000;

    /** Group setting prefix for all Parquet settings. */
    public static final Setting<Settings> PARQUET_SETTINGS = Setting.groupSetting("index.parquet.", Setting.Property.IndexScope);

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

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(
            PARQUET_SETTINGS,
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
            MERGE_IO_THREADS
        );
    }
}
