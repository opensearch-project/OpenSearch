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
 * <p>
 * Settings are split into two categories:
 * <ul>
 *   <li><b>Physical layout (immutable per-index)</b>: Control the on-disk Parquet format.
 *       Cluster-level defaults ({@code parquet.defaults.*}) are inherited at index creation
 *       and frozen as {@code index.parquet.*} — they cannot be changed on a live index.</li>
 *   <li><b>Operational/resource (dynamic)</b>: Control CPU/memory usage during write and
 *       merge operations. These can be updated on a live index or node without restart;
 *       changes take effect on the next operation.</li>
 * </ul>
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 50000;

    // ── Cluster-level defaults for physical layout (Dynamic + NodeScope) ──
    // New indices inherit these values at creation time.

    /** Cluster default for data page size limit (default 1MB). */
    public static final Setting<ByteSizeValue> DEFAULT_PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.defaults.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for max rows per data page (default 20000). */
    public static final Setting<Integer> DEFAULT_PAGE_ROW_LIMIT = Setting.intSetting(
        "index.parquet.defaults.page_row_limit",
        20000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for dictionary page size limit (default 2MB). */
    public static final Setting<ByteSizeValue> DEFAULT_DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.defaults.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for compression codec (default LZ4_RAW). */
    public static final Setting<String> DEFAULT_COMPRESSION_TYPE = Setting.simpleString(
        "index.parquet.defaults.compression_type",
        "LZ4_RAW",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for compression level (default 2, range 1–9). */
    public static final Setting<Integer> DEFAULT_COMPRESSION_LEVEL = Setting.intSetting(
        "index.parquet.defaults.compression_level",
        2,
        1,
        9,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for bloom filter enabled (default true). */
    public static final Setting<Boolean> DEFAULT_BLOOM_FILTER_ENABLED = Setting.boolSetting(
        "index.parquet.defaults.bloom_filter_enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for bloom filter false positive probability (default 0.1). */
    public static final Setting<Double> DEFAULT_BLOOM_FILTER_FPP = Setting.doubleSetting(
        "index.parquet.defaults.bloom_filter_fpp",
        0.1,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for bloom filter distinct values hint (default 100000). */
    public static final Setting<Long> DEFAULT_BLOOM_FILTER_NDV = Setting.longSetting(
        "index.parquet.defaults.bloom_filter_ndv",
        100_000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Cluster default for max rows per row group (default 1000000). */
    public static final Setting<Integer> DEFAULT_ROW_GROUP_MAX_ROWS = Setting.intSetting(
        "index.parquet.defaults.row_group_max_rows",
        1_000_000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ── Per-index physical layout (immutable after creation) ──

    /** Group setting prefix for all Parquet settings. */
    public static final Setting<Settings> PARQUET_SETTINGS = Setting.groupSetting("index.parquet.", Setting.Property.IndexScope);

    /** Data page size limit in bytes (default 1MB). Immutable after index creation. */
    public static final Setting<ByteSizeValue> PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Maximum number of rows per data page (default 20000). Immutable after index creation. */
    public static final Setting<Integer> PAGE_ROW_LIMIT = Setting.intSetting(
        "index.parquet.page_row_limit",
        20000,
        1,
        Setting.Property.IndexScope
    );

    /** Dictionary page size limit in bytes (default 2MB). Immutable after index creation. */
    public static final Setting<ByteSizeValue> DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Compression codec (default LZ4_RAW). Immutable after index creation. */
    public static final Setting<String> COMPRESSION_TYPE = Setting.simpleString(
        "index.parquet.compression_type",
        "LZ4_RAW",
        Setting.Property.IndexScope
    );

    /** Compression level 1–9 (default 2). Immutable after index creation. */
    public static final Setting<Integer> COMPRESSION_LEVEL = Setting.intSetting(
        "index.parquet.compression_level",
        2,
        1,
        9,
        Setting.Property.IndexScope
    );

    /** Bloom filters enabled (default true). Immutable after index creation. */
    public static final Setting<Boolean> BLOOM_FILTER_ENABLED = Setting.boolSetting(
        "index.parquet.bloom_filter_enabled",
        true,
        Setting.Property.IndexScope
    );

    /** Bloom filter FPP (default 0.1). Immutable after index creation. */
    public static final Setting<Double> BLOOM_FILTER_FPP = Setting.doubleSetting(
        "index.parquet.bloom_filter_fpp",
        0.1,
        0.0,
        1.0,
        Setting.Property.IndexScope
    );

    /** Bloom filter NDV hint (default 100000). Immutable after index creation. */
    public static final Setting<Long> BLOOM_FILTER_NDV = Setting.longSetting(
        "index.parquet.bloom_filter_ndv",
        100_000L,
        1L,
        Setting.Property.IndexScope
    );

    /** Max rows per row group (default 1000000). Immutable after index creation. */
    public static final Setting<Integer> ROW_GROUP_MAX_ROWS = Setting.intSetting(
        "index.parquet.row_group_max_rows",
        1_000_000,
        1,
        Setting.Property.IndexScope
    );

    // ── Operational/resource settings (Dynamic — impacts CPU/memory) ──

    /**
     * Maximum native memory allocation for Arrow buffers, as a percentage of
     * non-heap memory (default 10%). Static — the buffer pool is sized once at
     * engine creation; changing this requires an engine restart.
     */
    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "index.parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope
    );

    /**
     * Maximum rows per VectorSchemaRoot before rotation is triggered (default 50000).
     * Dynamic — controls memory pressure during indexing.
     */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "index.parquet.max_rows_per_vsr",
        DEFAULT_MAX_ROWS_PER_VSR,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * File size threshold for in-memory sort vs streaming merge sort (default 32MB).
     * Dynamic — controls memory/IO tradeoff during merge. Changes apply to next merge.
     */
    public static final Setting<ByteSizeValue> SORT_IN_MEMORY_THRESHOLD = Setting.byteSizeSetting(
        "index.parquet.sort_in_memory_threshold",
        new ByteSizeValue(32, ByteSizeUnit.MB),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Batch size for streaming merge sort (default 8192 rows).
     * Dynamic — controls memory usage during sort operations.
     */
    public static final Setting<Integer> SORT_BATCH_SIZE = Setting.intSetting(
        "index.parquet.sort_batch_size",
        8192,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Batch size for reading records during merge (default 100000 rows).
     * Dynamic — controls memory usage during merge reads.
     */
    public static final Setting<Integer> MERGE_BATCH_SIZE = Setting.intSetting(
        "index.parquet.merge_batch_size",
        100_000,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** Number of Rayon threads for parallel column encoding during merge (default num_cores/8, min 1). */
    public static final Setting<Integer> MERGE_RAYON_THREADS = Setting.intSetting(
        "index.parquet.merge_rayon_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 8),
        1,
        Setting.Property.NodeScope
    );

    /** Number of Tokio IO threads for async disk writes during merge (default num_cores/8, min 1). */
    public static final Setting<Integer> MERGE_IO_THREADS = Setting.intSetting(
        "index.parquet.merge_io_threads",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 8),
        1,
        Setting.Property.NodeScope
    );

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(
            // Cluster-level defaults for physical layout
            DEFAULT_PAGE_SIZE_BYTES,
            DEFAULT_PAGE_ROW_LIMIT,
            DEFAULT_DICT_SIZE_BYTES,
            DEFAULT_COMPRESSION_TYPE,
            DEFAULT_COMPRESSION_LEVEL,
            DEFAULT_BLOOM_FILTER_ENABLED,
            DEFAULT_BLOOM_FILTER_FPP,
            DEFAULT_BLOOM_FILTER_NDV,
            DEFAULT_ROW_GROUP_MAX_ROWS,
            // Per-index physical layout (immutable)
            PARQUET_SETTINGS,
            PAGE_SIZE_BYTES,
            PAGE_ROW_LIMIT,
            DICT_SIZE_BYTES,
            COMPRESSION_TYPE,
            COMPRESSION_LEVEL,
            BLOOM_FILTER_ENABLED,
            BLOOM_FILTER_FPP,
            BLOOM_FILTER_NDV,
            ROW_GROUP_MAX_ROWS,
            // Operational/resource (dynamic)
            MAX_NATIVE_ALLOCATION,
            MAX_ROWS_PER_VSR,
            SORT_IN_MEMORY_THRESHOLD,
            SORT_BATCH_SIZE,
            MERGE_BATCH_SIZE,
            MERGE_RAYON_THREADS,
            MERGE_IO_THREADS
        );
    }
}
