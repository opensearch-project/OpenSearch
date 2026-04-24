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
    public static final Setting<Settings> PARQUET_SETTINGS = Setting.groupSetting(
        "parquet.",
        Setting.Property.IndexScope
    );

    /** Maximum row group size in bytes (default 128MB). */
    public static final Setting<ByteSizeValue> ROW_GROUP_SIZE_BYTES = Setting.byteSizeSetting(
        "parquet.row_group_size_bytes",
        new ByteSizeValue(128, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Data page size limit in bytes (default 1MB). */
    public static final Setting<ByteSizeValue> PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "parquet.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Maximum number of rows per data page (default 20000). */
    public static final Setting<Integer> PAGE_ROW_LIMIT = Setting.intSetting(
        "parquet.page_row_limit",
        20000,
        1,
        Setting.Property.IndexScope
    );

    /** Dictionary page size limit in bytes (default 2MB). */
    public static final Setting<ByteSizeValue> DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "parquet.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    /** Compression codec for Parquet files, e.g. ZSTD, SNAPPY, LZ4_RAW (default LZ4_RAW). */
    public static final Setting<String> COMPRESSION_TYPE = Setting.simpleString(
        "parquet.compression_type",
        "LZ4_RAW",
        Setting.Property.IndexScope
    );

    /** Compression level for the chosen codec (default 2, range 1–9). */
    public static final Setting<Integer> COMPRESSION_LEVEL = Setting.intSetting(
        "parquet.compression_level",
        2,
        1,
        9,
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

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(
            PARQUET_SETTINGS,
            ROW_GROUP_SIZE_BYTES, PAGE_SIZE_BYTES, PAGE_ROW_LIMIT, DICT_SIZE_BYTES,
            COMPRESSION_TYPE, COMPRESSION_LEVEL,
            MAX_NATIVE_ALLOCATION, MAX_ROWS_PER_VSR
        );
    }
}
