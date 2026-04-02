/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Node-scoped settings for the Parquet data format plugin.
 *
 * <p>All settings are registered with OpenSearch via
 * {@link ParquetDataFormatPlugin#getSettings()} and can be configured in
 * {@code opensearch.yml} or via cluster settings API.
 *
 * <ul>
 *   <li>{@link #MAX_NATIVE_ALLOCATION} — Maximum native memory allocation for Arrow buffers,
 *       expressed as a percentage of available non-heap system memory (default {@code "10%"}).</li>
 *   <li>{@link #MAX_ROWS_PER_VSR} — Row count threshold that triggers VectorSchemaRoot rotation
 *       during document ingestion (default {@code 50000}).</li>
 * </ul>
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    /** Default maximum native memory allocation as a percentage of available non-heap memory. */
    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";
    /** Default maximum number of rows per VectorSchemaRoot before rotation. */
    public static final int DEFAULT_MAX_ROWS_PER_VSR = 50000;

    /** Maximum native memory allocation for Arrow buffers, as a percentage of non-heap memory. */
    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope
    );

    /** Maximum number of rows per VectorSchemaRoot before rotation is triggered. */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "parquet.max_rows_per_vsr",
        DEFAULT_MAX_ROWS_PER_VSR,
        1,
        Setting.Property.NodeScope
    );

    /** Returns all settings defined by the Parquet plugin. */
    public static List<Setting<?>> getSettings() {
        return List.of(MAX_NATIVE_ALLOCATION, MAX_ROWS_PER_VSR);
    }
}
