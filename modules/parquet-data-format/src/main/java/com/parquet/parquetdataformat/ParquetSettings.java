/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.common.settings.WriteableSetting.SettingType.ByteSizeValue;

/**
 * Settings for Parquet data format.
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";

    public static final Setting<Settings> PARQUET_SETTINGS = Setting.groupSetting(
        "index.parquet.",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> ROW_GROUP_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.row_group_size_bytes",
        new ByteSizeValue(128, ByteSizeUnit.MB),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> PAGE_ROW_LIMIT = Setting.intSetting(
        "index.parquet.page_row_limit",
        20000,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> COMPRESSION_TYPE = Setting.simpleString(
        "index.parquet.compression_type",
        "ZSTD",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> COMPRESSION_LEVEL = Setting.intSetting(
        "index.parquet.compression_level",
        2,
        1,
        9,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "index.parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
