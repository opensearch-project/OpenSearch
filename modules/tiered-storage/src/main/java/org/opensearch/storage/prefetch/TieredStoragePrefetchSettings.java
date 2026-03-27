/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Settings for tiered storage prefetch behavior.
 * Constructor, dynamic update consumers, and getters/setters will be added in the implementation PR.
 */
public class TieredStoragePrefetchSettings {

    /** Private constructor. */
    private TieredStoragePrefetchSettings() {}

    /** Default read ahead block count. */
    public static final int DEFAULT_READ_AHEAD_BLOCK_COUNT = 4;
    /** DVD file suffix. */
    public static final String DVD_FILE_SUFFIX = "dvd";
    /** CFS file suffix. */
    public static final String CFS_FILE_SUFFIX = "cfs";

    /** Setting for read ahead block count. */
    public static final Setting<Integer> READ_AHEAD_BLOCK_COUNT = Setting.intSetting(
        "tiering.service.prefetch.read_ahead.block_count",
        DEFAULT_READ_AHEAD_BLOCK_COUNT,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Setting for stored fields prefetch enabled. */
    public static final Setting<Boolean> STORED_FIELDS_PREFETCH_ENABLED_SETTING = Setting.boolSetting(
        "tiering.service.prefetch.stored_fields.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** File formats enabled for read ahead. */
    public static final List<String> READ_AHEAD_ENABLE_FILE_FORMATS = List.of(DVD_FILE_SUFFIX);
}
