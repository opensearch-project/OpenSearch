/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class CacheSettings {

    public static final String METADATA_CACHE_SIZE_LIMIT_KEY = "datafusion.metadata.cache.size.limit";
    public static final String STATISTICS_CACHE_SIZE_LIMIT_KEY = "datafusion.statistics.cache.size.limit";
    public static final Setting<ByteSizeValue> METADATA_CACHE_SIZE_LIMIT = new Setting<>(
        METADATA_CACHE_SIZE_LIMIT_KEY,
        "250mb",
        (s) -> ByteSizeValue.parseBytesSizeValue(s, new ByteSizeValue(1000, ByteSizeUnit.KB), METADATA_CACHE_SIZE_LIMIT_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> STATISTICS_CACHE_SIZE_LIMIT = new Setting<>(
        STATISTICS_CACHE_SIZE_LIMIT_KEY,
        "100mb",
        (s) -> ByteSizeValue.parseBytesSizeValue(s, new ByteSizeValue(0, ByteSizeUnit.KB), STATISTICS_CACHE_SIZE_LIMIT_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> METADATA_CACHE_EVICTION_TYPE = new Setting<String>(
        "datafusion.metadata.cache.eviction.type",
        "LRU",
        CacheSettings::validateEvictionType,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> STATISTICS_CACHE_EVICTION_TYPE = new Setting<String>(
        "datafusion.statistics.cache.eviction.type",
        "LRU",
        CacheSettings::validateEvictionType,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String METADATA_CACHE_ENABLED_KEY = "datafusion.metadata.cache.enabled";
    public static final Setting<Boolean> METADATA_CACHE_ENABLED = Setting.boolSetting(
        METADATA_CACHE_ENABLED_KEY,
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String STATISTICS_CACHE_ENABLED_KEY = "datafusion.statistics.cache.enabled";
    public static final Setting<Boolean> STATISTICS_CACHE_ENABLED = Setting.boolSetting(
        STATISTICS_CACHE_ENABLED_KEY,
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final List<Setting<?>> CACHE_SETTINGS = Arrays.asList(
        METADATA_CACHE_ENABLED,
        METADATA_CACHE_SIZE_LIMIT,
        METADATA_CACHE_EVICTION_TYPE,
        STATISTICS_CACHE_ENABLED,
        STATISTICS_CACHE_SIZE_LIMIT,
        STATISTICS_CACHE_EVICTION_TYPE
    );

    private static String validateEvictionType(String value) {
        String upper = value.toUpperCase(Locale.ROOT);
        if (!upper.equals("LRU") && !upper.equals("LFU")) {
            throw new IllegalArgumentException("Invalid eviction type '" + value + "'. Must be 'LRU' or 'LFU'.");
        }
        return upper;
    }
}
