/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;

import java.util.List;
import java.util.Locale;

/**
 * Liquid Cache settings. Names are kept under {@code datafusion.liquid_cache.*}
 * for backward compatibility.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LiquidCacheSettings {

    public static final Setting<Boolean> LIQUID_CACHE_ENABLED = Setting.boolSetting(
        "datafusion.liquid_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Long> LIQUID_CACHE_SIZE = Setting.longSetting(
        "datafusion.liquid_cache.size_bytes",
        1L * 1024 * 1024 * 1024, // 1GB default
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** {@code lru} (default) or {@code liquid}. Built once at startup ({@code Final}). */
    public static final Setting<String> LIQUID_CACHE_EVICTION_POLICY = new Setting<>(
        "datafusion.liquid_cache.eviction_policy",
        "lru",
        LiquidCacheSettings::validateEvictionPolicy,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    static String validateEvictionPolicy(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        if (!normalized.equals("liquid") && !normalized.equals("lru")) {
            throw new IllegalArgumentException(
                "Invalid value [" + value + "] for [datafusion.liquid_cache.eviction_policy]. Must be 'liquid' or 'lru'."
            );
        }
        return normalized;
    }

    /** Max output columns for LC engagement on the indexed-query path. */
    public static final Setting<Integer> LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS = Setting.intSetting(
        "datafusion.liquid_cache.indexed_query.max_columns",
        10,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Max output columns for LC engagement on the listing-table path. */
    public static final Setting<Integer> LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS = Setting.intSetting(
        "datafusion.liquid_cache.listing_table.max_columns",
        4,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        LIQUID_CACHE_ENABLED,
        LIQUID_CACHE_SIZE,
        LIQUID_CACHE_EVICTION_POLICY,
        LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS,
        LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS
    );

    private LiquidCacheSettings() {}
}
