/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LiquidCacheSettings}. Setting names remain under the
 * {@code datafusion.liquid_cache.*} namespace for backward compatibility.
 */
public class LiquidCacheSettingsTests extends OpenSearchTestCase {

    public void testEnabledSettingDefinition() {
        assertEquals("datafusion.liquid_cache.enabled", LiquidCacheSettings.LIQUID_CACHE_ENABLED.getKey());
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_ENABLED.get(Settings.EMPTY));
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_ENABLED.isDynamic());
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_ENABLED.hasNodeScope());
    }

    public void testSizeSettingDefinition() {
        assertEquals("datafusion.liquid_cache.size_bytes", LiquidCacheSettings.LIQUID_CACHE_SIZE.getKey());
        assertEquals(Long.valueOf(1024L * 1024 * 1024), LiquidCacheSettings.LIQUID_CACHE_SIZE.get(Settings.EMPTY));
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_SIZE.isDynamic());
    }

    public void testIndexedQueryMaxColumnsSettingDefinition() {
        assertEquals(
            "datafusion.liquid_cache.indexed_query.max_columns",
            LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS.getKey()
        );
        assertEquals(Integer.valueOf(10), LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS.get(Settings.EMPTY));
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS.isDynamic());
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS.hasNodeScope());
    }

    public void testListingTableMaxColumnsSettingDefinition() {
        assertEquals(
            "datafusion.liquid_cache.listing_table.max_columns",
            LiquidCacheSettings.LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS.getKey()
        );
        assertEquals(Integer.valueOf(4), LiquidCacheSettings.LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS.get(Settings.EMPTY));
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS.isDynamic());
    }

    public void testEvictionPolicySettingDefinition() {
        assertEquals("datafusion.liquid_cache.eviction_policy", LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.getKey());
        assertEquals("lru", LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.get(Settings.EMPTY));
        assertTrue(LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.hasNodeScope());
        assertFalse("eviction policy is Final, not dynamic", LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.isDynamic());
    }

    public void testEvictionPolicyValidationAndNormalization() {
        assertEquals("lru", LiquidCacheSettings.validateEvictionPolicy("LRU"));
        assertEquals("liquid", LiquidCacheSettings.validateEvictionPolicy("Liquid"));
        assertEquals(
            "lru",
            LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.get(
                Settings.builder().put("datafusion.liquid_cache.eviction_policy", "LRU").build()
            )
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LiquidCacheSettings.validateEvictionPolicy("fifo")
        );
        assertTrue(e.getMessage().contains("eviction_policy"));
    }

    public void testAllSettingsSize() {
        assertEquals(5, LiquidCacheSettings.ALL_SETTINGS.size());
    }
}
