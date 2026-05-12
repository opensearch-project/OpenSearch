/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link BlockCacheFoyerPlugin}.
 *
 * Covers constructor variants, getBlockCache lifecycle, requestedCapacityBytes,
 * dataToCapacityRatio, getSettings, and lifecycle guards — all without the native
 * library.
 */
public class BlockCacheFoyerPluginTests extends OpenSearchTestCase {

    // ── Constructor + getBlockCache ───────────────────────────────────────────

    public void testNoArgConstructor() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin);
        assertTrue("handle is empty before createComponents", plugin.getBlockCache().isEmpty());
    }

    public void testSettingsConstructor() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertTrue(plugin.getBlockCache().isEmpty());
    }

    public void testGetBlockCacheReturnsNonNullOptional() {
        assertNotNull(new BlockCacheFoyerPlugin().getBlockCache());
    }

    // ── Lifecycle guards ──────────────────────────────────────────────────────

    public void testCloseWithNoCacheDoesNotThrow() throws IOException {
        new BlockCacheFoyerPlugin().close();
    }

    public void testCloseIsIdempotent() throws IOException {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.close();
        plugin.close();
    }

    // ── requestedCapacityBytes ────────────────────────────────────────────────

    public void testRequestedCapacityBytesDefault25Percent() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        long budget = 800L * 1024 * 1024 * 1024;
        assertEquals(200L * 1024 * 1024 * 1024, plugin.requestedCapacityBytes(Settings.EMPTY, budget));
    }

    public void testRequestedCapacityBytes50Percent() {
        Settings s = Settings.builder().put("block_cache.size", "50%").build();
        assertEquals(500L, new BlockCacheFoyerPlugin().requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesZeroPercent() {
        Settings s = Settings.builder().put("block_cache.size", "0%").build();
        assertEquals(0L, new BlockCacheFoyerPlugin().requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesDecimalRatioForm() {
        Settings s = Settings.builder().put("block_cache.size", "0.25").build();
        assertEquals(250L, new BlockCacheFoyerPlugin().requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesRoundsCorrectly() {
        Settings s = Settings.builder().put("block_cache.size", "33%").build();
        assertEquals(33L, new BlockCacheFoyerPlugin().requestedCapacityBytes(s, 100L));
    }

    public void testRequestedCapacityBytesZeroBudget() {
        assertEquals(0L, new BlockCacheFoyerPlugin().requestedCapacityBytes(Settings.EMPTY, 0L));
    }

    public void testRequestedCapacityBytesNonNegative() {
        long result = new BlockCacheFoyerPlugin().requestedCapacityBytes(Settings.EMPTY, 1_000_000L);
        assertTrue(result >= 0);
    }

    public void testRequestedCapacityBytesDoesNotExceedBudget() {
        long budget = 1_000_000L;
        assertTrue(new BlockCacheFoyerPlugin().requestedCapacityBytes(Settings.EMPTY, budget) <= budget);
    }

    // ── dataToCapacityRatio ───────────────────────────────────────────────────

    public void testDataToCapacityRatioDefault() {
        assertEquals(5.0, new BlockCacheFoyerPlugin().dataToCapacityRatio(Settings.EMPTY), 0.0);
    }

    public void testDataToCapacityRatioCustom() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "10.0").build();
        assertEquals(10.0, new BlockCacheFoyerPlugin().dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioMinimumOf1() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "1.0").build();
        assertEquals(1.0, new BlockCacheFoyerPlugin().dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioAtLeastOne() {
        assertTrue(new BlockCacheFoyerPlugin().dataToCapacityRatio(Settings.EMPTY) >= 1.0);
    }

    // ── setReservedCapacityBytes ──────────────────────────────────────────────

    public void testSetReservedCapacityBytesDoesNotThrow() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.setReservedCapacityBytes(200L * 1024 * 1024 * 1024);
    }

    // ── getSettings ───────────────────────────────────────────────────────────

    public void testGetSettingsRegistersAllFourSettings() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(4, settings.size());
        assertTrue(settings.contains(FoyerBlockCacheSettings.CACHE_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.BLOCK_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.IO_ENGINE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING));
    }

    public void testGetSettingsNoNulls() {
        for (Setting<?> s : new BlockCacheFoyerPlugin().getSettings()) {
            assertNotNull(s);
        }
    }

    public void testGetSettingsIsStable() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertTrue(plugin.getSettings().containsAll(plugin.getSettings()));
    }
}
