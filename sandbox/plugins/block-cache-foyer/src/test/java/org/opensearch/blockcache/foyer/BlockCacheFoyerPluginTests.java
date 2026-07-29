/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTestCase.LockFeatureFlag;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertTrue("handle is empty before createComponents", plugin.getBlockCache().isEmpty());
    }

    public void testSettingsConstructor() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertTrue(plugin.getBlockCache().isEmpty());
    }

    public void testGetBlockCacheReturnsNonNullOptional() {
        assertNotNull(new BlockCacheFoyerPlugin(Settings.EMPTY).getBlockCache());
    }

    // ── Lifecycle guards ──────────────────────────────────────────────────────

    public void testCloseWithNoCacheDoesNotThrow() throws IOException {
        new BlockCacheFoyerPlugin(Settings.EMPTY).close();
    }

    public void testCloseIsIdempotent() throws IOException {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        plugin.close();
        plugin.close();
    }

    // ── requestedCapacityBytes ────────────────────────────────────────────────

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesDefault50Percent() {
        long budget = 800L * 1024 * 1024 * 1024;
        assertEquals(400L * 1024 * 1024 * 1024, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(Settings.EMPTY, budget));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytes50Percent() {
        Settings s = Settings.builder().put("block_cache.foyer.size", "50%").build();
        assertEquals(500L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(s, 1000L));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesZeroPercent() {
        Settings s = Settings.builder().put("block_cache.foyer.size", "0%").build();
        assertEquals(0L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(s, 1000L));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesDecimalRatioForm() {
        Settings s = Settings.builder().put("block_cache.foyer.size", "0.25").build();
        assertEquals(250L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(s, 1000L));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesRoundsCorrectly() {
        Settings s = Settings.builder().put("block_cache.foyer.size", "33%").build();
        assertEquals(33L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(s, 100L));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesZeroBudget() {
        assertEquals(0L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(Settings.EMPTY, 0L));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesNonNegative() {
        long result = new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(Settings.EMPTY, 1_000_000L);
        assertTrue(result >= 0);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testRequestedCapacityBytesDoesNotExceedBudget() {
        long budget = 1_000_000L;
        assertTrue(new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(Settings.EMPTY, budget) <= budget);
    }

    public void testRequestedCapacityBytesIsZeroWhenPluggableDataformatDisabled() {
        long budget = 100L * 1024 * 1024 * 1024;
        assertEquals(0L, new BlockCacheFoyerPlugin(Settings.EMPTY).requestedCapacityBytes(Settings.EMPTY, budget));
    }

    // ── dataToCapacityRatio ───────────────────────────────────────────────────

    public void testDataToCapacityRatioDefault() {
        assertEquals(5.0, new BlockCacheFoyerPlugin(Settings.EMPTY).dataToCapacityRatio(Settings.EMPTY), 0.0);
    }

    public void testDataToCapacityRatioCustom() {
        // Uses cluster.filecache.remote_data_ratio — the canonical server-side setting
        Settings s = Settings.builder().put("cluster.filecache.remote_data_ratio", "10.0").build();
        assertEquals(10.0, new BlockCacheFoyerPlugin(Settings.EMPTY).dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioMinimumOf1() {
        // Uses cluster.filecache.remote_data_ratio — the canonical server-side setting
        Settings s = Settings.builder().put("cluster.filecache.remote_data_ratio", "1.0").build();
        assertEquals(1.0, new BlockCacheFoyerPlugin(Settings.EMPTY).dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioUsesFileCacheRatioSetting() {
        // Explicitly verify cluster.filecache.remote_data_ratio drives the ratio
        Settings s = Settings.builder().put("cluster.filecache.remote_data_ratio", "7.0").build();
        assertEquals(7.0, new BlockCacheFoyerPlugin(Settings.EMPTY).dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioIgnoresRemovedSetting() {
        // block_cache.foyer.data_to_cache_ratio is removed — the plugin-owned setting list
        // no longer contains it, so it must not affect dataToCapacityRatio
        List<Setting<?>> pluginSettings = new BlockCacheFoyerPlugin(Settings.EMPTY).getSettings();
        boolean hasOldSetting = pluginSettings.stream().anyMatch(s -> s.getKey().equals("block_cache.foyer.data_to_cache_ratio"));
        assertFalse("Removed setting must not be registered by the plugin", hasOldSetting);
    }

    public void testDataToCapacityRatioAtLeastOne() {
        assertTrue(new BlockCacheFoyerPlugin(Settings.EMPTY).dataToCapacityRatio(Settings.EMPTY) >= 1.0);
    }

    // ── setReservedCapacityBytes ──────────────────────────────────────────────

    public void testSetReservedCapacityBytesDoesNotThrow() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        plugin.setReservedCapacityBytes(200L * 1024 * 1024 * 1024);
    }

    // ── getSettings ───────────────────────────────────────────────────────────

    public void testGetSettingsRegistersAllSettings() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(10, settings.size());
        assertTrue(settings.contains(FoyerBlockCacheSettings.CACHE_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.BLOCK_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.IO_ENGINE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.KEY_INDEX_SWEEP_INTERVAL_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.KEY_INDEX_PERSIST_INTERVAL_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING));
    }

    public void testGetSettingsNoNulls() {
        for (Setting<?> s : new BlockCacheFoyerPlugin(Settings.EMPTY).getSettings()) {
            assertNotNull(s);
        }
    }

    public void testGetSettingsIsStable() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertTrue(plugin.getSettings().containsAll(plugin.getSettings()));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testCreateComponentsThrowsWhenBlockSizeExceedsDiskBudget() {
        // block_size (2MB) >= disk budget (1MB) should throw SettingsException
        Settings settings = Settings.builder()
            .put(FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.getKey(), new ByteSizeValue(2, ByteSizeUnit.MB))
            .build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(settings);
        plugin.setReservedCapacityBytes(new ByteSizeValue(1, ByteSizeUnit.MB).getBytes());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);

        Environment environment = mock(Environment.class);
        when(environment.dataFiles()).thenReturn(new Path[] { createTempDir() });

        expectThrows(
            SettingsException.class,
            () -> plugin.createComponents(null, clusterService, null, null, null, null, environment, null, null, null, null)
        );
    }

    // flag is OFF by default — no @LockFeatureFlag annotation needed
    public void testCreateComponentsReturnsEmptyWhenPluggableDataformatDisabled() throws IOException {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        plugin.setReservedCapacityBytes(new ByteSizeValue(10, ByteSizeUnit.GB).getBytes());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        Environment environment = mock(Environment.class);
        when(environment.dataFiles()).thenReturn(new Path[] { createTempDir() });

        java.util.Collection<Object> result = plugin.createComponents(
            null,
            clusterService,
            null,
            null,
            null,
            null,
            environment,
            null,
            null,
            null,
            null
        );
        assertTrue("createComponents must return empty when flag is disabled", result.isEmpty());
        assertTrue("getBlockCache must be empty when flag is disabled", plugin.getBlockCache().isEmpty());
    }
}
