/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Verifies the settings declared by {@link DataFusionPlugin} — in particular that
 * {@code datafusion.memory_pool_limit} is registered and marked dynamic so
 * the cluster settings API can update it at runtime.
 */
public class DataFusionPluginSettingsTests extends OpenSearchTestCase {

    public void testMemoryPoolLimitIsDynamic() {
        assertTrue(
            "datafusion.memory_pool_limit must be dynamic to support runtime updates",
            DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.isDynamic()
        );
    }

    public void testMemoryPoolLimitHasNodeScope() {
        assertTrue("datafusion.memory_pool_limit must have node scope", DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.hasNodeScope());
    }

    public void testDiskSpillLimitIsStatic() {
        assertFalse(
            "datafusion.disk_spill_limit is intentionally static — DataFusion's DiskManager"
                + " has no thread-safe runtime setter (Arc::get_mut)",
            DataFusionPlugin.DATAFUSION_DISK_SPILL_LIMIT.isDynamic()
        );
    }

    public void testPluginRegistersMemoryPoolLimitSetting() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "Plugin must register DATAFUSION_MEMORY_POOL_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT)
            );
            assertTrue(
                "Plugin must register DATAFUSION_DISK_SPILL_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_DISK_SPILL_LIMIT)
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * H1 — the cluster-settings listener can fire before {@link DataFusionPlugin#createComponents}
     * is called (service field still null). {@code updateMemoryPoolLimit} must swallow this quietly
     * so the cluster-state update does not log a failure during node startup.
     */
    public void testUpdateMemoryPoolLimitBeforeServiceStartDoesNotThrow() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            // Service field is null — should be a no-op, not an NPE.
            plugin.updateMemoryPoolLimit(64L * 1024 * 1024);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testGetSettingsReturnsAllIndexedSettings() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            Set<String> settingKeys = settings.stream().map(Setting::getKey).collect(Collectors.toSet());

            assertTrue(settingKeys.contains("datafusion.indexed.batch_size"));
            assertTrue(settingKeys.contains("datafusion.indexed.parquet_pushdown_filters"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_default"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_selectivity_threshold"));
            assertTrue(settingKeys.contains("datafusion.indexed.single_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.tree_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.max_collector_parallelism"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testGetSettingsReturnsTotalExpectedCount() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertEquals(16, settings.size());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testDatafusionSettingsIsNullBeforeCreateComponents() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertNull(plugin.getDatafusionSettings());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testResolveDiskSpillBytesAbsoluteSize() {
        Settings settings = Settings.builder().put(DataFusionPlugin.DATAFUSION_DISK_SPILL_LIMIT.getKey(), "5gb").build();
        long resolved = DataFusionPlugin.resolveDiskSpillBytes(settings, createTempDir());
        assertEquals("Absolute byte size must pass through unchanged", 5L * 1024 * 1024 * 1024, resolved);
    }

    public void testResolveDiskSpillBytesPercentageAgainstSpillDirFilesystem() throws IOException {
        Path spillDir = createTempDir();
        Settings settings = Settings.builder().put(DataFusionPlugin.DATAFUSION_DISK_SPILL_LIMIT.getKey(), "20%").build();
        long resolved = DataFusionPlugin.resolveDiskSpillBytes(settings, spillDir);

        long usable = Environment.getFileStore(spillDir).getUsableSpace();
        long expected = (long) (usable * 0.20);
        // Allow ±5% drift — other processes (and OpenSearch's own indexing on shared CI runners)
        // can churn the FileStore between our resolve call and this assertion. 5% still validates
        // the right denominator was used; tighter tolerances flake on busy hosts.
        long tolerance = Math.max(1, expected / 20);
        assertTrue(
            "20% should resolve close to expected disk-relative bytes, got "
                + resolved
                + " expected ~"
                + expected
                + " (usable="
                + usable
                + ")",
            Math.abs(resolved - expected) <= tolerance
        );
    }

    public void testResolveDiskSpillBytesWalksUpToExistingParent() throws IOException {
        // The DataFusion `tmp` dir is created lazily — at resolve time it doesn't exist yet.
        Path parent = createTempDir();
        Path nonexistentSpillDir = parent.resolve("does-not-exist-yet").resolve("nested");
        Settings settings = Settings.builder().put(DataFusionPlugin.DATAFUSION_DISK_SPILL_LIMIT.getKey(), "10%").build();
        long resolved = DataFusionPlugin.resolveDiskSpillBytes(settings, nonexistentSpillDir);

        long usable = Environment.getFileStore(parent).getUsableSpace();
        long expected = (long) (usable * 0.10);
        long tolerance = Math.max(1, expected / 20);
        assertTrue(
            "Resolver must walk up to nearest existing parent on the same filesystem; got " + resolved + " expected ~" + expected,
            Math.abs(resolved - expected) <= tolerance
        );
    }
}
