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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Verifies the settings declared by {@link DataFusionPlugin} — in particular that
 * {@code datafusion.memory_pool_limit_bytes} is registered and marked dynamic so
 * the cluster settings API can update it at runtime.
 */
public class DataFusionPluginSettingsTests extends OpenSearchTestCase {

    public void testMemoryPoolLimitIsDynamic() {
        assertTrue(
            "datafusion.memory_pool_limit_bytes must be dynamic to support runtime updates",
            DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.isDynamic()
        );
    }

    public void testSpillMemoryLimitIsDynamic() {
        assertTrue(
            "datafusion.spill_memory_limit_bytes must be dynamic so cluster-state updates are accepted; "
                + "live propagation depends on the loaded native library",
            DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.isDynamic()
        );
        assertTrue(
            "datafusion.spill_memory_limit_bytes must have node scope",
            DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.hasNodeScope()
        );
    }

    /**
     * The cluster-settings listener can fire before {@link DataFusionPlugin#createComponents}
     * is called (service field still null). {@code updateSpillMemoryLimit} must swallow this
     * quietly to mirror {@link DataFusionPlugin#updateMemoryPoolLimit} so cluster-state
     * application does not log a spurious failure during node startup.
     */
    public void testUpdateSpillMemoryLimitBeforeServiceStartDoesNotThrow() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            plugin.updateSpillMemoryLimit(32L * 1024 * 1024);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testMemoryPoolLimitHasNodeScope() {
        assertTrue("datafusion.memory_pool_limit_bytes must have node scope", DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.hasNodeScope());
    }

    public void testPluginRegistersMemoryPoolLimitSetting() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "Plugin must register DATAFUSION_MEMORY_POOL_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT)
            );
            assertTrue(
                "Plugin must register DATAFUSION_SPILL_MEMORY_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT)
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
            assertTrue(settingKeys.contains("datafusion.indexed.bloom_filter_on_read"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_default"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_selectivity_threshold"));
            assertTrue(settingKeys.contains("datafusion.indexed.single_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.tree_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.max_collector_parallelism"));
            assertTrue(settingKeys.contains("datafusion.indexed.query_strategy"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testGetSettingsReturnsTotalExpectedCount() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertEquals(26, settings.size());
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

    public void testDeriveMemoryPoolLimitDefaultUnsetReturnsLongMaxValue() {
        // AC explicitly unconfigured — the default must be Long.MAX_VALUE (unbounded),
        // preserving pre-AC behaviour. The default for node.native_memory.limit is now
        // ram - heap, so to test the "unset" branch we must explicitly set it to 0.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.toString(Long.MAX_VALUE), DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testDeriveMemoryPoolLimitDefaultUsesNativeMemoryLimit() {
        // 10 GiB native memory limit — default takes 75% straight from limit, not
        // from limit - buffer_percent (which is AC's throttle margin, not a framework
        // budget reduction). 75% of 10 GiB.
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long expected = (10L * 1024 * 1024 * 1024) * 75 / 100;
        assertEquals(Long.toString(expected), DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testDeriveMemoryPoolLimitDefaultIgnoresBufferPercent() {
        // node.native_memory.buffer_percent is AC's throttle margin. The framework default
        // takes its fraction off node.native_memory.limit directly so the buffer can sit
        // between AC's throttle threshold and the framework's hard cap.
        // 1000 bytes limit, 20% buffer => pool max still 75% of 1000 = 750.
        Settings s = Settings.builder().put("node.native_memory.limit", "1000b").put("node.native_memory.buffer_percent", 20).build();
        assertEquals("750", DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testMemoryPoolLimitSettingExposesDerivedDefault() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long expected = (10L * 1024 * 1024 * 1024) * 75 / 100;
        assertEquals(Long.valueOf(expected), DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s));
    }

    public void testMemoryPoolLimitSettingExplicitOverridesDerived() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").put("datafusion.memory_pool_limit_bytes", 1024L).build();
        assertEquals(Long.valueOf(1024L), DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s));
    }

    public void testMemoryPoolLimitRejectsNegative() {
        Settings s = Settings.builder().put("datafusion.memory_pool_limit_bytes", -1L).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s)
        );
        assertTrue(e.getMessage().contains("must be >= 0"));
    }
}
