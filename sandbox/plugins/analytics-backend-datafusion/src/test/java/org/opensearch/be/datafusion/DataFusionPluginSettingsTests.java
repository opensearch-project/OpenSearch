/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Setting;
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

    /**
     * Verifies that {@code getSettings()} includes all 7 new indexed query settings
     * defined in {@link DatafusionSettings}.
     */
    public void testGetSettingsReturnsAllIndexedSettings() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            Set<String> settingKeys = settings.stream().map(Setting::getKey).collect(Collectors.toSet());

            assertTrue("Must contain datafusion.indexed.batch_size", settingKeys.contains("datafusion.indexed.batch_size"));
            assertTrue(
                "Must contain datafusion.indexed.parquet_pushdown_filters",
                settingKeys.contains("datafusion.indexed.parquet_pushdown_filters")
            );
            assertTrue(
                "Must contain datafusion.indexed.min_skip_run_default",
                settingKeys.contains("datafusion.indexed.min_skip_run_default")
            );
            assertTrue(
                "Must contain datafusion.indexed.min_skip_run_selectivity_threshold",
                settingKeys.contains("datafusion.indexed.min_skip_run_selectivity_threshold")
            );
            assertTrue("Must contain datafusion.indexed.cost_predicate", settingKeys.contains("datafusion.indexed.cost_predicate"));
            assertTrue("Must contain datafusion.indexed.cost_collector", settingKeys.contains("datafusion.indexed.cost_collector"));
            assertTrue(
                "Must contain datafusion.indexed.max_collector_parallelism",
                settingKeys.contains("datafusion.indexed.max_collector_parallelism")
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Verifies that {@code getSettings()} returns the total expected count of settings:
     * 9 existing (3 from DataFusionPlugin + 6 from CacheSettings) + 7 new indexed = 16.
     */
    public void testGetSettingsReturnsTotalExpectedCount() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertEquals("Plugin must register exactly 16 settings (9 existing + 7 new indexed)", 16, settings.size());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Verifies that {@code getDatafusionSettings()} returns null before {@code createComponents()}
     * is called, since the volatile field is initialized to null.
     */
    public void testDatafusionSettingsIsNullBeforeCreateComponents() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertNull("DatafusionSettings must be null before createComponents() is called", plugin.getDatafusionSettings());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
