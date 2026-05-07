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

public class DataFusionPluginSettingsTests extends OpenSearchTestCase {

    public void testMemoryPoolLimitIsDynamic() {
        assertTrue(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.isDynamic());
    }

    public void testMemoryPoolLimitHasNodeScope() {
        assertTrue(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.hasNodeScope());
    }

    public void testPluginRegistersMemoryPoolLimitSetting() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(settings.contains(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT));
            assertTrue(settings.contains(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testUpdateMemoryPoolLimitBeforeServiceStartDoesNotThrow() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
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
            assertTrue(settingKeys.contains("datafusion.indexed.cost_predicate"));
            assertTrue(settingKeys.contains("datafusion.indexed.cost_collector"));
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
}
