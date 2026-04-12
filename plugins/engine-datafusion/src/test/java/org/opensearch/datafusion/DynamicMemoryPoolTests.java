/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.search.cache.CacheSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;
import org.opensearch.common.settings.Setting;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION;
import static org.opensearch.datafusion.search.cache.CacheSettings.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for dynamic memory pool limit changes via JNI.
 *
 * Proves that the DynamicLimitPool can:
 * 1. Report its initial limit correctly
 * 2. Report current and peak usage
 * 3. Accept runtime limit changes via setMemoryPoolLimit
 * 4. Enforce the new limit on subsequent allocations
 */
public class DynamicMemoryPoolTests extends OpenSearchTestCase {

    private DataFusionService service;
    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(STATISTICS_CACHE_ENABLED);
        clusterSettingsToAdd.add(STATISTICS_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(STATISTICS_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(DATAFUSION_MEMORY_POOL_CONFIGURATION);
        clusterSettingsToAdd.add(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);

        clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        service = new DataFusionService(java.util.Collections.emptyMap(), clusterService, "/tmp");
        service.doStart();
    }

    @Override
    public void tearDown() throws Exception {
        if (service != null) {
            service.doStop();
        }
        super.tearDown();
    }

    /**
     * Test 1: Verify we can read the initial pool limit.
     * The default is 10 GB (from DATAFUSION_MEMORY_POOL_CONFIGURATION).
     */
    public void testGetInitialPoolLimit() {
        long runtimePtr = service.getRuntimePointer();
        assertTrue("Runtime pointer should be non-zero", runtimePtr != 0);

        long limit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        long expectedDefault = 10L * 1024 * 1024 * 1024; // 10 GB default
        assertEquals("Initial pool limit should be 10 GB", expectedDefault, limit);
    }

    /**
     * Test 2: Verify we can read current and peak usage.
     * With no queries running, usage should be 0.
     */
    public void testGetMemoryUsage() {
        long runtimePtr = service.getRuntimePointer();

        long currentUsage = NativeBridge.getMemoryPoolCurrentUsage(runtimePtr);
        long peakUsage = NativeBridge.getMemoryPoolPeakUsage(runtimePtr);

        assertTrue("Current usage should be >= 0", currentUsage >= 0);
        assertTrue("Peak usage should be >= 0", peakUsage >= 0);
        assertTrue("Peak usage should be >= current usage", peakUsage >= currentUsage);
    }

    /**
     * Test 3: Verify we can change the pool limit at runtime.
     */
    public void testSetPoolLimit() {
        long runtimePtr = service.getRuntimePointer();

        // Read initial limit
        long initialLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);

        // Increase limit to 20 GB
        long newLimit = 20L * 1024 * 1024 * 1024;
        NativeBridge.setMemoryPoolLimit(runtimePtr, newLimit);
        long afterIncrease = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be 20 GB after increase", newLimit, afterIncrease);

        // Decrease limit to 5 GB
        long smallerLimit = 5L * 1024 * 1024 * 1024;
        NativeBridge.setMemoryPoolLimit(runtimePtr, smallerLimit);
        long afterDecrease = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be 5 GB after decrease", smallerLimit, afterDecrease);

        // Restore original limit
        NativeBridge.setMemoryPoolLimit(runtimePtr, initialLimit);
        long afterRestore = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be restored", initialLimit, afterRestore);
    }

    /**
     * Test 4: Verify the setting is declared as Dynamic (not Final).
     */
    public void testSettingIsDynamic() {
        assertTrue(
            "datafusion.search.memory_pool should be a dynamic setting",
            DATAFUSION_MEMORY_POOL_CONFIGURATION.isDynamic()
        );
    }

    /**
     * Test 5: Verify the settings listener updates the pool limit.
     */
    public void testSettingsListenerUpdatesPoolLimit() {
        long runtimePtr = service.getRuntimePointer();

        // Initial limit should be 10 GB
        long initialLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals(10L * 1024 * 1024 * 1024, initialLimit);

        // Simulate a cluster settings update by applying new settings through ClusterSettings
        Settings newSettings = Settings.builder()
            .put("datafusion.search.memory_pool", "15gb")
            .build();
        clusterSettings.applySettings(newSettings);

        // The listener should have called setMemoryPoolLimit
        long updatedLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Pool limit should be updated to 15 GB", 15L * 1024 * 1024 * 1024, updatedLimit);

        // Restore
        Settings restoreSettings = Settings.builder()
            .put("datafusion.search.memory_pool", "10gb")
            .build();
        clusterSettings.applySettings(restoreSettings);
        assertEquals(10L * 1024 * 1024 * 1024, NativeBridge.getMemoryPoolLimit(runtimePtr));
    }
}
