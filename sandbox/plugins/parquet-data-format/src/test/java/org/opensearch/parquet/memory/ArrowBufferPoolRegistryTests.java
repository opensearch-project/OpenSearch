/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;

/**
 * End-to-end test that a cluster-settings PUT propagates through {@link ArrowBufferPoolRegistry}
 * to live {@link ArrowBufferPool} instances. We construct a real {@link ClusterSettings} so the
 * test exercises the actual listener wiring, not a mock.
 */
public class ArrowBufferPoolRegistryTests extends OpenSearchTestCase {

    private ClusterSettings buildClusterSettings(Settings nodeSettings) {
        Set<org.opensearch.common.settings.Setting<?>> all = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        all.add(ParquetSettings.MAX_NATIVE_ALLOCATION);
        return new ClusterSettings(nodeSettings, all);
    }

    public void testPutOnMaxNativeAllocationUpdatesRegisteredPool() {
        Settings nodeSettings = Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "80mb").build();
        ClusterSettings clusterSettings = buildClusterSettings(nodeSettings);
        ArrowBufferPoolRegistry registry = new ArrowBufferPoolRegistry(clusterSettings);

        try (ArrowBufferPool pool = new ArrowBufferPool(nodeSettings)) {
            registry.register(pool);
            assertEquals(1, registry.trackedPoolCount());
            assertEquals(80L * 1024 * 1024, pool.getRootLimit());
            assertEquals("child cap is root / 10", 8L * 1024 * 1024, pool.getMaxChildAllocation());

            // Simulate an operator PUT to the cluster settings API.
            clusterSettings.applySettings(Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "320mb").build());

            assertEquals("PUT must propagate through registry to the live pool", 320L * 1024 * 1024, pool.getRootLimit());
            assertEquals("child cap follows root / 10 after PUT", 32L * 1024 * 1024, pool.getMaxChildAllocation());
        }
    }

    public void testUnregisteredPoolDoesNotReceiveUpdates() {
        Settings nodeSettings = Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "80mb").build();
        ClusterSettings clusterSettings = buildClusterSettings(nodeSettings);
        ArrowBufferPoolRegistry registry = new ArrowBufferPoolRegistry(clusterSettings);

        try (ArrowBufferPool pool = new ArrowBufferPool(nodeSettings)) {
            registry.register(pool);
            registry.unregister(pool);
            assertEquals(0, registry.trackedPoolCount());

            clusterSettings.applySettings(Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "320mb").build());

            assertEquals("unregistered pool must not receive updates", 80L * 1024 * 1024, pool.getRootLimit());
        }
    }

    public void testMultiplePoolsAllReceiveUpdate() {
        Settings nodeSettings = Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "80mb").build();
        ClusterSettings clusterSettings = buildClusterSettings(nodeSettings);
        ArrowBufferPoolRegistry registry = new ArrowBufferPoolRegistry(clusterSettings);

        try (ArrowBufferPool poolA = new ArrowBufferPool(nodeSettings); ArrowBufferPool poolB = new ArrowBufferPool(nodeSettings)) {
            registry.register(poolA);
            registry.register(poolB);
            assertEquals(2, registry.trackedPoolCount());

            clusterSettings.applySettings(Settings.builder().put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "160mb").build());

            assertEquals(160L * 1024 * 1024, poolA.getRootLimit());
            assertEquals(160L * 1024 * 1024, poolB.getRootLimit());
        }
    }
}
