/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class ShardIndexingPressureSettingsTests extends OpenSearchTestCase {

    private final Settings settings = Settings.builder()
        .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10MB")
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 2000)
        .put(ShardIndexingPressureSettings.SHARD_MIN_LIMIT.getKey(), 0.001d)
        .build();

    final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    final ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

    public void testFromSettings() {
        ShardIndexingPressureSettings shardIndexingPressureSettings = new ShardIndexingPressureSettings(clusterService, settings,
            IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes());

        assertTrue(shardIndexingPressureSettings.isShardIndexingPressureEnabled());
        assertTrue(shardIndexingPressureSettings.isShardIndexingPressureEnforced());
        assertEquals(2000, shardIndexingPressureSettings.getRequestSizeWindow());

        // Node level limits
        long nodePrimaryAndCoordinatingLimits = shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits();
        long tenMB = 10 * 1024 * 1024;
        assertEquals(tenMB, nodePrimaryAndCoordinatingLimits);
        assertEquals((long)(tenMB * 1.5), shardIndexingPressureSettings.getNodeReplicaLimits());

        // Shard Level Limits
        long shardPrimaryAndCoordinatingBaseLimits = (long) (nodePrimaryAndCoordinatingLimits * 0.001d);
        assertEquals(shardPrimaryAndCoordinatingBaseLimits, shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits());
        assertEquals((long)(shardPrimaryAndCoordinatingBaseLimits * 1.5),
            shardIndexingPressureSettings.getShardReplicaBaseLimits());
    }

    public void testUpdateSettings() {
        ShardIndexingPressureSettings shardIndexingPressureSettings = new ShardIndexingPressureSettings(clusterService, settings,
            IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes());

        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
                .put(ShardIndexingPressureSettings.REQUEST_SIZE_WINDOW.getKey(), 4000)
                .put(ShardIndexingPressureSettings.SHARD_MIN_LIMIT.getKey(), 0.003d)
                .build(),
            Settings.builder().put(settings), updated, getTestClass().getName());
        clusterSettings.applySettings(updated.build());

        assertFalse(shardIndexingPressureSettings.isShardIndexingPressureEnabled());
        assertFalse(shardIndexingPressureSettings.isShardIndexingPressureEnforced());
        assertEquals(4000, shardIndexingPressureSettings.getRequestSizeWindow());

        // Node level limits
        long nodePrimaryAndCoordinatingLimits = shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits();
        long tenMB = 10 * 1024 * 1024;
        assertEquals(tenMB, nodePrimaryAndCoordinatingLimits);
        assertEquals((long)(tenMB * 1.5), shardIndexingPressureSettings.getNodeReplicaLimits());

        // Shard Level Limits
        long shardPrimaryAndCoordinatingBaseLimits = (long) (nodePrimaryAndCoordinatingLimits * 0.003d);
        assertEquals(shardPrimaryAndCoordinatingBaseLimits, shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits());
        assertEquals((long)(shardPrimaryAndCoordinatingBaseLimits * 1.5),
            shardIndexingPressureSettings.getShardReplicaBaseLimits());
    }
}
