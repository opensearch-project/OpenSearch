/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.MasterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public void testIsShardIndexingPressureAttributeEnabled() {
        // We are not able to test the condition when the ClusterService is null, as it is statically assigned from many tests
        ClusterApplierService mockClusterApplierService = mock(ClusterApplierService.class);
        when(mockClusterApplierService.isInitialClusterStateSet()).thenReturn(false);
        ClusterService localClusterService = new ClusterService(settings, clusterSettings,
            new MasterService(settings, clusterSettings, null), mockClusterApplierService);


        // False is returned when the initial cluster state is not set
        ShardIndexingPressureSettings shardIndexingPressureSettings = new ShardIndexingPressureSettings(localClusterService, settings,
            IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes());
        assertFalse(ShardIndexingPressureSettings.isShardIndexingPressureAttributeEnabled());

        // False is returned with empty node attributes
        when(mockClusterApplierService.isInitialClusterStateSet()).thenReturn(true);
        HashMap<String, String> attributes = new HashMap<>();
        DiscoveryNode discoveryNode = new DiscoveryNode("0", buildNewFakeTransportAddress(), attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode).build();
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterState.getNodes()).thenReturn(discoveryNodes);
        when(mockClusterApplierService.state()).thenReturn(mockClusterState);
        assertFalse(ShardIndexingPressureSettings.isShardIndexingPressureAttributeEnabled());

        // False is returned when the node attribute is set to false
        attributes.put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY, "false");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (Integer nodeID = 0; nodeID < random().nextInt(10); nodeID++) {
            DiscoveryNode node = new DiscoveryNode(nodeID.toString(), buildNewFakeTransportAddress(), attributes,
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
            builder.add(node);
        }
        discoveryNodes = builder.build();
        when(mockClusterState.getNodes()).thenReturn(discoveryNodes);
        assertFalse(ShardIndexingPressureSettings.isShardIndexingPressureAttributeEnabled());

        // True is returned when all nodes have the attribute set to true
        attributes.put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY, "true");
        builder = DiscoveryNodes.builder();
        for (Integer nodeID = 0; nodeID < random().nextInt(10); nodeID++) {
            DiscoveryNode node = new DiscoveryNode(nodeID.toString(), buildNewFakeTransportAddress(), attributes,
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
            builder.add(node);
        }
        discoveryNodes = builder.build();
        when(mockClusterState.getNodes()).thenReturn(discoveryNodes);
        assertTrue(ShardIndexingPressureSettings.isShardIndexingPressureAttributeEnabled());
    }
}
