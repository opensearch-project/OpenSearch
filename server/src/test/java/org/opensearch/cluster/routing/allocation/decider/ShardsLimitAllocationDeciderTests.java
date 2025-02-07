/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.opensearch.cluster.routing.allocation.decider.Decision.Type.YES;

public class ShardsLimitAllocationDeciderTests extends OpenSearchTestCase {

    public void testWithNoLimit() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ShardsLimitAllocationDecider decider = new ShardsLimitAllocationDecider(settings, clusterSettings);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(0))
            .build();

        // Create a RoutingTable with shards 0 and 1 initialized on node1, and shard 2 unassigned
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test").getIndex());

        // Shard 0 and 1: STARTED on node1
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED));
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 1, "node1", null, true, ShardRoutingState.STARTED));

        // Shard 2: Unassigned
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 2, null, null, true, ShardRoutingState.UNASSIGNED));

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting shard1 = routingTable.index("test").shard(0).primaryShard();
        ShardRouting shard2 = routingTable.index("test").shard(1).primaryShard();
        ShardRouting shard3 = routingTable.index("test").shard(2).primaryShard();

        // Test allocation decisions
        assertEquals(YES, decider.canAllocate(shard3, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canAllocate(shard3, clusterState.getRoutingNodes().node("node2"), allocation).type());
    }

    public void testClusterShardLimit() {
        Settings settings = Settings.builder().put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ShardsLimitAllocationDecider decider = new ShardsLimitAllocationDecider(settings, clusterSettings);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(0))
            .build();

        // Create a RoutingTable with shards 0 and 1 initialized on node1, and shard 2 unassigned
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test").getIndex());

        // Shard 0 and 1: STARTED on node1
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 0, "node1", null, true, ShardRoutingState.STARTED));
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 1, "node1", null, true, ShardRoutingState.STARTED));

        // Shard 2: Unassigned
        indexRoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test", 2, null, null, true, ShardRoutingState.UNASSIGNED));

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        ShardRouting shard1 = routingTable.index("test").shard(0).primaryShard();
        ShardRouting shard2 = routingTable.index("test").shard(1).primaryShard();
        ShardRouting shard3 = routingTable.index("test").shard(2).primaryShard();

        // Test allocation decisions
        assertEquals(NO, decider.canAllocate(shard3, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canRemain(shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canAllocate(shard3, clusterState.getRoutingNodes().node("node2"), allocation).type());
    }

    public void testIndexShardLimit() {
        Settings clusterSettings = Settings.builder()
            .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .build();
        ClusterSettings clusterSettingsObject = new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ShardsLimitAllocationDecider decider = new ShardsLimitAllocationDecider(clusterSettings, clusterSettingsObject);

        // Create index settings with INDEX_TOTAL_SHARDS_PER_NODE_SETTING and version
        Settings indexSettings = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)  // Set index-level limit to 1
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(indexSettings).numberOfShards(3).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(indexSettings).numberOfShards(3).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Set up routing table for test1
        IndexRoutingTable.Builder test1RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test1").getIndex());
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 0, "node1", null, true, ShardRoutingState.STARTED));
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 1, null, null, true, ShardRoutingState.UNASSIGNED));
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 2, null, null, true, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test1RoutingTableBuilder.build());

        // Set up routing table for test2
        IndexRoutingTable.Builder test2RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, true, ShardRoutingState.STARTED));
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 1, null, null, true, ShardRoutingState.UNASSIGNED));
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 2, null, null, true, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test2RoutingTableBuilder.build());

        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        // Test allocation decisions
        ShardRouting test1Shard1 = routingTable.index("test1").shard(1).primaryShard();
        ShardRouting test1Shard2 = routingTable.index("test1").shard(2).primaryShard();
        ShardRouting test2Shard1 = routingTable.index("test2").shard(1).primaryShard();
        ShardRouting test2Shard2 = routingTable.index("test2").shard(2).primaryShard();

        assertEquals(NO, decider.canAllocate(test1Shard2, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canRemain(test1Shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canAllocate(test1Shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());
        assertEquals(NO, decider.canAllocate(test2Shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());
        assertEquals(YES, decider.canRemain(test2Shard1, clusterState.getRoutingNodes().node("node2"), allocation).type());
        assertEquals(YES, decider.canAllocate(test2Shard2, clusterState.getRoutingNodes().node("node1"), allocation).type());
    }

    public void testIndexPrimaryShardLimit() {
        Settings clusterSettings = Settings.builder()
            .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), -1)
            .build();
        ClusterSettings clusterSettingsObject = new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ShardsLimitAllocationDecider decider = new ShardsLimitAllocationDecider(clusterSettings, clusterSettingsObject);

        // Create index settings with INDEX_TOTAL_SHARDS_PER_NODE_SETTING and version
        Settings indexSettingsTest1 = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)  // Set index-level limit to 2
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();
        Settings indexSettingsTest2 = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)  // Set index-level limit to 2
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(indexSettingsTest1).numberOfShards(3).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(indexSettingsTest2).numberOfShards(3).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Set up routing table for test1
        IndexRoutingTable.Builder test1RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test1").getIndex());
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 0, "node1", null, true, ShardRoutingState.STARTED));
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 0, "node2", null, false, ShardRoutingState.STARTED));
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 1, null, null, true, ShardRoutingState.UNASSIGNED));
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 1, null, null, false, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test1RoutingTableBuilder.build());

        // Set up routing table for test2
        IndexRoutingTable.Builder test2RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, true, ShardRoutingState.STARTED));
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, false, ShardRoutingState.STARTED));
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 1, null, null, true, ShardRoutingState.UNASSIGNED));
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 1, null, null, false, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test2RoutingTableBuilder.build());

        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        // Test allocation decisions
        ShardRouting test1Shard1Replica = routingTable.index("test1").shard(1).replicaShards().get(0);
        ShardRouting test1Shard1Primary = routingTable.index("test1").shard(1).primaryShard();
        ShardRouting test2Shard1Primary = routingTable.index("test2").shard(1).primaryShard();
        ShardRouting test2Shard0Replica = routingTable.index("test2").shard(0).replicaShards().get(0);

        assertEquals(NO, decider.canAllocate(test1Shard1Primary, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canAllocate(test1Shard1Primary, clusterState.getRoutingNodes().node("node2"), allocation).type());
        assertEquals(YES, decider.canAllocate(test1Shard1Replica, clusterState.getRoutingNodes().node("node1"), allocation).type());
        assertEquals(YES, decider.canAllocate(test2Shard1Primary, clusterState.getRoutingNodes().node("node2"), allocation).type());
        assertEquals(YES, decider.canAllocate(test2Shard1Primary, clusterState.getRoutingNodes().node("node1"), allocation).type());
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Version.CURRENT);
    }
}
