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
import org.opensearch.cluster.node.DiscoveryNodeRole;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

    public void testClusterPrimaryShardLimit() {
        Settings settings = Settings.builder()
            .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 3)
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ShardsLimitAllocationDecider decider = new ShardsLimitAllocationDecider(settings, clusterSettings);

        // Create metadata for two indices
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();

        // Create routing table
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Setup routing for test1 (3 primaries)
        IndexRoutingTable.Builder test1RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test1").getIndex());

        // test1: First primary on node1
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 0, "node1", null, true, ShardRoutingState.STARTED));

        // test1: Second primary on node2
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 1, "node2", null, true, ShardRoutingState.STARTED));

        // test1: Third primary unassigned
        test1RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test1", 2, null, null, true, ShardRoutingState.UNASSIGNED));

        // Setup routing for test2 (2 primaries, 1 replica)
        IndexRoutingTable.Builder test2RoutingTableBuilder = IndexRoutingTable.builder(metadata.index("test2").getIndex());

        // test2: First primary on node1
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, true, ShardRoutingState.STARTED));

        // test2: Second primary on node2
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 1, "node2", null, true, ShardRoutingState.STARTED));

        // test2: First replica on node2
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 0, "node2", null, false, ShardRoutingState.STARTED));
        // test2: Second replica unassigned
        test2RoutingTableBuilder.addShard(TestShardRouting.newShardRouting("test2", 1, null, null, false, ShardRoutingState.UNASSIGNED));

        routingTableBuilder.add(test1RoutingTableBuilder.build());
        routingTableBuilder.add(test2RoutingTableBuilder.build());
        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        // Get shards for testing
        ShardRouting test1Shard1 = routingTable.index("test1").shard(0).primaryShard();
        ShardRouting test1Shard3 = routingTable.index("test1").shard(2).primaryShard();
        ShardRouting test2Replica2 = routingTable.index("test2").shard(1).replicaShards().get(0);

        // Test allocation decisions
        // Cannot allocate third primary to node1 (would exceed primary shard limit)
        assertEquals(NO, decider.canAllocate(test1Shard3, clusterState.getRoutingNodes().node("node1"), allocation).type());

        // Cannot allocate third primary to node2 (would exceed primary shard limit)
        assertEquals(NO, decider.canAllocate(test1Shard3, clusterState.getRoutingNodes().node("node2"), allocation).type());

        // Can allocate second replica to node1 (within total shard limit)
        assertEquals(YES, decider.canAllocate(test2Replica2, clusterState.getRoutingNodes().node("node1"), allocation).type());

        // Cannot allocate second replica to node2 (would exceed total shard limit)
        assertEquals(NO, decider.canAllocate(test2Replica2, clusterState.getRoutingNodes().node("node2"), allocation).type());

        // Existing primary can remain
        assertEquals(YES, decider.canRemain(test1Shard1, clusterState.getRoutingNodes().node("node1"), allocation).type());

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

        // Create index settings for three indices
        Settings indexSettingsTest1 = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
            .build();

        Settings indexSettingsTest2 = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        Settings indexSettingsTest3 = Settings.builder()
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(indexSettingsTest1).numberOfShards(3).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(indexSettingsTest2).numberOfShards(3).numberOfReplicas(0))
            .put(IndexMetadata.builder("test3").settings(indexSettingsTest3).numberOfShards(3).numberOfReplicas(0))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        // Set up routing table for test1
        IndexRoutingTable.Builder test1Builder = IndexRoutingTable.builder(metadata.index("test1").getIndex());
        test1Builder.addShard(TestShardRouting.newShardRouting("test1", 0, "node1", null, true, ShardRoutingState.STARTED));
        test1Builder.addShard(TestShardRouting.newShardRouting("test1", 1, "node2", null, true, ShardRoutingState.STARTED));
        test1Builder.addShard(TestShardRouting.newShardRouting("test1", 2, null, null, true, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test1Builder.build());

        // Set up routing table for test2
        IndexRoutingTable.Builder test2Builder = IndexRoutingTable.builder(metadata.index("test2").getIndex());
        test2Builder.addShard(TestShardRouting.newShardRouting("test2", 0, "node1", null, true, ShardRoutingState.STARTED));
        test2Builder.addShard(TestShardRouting.newShardRouting("test2", 1, "node2", null, true, ShardRoutingState.STARTED));
        test2Builder.addShard(TestShardRouting.newShardRouting("test2", 2, null, null, true, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test2Builder.build());

        // Set up routing table for test3
        IndexRoutingTable.Builder test3Builder = IndexRoutingTable.builder(metadata.index("test3").getIndex());
        test3Builder.addShard(TestShardRouting.newShardRouting("test3", 0, "node1", null, true, ShardRoutingState.STARTED));
        test3Builder.addShard(TestShardRouting.newShardRouting("test3", 1, "node2", null, true, ShardRoutingState.STARTED));
        test3Builder.addShard(TestShardRouting.newShardRouting("test3", 2, null, null, true, ShardRoutingState.UNASSIGNED));
        routingTableBuilder.add(test3Builder.build());

        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        RoutingAllocation allocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        allocation.debugDecision(true);

        // Get unassigned shards for testing
        ShardRouting test1Shard2 = routingTable.index("test1").shard(2).primaryShard();
        ShardRouting test2Shard2 = routingTable.index("test2").shard(2).primaryShard();
        ShardRouting test3Shard2 = routingTable.index("test3").shard(2).primaryShard();

        // Test assertions
        assertEquals(NO, decider.canAllocate(test1Shard2, clusterState.getRoutingNodes().node("node1"), allocation).type());  // Cannot
                                                                                                                              // assign 3rd
                                                                                                                              // shard of
                                                                                                                              // test1 to
                                                                                                                              // node1
        assertEquals(NO, decider.canAllocate(test3Shard2, clusterState.getRoutingNodes().node("node2"), allocation).type());  // Cannot
                                                                                                                              // assign 3rd
                                                                                                                              // shard of
                                                                                                                              // test3 to
                                                                                                                              // node2
        assertEquals(YES, decider.canAllocate(test2Shard2, clusterState.getRoutingNodes().node("node1"), allocation).type()); // Can assign
                                                                                                                              // 3rd shard
                                                                                                                              // of test2 to
                                                                                                                              // node1
        assertEquals(YES, decider.canAllocate(test2Shard2, clusterState.getRoutingNodes().node("node2"), allocation).type()); // Can assign
                                                                                                                              // 3rd shard
                                                                                                                              // of test2 to
                                                                                                                              // node2
    }

    private DiscoveryNode newNode(String nodeId) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT);
    }
}
