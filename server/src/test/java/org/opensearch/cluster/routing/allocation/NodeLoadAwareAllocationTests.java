/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.NodeLoadAwareAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.settings.Settings;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class NodeLoadAwareAllocationTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(NodeLoadAwareAllocationTests.class);

    public void testSingleZoneZeroReplicaUnassignedPrimaryAllocation() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),5)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true)
            .build());

        logger.info("Building initial routing table for 'testSingleZoneZeroReplicaUnassignedPrimaryAllocation'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding three nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone_1")))
            .add(newNode("node2", singletonMap("zone", "zone_1")))
            .add(newNode("node3", singletonMap("zone", "zone_1")))
            .add(newNode("node4", singletonMap("zone", "zone_1")))
            .add(newNode("node5", singletonMap("zone", "zone_1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));

        logger.info("--> Remove node from zone holding primaries");
        ClusterState newState = removeNode(clusterState, "node1", strategy);
        logger.info("--> Remove node from zone holding primaries");
        newState = removeNode(newState, "node2", strategy);
        logger.info("--> Remove node from zone holding primaries");
        newState = removeNode(newState, "node3", strategy);

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable())
            .addAsNew(metadata.index("test1"))
            .build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        logger.info("no limits should be applied on newly create primaries");
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(28));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(12));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
        }

        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node1", singletonMap("zone", "zone_1"))))
            .build();

        //4 existing shards from this node's local store get started
        newState = strategy.reroute(newState, "reroute");
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(32));

        //add back node2 when skewness is still breached
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node2", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        //add back node3
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node3", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
    }

    public void testSingleZoneOneReplicaLimitsShardAllocationOnOverloadNoUnassignedPrimaries() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(), 5)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false)
            .build());

        logger.info("Building initial routing table for 'testSingleZoneOneReplicaLimitsShardAllocationOnOverloadNoUnassignedPrimaries'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding three nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone_1")))
            .add(newNode("node2", singletonMap("zone", "zone_1")))
            .add(newNode("node3", singletonMap("zone", "zone_1")))
            .add(newNode("node4", singletonMap("zone", "zone_1")))
            .add(newNode("node5", singletonMap("zone", "zone_1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));

        logger.info("--> Remove node from zone holding primaries");
        ClusterState newState = removeNode(clusterState, "node1", strategy);
        logger.info("--> Remove node from zone holding primaries");
        newState = removeNode(newState, "node2", strategy);
        logger.info("--> Remove node from zone holding primaries");
        newState = removeNode(newState, "node3", strategy);

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable())
            .addAsNew(metadata.index("test1"))
            .build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        logger.info("no limits should be applied on newly create primaries");
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(20));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), is(oneOf(UnassignedInfo.Reason.NODE_LEFT, UnassignedInfo.Reason.INDEX_CREATED)));
        }

        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node1", singletonMap("zone", "zone_1"))))
            .build();

        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(30));

        //add back node2 when skewness is still breached
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node2", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        //add back node3
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node3", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
    }

    public void testSingleZoneOneReplicaLimitsShardAllocationOnOverload() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),5)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false)
            .build());

        logger.info("Building initial routing table for 'testSingleZoneOneReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding three nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone_1")))
            .add(newNode("node2", singletonMap("zone", "zone_1")))
            .add(newNode("node3", singletonMap("zone", "zone_1")))
            .add(newNode("node4", singletonMap("zone", "zone_1")))
            .add(newNode("node5", singletonMap("zone", "zone_1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));

        logger.info("--> Remove node from zone");
        ClusterState newState = removeNode(clusterState, "node1", strategy);
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(10));
        }

        logger.info("--> Remove node from zone when the limit of overload is reached");
        newState = removeNode(newState, "node2", strategy);
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(30));

        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
            assertFalse(shard.primary());
        }

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            ))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable())
            .addAsNew(metadata.index("test1"))
            .build();
        //increases avg shard per node to 80/5 = 16, overload factor 1.2, total allowed 20
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(20));

        logger.info("add another index with 60 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 60)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ))
            .build();
        updatedRoutingTable = RoutingTable.builder(newState.routingTable())
            .addAsNew(metadata.index("test2"))
            .build();
        //increases avg shard per node to 140/5 = 28, overload factor 1.2, total allowed 34 per node but still ALL primaries get assigned
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(120));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(20));

        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertFalse(shard.primary());
        }

        strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(), 5)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false)
            .build());

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(40));
        }

        logger.info("add another index with 5 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(IndexMetadata.builder("test3").settings(settings(Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ))
            .build();
        updatedRoutingTable = RoutingTable.builder(newState.routingTable())
            .addAsNew(metadata.index("test3"))
            .build();
        //increases avg shard per node to 145/5 = 29, overload factor 1.2, total allowed 35 per node and NO primaries get assigned
        //since total owning shards are 40 per node already
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(120));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(25));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).stream().filter(x -> x.primary()).count(), equalTo(5L));
    }

    public void testThreeZoneTwoReplicaLimitsShardAllocationOnOverload(){
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),15)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "zone_1,zone_2,zone_3")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .build());

        logger.info("Building initial routing table for 'testThreeZoneTwoReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone1")))
            .add(newNode("node2", singletonMap("zone", "zone1")))
            .add(newNode("node3", singletonMap("zone", "zone1")))
            .add(newNode("node4", singletonMap("zone", "zone1")))
            .add(newNode("node5", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one zone value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        //replicas are unassigned
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(40));

        logger.info("--> add five new node in new zone and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node6", singletonMap("zone", "zone2")))
            .add(newNode("node7", singletonMap("zone", "zone2")))
            .add(newNode("node8", singletonMap("zone", "zone2")))
            .add(newNode("node9", singletonMap("zone", "zone2")))
            .add(newNode("node10", singletonMap("zone", "zone2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(20));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(40));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another five node in new zone and reroute");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node11", singletonMap("zone", "zone3")))
            .add(newNode("node12", singletonMap("zone", "zone3")))
            .add(newNode("node13", singletonMap("zone", "zone3")))
            .add(newNode("node14", singletonMap("zone", "zone3")))
            .add(newNode("node15", singletonMap("zone", "zone3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));

        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(4));

        logger.info("--> Remove three node from zone3 holding primary and replicas");
        //remove one nodes in one zone to cause distribution zone1->5 , zone2->5, zone3->4
        newState = removeNode(newState, "node11", strategy);
        newState = removeNode(newState, "node12", strategy);
        newState = removeNode(newState, "node13", strategy);
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(5));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(5));

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node11", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        assertThat(newState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(5));
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(5));

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node12", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        assertThat(newState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(5));
        newState = startInitializingShardsAndReroute(strategy, newState);
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(5));

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node13", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

       while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        //ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThreeZoneOneReplicaLimitsShardAllocationOnOverload(){
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),15)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "zone_1,zone_2,zone_3")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 20)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true)
            .build());

        logger.info("Building initial routing table for 'testThreeZoneOneReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(30).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone1")))
            .add(newNode("node2", singletonMap("zone", "zone1")))
            .add(newNode("node3", singletonMap("zone", "zone1")))
            .add(newNode("node4", singletonMap("zone", "zone1")))
            .add(newNode("node5", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(30));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one zone value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(30));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        //replicas are unassigned
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(30));

        logger.info("--> add five new node in new zone and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node6", singletonMap("zone", "zone2")))
            .add(newNode("node7", singletonMap("zone", "zone2")))
            .add(newNode("node8", singletonMap("zone", "zone2")))
            .add(newNode("node9", singletonMap("zone", "zone2")))
            .add(newNode("node10", singletonMap("zone", "zone2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(30));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(25));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(55));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another five node in new zone and reroute");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node11", singletonMap("zone", "zone3")))
            .add(newNode("node12", singletonMap("zone", "zone3")))
            .add(newNode("node13", singletonMap("zone", "zone3")))
            .add(newNode("node14", singletonMap("zone", "zone3")))
            .add(newNode("node15", singletonMap("zone", "zone3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));

        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(4));

        logger.info("--> Remove three node from zone3");
        //remove one nodes in one zone to cause distribution zone1->5 , zone2->5, zone3->4
        newState = removeNode(newState, "node11", strategy);
        newState = removeNode(newState, "node12", strategy);
        newState = removeNode(newState, "node13", strategy);
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(5));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(5));

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node11", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node12", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        //add the removed node
        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node13", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        //ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThreeZoneTwoReplicaLimitsShardAllocationOnOverloadAcrossZones() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 21)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 21)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 21)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),9)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 10)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "zone_1,zone_2,zone_3")
            .build());

        logger.info("Building initial routing table for 'testThreeZoneTwoReplicaLimitsShardAllocationOnOverloadAcrossZones'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(21).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding three nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone_1")))
            .add(newNode("node2", singletonMap("zone", "zone_1")))
            .add(newNode("node3", singletonMap("zone", "zone_1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(21));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> add three new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node4", singletonMap("zone", "zone_2")))
            .add(newNode("node5", singletonMap("zone", "zone_2")))
            .add(newNode("node6", singletonMap("zone", "zone_2")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node4"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(42));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node7", singletonMap("zone", "zone_3")))
            .add(newNode("node8", singletonMap("zone", "zone_3")))
            .add(newNode("node9", singletonMap("zone", "zone_3")))
        ).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(63));

        logger.info("--> Remove one node from zone1");
        //remove one nodes in one zone to cause distribution zone1->2 , zone2->3, zone3->2
        newState = removeNode(newState, "node7", strategy);
        logger.info("--> Remove another node from zones2");
        newState = removeNode(newState, "node2", strategy);
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        //ensure minority zone doesn't get overloaded
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(53));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(10));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
        }

        newState = ClusterState.builder(newState).nodes(DiscoveryNodes.builder(newState.nodes())
            .add(newNode("node7", singletonMap("zone", "zone_3")))
            .add(newNode("node2", singletonMap("zone", "zone_1")))
        ).build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(7));
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(63));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testSingleZoneTwoReplicaLimitsReplicaAllocationOnOverload() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 10)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),3)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 10)
            .build());

        logger.info("Building initial routing table for 'testSingleZoneTwoReplicaLimitsReplicaAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone1")))
            .add(newNode("node2", singletonMap("zone", "zone1")))
            .add(newNode("node3", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(3));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replicas are initializing");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(6));

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> all shards are started");
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(9));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        //remove one node to make zone1 skewed
        clusterState = removeNode(clusterState, randomFrom("node1", "node2", "node3"), strategy);
        clusterState = strategy.reroute(clusterState, "reroute");

        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(6));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(3));

        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
            assertFalse(shard.primary());
        }
    }

    public void testSingleZoneOneReplicaLimitsReplicaAllocationOnOverload() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),5)
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FACTOR_SETTING.getKey(), 10)
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put(NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true)
            .build());

        logger.info("Building initial routing table for 'testSingleZoneOneReplicaLimitsReplicaAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1", singletonMap("zone", "zone1")))
            .add(newNode("node2", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        //skewness limit doesn't apply to primary
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        //assert replicas are not assigned but primaries are
        logger.info("--> replicas are not initializing");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.INDEX_CREATED);
            assertFalse(shard.primary());
        }

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        //add the third and fourth node
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node3", singletonMap("zone", "zone1")))
            .add(newNode("node4", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(18));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replicas are started");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(38));

        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.INDEX_CREATED);
            assertFalse(shard.primary());
        }

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node5", singletonMap("zone", "zone1")))
        ).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        for (RoutingNode node : clusterState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(8));
        }
    }

    private ClusterState removeNode(ClusterState clusterState, String nodeName, AllocationService allocationService) {
        return allocationService.disassociateDeadNodes(ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.getNodes()).remove(nodeName)).build(), true, "reroute");
    }
}
