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
import org.opensearch.cluster.routing.allocation.decider.NodeLoadAwareAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class NodeLoadAwareAllocationTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(NodeLoadAwareAllocationTests.class);

    public void testNewUnassignedPrimaryAllocationOnOverload() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true
            )
        );

        logger.info("Building initial routing table for 'testNewUnassignedPrimaryAllocationOnOverload'");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone_1", "node1", "node2", "node3", "node4", "node5");

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));

        logger.info("--> Remove nodes from zone holding primaries");
        ClusterState newState = removeNodes(clusterState, strategy, "node1", "node2", "node3");

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test1")).build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        logger.info("no limits should be applied on newly created primaries");
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(28));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(12));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
        }

        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node1", singletonMap("zone", "zone_1"))))
            .build();

        // 4 existing shards from this node's local store get started and cluster rebalances
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(32));

        // add back node2 when skewness is still breached
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node2", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        // add back node3
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node3", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
    }

    public void testNoAllocationLimitsOnOverloadForDisabledLoadFactor() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                -1,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false
            )
        );

        logger.info("Building initial routing table for 'testNoAllocationLimitsOnOverloadForDisabledLoadFactor'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone_1", "node1", "node2", "node3", "node4", "node5");

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));

        logger.info("--> Remove nodes from zone holding primaries");
        ClusterState newState = removeNodes(clusterState, strategy, "node1", "node2", "node3");

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test1")).build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        logger.info("no limits should be applied on newly created primaries");
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(28));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(12));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
        }

        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node1", singletonMap("zone", "zone_1"))))
            .build();

        // 4 existing shards from this node's local store get started
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(32));

        // add back node2 when skewness is still breached
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node2", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        // add back node3
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node3", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
    }

    public void testExistingPrimariesAllocationOnOverload() {
        GatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                50,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false
            ),
            gatewayAllocator
        );

        logger.info("Building initial routing table for 'testExistingPrimariesAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone_1", "node1", "node2", "node3", "node4", "node5");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));

        logger.info("--> Remove nodes from zone holding primaries");
        ClusterState newState = removeNodes(clusterState, strategy, "node1", "node2", "node3");

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(8));

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test1")).build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        // 28 shards should be assigned (14 on each node -> 8 * 1.5 + 2)
        logger.info("limits should be applied on newly create primaries");
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(28));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(12));

        assertEquals(
            12L,
            newState.getRoutingNodes()
                .shardsWithState(UNASSIGNED)
                .stream()
                .filter(r -> r.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_LEFT)
                .count()
        );

        assertEquals(
            0L,
            newState.getRoutingNodes()
                .shardsWithState(UNASSIGNED)
                .stream()
                .filter(r -> r.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED)
                .count()
        );

        assertThat(newState.getRoutingNodes().node("node4").size(), equalTo(14));

        logger.info("--> Remove node4 from zone holding primaries");
        newState = removeNodes(newState, strategy, "node4");

        logger.info("--> change the overload load factor to zero and verify if unassigned primaries on disk get assigned despite overload");
        strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                0,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false
            ),
            gatewayAllocator
        );

        newState = strategy.reroute(newState, "reroute");

        logger.info("--> Add back node4 and ensure existing primaries are assigned");
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node4", singletonMap("zone", "zone_1"))))
            .build();

        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(newState, "reroute").routingTable(), sameInstance(newState.routingTable()));

        assertThat(newState.getRoutingNodes().node("node4").size(), equalTo(14));
        assertThat(newState.getRoutingNodes().node("node5").size(), equalTo(14));

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(28));

        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node1", singletonMap("zone", "zone_1"))))
            .build();

        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(32));

        // add back node2 when skewness is still breached
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node2", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        // add back node3
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node3", singletonMap("zone", "zone_1"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));
    }

    public void testSingleZoneOneReplicaLimitsShardAllocationOnOverload() {
        GatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true
            ),
            gatewayAllocator
        );

        logger.info("Building initial routing table for 'testSingleZoneOneReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone_1", "node1", "node2", "node3", "node4", "node5");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));

        logger.info("--> Remove node1 from zone");
        ClusterState newState = removeNodes(clusterState, strategy, "node1");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(10));
        }

        logger.info("--> Remove node2 when the limit of overload is reached");
        newState = removeNodes(newState, strategy, "node2");
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        // Each node can take 12 shards each (2 + ceil(8*1.2))
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(36));

        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
            assertFalse(shard.primary());
        }

        logger.info("add another index with 20 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 20)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    )
            )
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test1")).build();
        // increases avg shard per node to 80/5 = 16, overload factor 1.2, total allowed 20
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        while (!newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty()) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(66));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("add another index with 60 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test2")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 60)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test2")).build();
        // increases avg shard per node to 140/5 = 28, overload factor 1.2, total allowed 34 per node but still ALL primaries get assigned
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(126));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("change settings to allow unassigned primaries");
        strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                false
            ),
            gatewayAllocator
        );

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(42));
        }

        logger.info("add another index with 5 shards");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test3")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test3")).build();
        // increases avg shard per node to 145/5 = 29, overload factor 1.2, total allowed 35+2=37 per node and NO primaries get assigned
        // since total owning shards are 42 per node already
        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(126));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(19));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).stream().filter(ShardRouting::primary).count(), equalTo(5L));
    }

    public void testThreeZoneTwoReplicaLimitsShardAllocationOnOverload() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                15,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20,
                "cluster.routing.allocation.awareness.force.zone.values",
                "zone_1,zone_2,zone_3"
            )
        );

        logger.info("Building initial routing table for 'testThreeZoneTwoReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2", "node3", "node4", "node5");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one zone value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        // replicas are unassigned
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(40));

        logger.info("--> add five new node in new zone and reroute");
        clusterState = addNodes(clusterState, strategy, "zone2", "node6", "node7", "node8", "node9", "node10");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(20));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(40));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another five node in new zone and reroute");

        ClusterState newState = addNodes(clusterState, strategy, "zone3", "node11", "node12", "node13", "node14", "node15");

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

        newState = removeNodes(newState, strategy, "node11", "node12", "node13");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(7));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(7));

        // add the removed node
        newState = addNodes(newState, strategy, "zone3", "node11");

        assertThat(newState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(6));
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));

        // add the removed node
        newState = addNodes(newState, strategy, "zone3", "node12");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(5));

        // add the removed node
        newState = addNodes(newState, strategy, "zone3", "node13");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        // ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThreeZoneOneReplicaLimitsShardAllocationOnOverload() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                15,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true,
                "cluster.routing.allocation.awareness.force.zone.values",
                "zone_1,zone_2,zone_3"
            )
        );

        logger.info("Building initial routing table for 'testThreeZoneOneReplicaLimitsShardAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(30).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2", "node3", "node4", "node5");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(30));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one zone value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(30));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        // replicas are unassigned
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(30));

        logger.info("--> add five new node in new zone and reroute");
        clusterState = addNodes(clusterState, strategy, "zone2", "node6", "node7", "node8", "node9", "node10");

        // Each node can take 7 shards each now (2 + ceil(4*1.2))
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(30));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(30));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(60));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another five node in new zone and reroute");

        ClusterState newState = addNodes(clusterState, strategy, "zone3", "node11", "node12", "node13", "node14", "node15");
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

        newState = removeNodes(newState, strategy, "node11", "node12", "node13");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        // Each node can now have 5 shards each
        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(5));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(5));

        // add the removed nodes
        newState = addNodes(clusterState, strategy, "zone3", "node11", "node12", "node13");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        // ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThreeZoneTwoReplicaLimitsShardAllocationOnOverloadAcrossZones() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                9,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                10,
                "cluster.routing.allocation.awareness.force.zone.values",
                "zone_1,zone_2,zone_3"
            )
        );

        logger.info("Building initial routing table for 'testThreeZoneTwoReplicaLimitsShardAllocationOnOverloadAcrossZones'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(21).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding three nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone_1", "node1", "node2", "node3");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(21));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> add three new node with a new rack and reroute");
        clusterState = addNodes(clusterState, strategy, "zone_2", "node4", "node5", "node6");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(21));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo("node4"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(42));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new rack, make sure nothing moves");

        ClusterState newState = addNodes(clusterState, strategy, "zone_3", "node7", "node8", "node9");
        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(63));

        logger.info("--> Remove two nodes from zones");
        // remove one nodes in one zone to cause distribution zone1->2 , zone2->3, zone3->2
        newState = removeNodes(newState, strategy, "node7", "node2");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        // ensure minority zone doesn't get overloaded
        // each node can take 10 shards each (2 + ceil(7*1.1))
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(61));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        for (ShardRouting shard : newState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.NODE_LEFT);
        }

        newState = ClusterState.builder(newState)
            .nodes(
                DiscoveryNodes.builder(newState.nodes())
                    .add(newNode("node7", singletonMap("zone", "zone_3")))
                    .add(newNode("node2", singletonMap("zone", "zone_1")))
            )
            .build();
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
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                3,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                10
            )
        );

        logger.info("Building initial routing table for 'testSingleZoneTwoReplicaLimitsReplicaAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding three nodes on same rack and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2", "node3");

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

        // remove one node to make zone1 skewed
        clusterState = removeNodes(clusterState, strategy, randomFrom("node1", "node2", "node3"));

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
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                5,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                10,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey(),
                true
            )
        );

        logger.info("Building initial routing table for 'testSingleZoneOneReplicaLimitsReplicaAllocationOnOverload'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2");
        // skewness limit doesn't apply to primary
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(20));
        // Each node can take 11 shards each (2 + ceil(8*1.1)), hence 2 replicas will also start
        logger.info("--> 2 replicas are initializing");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.INDEX_CREATED);
            assertFalse(shard.primary());
        }

        logger.info("--> start the shards (replicas)");
        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        // add the third and fourth node
        clusterState = addNodes(clusterState, strategy, "zone1", "node3", "node4");

        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(18));

        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }

        logger.info("--> replicas are started");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(40));

        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertEquals(shard.unassignedInfo().getReason(), UnassignedInfo.Reason.INDEX_CREATED);
            assertFalse(shard.primary());
        }

        clusterState = addNodes(clusterState, strategy, "zone1", "node5");

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

    public void testThreeZoneTwoReplicaLimitsReplicaAllocationUnderFullZoneFailure() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                15,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                20
            )
        );

        logger.info("Building initial routing table for 'testThreeZoneTwoReplicaLimitsUnderFullZoneFailure'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(20).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2", "node3", "node4", "node5");
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(20));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> add five new node in new zone and reroute");
        clusterState = addNodes(clusterState, strategy, "zone2", "node6", "node7", "node8", "node9", "node10");

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        ClusterState newState = addNodes(clusterState, strategy, "zone3", "node11", "node12", "node13", "node14", "node15");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(4));

        logger.info("--> Remove complete zone3 holding primary and replicas");
        newState = removeNodes(newState, strategy, "node11", "node12", "node13", "node14", "node15");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        // Each node can take 7 shards max ( 2 + ceil(4*1.2))
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        for (RoutingNode node : newState.getRoutingNodes()) {
            assertThat(node.size(), equalTo(6));
        }

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node11", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node12", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node13", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node14", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node15", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        // ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThreeZoneOneReplicaWithSkewFactorZeroAllShardsAssignedAfterRecovery() {
        AllocationService strategy = createAllocationServiceWithAdditionalSettings(
            Map.of(
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey(),
                15,
                NodeLoadAwareAllocationDecider.CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                0,
                "cluster.routing.allocation.awareness.force.zone.values",
                "zone1,zone2,zone3"
            )
        );

        logger.info("Building initial routing table for 'testThreeZoneOneReplicaWithSkewFactorZeroAllShardsAssignedAfterRecovery'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(30).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding five nodes on same zone and do rerouting");
        clusterState = addNodes(clusterState, strategy, "zone1", "node1", "node2", "node3", "node4", "node5");
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(30));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> add five new node in new zone and reroute");
        clusterState = addNodes(clusterState, strategy, "zone2", "node6", "node7", "node8", "node9", "node10");

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        ClusterState newState = addNodes(clusterState, strategy, "zone3", "node11", "node12", "node13", "node14", "node15");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node14").size(), equalTo(4));
        assertThat(newState.getRoutingNodes().node("node15").size(), equalTo(4));

        logger.info("--> Removing three nodes from zone3");
        newState = removeNodes(newState, strategy, "node11", "node12", "node13");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }

        // Each node can take 6 shards max (2 + ceil(4*1.0)), so all shards should be assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(60));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("add another index with 30 primary 1 replica");
        metadata = Metadata.builder(newState.metadata())
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 30)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    )
            )
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(newState.routingTable()).addAsNew(metadata.index("test1")).build();

        newState = ClusterState.builder(newState).metadata(metadata).routingTable(updatedRoutingTable).build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node11", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node12", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        newState = startInitializingShardsAndReroute(strategy, newState);

        // add the removed node
        newState = ClusterState.builder(newState)
            .nodes(DiscoveryNodes.builder(newState.nodes()).add(newNode("node13", singletonMap("zone", "zone3"))))
            .build();
        newState = strategy.reroute(newState, "reroute");

        while (newState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            newState = startInitializingShardsAndReroute(strategy, newState);
        }
        assertThat(newState.getRoutingNodes().node("node13").size(), equalTo(8));
        assertThat(newState.getRoutingNodes().node("node12").size(), equalTo(8));
        assertThat(newState.getRoutingNodes().node("node11").size(), equalTo(8));
        // ensure all shards are assigned
        assertThat(newState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(120));
        assertThat(newState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService allocationService, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.getNodes());
        List.of(nodeIds).forEach(nodeId -> nodeBuilder.remove(nodeId));
        return allocationService.disassociateDeadNodes(ClusterState.builder(clusterState).nodes(nodeBuilder).build(), true, "reroute");
    }

    private ClusterState addNodes(ClusterState clusterState, AllocationService allocationService, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return allocationService.reroute(clusterState, "reroute");
    }

    private AllocationService createAllocationServiceWithAdditionalSettings(Map<String, Object> settingsValue) {
        return createAllocationService(buildSettings(settingsValue));
    }

    private AllocationService createAllocationServiceWithAdditionalSettings(
        Map<String, Object> settingsValue,
        GatewayAllocator gatewayAllocator
    ) {
        return createAllocationService(buildSettings(settingsValue), gatewayAllocator);
    }

    private Settings buildSettings(Map<String, Object> settingsValue) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 20)
            .put("cluster.routing.allocation.awareness.attributes", "zone");
        settingsValue.forEach((k, v) -> {
            if (v instanceof Integer) settingsBuilder.put(k, (Integer) (v));
            else if (v instanceof Boolean) settingsBuilder.put(k, (Boolean) (v));
            else if (v instanceof String) settingsBuilder.put(k, (String) (v));
            else {
                throw new UnsupportedOperationException("Unsupported type for key :" + k);
            }
        });
        return settingsBuilder.build();
    }
}
