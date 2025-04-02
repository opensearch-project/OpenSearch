/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SearchReplicaAwarenessAllocationTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(SearchReplicaAwarenessAllocationTests.class);

    public void testAllocationAwarenessForIndexWithSearchReplica() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build()
        );

        logger.info("--> Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .numberOfSearchReplicas(2)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding four nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", Map.of("zone", "a")))
                    .add(newNode("node2", Map.of("zone", "a")))
                    .add(newSearchNode("node3", Map.of("zone", "a")))
                    .add(newSearchNode("node4", Map.of("zone", "a")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));

        logger.info("--> add a two nodes with a new zone and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(newNode("node5", Map.of("zone", "b")))
                    .add(newSearchNode("node6", Map.of("zone", "b")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(2));

        List<ShardRouting> shardRoutings = clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING);
        shardRoutings.sort(Comparator.comparing(ShardRouting::currentNodeId));
        assertThat(shardRoutings.get(0).relocatingNodeId(), equalTo("node5"));
        assertThat(shardRoutings.get(1).relocatingNodeId(), equalTo("node6"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add a new node with a new zone and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node7", Map.of("zone", "c"))))
            .build();

        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));
    }

    public void testMoveShardOnceNewNodeWithOutAwarenessAttributeAdded() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build()
        );

        logger.info("--> Building initial routing table'");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .numberOfSearchReplicas(1)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding four nodes on same zone and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", Map.of("zone", "a"))).add(newSearchNode("node2", Map.of("zone", "a"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));

        logger.info("--> add a search node without zone and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newSearchNode("node3", Map.of("searchonly", "true"))))
            .build();

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> try to move the replica to node without zone attribute");
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node3")),
            true,
            false
        );

        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.NO);
        List<Decision> decisions = commandsResult.explanations()
            .explanations()
            .get(0)
            .decisions()
            .getDecisions()
            .stream()
            .filter(item -> item.type() == Decision.Type.NO)
            .toList();
        assertEquals(
            "node does not contain the awareness attribute [zone]; "
                + "required attributes cluster setting [cluster.routing.allocation.awareness.attributes=zone]",
            decisions.get(0).getExplanation()
        );
    }

    public void testFullAwarenessWithSearchReplica() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .build()
        );

        logger.info("Building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .numberOfSearchReplicas(2)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("--> adding three nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", Map.of("zone", "a"))).add(newSearchNode("node2", Map.of("zone", "a"))))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> one search replica will not start because we have only one zone value");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(1));

        logger.info("--> add a new node with a new zome and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newSearchNode("node3", Map.of("zone", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo("node3"));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(3));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(strategy.reroute(clusterState, "reroute").routingTable(), sameInstance(clusterState.routingTable()));

        logger.info("--> add another node with a new zone, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("zone", "c"))))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));
    }
}
