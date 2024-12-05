/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FailedShardsRoutingTests extends OpenSearchAllocationTestCase {
    private final Logger logger = LogManager.getLogger(FailedShardsRoutingTests.class);

    public void testFailedShardPrimaryRelocatingToAndFrom() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = allocation.reroute(clusterState, "reroute");

        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        AllocationService.CommandsResult commandsResult = allocation.reroute(
            clusterState,
            new AllocationCommands(
                new MoveAllocationCommand(
                    "test",
                    0,
                    clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
                    "node3"
                )
            ),
            false,
            false
        );
        assertThat(commandsResult.getClusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized");
        clusterState = allocation.applyFailedShard(
            clusterState,
            clusterState.getRoutingNodes().node("node3").iterator().next(),
            randomBoolean()
        );

        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        logger.info("--> moving primary shard to node3");
        commandsResult = allocation.reroute(
            clusterState,
            new AllocationCommands(
                new MoveAllocationCommand(
                    "test",
                    0,
                    clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
                    "node3"
                )
            ),
            false,
            false
        );
        assertThat(commandsResult.getClusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node1 being relocated");
        clusterState = allocation.applyFailedShard(
            clusterState,
            clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next(),
            randomBoolean()
        );

        // check promotion of replica to primary
        assertThat(clusterState.getRoutingNodes().node(origReplicaNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(origReplicaNodeId));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            anyOf(equalTo(origPrimaryNodeId), equalTo("node3"))
        );
    }

    public void testFailPrimaryStartedCheckReplicaElected() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the shards (primaries)");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(),
                anyOf(equalTo("node1"), equalTo("node2"))
            );
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(
                clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(),
                anyOf(equalTo("node2"), equalTo("node1"))
            );
        }

        logger.info("Start the shards (backups)");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(),
                anyOf(equalTo("node1"), equalTo("node2"))
            );
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(),
                anyOf(equalTo("node2"), equalTo("node1"))
            );
        }

        logger.info("fail the primary shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting shardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        newState = strategy.applyFailedShard(clusterState, shardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(
            clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
            not(equalTo(shardToFail.currentNodeId()))
        );
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(
            clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
            anyOf(equalTo("node1"), equalTo("node2"))
        );
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(UNASSIGNED));
    }

    public void testFirstAllocationFailureSingleNode() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("Adding single node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), equalTo("node1"));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will have no place to be rerouted to (single node), so stays unassigned");
        ShardRouting firstShard = clusterState.getRoutingNodes().node("node1").iterator().next();
        newState = strategy.applyFailedShard(clusterState, firstShard, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }
    }

    public void testSingleShardMultipleAllocationFailures() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table");
        int numberOfReplicas = scaledRandomIntBetween(2, 10);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(numberOfReplicas))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("Adding {} nodes and performing rerouting", numberOfReplicas + 1);
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfReplicas + 1; i++) {
            nodeBuilder.add(newNode("node" + Integer.toString(i)));
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        while (!clusterState.routingTable().shardsWithState(UNASSIGNED).isEmpty()) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }

        int shardsToFail = randomIntBetween(1, numberOfReplicas);
        ArrayList<FailedShard> failedShards = new ArrayList<>();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        Set<String> failedNodes = new HashSet<>();
        Set<ShardRouting> shardRoutingsToFail = new HashSet<>();
        for (int i = 0; i < shardsToFail; i++) {
            String failedNode = "node" + Integer.toString(randomInt(numberOfReplicas));
            logger.info("failing shard on node [{}]", failedNode);
            ShardRouting shardToFail = routingNodes.node(failedNode).iterator().next();
            if (shardRoutingsToFail.contains(shardToFail) == false) {
                failedShards.add(new FailedShard(shardToFail, null, null, randomBoolean()));
                failedNodes.add(failedNode);
                shardRoutingsToFail.add(shardToFail);
            }
        }

        clusterState = strategy.applyFailedShards(clusterState, failedShards);
        routingNodes = clusterState.getRoutingNodes();
        for (FailedShard failedShard : failedShards) {
            if (routingNodes.getByAllocationId(
                failedShard.getRoutingEntry().shardId(),
                failedShard.getRoutingEntry().allocationId().getId()
            ) != null) {
                fail("shard " + failedShard + " was not failed");
            }
        }

        for (String failedNode : failedNodes) {
            if (!routingNodes.node(failedNode).isEmpty()) {
                fail("shard was re-assigned to failed node " + failedNode);
            }
        }
    }

    public void testFirstAllocationFailureTwoNodes() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(clusterState));
        clusterState = newState;
        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("fail the first shard, will start INITIALIZING on the second node");
        final ShardRouting firstShard = clusterState.getRoutingNodes().node(nodeHoldingPrimary).iterator().next();
        newState = strategy.applyFailedShard(clusterState, firstShard, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        final String nodeHoldingPrimary2 = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        assertThat(nodeHoldingPrimary2, not(equalTo(nodeHoldingPrimary)));

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(), not(equalTo(nodeHoldingPrimary)));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }
    }

    public void testRebalanceFailure() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the shards (primaries)");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(),
                anyOf(equalTo("node1"), equalTo("node2"))
            );
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(
                clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(),
                anyOf(equalTo("node2"), equalTo("node1"))
            );
        }

        logger.info("Start the shards (backups)");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).primaryShard().currentNodeId(),
                anyOf(equalTo("node1"), equalTo("node2"))
            );
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
            assertThat(
                clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(),
                anyOf(equalTo("node2"), equalTo("node1"))
            );
        }

        logger.info("Adding third node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));

        logger.info("Fail the shards on node 3");
        ShardRouting shardToFail = routingNodes.node("node3").iterator().next();
        newState = strategy.applyFailedShard(clusterState, shardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED, RELOCATING), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), lessThan(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // make sure the failedShard is not INITIALIZING again on node3
        assertThat(routingNodes.node("node3").iterator().next().shardId(), not(equalTo(shardToFail.shardId())));
    }

    public void testFailAllReplicasInitializingOnPrimaryFail() {
        AllocationService allocation = createAllocationService(Settings.builder().build());

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        ShardId shardId = new ShardId(metadata.index("test").getIndex(), 0);

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState, "reroute").routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // start one replica so it can take over.
        clusterState = startShardsAndReroute(allocation, clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        ShardRouting startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);

        // fail the primary shard, check replicas get removed as well...
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node, replicas are initializing
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));
    }

    public void testFailAllReplicasInitializingOnPrimaryFailWhileHavingAReplicaToElect() {
        AllocationService allocation = createAllocationService(Settings.builder().build());

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        // add 4 nodes
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));
        // start primary shards
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        // start another replica shard, while keep one initializing
        clusterState = startShardsAndReroute(allocation, clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));

        // fail the primary shard, check one replica gets elected to primary, others become INITIALIZING (from it)
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
    }

    public void testReplicaOnNewestVersionIsPromoted() {
        testReplicaIsPromoted(false);
    }

    public void testReplicaOnOldestVersionIsPromoted() {
        testReplicaIsPromoted(true);
    }

    private void testReplicaIsPromoted(boolean isSegmentReplicationEnabled) {
        AllocationService allocation = createAllocationService(Settings.builder().build());

        Settings.Builder settingsBuilder = isSegmentReplicationEnabled
            ? settings(Version.CURRENT).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            : settings(Version.CURRENT);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settingsBuilder).numberOfShards(1).numberOfReplicas(3))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        ShardId shardId = new ShardId(metadata.index("test").getIndex(), 0);

        // add a single node
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1-5.x", Version.fromId(5060099))))
            .build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState, "reroute").routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(3));

        // start primary shard
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(3));

        // add another 5.6 node
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2-5.x", Version.fromId(5060099))))
            .build();

        // start the shards, should have 1 primary and 1 replica available
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(2));

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(
                        newNode(
                            "node3-old",
                            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), null)
                        )
                    )
                    .add(
                        newNode(
                            "node4-old",
                            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), null)
                        )
                    )
            )
            .build();

        // start all the replicas
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(0));

        ShardRouting startedReplica;
        if (isSegmentReplicationEnabled) {
            startedReplica = clusterState.getRoutingNodes().activeReplicaWithOldestVersion(shardId);
        } else {
            startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);
        }
        logger.info("--> all shards allocated, replica that should be promoted: {}", startedReplica);

        // fail the primary shard again and make sure the correct replica is promoted
        ShardRouting primaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(3));

        ShardRouting newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(primaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));

        Version replicaNodeVersion = clusterState.nodes().getDataNodes().get(startedReplica.currentNodeId()).getVersion();
        assertNotNull(replicaNodeVersion);
        logger.info("--> shard {} got assigned to node with version {}", startedReplica, replicaNodeVersion);

        for (final DiscoveryNode cursor : clusterState.nodes().getDataNodes().values()) {
            if ("node1".equals(cursor.getId())) {
                // Skip the node that the primary was on, it doesn't have a replica so doesn't need a version check
                continue;
            }
            Version nodeVer = cursor.getVersion();
            if (isSegmentReplicationEnabled) {
                assertTrue(
                    "expected node [" + cursor.getId() + "] with version " + nodeVer + " to be after " + replicaNodeVersion,
                    replicaNodeVersion.onOrBefore(nodeVer)
                );
            } else {
                assertTrue(
                    "expected node [" + cursor.getId() + "] with version " + nodeVer + " to be before " + replicaNodeVersion,
                    replicaNodeVersion.onOrAfter(nodeVer)
                );
            }
        }

        if (isSegmentReplicationEnabled) {
            startedReplica = clusterState.getRoutingNodes().activeReplicaWithOldestVersion(shardId);
        } else {
            startedReplica = clusterState.getRoutingNodes().activeReplicaWithHighestVersion(shardId);
        }
        logger.info("--> failing primary shard a second time, should select: {}", startedReplica);

        // fail the primary shard again, and ensure the same thing happens
        ShardRouting secondPrimaryShardToFail = clusterState.routingTable().index("test").shard(0).primaryShard();
        newState = allocation.applyFailedShard(clusterState, secondPrimaryShardToFail, randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        // the primary gets allocated on another node
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(2));

        newPrimaryShard = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertThat(newPrimaryShard, not(equalTo(secondPrimaryShardToFail)));
        assertThat(newPrimaryShard.allocationId(), equalTo(startedReplica.allocationId()));

        replicaNodeVersion = clusterState.nodes().getDataNodes().get(startedReplica.currentNodeId()).getVersion();
        assertNotNull(replicaNodeVersion);
        logger.info("--> shard {} got assigned to node with version {}", startedReplica, replicaNodeVersion);

        for (final DiscoveryNode cursor : clusterState.nodes().getDataNodes().values()) {
            if (primaryShardToFail.currentNodeId().equals(cursor.getId())
                || secondPrimaryShardToFail.currentNodeId().equals(cursor.getId())) {
                // Skip the node that the primary was on, it doesn't have a replica so doesn't need a version check
                continue;
            }
            Version nodeVer = cursor.getVersion();
            if (isSegmentReplicationEnabled) {
                assertTrue(
                    "expected node [" + cursor.getId() + "] with version " + nodeVer + " to be after " + replicaNodeVersion,
                    replicaNodeVersion.onOrBefore(nodeVer)
                );
            } else {
                assertTrue(
                    "expected node [" + cursor.getId() + "] with version " + nodeVer + " to be before " + replicaNodeVersion,
                    replicaNodeVersion.onOrAfter(nodeVer)
                );
            }
        }
    }

    public void testPreferReplicaOnRemoteNodeForPrimaryPromotion() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build());
        AllocationService allocation = createAllocationService(Settings.builder().build());

        // segment replication enabled
        Settings.Builder settingsBuilder = settings(Version.CURRENT).put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);

        // remote store migration metadata settings
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settingsBuilder).numberOfShards(1).numberOfReplicas(4))
            .persistentSettings(
                Settings.builder()
                    .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED.mode)
                    .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE.direction)
                    .build()
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        ShardId shardId = new ShardId(metadata.index("test").getIndex(), 0);

        // add a remote node and start primary shard
        Map<String, String> remoteStoreNodeAttributes = Map.of(
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY",
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_VALUE",
            REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_VALUE"
        );
        DiscoveryNode remoteNode1 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(remoteNode1)).build();
        clusterState = ClusterState.builder(clusterState).routingTable(allocation.reroute(clusterState, "reroute").routingTable()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(4));

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(4));

        // add remote and non-remote nodes and start replica shards
        DiscoveryNode remoteNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNode remoteNode3 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNode nonRemoteNode1 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode nonRemoteNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        List<DiscoveryNode> replicaShardNodes = List.of(remoteNode2, remoteNode3, nonRemoteNode1, nonRemoteNode2);

        for (int i = 0; i < 4; i++) {
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(replicaShardNodes.get(i)))
                .build();

            clusterState = allocation.reroute(clusterState, "reroute");
            assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1 + i));
            assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(1));
            assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(4 - (i + 1)));

            clusterState = startInitializingShardsAndReroute(allocation, clusterState);
            assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(1 + (i + 1)));
            assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(4 - (i + 1)));
        }

        // fail primary shard
        ShardRouting primaryShard0 = clusterState.routingTable().index("test").shard(0).primaryShard();
        ClusterState newState = allocation.applyFailedShard(clusterState, primaryShard0, randomBoolean());
        assertNotEquals(clusterState, newState);
        clusterState = newState;

        // verify that promoted replica exists on a remote node
        assertEquals(4, clusterState.getRoutingNodes().shardsWithState(STARTED).size());
        ShardRouting primaryShardRouting1 = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertNotEquals(primaryShard0, primaryShardRouting1);
        assertTrue(
            primaryShardRouting1.currentNodeId().equals(remoteNode2.getId())
                || primaryShardRouting1.currentNodeId().equals(remoteNode3.getId())
        );

        // fail primary shard again
        newState = allocation.applyFailedShard(clusterState, primaryShardRouting1, randomBoolean());
        assertNotEquals(clusterState, newState);
        clusterState = newState;

        // verify that promoted replica again exists on a remote node
        assertEquals(3, clusterState.getRoutingNodes().shardsWithState(STARTED).size());
        ShardRouting primaryShardRouting2 = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertNotEquals(primaryShardRouting1, primaryShardRouting2);
        assertTrue(
            primaryShardRouting2.currentNodeId().equals(remoteNode2.getId())
                || primaryShardRouting2.currentNodeId().equals(remoteNode3.getId())
        );
        assertNotEquals(primaryShardRouting1.currentNodeId(), primaryShardRouting2.currentNodeId());

        ShardRouting expectedCandidateForSegRep = clusterState.getRoutingNodes().activeReplicaWithOldestVersion(shardId);

        // fail primary shard again
        newState = allocation.applyFailedShard(clusterState, primaryShardRouting2, randomBoolean());
        assertNotEquals(clusterState, newState);
        clusterState = newState;

        // verify that promoted replica exists on a non-remote node
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(STARTED).size());
        ShardRouting primaryShardRouting3 = clusterState.routingTable().index("test").shard(0).primaryShard();
        assertNotEquals(primaryShardRouting2, primaryShardRouting3);
        assertTrue(
            primaryShardRouting3.currentNodeId().equals(nonRemoteNode1.getId())
                || primaryShardRouting3.currentNodeId().equals(nonRemoteNode2.getId())
        );
        assertEquals(expectedCandidateForSegRep.allocationId(), primaryShardRouting3.allocationId());
    }
}
