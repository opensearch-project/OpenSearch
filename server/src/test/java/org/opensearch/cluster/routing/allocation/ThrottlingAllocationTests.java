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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.InternalSnapshotsInfoService;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.opensearch.snapshots.SnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class ThrottlingAllocationTests extends OpenSearchAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ThrottlingAllocationTests.class);

    public void testPrimaryRecoveryThrottling() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .put(
                    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(),
                    randomIntBetween(3, 10)
                )
                .build(),
            gatewayAllocator,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(6));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(9));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(10));
    }

    public void testReplicaAndPrimaryRecoveryThrottling() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.concurrent_source_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 3)
                .build(),
            gatewayAllocator,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("with one node, do reroute, only 3 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThrottleIncomingAndOutgoing() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 5)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 5)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 5)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 5)
            .build();
        AllocationService strategy = createAllocationService(settings, gatewayAllocator, snapshotsInfoService);
        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(9).numberOfReplicas(0))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("with one node, do reroute, only 5 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(4));
        assertEquals(clusterState.getRoutingNodes().getInitialPrimariesIncomingRecoveries("node1"), 5);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(4));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start another 2 nodes, 5 shards should be relocating - at most 5 are allowed per node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(4));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 3);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 2);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 5);

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the relocating shards, one more shard should relocate away from node1");
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 0);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 1);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
    }

    public void testOutgoingThrottlesAllocationOldIndex() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 1)
                .build(),
            gatewayAllocator,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService, 1);

        logger.info("with one node, do reroute, only 1 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start one more node, first non-primary should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 0);

        logger.info("start initializing non-primary");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);

        logger.info("start one more node, initializing second non-primary");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 0);

        logger.info("start one more node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("move started non-primary to new node");
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node4")),
            true,
            false
        );
        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.THROTTLE);
        boolean foundThrottledMessage = false;
        for (Decision decision : commandsResult.explanations().explanations().get(0).decisions().getDecisions()) {
            if (decision.label().equals(ThrottlingAllocationDecider.NAME)) {
                assertEquals(
                    "reached the limit of outgoing shard recoveries [1] on the node [node1] which holds the primary, "
                        + "cluster setting [cluster.routing.allocation.node_concurrent_outgoing_recoveries=1] "
                        + "(can also be set via [cluster.routing.allocation.node_concurrent_recoveries])",
                    decision.getExplanation()
                );
                assertEquals(Decision.Type.THROTTLE, decision.type());
                foundThrottledMessage = true;
            }
        }
        assertTrue(foundThrottledMessage);
        // even though it is throttled, move command still forces allocation

        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 2);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), 0);
    }

    public void testOutgoingThrottlesAllocationNewIndex() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 1)
                .build(),
            gatewayAllocator,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService, 5);

        logger.info("with one node, do reroute, only 1 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start one more node, first non-primary should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 1);

        logger.info("start initializing non-primary");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);

        logger.info("start one more node, initializing second non-primary");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 1);

        logger.info("start one more node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 1);

        logger.info("move started non-primary to new node");
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node4")),
            true,
            false
        );
        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.YES);

        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), 0);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), 1);
        assertEquals(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node2"), 0);
    }

    public void testNewIndexReplicaAllocationIncomingAndOutgoingLimitBreached() {
        int primaries = randomIntBetween(5, 30);
        int replicas = randomIntBetween(1, 2);
        int replicasConcurrentRecoveries = randomIntBetween(1, 2);
        int newIndexInitializingAfterReroute = replicasConcurrentRecoveries * 2;
        verifyNewIndexReplicaAllocation(
            primaries,
            replicas,
            newIndexInitializingAfterReroute,
            primaries * replicas - newIndexInitializingAfterReroute,
            replicasConcurrentRecoveries,
            false
        );
    }

    public void testNewIndexReplicaAllocationOutgoingLimitBreachedIncomingNotBreached() {
        verifyNewIndexReplicaAllocation(5, 1, 2, 3, 1, true);
    }

    public void testNewIndexReplicaAllocationLimitNotBreached() {
        int primaries = randomIntBetween(5, 30);
        int newIndexReplicas = 1;
        int replicasConcurrentRecoveries = primaries * newIndexReplicas + randomIntBetween(1, 10);
        verifyNewIndexReplicaAllocation(primaries, newIndexReplicas, primaries * newIndexReplicas, 0, replicasConcurrentRecoveries, false);
    }

    private void verifyNewIndexReplicaAllocation(
        int newIndexPrimaries,
        int newIndexReplicas,
        int newIndexInitializingAfterReroute,
        int newIndexUnassignedAfterReroute,
        int replicaConcurrentRecoveriesLimit,
        boolean additionalNodeAfterPrimaryAssignment
    ) {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 20)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(),
                replicaConcurrentRecoveriesLimit
            )
            .build();
        AllocationService strategy = createAllocationService(settings, gatewayAllocator);

        DiscoveryNode node1 = newNode("node1");
        DiscoveryNode node2 = newNode("node2");

        ClusterState clusterState = createPrimaryAndWaitForAllocation(strategy, node1, node2, settings);

        String[] indices = { "test1", "test2" };
        clusterState = increaseReplicaCountAndTriggerReroute(strategy, clusterState, indices, 1);

        logger.info("1 replica should be initializing now for the existing indices (we throttle to 1) on each node");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node2"), equalTo(0));

        logger.info("create a new index");
        clusterState = createNewIndexAndStartAllPrimaries(newIndexPrimaries, newIndexReplicas, strategy, clusterState);

        if (additionalNodeAfterPrimaryAssignment) {
            clusterState = strategy.reroute(clusterState, "reroute");
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3")))
                .build();
        }
        clusterState = strategy.reroute(clusterState, "reroute");

        IndexRoutingTable indexRouting = clusterState.routingTable().index("new_index");
        assertThat(indexRouting.shardsWithState(UNASSIGNED).size(), equalTo(newIndexUnassignedAfterReroute));
        assertThat(indexRouting.shardsWithState(INITIALIZING).size(), equalTo(newIndexInitializingAfterReroute));

        int totalIncomingRecoveriesNewIndex = clusterState.getRoutingNodes().getInitialIncomingRecoveries("node1") + clusterState
            .getRoutingNodes()
            .getInitialIncomingRecoveries("node2") + clusterState.getRoutingNodes().getInitialIncomingRecoveries("node3");

        assertThat(totalIncomingRecoveriesNewIndex, equalTo(newIndexInitializingAfterReroute));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), equalTo(0));

        clusterState = strategy.reroute(clusterState, "reroute");
        indexRouting = clusterState.routingTable().index("new_index");

        assertThat(indexRouting.shardsWithState(UNASSIGNED).size(), equalTo(newIndexUnassignedAfterReroute));
        assertThat(indexRouting.shardsWithState(INITIALIZING).size(), equalTo(newIndexInitializingAfterReroute));

    }

    public void testRecoveryCounts() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        Settings settings = Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 2)
            .build();
        AllocationService strategy = createAllocationService(settings, gatewayAllocator);

        Metadata metadata = Metadata.builder().persistentSettings(settings).build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        logger.info("create a new index and start all primaries");
        clusterState = createNewIndexAndStartAllPrimaries(2, 2, strategy, clusterState);

        logger.info("Add a new node for all replica assignment. 4 replicas move to INIT after reroute.");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();

        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().index("new_index").shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING).size(), equalTo(4));

        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node2"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node3"), equalTo(2));

        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node3"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node2"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node3"), equalTo(0));

        logger.info(
            "Exclude node1 and add node4. After this, primary shard on node1 moves to RELOCATING state and only "
                + "non initial replica recoveries are impacted"
        );
        settings = Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 20)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_REPLICAS_RECOVERIES_SETTING.getKey(), 2)
            .put("cluster.routing.allocation.exclude._id", "node1")
            .build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        strategy = createAllocationService(settings, gatewayAllocator);

        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(RELOCATING).size(), equalTo(1));

        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node4"), equalTo(1));

        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node2"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node3"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node4"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node4"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node2"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node4"), equalTo(0));

        logger.info("Start primary on node4. Now all replicas for that primary start getting accounted for from node4.");

        clusterState = strategy.applyStartedShards(
            clusterState,
            clusterState.routingTable()
                .index("new_index")
                .shardsWithState(INITIALIZING)
                .stream()
                .filter(routing -> routing.primary() && routing.isRelocationTarget())
                .collect(Collectors.toList())
        );
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING).size(), equalTo(4));
        assertThat(clusterState.routingTable().index("new_index").shardsWithState(RELOCATING).size(), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getIncomingRecoveries("node4"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node1"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node2"), equalTo(1));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node3"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialIncomingRecoveries("node4"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getOutgoingRecoveries("node4"), equalTo(0));

        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node1"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node2"), equalTo(2));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node3"), equalTo(0));
        assertThat(clusterState.getRoutingNodes().getInitialOutgoingRecoveries("node4"), equalTo(2));
    }

    private ClusterState createPrimaryAndWaitForAllocation(
        AllocationService strategy,
        DiscoveryNode node1,
        DiscoveryNode node2,
        Settings settings
    ) {
        Metadata metaData = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(0))
            .persistentSettings(settings)
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test1"))
            .addAsNew(metaData.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metaData)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("adding two nodes and performing rerouting till all are allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node1).add(node2)).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        while (!clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty()) {
            clusterState = strategy.applyStartedShards(clusterState, clusterState.routingTable().shardsWithState(INITIALIZING));
        }
        return clusterState;
    }

    private ClusterState createNewIndexAndStartAllPrimaries(
        int newIndexPrimaries,
        int newIndexReplicas,
        AllocationService strategy,
        ClusterState clusterState
    ) {
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .put(
                IndexMetadata.builder("new_index")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(newIndexPrimaries)
                    .numberOfReplicas(newIndexReplicas)
            )
            .build();

        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable()).addAsNew(metadata.index("new_index")).build();

        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(updatedRoutingTable).build();

        logger.info("reroute, verify that primaries for the new index primary shards are allocated");
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Apply started shards for new index");
        clusterState = strategy.applyStartedShards(
            clusterState,
            clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING)
        );
        return clusterState;
    }

    private ClusterState increaseReplicaCountAndTriggerReroute(
        AllocationService strategy,
        ClusterState clusterState,
        String[] indices,
        int replicaCount
    ) {
        Metadata metaData;
        logger.info("increasing the number of replicas to 1, and perform a reroute (to get the replicas allocation going)");
        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable())
            .updateNumberOfReplicas(replicaCount, indices)
            .build();
        metaData = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(replicaCount, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metaData).build();

        clusterState = strategy.reroute(clusterState, "reroute");
        return clusterState;
    }

    private ClusterState createRecoveryStateAndInitializeAllocations(
        final Metadata metadata,
        final TestGatewayAllocator gatewayAllocator,
        final TestSnapshotsInfoService snapshotsInfoService
    ) {
        return createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService, null);

    }

    private ClusterState createRecoveryStateAndInitializeAllocations(
        final Metadata metadata,
        final TestGatewayAllocator gatewayAllocator,
        final TestSnapshotsInfoService snapshotsInfoService,
        final Integer inputRecoveryType
    ) {
        DiscoveryNode node1 = newNode("node1");
        Metadata.Builder metadataBuilder = new Metadata.Builder(metadata);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", "randomId"));
        Set<String> snapshotIndices = new HashSet<>();
        String restoreUUID = UUIDs.randomBase64UUID();
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            Index index = cursor.value.getIndex();
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(cursor.value);

            final int recoveryType = inputRecoveryType == null ? randomInt(5) : inputRecoveryType.intValue();
            if (recoveryType <= 4) {
                addInSyncAllocationIds(index, indexMetadataBuilder, gatewayAllocator, node1);
            }
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            metadataBuilder.put(indexMetadata, false);
            switch (recoveryType) {
                case 0:
                    routingTableBuilder.addAsRecovery(indexMetadata);
                    break;
                case 1:
                    routingTableBuilder.addAsFromCloseToOpen(indexMetadata);
                    break;
                case 2:
                    routingTableBuilder.addAsFromDangling(indexMetadata);
                    break;
                case 3:
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsNewRestore(
                        indexMetadata,
                        new SnapshotRecoverySource(
                            restoreUUID,
                            snapshot,
                            Version.CURRENT,
                            new IndexId(indexMetadata.getIndex().getName(), UUIDs.randomBase64UUID(random()))
                        ),
                        new IntHashSet()
                    );
                    break;
                case 4:
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsRestore(
                        indexMetadata,
                        new SnapshotRecoverySource(
                            restoreUUID,
                            snapshot,
                            Version.CURRENT,
                            new IndexId(indexMetadata.getIndex().getName(), UUIDs.randomBase64UUID(random()))
                        )
                    );
                    break;
                case 5:
                    routingTableBuilder.addAsNew(indexMetadata);
                    break;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }

        final RoutingTable routingTable = routingTableBuilder.build();

        final ImmutableOpenMap.Builder<String, ClusterState.Custom> restores = ImmutableOpenMap.builder();
        if (snapshotIndices.isEmpty() == false) {
            // Some indices are restored from snapshot, the RestoreInProgress must be set accordingly
            ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> restoreShards = ImmutableOpenMap.builder();
            for (ShardRouting shard : routingTable.allShards()) {
                if (shard.primary() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    final ShardId shardId = shard.shardId();
                    restoreShards.put(shardId, new RestoreInProgress.ShardRestoreStatus(node1.getId(), RestoreInProgress.State.INIT));
                    // Also set the snapshot shard size
                    final SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) shard.recoverySource();
                    final long shardSize = randomNonNegativeLong();
                    snapshotsInfoService.addSnapshotShardSize(recoverySource.snapshot(), recoverySource.index(), shardId, shardSize);
                }
            }

            RestoreInProgress.Entry restore = new RestoreInProgress.Entry(
                restoreUUID,
                snapshot,
                RestoreInProgress.State.INIT,
                new ArrayList<>(snapshotIndices),
                restoreShards.build()
            );
            restores.put(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build());
        }

        return ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(node1))
            .metadata(metadataBuilder.build())
            .routingTable(routingTable)
            .customs(restores.build())
            .build();
    }

    private void addInSyncAllocationIds(
        Index index,
        IndexMetadata.Builder indexMetadata,
        TestGatewayAllocator gatewayAllocator,
        DiscoveryNode node1
    ) {
        for (int shard = 0; shard < indexMetadata.numberOfShards(); shard++) {

            final boolean primary = randomBoolean();
            final ShardRouting unassigned = ShardRouting.newUnassigned(
                new ShardId(index, shard),
                primary,
                primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
            );
            ShardRouting started = ShardRoutingHelper.moveToStarted(ShardRoutingHelper.initialize(unassigned, node1.getId()));
            indexMetadata.putInSyncAllocationIds(shard, Collections.singleton(started.allocationId().getId()));
            gatewayAllocator.addKnownAllocation(started);
        }
    }

    private static class TestSnapshotsInfoService implements SnapshotsInfoService {

        private volatile ImmutableOpenMap<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes = ImmutableOpenMap.of();

        synchronized void addSnapshotShardSize(Snapshot snapshot, IndexId index, ShardId shard, Long size) {
            final ImmutableOpenMap.Builder<InternalSnapshotsInfoService.SnapshotShard, Long> newSnapshotShardSizes = ImmutableOpenMap
                .builder(snapshotShardSizes);
            boolean added = newSnapshotShardSizes.put(new InternalSnapshotsInfoService.SnapshotShard(snapshot, index, shard), size) == null;
            assert added : "cannot add snapshot shard size twice";
            this.snapshotShardSizes = newSnapshotShardSizes.build();
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizes() {
            return new SnapshotShardSizeInfo(snapshotShardSizes);
        }
    }
}
