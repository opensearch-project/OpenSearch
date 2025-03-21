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
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.cluster.ClusterStateChanges;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class FailedNodeRoutingTests extends OpenSearchAllocationTestCase {
    private final Logger logger = LogManager.getLogger(FailedNodeRoutingTests.class);

    public void testSimpleFailedNodeTest() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();

        logger.info("start 4 nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("remove 2 nodes where primaries are allocated, reroute");

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .remove(clusterState.routingTable().index("test1").shard(0).primaryShard().currentNodeId())
                    .remove(clusterState.routingTable().index("test2").shard(0).primaryShard().currentNodeId())
            )
            .build();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(1));
            assertThat(routingNode.numberOfShardsWithState(INITIALIZING), equalTo(1));
        }
    }

    public void testRandomClusterPromotesOldestReplica() throws InterruptedException {
        testRandomClusterPromotesReplica(true);
    }

    public void testRandomClusterPromotesNewestReplica() throws InterruptedException {
        testRandomClusterPromotesReplica(false);
    }

    void testRandomClusterPromotesReplica(boolean isSegmentReplicationEnabled) throws InterruptedException {

        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
        ClusterState state = randomInitialClusterState();

        // randomly add nodes of mixed versions
        logger.info("--> adding random nodes");
        for (int i = 0; i < randomIntBetween(4, 8); i++) {
            DiscoveryNodes newNodes = DiscoveryNodes.builder(state.nodes()).add(createNode()).build();
            state = ClusterState.builder(state).nodes(newNodes).build();
            state = cluster.reroute(state, new ClusterRerouteRequest()); // always reroute after adding node
        }

        // Log the node versions (for debugging if necessary)
        for (final DiscoveryNode cursor : state.nodes().getDataNodes().values()) {
            Version nodeVer = cursor.getVersion();
            logger.info("--> node [{}] has version [{}]", cursor.getId(), nodeVer);
        }

        // randomly create some indices
        logger.info("--> creating some indices");
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            String name = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Settings.Builder settingsBuilder = Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 4))
                .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(2, 4));
            if (isSegmentReplicationEnabled) {
                settingsBuilder.put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
            }
            CreateIndexRequest request = new CreateIndexRequest(name, settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex(name));
        }

        logger.info("--> starting shards");
        state = cluster.applyStartedShards(state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        logger.info("--> starting replicas a random number of times");
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            state = cluster.applyStartedShards(state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        }

        boolean keepGoing = true;
        while (keepGoing) {
            List<ShardRouting> primaries = state.getRoutingNodes()
                .shardsWithState(STARTED)
                .stream()
                .filter(ShardRouting::primary)
                .collect(Collectors.toList());

            // Pick a random subset of primaries to fail
            List<FailedShard> shardsToFail = new ArrayList<>();
            List<ShardRouting> failedPrimaries = randomSubsetOf(primaries);
            failedPrimaries.stream().forEach(sr -> {
                shardsToFail.add(new FailedShard(randomFrom(sr), "failed primary", new Exception(), randomBoolean()));
            });

            logger.info("--> state before failing shards: {}", state);
            state = cluster.applyFailedShards(state, shardsToFail);

            final ClusterState compareState = state;
            failedPrimaries.forEach(shardRouting -> {
                logger.info("--> verifying version for {}", shardRouting);

                ShardRouting newPrimary = compareState.routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryShard();
                Version newPrimaryVersion = getNodeVersion(newPrimary, compareState);

                logger.info("--> new primary is on version {}: {}", newPrimaryVersion, newPrimary);
                compareState.routingTable().shardRoutingTable(newPrimary.shardId()).shardsWithState(STARTED).stream().forEach(sr -> {
                    Version candidateVer = getNodeVersion(sr, compareState);
                    if (candidateVer != null) {
                        logger.info("--> candidate on {} node; shard routing: {}", candidateVer, sr);
                        if (isSegmentReplicationEnabled) {
                            assertTrue(
                                "candidate was not on the oldest version, new primary is on "
                                    + newPrimaryVersion
                                    + " and there is a candidate on "
                                    + candidateVer,
                                candidateVer.onOrAfter(newPrimaryVersion)
                            );
                        } else {
                            assertTrue(
                                "candidate was not on the newest version, new primary is on "
                                    + newPrimaryVersion
                                    + " and there is a candidate on "
                                    + candidateVer,
                                candidateVer.onOrBefore(newPrimaryVersion)
                            );
                        }
                    }
                });
            });

            keepGoing = randomBoolean();
        }
        terminate(threadPool);
    }

    private static Version getNodeVersion(ShardRouting shardRouting, ClusterState state) {
        return Optional.ofNullable(state.getNodes().get(shardRouting.currentNodeId())).map(DiscoveryNode::getVersion).orElse(null);
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    public ClusterState randomInitialClusterState() {
        List<DiscoveryNode> allNodes = new ArrayList<>();
        DiscoveryNode localNode = createNode(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE); // local node is the cluster-manager
        allNodes.add(localNode);
        // at least two nodes that have the data role so that we can allocate shards
        allNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
        allNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            allNodes.add(createNode());
        }
        ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));
        return state;
    }

    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(
            id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            roles,
            VersionUtils.randomIndexCompatibleVersion(random())
        );
    }

}
