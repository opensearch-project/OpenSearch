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

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.cluster.ClusterStateChanges;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class AutoExpandReplicasTests extends OpenSearchTestCase {

    public void testParseSettings() {
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_replicas", "0-5").build()
        );
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(8));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "0-all").build());
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-all").build());
        assertEquals(1, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

    }

    public void testInvalidValues() {
        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom] at index -1", ex.getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [1-boom] at index 1", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom-1").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom-1] at index 4", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "2-1").build());
        } catch (IllegalArgumentException ex) {
            assertEquals("[index.auto_expand_replicas] minReplicas must be =< maxReplicas but wasn't 2 > 1", ex.getMessage());
        }

    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(Version version, DiscoveryNodeRole... mustHaveRoles) {
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), Set.of(mustHaveRoles), version);
    }

    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        return createNode(Version.CURRENT, mustHaveRoles);
    }

    /**
     * Checks that when nodes leave the cluster that the auto-expand-replica functionality only triggers after failing the shards on
     * the removed nodes. This ensures that active shards on other live nodes are not failed if the primary resided on a now dead node.
     * Instead, one of the replicas on the live nodes first gets promoted to primary, and the auto-expansion (removing replicas) only
     * triggers in a follow-up step.
     */
    public void testAutoExpandWhenNodeLeavesAndPossiblyRejoins() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode localNode = createNode(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE); // local node is the cluster-manager
            allNodes.add(localNode);
            int numDataNodes = randomIntBetween(3, 5);
            List<DiscoveryNode> dataNodes = new ArrayList<>(numDataNodes);
            for (int i = 0; i < numDataNodes; i++) {
                dataNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
            }
            allNodes.addAll(dataNodes);
            ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

            CreateIndexRequest request = new CreateIndexRequest(
                "index",
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build()
            ).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            IndexShardRoutingTable preTable = state.routingTable().index("index").shard(0);
            final Set<String> unchangedNodeIds;
            final IndexShardRoutingTable postTable;

            if (randomBoolean()) {
                // simulate node removal
                List<DiscoveryNode> nodesToRemove = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream()
                    .filter(n -> nodesToRemove.contains(n) == false)
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());

                state = cluster.removeNodes(state, nodesToRemove);
                postTable = state.routingTable().index("index").shard(0);

                assertTrue("not all shards started in " + state.toString(), postTable.allShardsStarted());
                assertThat(postTable.toString(), postTable.getAllAllocationIds(), everyItem(is(in(preTable.getAllAllocationIds()))));
            } else {
                // fake an election where conflicting nodes are removed and readded
                state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null).build()).build();

                List<DiscoveryNode> conflictingNodes = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream()
                    .filter(n -> conflictingNodes.contains(n) == false)
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());

                List<DiscoveryNode> nodesToAdd = conflictingNodes.stream()
                    .map(
                        n -> new DiscoveryNode(
                            n.getName(),
                            n.getId(),
                            buildNewFakeTransportAddress(),
                            n.getAttributes(),
                            n.getRoles(),
                            n.getVersion()
                        )
                    )
                    .collect(Collectors.toList());

                if (randomBoolean()) {
                    nodesToAdd.add(createNode(DiscoveryNodeRole.DATA_ROLE));
                }

                state = cluster.joinNodesAndBecomeClusterManager(state, nodesToAdd);
                postTable = state.routingTable().index("index").shard(0);
            }

            Set<String> unchangedAllocationIds = preTable.getShards()
                .stream()
                .filter(shr -> unchangedNodeIds.contains(shr.currentNodeId()))
                .map(shr -> shr.allocationId().getId())
                .collect(Collectors.toSet());

            assertThat(postTable.toString(), unchangedAllocationIds, everyItem(is(in(postTable.getAllAllocationIds()))));

            postTable.getShards().forEach(shardRouting -> {
                if (shardRouting.assignedToNode() && unchangedAllocationIds.contains(shardRouting.allocationId().getId())) {
                    assertTrue("Shard should be active: " + shardRouting, shardRouting.active());
                }
            });
        } finally {
            terminate(threadPool);
        }
    }

    public void testOnlyAutoExpandAllocationFilteringAfterAllNodesUpgraded() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode oldNode = createNode(
                VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_2_1),
                DiscoveryNodeRole.CLUSTER_MANAGER_ROLE,
                DiscoveryNodeRole.DATA_ROLE
            ); // local node is the cluster-manager
            allNodes.add(oldNode);
            ClusterState state = ClusterStateCreationUtils.state(oldNode, oldNode, allNodes.toArray(new DiscoveryNode[0]));

            CreateIndexRequest request = new CreateIndexRequest(
                "index",
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build()
            ).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            DiscoveryNode newNode = createNode(Version.V_2_3_0, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE); // local
                                                                                                                                      // node
                                                                                                                                      // is
                                                                                                                                      // the
                                                                                                                                      // cluster_manager

            state = cluster.addNodes(state, Collections.singletonList(newNode));

            // use allocation filtering
            state = cluster.updateSettings(
                state,
                new UpdateSettingsRequest("index").settings(
                    Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", oldNode.getName()).build()
                )
            );

            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(
                    state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING)
                );
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            // check that presence of old node means that auto-expansion does not take allocation filtering into account
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(2));

            // remove old node and check that auto-expansion takes allocation filtering into account
            state = cluster.removeNodes(state, Collections.singletonList(oldNode));
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(1));
        } finally {
            terminate(threadPool);
        }
    }

    public void testSkipSearchOnlyIndexForAutoExpandReplicas() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            DiscoveryNode localNode = createNode(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
            ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, localNode);

            state = cluster.createIndex(
                state,
                new CreateIndexRequest(
                    "search-only-index",
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                        .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
                        .build()
                ).waitForActiveShards(ActiveShardCount.NONE)
            );

            state = cluster.createIndex(
                state,
                new CreateIndexRequest(
                    "regular-index",
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                        .build()
                ).waitForActiveShards(ActiveShardCount.NONE)
            );

            List<DiscoveryNode> additionalNodes = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                additionalNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
            }
            state = cluster.addNodes(state, additionalNodes);

            while (state.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false
                || state.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).isEmpty() == false) {
                state = cluster.applyStartedShards(state, state.routingTable().shardsWithState(ShardRoutingState.INITIALIZING));
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            assertEquals(3, state.metadata().index("regular-index").getNumberOfReplicas());
            assertEquals(0, state.metadata().index("search-only-index").getNumberOfReplicas());

            AllocationDeciders allocationDeciders = new AllocationDeciders(Collections.emptyList()) {
                @Override
                public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                    return Decision.YES;
                }
            };
            RoutingAllocation allocation = new RoutingAllocation(
                allocationDeciders,
                state.getRoutingNodes(),
                state,
                null,
                null,
                System.nanoTime()
            );

            // To force the auto expand scenario as the expand might have already triggered upon adding a new node.
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
            IndexMetadata originalMeta = state.metadata().index("regular-index");
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(originalMeta);
            indexMetaBuilder.numberOfReplicas(0);
            metadataBuilder.put(indexMetaBuilder);

            Map<Integer, List<String>> changes = AutoExpandReplicas.getAutoExpandReplicaChanges(metadataBuilder.build(), allocation);

            assertFalse(
                "Search-only index should not be auto-expanded",
                changes.values().stream().anyMatch(indices -> indices.contains("search-only-index"))
            );

            assertTrue(
                "Regular index should be auto-expanded",
                changes.values().stream().anyMatch(indices -> indices.contains("regular-index"))
            );

        } finally {
            terminate(threadPool);
        }
    }
}
