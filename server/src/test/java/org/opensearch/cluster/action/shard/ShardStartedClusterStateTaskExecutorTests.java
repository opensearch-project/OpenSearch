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

package org.opensearch.cluster.action.shard;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShardTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.stateWithMixedNodes;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class ShardStartedClusterStateTaskExecutorTests extends OpenSearchAllocationTestCase {

    private ShardStateAction.ShardStartedClusterStateTaskExecutor executor;

    @SuppressWarnings("unused")
    private static void neverReroutes(String reason, Priority priority, ActionListener<ClusterState> listener) {
        fail("unexpectedly ran a deferred reroute");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AllocationService allocationService = createAllocationService(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE).build()
        );
        executor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(
            allocationService,
            ShardStartedClusterStateTaskExecutorTests::neverReroutes,
            () -> Priority.NORMAL,
            logger
        );
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, Collections.emptyList());
        assertSame(clusterState, result.resultingState);
    }

    public void testNonExistentIndexMarkedAsSuccessful() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final StartedShardEntry entry = new StartedShardEntry(new ShardId("test", "_na", 0), "aId", randomNonNegativeLong(), "test");

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(entry));
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(1));
        assertThat(result.executionResults.containsKey(entry), is(true));
        assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(entry)).isSuccess(), is(true));
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithActivePrimary(indexName, true, randomInt(2), randomInt(2));

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final List<StartedShardEntry> tasks = Stream.concat(
            // Existent shard id but different allocation id
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetadata.getIndex(), 0), String.valueOf(i), 0L, "allocation id")),
            // Non existent shard id
            IntStream.range(1, randomIntBetween(2, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetadata.getIndex(), i), String.valueOf(i), 0L, "shard id"))

        ).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testNonInitializingShardAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithAssignedPrimariesAndReplicas(new String[] { indexName }, randomIntBetween(2, 10), 1);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(1, indexMetadata.getNumberOfShards())).mapToObj(i -> {
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
            final IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().shardRoutingTable(shardId);
            final String allocationId;
            if (randomBoolean()) {
                allocationId = shardRoutingTable.primaryShard().allocationId().getId();
            } else {
                allocationId = shardRoutingTable.replicaShards().iterator().next().allocationId().getId();
            }
            final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
            return new StartedShardEntry(shardId, allocationId, primaryTerm, "test");
        }).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testStartedShards() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        final List<StartedShardEntry> tasks = new ArrayList<>();
        tasks.add(new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "test"));
        if (randomBoolean()) {
            final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
            final String replicaAllocationId = replicaShard.allocationId().getId();
            tasks.add(new StartedShardEntry(shardId, replicaAllocationId, primaryTerm, "test"));
        }
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testDuplicateStartsAreOkay() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting shardRouting = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String allocationId = shardRouting.allocationId().getId();
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());

        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> new StartedShardEntry(shardId, allocationId, primaryTerm, "test"))
            .collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testPrimaryTermsMismatch() throws Exception {
        final String indexName = "test";
        final int shard = 0;
        final int primaryTerm = 2 + randomInt(200);

        ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(IndexMetadata.builder(clusterState.metadata().index(indexName)).primaryTerm(shard, primaryTerm).build(), true)
                    .build()
            )
            .build();
        final ShardId shardId = new ShardId(clusterState.metadata().index(indexName).getIndex(), shard);
        final String primaryAllocationId = clusterState.routingTable().shardRoutingTable(shardId).primaryShard().allocationId().getId();
        {
            final StartedShardEntry task = new StartedShardEntry(
                shardId,
                primaryAllocationId,
                primaryTerm - 1,
                "primary terms does not match on primary"
            );

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.INITIALIZING));
            assertSame(clusterState, result.resultingState);
        }
        {
            final StartedShardEntry task = new StartedShardEntry(
                shardId,
                primaryAllocationId,
                primaryTerm,
                "primary terms match on primary"
            );

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
            clusterState = result.resultingState;
        }
        {
            final long replicaPrimaryTerm = randomBoolean() ? primaryTerm : primaryTerm - 1;
            final String replicaAllocationId = clusterState.routingTable()
                .shardRoutingTable(shardId)
                .replicaShards()
                .iterator()
                .next()
                .allocationId()
                .getId();

            final StartedShardEntry task = new StartedShardEntry(shardId, replicaAllocationId, replicaPrimaryTerm, "test on replica");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
        }
    }

    public void testMaybeAddRemoteIndexSettings() {
        final String indexName = "test-add-remote-index-settings";
        final ShardId shardId = new ShardId(indexName, UUID.randomUUID().toString(), 0);
        final int numberOfNodes = 3;
        final int numberOfShards = 1;
        final int numberOfReplicas = 2;
        final ShardStateAction.ShardStartedClusterStateTaskExecutor mockShardStartedClusterStateTaskExecutor =
            new ShardStateAction.ShardStartedClusterStateTaskExecutor(
                mock(AllocationService.class),
                mock(RerouteService.class),
                mock(Supplier.class),
                mock(Logger.class)
            );
        // Index Settings are not updated when all shard copies are in docrep nodes
        {
            ClusterState initialClusterState = stateWithMixedNodes(
                numberOfNodes,
                numberOfNodes,
                true,
                List.of(indexName).toArray(String[]::new),
                numberOfShards,
                numberOfReplicas,
                ShardRoutingState.STARTED
            );
            assertRemoteStoreIndexSettingsAreAbsent(initialClusterState, indexName);
            List<DiscoveryNode> docrepNodeSet = initialClusterState.getNodes()
                .getDataNodes()
                .values()
                .stream()
                .filter(discoveryNode -> discoveryNode.isRemoteStoreNode() == false)
                .collect(Collectors.toList());
            List<ShardRouting> seenShardRoutings = generateMockShardRoutings(shardId, docrepNodeSet);
            ClusterState resultingState = mockShardStartedClusterStateTaskExecutor.maybeAddRemoteIndexSettings(
                loadShardRoutingsIntoClusterState(shardId, initialClusterState, seenShardRoutings),
                new HashSet<>(seenShardRoutings)
            );
            assertRemoteStoreIndexSettingsAreAbsent(resultingState, indexName);
        }
        // Index Settings are updated when all shard copies are in remote nodes and existing metadata
        // does not have remote enabled settings
        {
            ClusterState initialClusterState = stateWithMixedNodes(
                numberOfNodes,
                numberOfNodes,
                true,
                List.of(indexName).toArray(String[]::new),
                numberOfShards,
                numberOfReplicas,
                ShardRoutingState.STARTED
            );
            assertRemoteStoreIndexSettingsAreAbsent(initialClusterState, indexName);
            List<DiscoveryNode> remoteNodeSet = initialClusterState.getNodes()
                .getDataNodes()
                .values()
                .stream()
                .filter(DiscoveryNode::isRemoteStoreNode)
                .collect(Collectors.toList());
            List<ShardRouting> seenShardRoutings = generateMockShardRoutings(shardId, remoteNodeSet);

            ClusterState resultingState = mockShardStartedClusterStateTaskExecutor.maybeAddRemoteIndexSettings(
                loadShardRoutingsIntoClusterState(shardId, initialClusterState, seenShardRoutings),
                new HashSet<>(seenShardRoutings)
            );
            assertRemoteStoreIndexSettingsArePresent(resultingState, indexName);
        }
        // Index Settings are updated when some shard copies are in remote nodes while others are still on docrep
        {
            ClusterState initialClusterState = stateWithMixedNodes(
                numberOfNodes,
                numberOfNodes,
                true,
                List.of(indexName).toArray(String[]::new),
                numberOfShards,
                numberOfReplicas,
                ShardRoutingState.STARTED
            );
            assertRemoteStoreIndexSettingsAreAbsent(initialClusterState, indexName);
            List<DiscoveryNode> mixedNodeSet = new ArrayList<>();
            mixedNodeSet.addAll(
                initialClusterState.getNodes()
                    .getDataNodes()
                    .values()
                    .stream()
                    .filter(DiscoveryNode::isRemoteStoreNode)
                    .limit(2)
                    .collect(Collectors.toList())
            );
            mixedNodeSet.addAll(
                initialClusterState.getNodes()
                    .getDataNodes()
                    .values()
                    .stream()
                    .filter(discoveryNode -> discoveryNode.isRemoteStoreNode() == false)
                    .limit(1)
                    .collect(Collectors.toList())
            );
            List<ShardRouting> seenShardRoutings = generateMockShardRoutings(shardId, mixedNodeSet);
            ClusterState resultingState = mockShardStartedClusterStateTaskExecutor.maybeAddRemoteIndexSettings(
                loadShardRoutingsIntoClusterState(shardId, initialClusterState, seenShardRoutings),
                new HashSet<>(seenShardRoutings)
            );
            assertRemoteStoreIndexSettingsAreAbsent(resultingState, indexName);
        }
    }

    private ClusterStateTaskExecutor.ClusterTasksResult executeTasks(final ClusterState state, final List<StartedShardEntry> tasks)
        throws Exception {
        final ClusterStateTaskExecutor.ClusterTasksResult<StartedShardEntry> result = executor.execute(state, tasks);
        assertThat(result, notNullValue());
        return result;
    }

    private static void assertRemoteStoreIndexSettingsArePresent(ClusterState resultingState, String indexName) {
        Settings resultingIndexSettings = resultingState.metadata().index(indexName).getSettings();
        assertEquals("true", resultingIndexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals("SEGMENT", resultingIndexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals(IndexShardTestUtils.MOCK_SEGMENT_REPO_NAME, resultingIndexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(IndexShardTestUtils.MOCK_TLOG_REPO_NAME, resultingIndexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    private static void assertRemoteStoreIndexSettingsAreAbsent(ClusterState initialClusterState, String indexName) {
        Settings initialIndexSettings = initialClusterState.metadata().index(indexName).getSettings();
        assertNull(initialIndexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(initialIndexSettings.get(SETTING_REPLICATION_TYPE));
        assertNull(initialIndexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(initialIndexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    private List<ShardRouting> generateMockShardRoutings(ShardId shardId, List<DiscoveryNode> nodeSet) {
        List<ShardRouting> seenShardRoutings = new ArrayList<>(nodeSet.size());
        for (DiscoveryNode node : nodeSet) {
            if (node.getId().contains("node_0")) {
                seenShardRoutings.add(TestShardRouting.newShardRouting(shardId, node.getId(), true, ShardRoutingState.STARTED));
            } else {
                seenShardRoutings.add(TestShardRouting.newShardRouting(shardId, node.getId(), false, ShardRoutingState.STARTED));
            }
        }
        return seenShardRoutings;
    }

    private ClusterState loadShardRoutingsIntoClusterState(ShardId shardId, ClusterState currentState, List<ShardRouting> shardRoutings) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(
            currentState.metadata().index(shardId.getIndexName()).getIndex()
        );
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        for (ShardRouting shardRouting : shardRoutings) {
            indexShardRoutingBuilder.addShard(shardRouting);
        }
        return clusterStateBuilder.routingTable(
            routingTableBuilder.add(indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build()).build()).build()
        ).build();
    }
}
