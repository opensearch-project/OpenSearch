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

package org.opensearch.indices.cluster;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.PrimaryReplicaSyncer;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesClusterStateServiceRandomUpdatesTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterStateChanges cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        cluster = new ClusterStateChanges(xContentRegistry(), threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRandomClusterStateUpdates() {
        // we have an IndicesClusterStateService per node in the cluster
        final Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap = new HashMap<>();
        ClusterState state = randomInitialClusterState(clusterStateServiceMap, MockIndicesService::new);
        // each of the following iterations represents a new cluster state update processed on all nodes
        for (int i = 0; i < 30; i++) {
            logger.info("Iteration {}", i);
            final ClusterState previousState = state;

            // calculate new cluster state
            for (int j = 0; j < randomInt(3); j++) { // multiple iterations to simulate batching of cluster states
                try {
                    state = randomlyUpdateClusterState(state, clusterStateServiceMap, MockIndicesService::new);
                } catch (AssertionError error) {
                    ClusterState finalState = state;
                    logger.error(() -> new ParameterizedMessage("failed to random change state. last good state: \n{}", finalState), error);
                    throw error;
                }
            }

            // apply cluster state to nodes (incl. cluster-manager)
            for (DiscoveryNode node : state.nodes()) {
                IndicesClusterStateService indicesClusterStateService = clusterStateServiceMap.get(node);
                ClusterState localState = adaptClusterStateToLocalNode(state, node);
                ClusterState previousLocalState = adaptClusterStateToLocalNode(previousState, node);
                final ClusterChangedEvent event = new ClusterChangedEvent("simulated change " + i, localState, previousLocalState);
                try {
                    indicesClusterStateService.applyClusterState(event);
                } catch (AssertionError error) {
                    logger.error(
                        new ParameterizedMessage(
                            "failed to apply change on [{}].\n ***  Previous state ***\n{}\n ***  New state ***\n{}",
                            node,
                            event.previousState(),
                            event.state()
                        ),
                        error
                    );
                    throw error;
                }

                // check that cluster state has been properly applied to node
                assertClusterStateMatchesNodeState(localState, indicesClusterStateService);
            }
        }

        // TODO: check if we can go to green by starting all shards and finishing all iterations
        logger.info("Final cluster state: {}", state);
    }

    /**
     * This test ensures that when a node joins a brand new cluster (different cluster UUID),
     * different from the cluster it was previously a part of, the in-memory index data structures
     * are all removed but the on disk contents of those indices remain so that they can later be
     * imported as dangling indices.  Normally, the first cluster state update that the node
     * receives from the new cluster would contain a cluster block that would cause all in-memory
     * structures to be removed (see {@link IndicesClusterStateService#applyClusterState(ClusterChangedEvent)}),
     * but in the case where the node joined and was a few cluster state updates behind, it would
     * not have received the cluster block, in which case we still need to remove the in-memory
     * structures while ensuring the data remains on disk.  This test executes this particular
     * scenario.
     */
    public void testJoiningNewClusterOnlyRemovesInMemoryIndexStructures() {
        // a cluster state derived from the initial state that includes a created index
        String name = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        ShardRoutingState[] replicaStates = new ShardRoutingState[randomIntBetween(0, 3)];
        Arrays.fill(replicaStates, ShardRoutingState.INITIALIZING);
        ClusterState stateWithIndex = ClusterStateCreationUtils.state(name, randomBoolean(), ShardRoutingState.INITIALIZING, replicaStates);

        // the initial state which is derived from the newly created cluster state but doesn't contain the index
        ClusterState initialState = ClusterState.builder(stateWithIndex)
            .metadata(Metadata.builder(stateWithIndex.metadata()).remove(name))
            .routingTable(RoutingTable.builder().build())
            .build();

        // pick a data node to simulate the adding an index cluster state change event on, that has shards assigned to it
        DiscoveryNode node = stateWithIndex.nodes()
            .get(randomFrom(stateWithIndex.routingTable().index(name).shardsWithState(INITIALIZING)).currentNodeId());

        // simulate the cluster state change on the node
        ClusterState localState = adaptClusterStateToLocalNode(stateWithIndex, node);
        ClusterState previousLocalState = adaptClusterStateToLocalNode(initialState, node);
        IndicesClusterStateService indicesCSSvc = createIndicesClusterStateService(node, RecordingIndicesService::new);
        indicesCSSvc.start();
        indicesCSSvc.applyClusterState(new ClusterChangedEvent("cluster state change that adds the index", localState, previousLocalState));

        // create a new empty cluster state with a brand new cluster UUID
        ClusterState newClusterState = ClusterState.builder(initialState)
            .metadata(Metadata.builder(initialState.metadata()).clusterUUID(UUIDs.randomBase64UUID()))
            .build();

        // simulate the cluster state change on the node
        localState = adaptClusterStateToLocalNode(newClusterState, node);
        previousLocalState = adaptClusterStateToLocalNode(stateWithIndex, node);
        indicesCSSvc.applyClusterState(
            new ClusterChangedEvent(
                "cluster state change with a new cluster UUID (and doesn't contain the index)",
                localState,
                previousLocalState
            )
        );

        // check that in memory data structures have been removed once the new cluster state is applied,
        // but the persistent data is still there
        RecordingIndicesService indicesService = (RecordingIndicesService) indicesCSSvc.indicesService;
        for (IndexMetadata indexMetadata : stateWithIndex.metadata()) {
            Index index = indexMetadata.getIndex();
            assertNull(indicesService.indexService(index));
            assertFalse(indicesService.isDeleted(index));
        }
    }

    /**
     * In rare cases it is possible that a nodes gets an instruction to replace a replica
     * shard that's in POST_RECOVERY with a new initializing primary with the same allocation id.
     * This can happen by batching cluster states that include the starting of the replica, with
     * closing of the indices, opening it up again and allocating the primary shard to the node in
     * question. The node should then clean it's initializing replica and replace it with a new
     * initializing primary.
     */
    public void testInitializingPrimaryRemovesInitializingReplicaWithSameAID() {
        disableRandomFailures();
        String index = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        ClusterState state = ClusterStateCreationUtils.state(
            index,
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.INITIALIZING
        );

        // the initial state which is derived from the newly created cluster state but doesn't contain the index
        ClusterState previousState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).remove(index))
            .routingTable(RoutingTable.builder().build())
            .build();

        // pick a data node to simulate the adding an index cluster state change event on, that has shards assigned to it
        final ShardRouting shardRouting = state.routingTable().index(index).shard(0).replicaShards().get(0);
        final ShardId shardId = shardRouting.shardId();
        DiscoveryNode node = state.nodes().get(shardRouting.currentNodeId());

        // simulate the cluster state change on the node
        ClusterState localState = adaptClusterStateToLocalNode(state, node);
        ClusterState previousLocalState = adaptClusterStateToLocalNode(previousState, node);
        IndicesClusterStateService indicesCSSvc = createIndicesClusterStateService(node, RecordingIndicesService::new);
        indicesCSSvc.start();
        indicesCSSvc.applyClusterState(new ClusterChangedEvent("cluster state change that adds the index", localState, previousLocalState));
        previousState = state;

        // start the replica
        state = cluster.applyStartedShards(state, state.routingTable().index(index).shard(0).replicaShards());

        // close the index and open it up again (this will sometimes swap roles between primary and replica)
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(state.metadata().index(index).getIndex().getName());
        state = cluster.closeIndices(state, closeIndexRequest);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(state.metadata().index(index).getIndex().getName());
        state = cluster.openIndices(state, openIndexRequest);

        localState = adaptClusterStateToLocalNode(state, node);
        previousLocalState = adaptClusterStateToLocalNode(previousState, node);

        indicesCSSvc.applyClusterState(new ClusterChangedEvent("new cluster state", localState, previousLocalState));

        final MockIndexShard shardOrNull = ((RecordingIndicesService) indicesCSSvc.indicesService).getShardOrNull(shardId);
        assertThat(
            shardOrNull == null ? null : shardOrNull.routingEntry(),
            equalTo(state.getRoutingNodes().node(node.getId()).getByShardId(shardId))
        );

    }

    public void testRecoveryFailures() {
        disableRandomFailures();
        String index = "index_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        ClusterState state = ClusterStateCreationUtils.state(
            index,
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.INITIALIZING
        );

        // the initial state which is derived from the newly created cluster state but doesn't contain the index
        ClusterState previousState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).remove(index))
            .routingTable(RoutingTable.builder().build())
            .build();

        // pick a data node to simulate the adding an index cluster state change event on, that has shards assigned to it
        final ShardRouting shardRouting = state.routingTable().index(index).shard(0).replicaShards().get(0);
        final ShardId shardId = shardRouting.shardId();
        DiscoveryNode node = state.nodes().get(shardRouting.currentNodeId());

        // simulate the cluster state change on the node
        ClusterState localState = adaptClusterStateToLocalNode(state, node);
        ClusterState previousLocalState = adaptClusterStateToLocalNode(previousState, node);
        IndicesClusterStateService indicesCSSvc = createIndicesClusterStateService(node, RecordingIndicesService::new);
        indicesCSSvc.start();
        indicesCSSvc.applyClusterState(new ClusterChangedEvent("cluster state change that adds the index", localState, previousLocalState));

        assertNotNull(indicesCSSvc.indicesService.getShardOrNull(shardId));

        // check that failing unrelated allocation does not remove shard
        indicesCSSvc.handleRecoveryFailure(shardRouting.reinitializeReplicaShard(), false, new Exception("dummy"));
        assertNotNull(indicesCSSvc.indicesService.getShardOrNull(shardId));

        indicesCSSvc.handleRecoveryFailure(shardRouting, false, new Exception("dummy"));
        assertNull(indicesCSSvc.indicesService.getShardOrNull(shardId));
    }

    public ClusterState randomInitialClusterState(
        Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
        Supplier<MockIndicesService> indicesServiceSupplier
    ) {
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
        // add nodes to clusterStateServiceMap
        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
        return state;
    }

    private void updateNodes(
        ClusterState state,
        Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
        Supplier<MockIndicesService> indicesServiceSupplier
    ) {
        for (DiscoveryNode node : state.nodes()) {
            clusterStateServiceMap.computeIfAbsent(node, discoveryNode -> {
                IndicesClusterStateService ics = createIndicesClusterStateService(discoveryNode, indicesServiceSupplier);
                ics.start();
                return ics;
            });
        }

        for (Iterator<Entry<DiscoveryNode, IndicesClusterStateService>> it = clusterStateServiceMap.entrySet().iterator(); it.hasNext();) {
            DiscoveryNode node = it.next().getKey();
            if (state.nodes().nodeExists(node) == false) {
                it.remove();
            }
        }
    }

    public ClusterState randomlyUpdateClusterState(
        ClusterState state,
        Map<DiscoveryNode, IndicesClusterStateService> clusterStateServiceMap,
        Supplier<MockIndicesService> indicesServiceSupplier
    ) {
        // randomly remove no_cluster_manager blocks
        if (randomBoolean() && state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID)) {
            state = ClusterState.builder(state)
                .blocks(
                    ClusterBlocks.builder()
                        .blocks(state.blocks())
                        .removeGlobalBlock(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID)
                )
                .build();
        }

        // randomly add no_cluster_manager blocks
        if (rarely() && state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID) == false) {
            ClusterBlock block = randomBoolean()
                ? NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL
                : NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES;
            state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(block)).build();
        }

        // if no_cluster_manager block is in place, make no other cluster state changes
        if (state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID)) {
            return state;
        }

        // randomly create new indices (until we have 200 max)
        for (int i = 0; i < randomInt(5); i++) {
            if (state.metadata().indices().size() > 200) {
                break;
            }
            String name = "index_" + randomAlphaOfLength(15).toLowerCase(Locale.ROOT);
            Settings.Builder settingsBuilder = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3));
            if (randomBoolean()) {
                int min = randomInt(2);
                int max = min + randomInt(3);
                settingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, randomBoolean() ? min + "-" + max : min + "-all");
            } else {
                settingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, randomInt(2));
            }
            CreateIndexRequest request = new CreateIndexRequest(name, settingsBuilder.build()).waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex(name));
        }

        // randomly delete indices
        Set<String> indicesToDelete = new HashSet<>();
        int numberOfIndicesToDelete = randomInt(Math.min(2, state.metadata().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToDelete, state.metadata().indices().keySet().toArray(new String[0]))) {
            indicesToDelete.add(state.metadata().index(index).getIndex().getName());
        }
        if (indicesToDelete.isEmpty() == false) {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indicesToDelete.toArray(new String[0]));
            state = cluster.deleteIndices(state, deleteRequest);
            for (String index : indicesToDelete) {
                assertFalse(state.metadata().hasIndex(index));
            }
        }

        // randomly close indices
        int numberOfIndicesToClose = randomInt(Math.min(1, state.metadata().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToClose, state.metadata().indices().keySet().toArray(new String[0]))) {
            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(state.metadata().index(index).getIndex().getName());
            state = cluster.closeIndices(state, closeIndexRequest);
        }

        // randomly open indices
        int numberOfIndicesToOpen = randomInt(Math.min(1, state.metadata().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToOpen, state.metadata().indices().keySet().toArray(new String[0]))) {
            OpenIndexRequest openIndexRequest = new OpenIndexRequest(state.metadata().index(index).getIndex().getName());
            state = cluster.openIndices(state, openIndexRequest);
        }

        // randomly update settings
        Set<String> indicesToUpdate = new HashSet<>();
        boolean containsClosedIndex = false;
        int numberOfIndicesToUpdate = randomInt(Math.min(2, state.metadata().indices().size()));
        for (String index : randomSubsetOf(numberOfIndicesToUpdate, state.metadata().indices().keySet().toArray(new String[0]))) {
            indicesToUpdate.add(state.metadata().index(index).getIndex().getName());
            if (state.metadata().index(index).getState() == IndexMetadata.State.CLOSE) {
                containsClosedIndex = true;
            }
        }
        if (indicesToUpdate.isEmpty() == false) {
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indicesToUpdate.toArray(new String[0]));
            Settings.Builder settings = Settings.builder();
            if (containsClosedIndex == false) {
                settings.put(SETTING_NUMBER_OF_REPLICAS, randomInt(2));
            }
            settings.put("index.refresh_interval", randomIntBetween(1, 5) + "s");
            updateSettingsRequest.settings(settings.build());
            state = cluster.updateSettings(state, updateSettingsRequest);
        }

        // randomly reroute
        if (rarely()) {
            state = cluster.reroute(state, new ClusterRerouteRequest());
        }

        // randomly start and fail allocated shards
        final Map<ShardRouting, Long> startedShards = new HashMap<>();
        List<FailedShard> failedShards = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            IndicesClusterStateService indicesClusterStateService = clusterStateServiceMap.get(node);
            MockIndicesService indicesService = (MockIndicesService) indicesClusterStateService.indicesService;
            for (MockIndexService indexService : indicesService) {
                for (MockIndexShard indexShard : indexService) {
                    ShardRouting persistedShardRouting = indexShard.routingEntry();
                    if (persistedShardRouting.initializing() && randomBoolean()) {
                        startedShards.put(persistedShardRouting, indexShard.term());
                    } else if (rarely()) {
                        failedShards.add(new FailedShard(persistedShardRouting, "fake shard failure", new Exception(), randomBoolean()));
                    }
                }
            }
        }
        state = cluster.applyFailedShards(state, failedShards);
        state = cluster.applyStartedShards(state, startedShards);

        // randomly add and remove nodes (except current cluster-manager)
        if (rarely()) {
            if (randomBoolean()) {
                // add node
                if (state.nodes().getSize() < 10) {
                    state = cluster.addNodes(state, Collections.singletonList(createNode()));
                    updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                }
            } else {
                // remove node
                if (state.nodes().getDataNodes().size() > 3) {
                    DiscoveryNode discoveryNode = randomFrom(state.nodes().getNodes().values().toArray(new DiscoveryNode[0]));
                    if (discoveryNode.equals(state.nodes().getClusterManagerNode()) == false) {
                        state = cluster.removeNodes(state, Collections.singletonList(discoveryNode));
                        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                    }
                    if (randomBoolean()) {
                        // and add it back
                        state = cluster.addNodes(state, Collections.singletonList(discoveryNode));
                        updateNodes(state, clusterStateServiceMap, indicesServiceSupplier);
                    }
                }
            }
        }

        // TODO: go cluster-managerless?

        return state;
    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT);
    }

    private static ClusterState adaptClusterStateToLocalNode(ClusterState state, DiscoveryNode node) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(node.getId())).build();
    }

    private IndicesClusterStateService createIndicesClusterStateService(
        DiscoveryNode discoveryNode,
        final Supplier<MockIndicesService> indicesServiceSupplier
    ) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(mock(ExecutorService.class));
        final MockIndicesService indicesService = indicesServiceSupplier.get();
        final Settings settings = Settings.builder().put("node.name", discoveryNode.getName()).build();
        final TransportService transportService = new TransportService(
            settings,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        final ClusterService clusterService = mock(ClusterService.class);
        final RepositoriesService repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Collections.emptyMap(),
            Collections.emptyMap(),
            threadPool
        );
        final PeerRecoveryTargetService recoveryTargetService = new PeerRecoveryTargetService(
            threadPool,
            transportService,
            null,
            clusterService
        );
        final ShardStateAction shardStateAction = mock(ShardStateAction.class);
        final PrimaryReplicaSyncer primaryReplicaSyncer = mock(PrimaryReplicaSyncer.class);
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            SegmentReplicationCheckpointPublisher.EMPTY,
            mock(SegmentReplicationTargetService.class),
            mock(SegmentReplicationSourceService.class),
            recoveryTargetService,
            shardStateAction,
            null,
            repositoriesService,
            null,
            null,
            null,
            primaryReplicaSyncer,
            s -> {},
            RetentionLeaseSyncer.EMPTY,
            null
        );
    }

    private class RecordingIndicesService extends MockIndicesService {
        private Set<Index> deletedIndices = Collections.emptySet();

        @Override
        public synchronized void removeIndex(Index index, IndexRemovalReason reason, String extraInfo) {
            super.removeIndex(index, reason, extraInfo);
            if (reason == IndexRemovalReason.DELETED) {
                Set<Index> newSet = Sets.newHashSet(deletedIndices);
                newSet.add(index);
                deletedIndices = Collections.unmodifiableSet(newSet);
            }
        }

        public synchronized boolean isDeleted(Index index) {
            return deletedIndices.contains(index);
        }
    }

}
