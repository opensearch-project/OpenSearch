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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.close;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.FanoutReplicationProxy;
import org.opensearch.action.support.replication.PendingReplicationActions;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationOperation;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

public class TransportVerifyShardBeforeCloseActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;

    private IndexShard indexShard;
    private TransportVerifyShardBeforeCloseAction action;
    private ClusterService clusterService;
    private ClusterBlock clusterBlock;
    private CapturingTransport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        indexShard = mock(IndexShard.class);
        when(indexShard.getActiveOperationsCount()).thenReturn(IndexShard.OPERATIONS_BLOCKED);

        final ShardId shardId = new ShardId("index", "_na_", randomIntBetween(0, 3));
        when(indexShard.shardId()).thenReturn(shardId);

        clusterService = createClusterService(threadPool);

        clusterBlock = MetadataIndexStateService.createIndexClosingBlock();
        setState(
            clusterService,
            new ClusterState.Builder(clusterService.state()).blocks(
                ClusterBlocks.builder().blocks(clusterService.state().blocks()).addIndexBlock("index", clusterBlock).build()
            ).build()
        );

        transport = new CapturingTransport();
        TransportService transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        action = new TransportVerifyShardBeforeCloseAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            mock(IndicesService.class),
            mock(ThreadPool.class),
            shardStateAction,
            mock(ActionFilters.class)
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    private void executeOnPrimaryOrReplica() throws Throwable {
        executeOnPrimaryOrReplica(false);
    }

    private void executeOnPrimaryOrReplica(boolean phase1) throws Throwable {
        final TaskId taskId = new TaskId("_node_id", randomNonNegativeLong());
        final TransportVerifyShardBeforeCloseAction.ShardRequest request = new TransportVerifyShardBeforeCloseAction.ShardRequest(
            indexShard.shardId(),
            clusterBlock,
            phase1,
            taskId
        );
        final PlainActionFuture<Void> res = PlainActionFuture.newFuture();
        action.shardOperationOnPrimary(request, indexShard, ActionListener.wrap(r -> {
            assertNotNull(r);
            res.onResponse(null);
        }, res::onFailure));
        try {
            res.get();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public void testShardIsFlushed() throws Throwable {
        final ArgumentCaptor<FlushRequest> flushRequest = ArgumentCaptor.forClass(FlushRequest.class);
        doNothing().when(indexShard).flush(flushRequest.capture());
        executeOnPrimaryOrReplica();
        verify(indexShard, times(1)).flush(any(FlushRequest.class));
        assertThat(flushRequest.getValue().force(), is(true));
    }

    public void testShardIsSynced() throws Throwable {
        executeOnPrimaryOrReplica(true);
        verify(indexShard, times(1)).sync();
    }

    public void testOperationFailsWhenNotBlocked() {
        when(indexShard.getActiveOperationsCount()).thenReturn(randomIntBetween(0, 10));

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(
            exception.getMessage(),
            equalTo("Index shard " + indexShard.shardId() + " is not blocking all operations during closing")
        );
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    public void testOperationFailsWithNoBlock() {
        setState(clusterService, new ClusterState.Builder(new ClusterName("test")).build());

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(
            exception.getMessage(),
            equalTo("Index shard " + indexShard.shardId() + " must be blocked by " + clusterBlock + " before closing")
        );
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    public void testVerifyShardBeforeIndexClosing() throws Throwable {
        executeOnPrimaryOrReplica();
        verify(indexShard, times(1)).verifyShardBeforeIndexClosing();
        verify(indexShard, times(1)).flush(any(FlushRequest.class));
    }

    public void testVerifyShardBeforeIndexClosingFailed() {
        doThrow(new IllegalStateException("test")).when(indexShard).verifyShardBeforeIndexClosing();
        expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        verify(indexShard, times(1)).verifyShardBeforeIndexClosing();
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    public void testUnavailableShardsMarkedAsStale() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        final int nbReplicas = randomIntBetween(1, 10);
        final ShardRoutingState[] replicaStates = new ShardRoutingState[nbReplicas];
        for (int i = 0; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.STARTED;
        }
        final ClusterState clusterState = state(index, true, ShardRoutingState.STARTED, replicaStates);
        setState(clusterService, clusterState);

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index(index).shard(shardId.id());
        final IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
        final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
        final long primaryTerm = indexMetadata.primaryTerm(0);

        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(0);
        final Set<String> trackedShards = shardRoutingTable.getAllAllocationIds();

        List<ShardRouting> unavailableShards = randomSubsetOf(randomIntBetween(1, nbReplicas), shardRoutingTable.replicaShards());
        IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardRoutingTable);
        unavailableShards.forEach(shardRoutingTableBuilder::removeShard);
        shardRoutingTable = shardRoutingTableBuilder.build();

        final ReplicationGroup replicationGroup = new ReplicationGroup(shardRoutingTable, inSyncAllocationIds, trackedShards, 0);
        assertThat(replicationGroup.getUnavailableInSyncShards().size(), greaterThan(0));

        final PlainActionFuture<PrimaryResult> listener = new PlainActionFuture<>();
        TaskId taskId = new TaskId(clusterService.localNode().getId(), 0L);
        TransportVerifyShardBeforeCloseAction.ShardRequest request = new TransportVerifyShardBeforeCloseAction.ShardRequest(
            shardId,
            clusterBlock,
            false,
            taskId
        );
        ReplicationOperation.Replicas<TransportVerifyShardBeforeCloseAction.ShardRequest> proxy = action.newReplicasProxy();
        ReplicationOperation<
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            PrimaryResult> operation = new ReplicationOperation<>(
                request,
                createPrimary(primaryRouting, replicationGroup),
                listener,
                proxy,
                logger,
                threadPool,
                "test",
                primaryTerm,
                TimeValue.timeValueMillis(20),
                TimeValue.timeValueSeconds(60),
                new FanoutReplicationProxy<>(proxy)
            );
        operation.execute();

        final CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(nbReplicas));

        for (CapturingTransport.CapturedRequest capturedRequest : capturedRequests) {
            final String actionName = capturedRequest.action;
            if (actionName.startsWith(ShardStateAction.SHARD_FAILED_ACTION_NAME)) {
                assertThat(capturedRequest.request, instanceOf(ShardStateAction.FailedShardEntry.class));
                String allocationId = ((ShardStateAction.FailedShardEntry) capturedRequest.request).getAllocationId();
                assertTrue(unavailableShards.stream().anyMatch(shardRouting -> shardRouting.allocationId().getId().equals(allocationId)));
                transport.handleResponse(capturedRequest.requestId, TransportResponse.Empty.INSTANCE);

            } else if (actionName.startsWith(TransportVerifyShardBeforeCloseAction.NAME)) {
                assertThat(capturedRequest.request, instanceOf(ConcreteShardRequest.class));
                String allocationId = ((ConcreteShardRequest) capturedRequest.request).getTargetAllocationID();
                assertFalse(unavailableShards.stream().anyMatch(shardRouting -> shardRouting.allocationId().getId().equals(allocationId)));
                assertTrue(inSyncAllocationIds.stream().anyMatch(inSyncAllocationId -> inSyncAllocationId.equals(allocationId)));
                transport.handleResponse(capturedRequest.requestId, new TransportReplicationAction.ReplicaResponse(0L, 0L));

            } else {
                fail("Test does not support action " + capturedRequest.action);
            }
        }

        final ReplicationResponse.ShardInfo shardInfo = listener.get().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(0));
        assertThat(shardInfo.getFailures(), arrayWithSize(0));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + nbReplicas - unavailableShards.size()));
    }

    public void testGetReplicationModeWithRemoteTranslog() {
        TransportVerifyShardBeforeCloseAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(true);
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        TransportVerifyShardBeforeCloseAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(true);
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    private TransportVerifyShardBeforeCloseAction createAction() {
        return new TransportVerifyShardBeforeCloseAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(IndicesService.class),
            mock(ThreadPool.class),
            mock(ShardStateAction.class),
            mock(ActionFilters.class)
        );
    }

    private static
        ReplicationOperation.Primary<
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            PrimaryResult>
        createPrimary(final ShardRouting primary, final ReplicationGroup replicationGroup) {
        final PendingReplicationActions replicationActions = new PendingReplicationActions(primary.shardId(), threadPool);
        replicationActions.accept(replicationGroup);
        return new ReplicationOperation.Primary<
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            TransportVerifyShardBeforeCloseAction.ShardRequest,
            PrimaryResult>() {

            @Override
            public ShardRouting routingEntry() {
                return primary;
            }

            @Override
            public PendingReplicationActions getPendingReplicationActions() {
                return replicationActions;
            }

            @Override
            public ReplicationGroup getReplicationGroup() {
                return replicationGroup;
            }

            @Override
            public void perform(TransportVerifyShardBeforeCloseAction.ShardRequest request, ActionListener<PrimaryResult> listener) {
                listener.onResponse(new PrimaryResult(request));
            }

            @Override
            public void failShard(String message, Exception exception) {

            }

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {}

            @Override
            public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {}

            @Override
            public long localCheckpoint() {
                return 0;
            }

            @Override
            public long computedGlobalCheckpoint() {
                return 0;
            }

            @Override
            public long globalCheckpoint() {
                return 0;
            }

            @Override
            public long maxSeqNoOfUpdatesOrDeletes() {
                return 0;
            }
        };
    }

    private static class PrimaryResult implements ReplicationOperation.PrimaryResult<TransportVerifyShardBeforeCloseAction.ShardRequest> {

        private final TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest;
        private final SetOnce<ReplicationResponse.ShardInfo> shardInfo;

        private PrimaryResult(final TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest) {
            this.replicaRequest = replicaRequest;
            this.shardInfo = new SetOnce<>();
        }

        @Override
        public TransportVerifyShardBeforeCloseAction.ShardRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            this.shardInfo.set(shardInfo);
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        public ReplicationResponse.ShardInfo getShardInfo() {
            return shardInfo.get();
        }
    }

}
