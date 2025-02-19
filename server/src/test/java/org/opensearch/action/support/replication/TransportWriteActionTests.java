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

package org.opensearch.action.support.replication;

import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.support.WriteResponse;
import org.opensearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.PrimaryShardClosedException;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.node.NodeClosedException;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.transport.NoNodeAvailableException;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportWriteActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private IndexShard indexShard;
    private Translog.Location location;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @Before
    public void initCommonMocks() {
        indexShard = mock(IndexShard.class);
        location = mock(Translog.Location.class);
        clusterService = createClusterService(threadPool);
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

    <T> void assertListenerThrows(String msg, PlainActionFuture<T> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

    public void testPrimaryNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(ActionListener.map(listener, ignore -> result.finalResponseIfSuccessful));
            assertNotNull(listener.response);
            assertNull(listener.failure);
            verify(indexShard, never()).refresh(any());
            verify(indexShard, never()).addRefreshListener(any(), any());
        }));
    }

    public void testReplicaNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = PlainActionFuture.newFuture();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(ActionListener.map(listener, ignore -> TransportResponse.Empty.INSTANCE));
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(ActionListener.map(listener, ignore -> result.finalResponseIfSuccessful));
            assertNotNull(listener.response);
            assertNull(listener.failure);
            assertTrue(listener.response.forcedRefresh);
            verify(indexShard).refresh("refresh_flag_index");
            verify(indexShard, never()).addRefreshListener(any(), any());
        }));
    }

    public void testReplicaImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = PlainActionFuture.newFuture();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(ActionListener.map(listener, ignore -> TransportResponse.Empty.INSTANCE));
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard).refresh("refresh_flag_index");
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);

        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(ActionListener.map(listener, ignore -> result.finalResponseIfSuccessful));
            assertNull(listener.response); // Haven't really responded yet

            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
            verify(indexShard, never()).refresh(any());
            verify(indexShard).addRefreshListener(any(), refreshListener.capture());

            // Now we can fire the listener manually and we'll get a response
            boolean forcedRefresh = randomBoolean();
            refreshListener.getValue().accept(forcedRefresh);
            assertNotNull(listener.response);
            assertNull(listener.failure);
            assertEquals(forcedRefresh, listener.response.forcedRefresh);
        }));
    }

    public void testReplicaWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = PlainActionFuture.newFuture();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(ActionListener.map(listener, ignore -> TransportResponse.Empty.INSTANCE));
        assertNull(listener.response); // Haven't responded yet
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(indexShard, never()).refresh(any());
        verify(indexShard).addRefreshListener(any(), refreshListener.capture());

        // Now we can fire the listener manually and we'll get a response
        boolean forcedRefresh = randomBoolean();
        refreshListener.getValue().accept(forcedRefresh);
        assertNotNull(listener.response);
        assertNull(listener.failure);
    }

    public void testDocumentFailureInShardOperationOnPrimary() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(true, true);
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(ActionListener.map(listener, ignore -> result.finalResponseIfSuccessful));
            assertNull(listener.response);
            assertNotNull(listener.failure);
        }));
    }

    public void testDocumentFailureInShardOperationOnReplica() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(randomBoolean(), true);
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = PlainActionFuture.newFuture();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(ActionListener.map(listener, ignore -> TransportResponse.Empty.INSTANCE));
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    public void testReplicaProxy() throws InterruptedException, ExecutionException {
        CapturingTransport transport = new CapturingTransport();
        TransportService transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testAction",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(index, true, 1 + randomInt(3), randomInt(2));
        logger.info("using state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        final long primaryTerm = state.metadata().index(index).primaryTerm(0);
        ReplicationOperation.Replicas<TestRequest> proxy = action.newReplicasProxy();

        // check that at unknown node fails
        PlainActionFuture<ReplicaResponse> listener = new PlainActionFuture<>();
        ShardRoutingState routingState = randomFrom(
            ShardRoutingState.INITIALIZING,
            ShardRoutingState.STARTED,
            ShardRoutingState.RELOCATING
        );
        proxy.performOn(
            TestShardRouting.newShardRouting(
                shardId,
                "NOT THERE",
                routingState == ShardRoutingState.RELOCATING ? state.nodes().iterator().next().getId() : null,
                false,
                routingState
            ),
            new TestRequest(),
            primaryTerm,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            listener
        );
        assertTrue(listener.isDone());
        assertListenerThrows("non existent node should throw a NoNodeAvailableException", listener, NoNodeAvailableException.class);

        final IndexShardRoutingTable shardRoutings = state.routingTable().shardRoutingTable(shardId);
        final ShardRouting replica = randomFrom(
            shardRoutings.replicaShards().stream().filter(ShardRouting::assignedToNode).collect(Collectors.toList())
        );
        listener = new PlainActionFuture<>();
        proxy.performOn(replica, new TestRequest(), primaryTerm, randomNonNegativeLong(), randomNonNegativeLong(), listener);
        assertFalse(listener.isDone());

        CapturingTransport.CapturedRequest[] captures = transport.getCapturedRequestsAndClear();
        assertThat(captures, arrayWithSize(1));
        if (randomBoolean()) {
            final TransportReplicationAction.ReplicaResponse response = new TransportReplicationAction.ReplicaResponse(
                randomLong(),
                randomLong()
            );
            transport.handleResponse(captures[0].requestId, response);
            assertTrue(listener.isDone());
            assertThat(listener.get(), equalTo(response));
        } else if (randomBoolean()) {
            transport.handleRemoteError(captures[0].requestId, new OpenSearchException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, OpenSearchException.class);
        } else {
            transport.handleError(captures[0].requestId, new TransportException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, TransportException.class);
        }

        AtomicReference<Object> failure = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();
        proxy.failShardIfNeeded(
            replica,
            primaryTerm,
            "test",
            new OpenSearchException("simulated"),
            ActionListener.wrap(r -> success.set(true), failure::set)
        );
        CapturingTransport.CapturedRequest[] shardFailedRequests = transport.getCapturedRequestsAndClear();
        // A write replication action proxy should fail the shard
        assertEquals(1, shardFailedRequests.length);
        CapturingTransport.CapturedRequest shardFailedRequest = shardFailedRequests[0];
        ShardStateAction.FailedShardEntry shardEntry = (ShardStateAction.FailedShardEntry) shardFailedRequest.request;
        // the shard the request was sent to and the shard to be failed should be the same
        assertEquals(shardEntry.getShardId(), replica.shardId());
        assertEquals(shardEntry.getAllocationId(), replica.allocationId().getId());
        if (randomBoolean()) {
            // simulate success
            transport.handleResponse(shardFailedRequest.requestId, TransportResponse.Empty.INSTANCE);
            assertTrue(success.get());
            assertNull(failure.get());
        } else if (randomBoolean()) {
            // simulate the primary has been demoted
            transport.handleRemoteError(
                shardFailedRequest.requestId,
                new ShardStateAction.NoLongerPrimaryShardException(replica.shardId(), "shard-failed-test")
            );
            assertFalse(success.get());
            assertNotNull(failure.get());
        } else {
            // simulated a node closing exception
            transport.handleRemoteError(shardFailedRequest.requestId, new NodeClosedException(state.nodes().getLocalNode()));
            assertFalse(success.get());
            assertNotNull(failure.get());
        }
    }

    public void testPrimaryClosedDoesNotFailShard() {
        final CapturingTransport transport = new CapturingTransport();
        final TransportService transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        final ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        final TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testAction",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        final ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(index, true, 1, 0);
        ClusterServiceUtils.setState(clusterService, state);
        final long primaryTerm = state.metadata().index(index).primaryTerm(0);
        final ShardRouting shardRouting = state.routingTable().shardRoutingTable(shardId).replicaShards().get(0);

        // Assert that failShardIfNeeded is a no-op for the PrimaryShardClosedException failure
        final AtomicInteger callbackCount = new AtomicInteger(0);
        action.newReplicasProxy()
            .failShardIfNeeded(
                shardRouting,
                primaryTerm,
                "test",
                new PrimaryShardClosedException(shardId),
                ActionListener.wrap(callbackCount::incrementAndGet)
            );
        MatcherAssert.assertThat(transport.getCapturedRequestsAndClear(), emptyArray());
        MatcherAssert.assertThat(callbackCount.get(), equalTo(0));
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestRequest, TestResponse> {

        private final boolean withDocumentFailureOnPrimary;
        private final boolean withDocumentFailureOnReplica;

        protected TestAction() {
            this(false, false);
        }

        protected TestAction(boolean withDocumentFailureOnPrimary, boolean withDocumentFailureOnReplica) {
            super(
                Settings.EMPTY,
                "internal:test",
                new TransportService(
                    Settings.EMPTY,
                    mock(Transport.class),
                    null,
                    TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                    x -> null,
                    null,
                    Collections.emptySet(),
                    NoopTracer.INSTANCE
                ),
                TransportWriteActionTests.this.clusterService,
                null,
                null,
                null,
                new ActionFilters(new HashSet<>()),
                TestRequest::new,
                TestRequest::new,
                ignore -> ThreadPool.Names.SAME,
                false,
                new IndexingPressureService(Settings.EMPTY, TransportWriteActionTests.this.clusterService),
                new SystemIndices(emptyMap()),
                NoopTracer.INSTANCE
            );
            this.withDocumentFailureOnPrimary = withDocumentFailureOnPrimary;
            this.withDocumentFailureOnReplica = withDocumentFailureOnReplica;
        }

        protected TestAction(
            Settings settings,
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ShardStateAction shardStateAction,
            ThreadPool threadPool
        ) {
            super(
                settings,
                actionName,
                transportService,
                clusterService,
                mockIndicesService(clusterService),
                threadPool,
                shardStateAction,
                new ActionFilters(new HashSet<>()),
                TestRequest::new,
                TestRequest::new,
                ignore -> ThreadPool.Names.SAME,
                false,
                new IndexingPressureService(settings, clusterService),
                new SystemIndices(emptyMap()),
                NoopTracer.INSTANCE
            );
            this.withDocumentFailureOnPrimary = false;
            this.withDocumentFailureOnReplica = false;
        }

        @Override
        protected TestResponse newResponseInstance(StreamInput in) throws IOException {
            return new TestResponse();
        }

        @Override
        protected void dispatchedShardOperationOnPrimary(
            TestRequest request,
            IndexShard primary,
            ActionListener<PrimaryResult<TestRequest, TestResponse>> listener
        ) {
            ActionListener.completeWith(listener, () -> {
                if (withDocumentFailureOnPrimary) {
                    return new WritePrimaryResult<>(request, null, null, new RuntimeException("simulated"), primary, logger);
                } else {
                    return new WritePrimaryResult<>(request, new TestResponse(), location, null, primary, logger);
                }
            });
        }

        @Override
        protected void dispatchedShardOperationOnReplica(TestRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
            ActionListener.completeWith(listener, () -> {
                final WriteReplicaResult<TestRequest> replicaResult;
                if (withDocumentFailureOnReplica) {
                    replicaResult = new WriteReplicaResult<>(request, null, new RuntimeException("simulated"), replica, logger);
                } else {
                    replicaResult = new WriteReplicaResult<>(request, location, null, replica, logger);
                }
                return replicaResult;
            });
        }
    }

    final IndexService mockIndexService(final IndexMetadata indexMetadata, ClusterService clusterService) {
        final IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).then(invocation -> {
            int shard = (Integer) invocation.getArguments()[0];
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), shard);
            if (shard > indexMetadata.getNumberOfShards()) {
                throw new ShardNotFoundException(shardId);
            }
            return mockIndexShard(shardId, clusterService);
        });
        return indexService;
    }

    final IndicesService mockIndicesService(ClusterService clusterService) {
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            final IndexMetadata indexSafe = state.metadata().getIndexSafe(index);
            return mockIndexService(indexSafe, clusterService);
        });
        when(indicesService.indexService(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            if (state.metadata().hasIndex(index.getName())) {
                return mockIndexService(clusterService.state().metadata().getIndexSafe(index), clusterService);
            } else {
                return null;
            }
        });
        return indicesService;
    }

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicBoolean isRelocated = new AtomicBoolean(false);

    private IndexShard mockIndexShard(ShardId shardId, ClusterService clusterService) {
        final IndexShard indexShard = mock(IndexShard.class);
        doAnswer(invocation -> {
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquirePrimaryOperationPermit(any(ActionListener.class), anyString(), any());
        doAnswer(invocation -> {
            long term = (Long) invocation.getArguments()[0];
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[1];
            final long primaryTerm = indexShard.getPendingPrimaryTerm();
            if (term < primaryTerm) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s operation term [%d] is too old (current [%d])", shardId, term, primaryTerm)
                );
            }
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquireReplicaOperationPermit(anyLong(), anyLong(), anyLong(), any(ActionListener.class), anyString(), any());
        when(indexShard.routingEntry()).thenAnswer(invocationOnMock -> {
            final ClusterState state = clusterService.state();
            final RoutingNode node = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
            final ShardRouting routing = node.getByShardId(shardId);
            if (routing == null) {
                throw new ShardNotFoundException(shardId, "shard is no longer assigned to current node");
            }
            return routing;
        });
        when(indexShard.isRelocatedPrimary()).thenAnswer(invocationOnMock -> isRelocated.get());
        doThrow(new AssertionError("failed shard is not supported")).when(indexShard).failShard(anyString(), any(Exception.class));
        when(indexShard.getPendingPrimaryTerm()).thenAnswer(
            i -> clusterService.state().metadata().getIndexSafe(shardId.getIndex()).primaryTerm(shardId.id())
        );
        return indexShard;
    }

    private static class TestRequest extends ReplicatedWriteRequest<TestRequest> {
        TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestRequest() {
            super(new ShardId("test", "test", 1));
        }

        @Override
        public String toString() {
            return "TestRequest{}";
        }
    }

    private static class TestResponse extends ReplicationResponse implements WriteResponse {
        boolean forcedRefresh;

        @Override
        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }
    }

    private static class CapturingActionListener<R> implements ActionListener<R> {
        private R response;
        private Exception failure;

        @Override
        public void onResponse(R response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception failure) {
            this.failure = failure;
        }
    }
}
