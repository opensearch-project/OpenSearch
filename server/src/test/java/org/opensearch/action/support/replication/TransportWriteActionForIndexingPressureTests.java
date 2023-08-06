/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.ShardIndexingPressureSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.opensearch.index.stats.IndexingPressurePerShardStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static org.opensearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportWriteActionForIndexingPressureTests extends OpenSearchTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private ShardStateAction shardStateAction;
    private Translog.Location location;
    private Releasable releasable;
    private IndexingPressureService indexingPressureService;

    public static final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        releasable = mock(Releasable.class);
        location = mock(Translog.Location.class);
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

    public void testIndexingPressureOperationStartedForReplicaNode() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(settings, "internal:testAction", transportService, clusterService, shardStateAction, threadPool);

        action.handleReplicaRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(CommonStatsFlags.ALL)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(Objects.isNull(shardStats));
    }

    public void testIndexingPressureOperationStartedForReplicaShard() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(settings, "internal:testAction", transportService, clusterService, shardStateAction, threadPool);

        action.handleReplicaRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(!Objects.isNull(shardStats));
        assertEquals(100, shardStats.getTotalReplicaBytes());
    }

    public void testIndexingPressureOperationStartedForPrimaryNode() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(
            settings,
            "internal:testActionWithExceptions",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );

        action.handlePrimaryRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(CommonStatsFlags.ALL)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(Objects.isNull(shardStats));
    }

    public void testIndexingPressureOperationStartedForPrimaryShard() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(
            settings,
            "internal:testActionWithExceptions",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );

        action.handlePrimaryRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(!Objects.isNull(shardStats));
        assertEquals(100, shardStats.getTotalPrimaryBytes());
    }

    public void testIndexingPressureOperationStartedForLocalPrimaryNode() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(settings, "internal:testAction", transportService, clusterService, shardStateAction, threadPool);

        action.handlePrimaryRequest(
            new TransportReplicationAction.ConcreteShardRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                true,
                true
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(CommonStatsFlags.ALL)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(Objects.isNull(shardStats));
    }

    public void testIndexingPressureOperationStartedForLocalPrimaryShard() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final ReplicationTask task = maybeTask();
        final Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .build();
        this.indexingPressureService = new IndexingPressureService(settings, clusterService);

        TestAction action = new TestAction(settings, "internal:testAction", transportService, clusterService, shardStateAction, threadPool);

        action.handlePrimaryRequest(
            new TransportReplicationAction.ConcreteShardRequest<>(
                new TestRequest(),
                replicaRouting.allocationId().getId(),
                randomNonNegativeLong(),
                true,
                true
            ),
            createTransportChannel(new PlainActionFuture<>()),
            task
        );

        CommonStatsFlags statsFlag = new CommonStatsFlags();
        statsFlag.includeAllShardIndexingPressureTrackers(true);
        IndexingPressurePerShardStats shardStats = this.indexingPressureService.shardStats(statsFlag)
            .getIndexingPressureShardStats(shardId);

        assertPhase(task, "finished");
        assertTrue(!Objects.isNull(shardStats));
    }

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicBoolean isRelocated = new AtomicBoolean(false);

    private final AtomicBoolean isPrimaryMode = new AtomicBoolean(true);

    /**
     * Sometimes build a ReplicationTask for tracking the phase of the
     * TransportReplicationAction. Since TransportReplicationAction has to work
     * if the task as null just as well as if it is supplied this returns null
     * half the time.
     */
    ReplicationTask maybeTask() {
        return random().nextBoolean() ? new ReplicationTask(0, null, null, null, null, null) : null;
    }

    /**
     * If the task is non-null this asserts that the phrase matches.
     */
    void assertPhase(@Nullable ReplicationTask task, String phase) {
        assertPhase(task, equalTo(phase));
    }

    private void assertPhase(@Nullable ReplicationTask task, Matcher<String> phaseMatcher) {
        if (task != null) {
            assertThat(task.getPhase(), phaseMatcher);
        }
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestRequest, TestResponse> {
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
                TransportWriteActionForIndexingPressureTests.this.indexingPressureService,
                new SystemIndices(emptyMap())
            );
        }

        @Override
        protected TestResponse newResponseInstance(StreamInput in) throws IOException {
            return new TestResponse();
        }

        @Override
        protected long primaryOperationSize(TestRequest request) {
            return 100;
        }

        @Override
        protected long replicaOperationSize(TestRequest request) {
            return 100;
        }

        @Override
        protected void dispatchedShardOperationOnPrimary(
            TestRequest request,
            IndexShard primary,
            ActionListener<PrimaryResult<TestRequest, TestResponse>> listener
        ) {
            ActionListener.completeWith(
                listener,
                () -> new WritePrimaryResult<>(request, new TestResponse(), location, null, primary, logger)
            );
        }

        @Override
        protected void dispatchedShardOperationOnReplica(TestRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
            ActionListener.completeWith(listener, () -> new WriteReplicaResult<>(request, location, null, replica, logger));
        }

    }

    private static class TestRequest extends ReplicatedWriteRequest<TestRequest> {
        TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestRequest() {
            super(new ShardId("test", "_na_", 0));
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

    private IndicesService mockIndicesService(ClusterService clusterService) {
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

    private IndexService mockIndexService(final IndexMetadata indexMetaData, ClusterService clusterService) {
        final IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).then(invocation -> {
            int shard = (Integer) invocation.getArguments()[0];
            final ShardId shardId = new ShardId(indexMetaData.getIndex(), shard);
            if (shard > indexMetaData.getNumberOfShards()) {
                throw new ShardNotFoundException(shardId);
            }
            return mockIndexShard(shardId, clusterService);
        });
        return indexService;
    }

    @SuppressWarnings("unchecked")
    private IndexShard mockIndexShard(ShardId shardId, ClusterService clusterService) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        doAnswer(invocation -> {
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
            if (isPrimaryMode.get()) {
                count.incrementAndGet();
                callback.onResponse(count::decrementAndGet);

            } else {
                callback.onFailure(new ShardNotInPrimaryModeException(shardId, IndexShardState.STARTED));
            }
            return null;
        }).when(indexShard).acquirePrimaryOperationPermit(any(ActionListener.class), anyString(), any());
        doAnswer(invocation -> {
            long term = (Long) invocation.getArguments()[0];
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[3];
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
        when(indexShard.getActiveOperationsCount()).thenAnswer(i -> count.get());

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

        ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
        when(indexShard.getReplicationGroup()).thenReturn(replicationGroup);
        return indexShard;
    }

    /**
     * Transport channel that is needed for testing.
     */
    public TransportChannel createTransportChannel(final PlainActionFuture<TestResponse> listener) {
        return new TransportChannel() {

            @Override
            public String getProfileName() {
                return "";
            }

            @Override
            public void sendResponse(TransportResponse response) {
                listener.onResponse(((TestResponse) response));
            }

            @Override
            public void sendResponse(Exception exception) {
                listener.onFailure(exception);
            }

            @Override
            public String getChannelType() {
                return "replica_test";
            }
        };
    }
}
