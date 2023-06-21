/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.ForceSyncRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.test.transport.CapturingTransport;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.indices.replication.SegmentReplicationState.Stage.CANCELLED;

public class SegmentReplicationTargetServiceTests extends IndexShardTestCase {

    private IndexShard replicaShard;
    private IndexShard primaryShard;
    private ReplicationCheckpoint checkpoint;
    private SegmentReplicationSource replicationSource;
    private SegmentReplicationTargetService sut;

    private ReplicationCheckpoint initialCheckpoint;
    private ReplicationCheckpoint aheadCheckpoint;

    private ReplicationCheckpoint newPrimaryCheckpoint;

    private TransportService transportService;
    private TestThreadPool testThreadPool;
    private DiscoveryNode localNode;

    private IndicesService indicesService;
    private ClusterService clusterService;

    private SegmentReplicationState state;

    private static long TRANSPORT_TIMEOUT = 30000;// 30sec

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName())
            .build();
        primaryShard = newStartedShard(true, settings);
        String primaryCodec = primaryShard.getLatestReplicationCheckpoint().getCodec();
        replicaShard = newShard(false, settings, new NRTReplicationEngineFactory());
        recoverReplica(replicaShard, primaryShard, true, getReplicationFunc(replicaShard));
        checkpoint = new ReplicationCheckpoint(
            replicaShard.shardId(),
            0L,
            0L,
            0L,
            replicaShard.getLatestReplicationCheckpoint().getCodec()
        );
        SegmentReplicationSourceFactory replicationSourceFactory = mock(SegmentReplicationSourceFactory.class);
        replicationSource = mock(SegmentReplicationSource.class);
        when(replicationSourceFactory.get(replicaShard)).thenReturn(replicationSource);

        testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        CapturingTransport transport = new CapturingTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            testThreadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> localNode,
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        indicesService = mock(IndicesService.class);
        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.routingTable()).thenReturn(mockRoutingTable);
        when(mockRoutingTable.shardRoutingTable(any())).thenReturn(primaryShard.getReplicationGroup().getRoutingTable());

        when(clusterState.nodes()).thenReturn(DiscoveryNodes.builder().add(localNode).build());
        sut = prepareForReplication(primaryShard, replicaShard, transportService, indicesService, clusterService);
        initialCheckpoint = replicaShard.getLatestReplicationCheckpoint();
        aheadCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm(),
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1,
            primaryCodec
        );
        newPrimaryCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm() + 1,
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1,
            primaryCodec
        );

        state = new SegmentReplicationState(
            replicaShard.routingEntry(),
            new ReplicationLuceneIndex(),
            0L,
            "",
            new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT)
        );
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primaryShard, replicaShard);
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        testThreadPool = null;
        super.tearDown();
    }

    public void testsSuccessfulReplication_listenerCompletes() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        sut.startReplication(replicaShard, new SegmentReplicationTargetService.SegmentReplicationListener() {
            @Override
            public void onReplicationDone(SegmentReplicationState state) {
                assertEquals(SegmentReplicationState.Stage.DONE, state.getStage());
                latch.countDown();
            }

            @Override
            public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                logger.error("Unexpected error", e);
                Assert.fail("Test should succeed");
            }
        });
        latch.await(2, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    public void testReplicationFails() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final OpenSearchException expectedError = new OpenSearchException("Fail");
        SegmentReplicationSource source = new TestReplicationSource() {

            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                listener.onFailure(expectedError);
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                Assert.fail("Should not be called");
            }
        };
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            replicaShard,
            source,
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    Assert.fail();
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    // failures leave state object in last entered stage.
                    assertEquals(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO, state.getStage());
                    assertEquals(expectedError, e.getCause());
                    latch.countDown();
                }
            }
        );
        sut.startReplication(target);
        latch.await(2, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    public void testAlreadyOnNewCheckpoint() {
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(replicaShard.getLatestReplicationCheckpoint(), replicaShard);
        verify(spy, times(0)).startReplication(any(), any());
    }

    public void testShardAlreadyReplicating() throws InterruptedException {
        // Create a spy of Target Service so that we can verify invocation of startReplication call with specific checkpoint on it.
        SegmentReplicationTargetService serviceSpy = spy(sut);
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            replicaShard,
            replicationSource,
            mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
        );
        // Create a Mockito spy of target to stub response of few method calls.
        final SegmentReplicationTarget targetSpy = Mockito.spy(target);
        CountDownLatch latch = new CountDownLatch(1);
        // Mocking response when startReplication is called on targetSpy we send a new checkpoint to serviceSpy and later reduce countdown
        // of latch.
        doAnswer(invocation -> {
            final ActionListener<Void> listener = invocation.getArgument(0);
            // a new checkpoint arrives before we've completed.
            serviceSpy.onNewCheckpoint(aheadCheckpoint, replicaShard);
            listener.onResponse(null);
            latch.countDown();
            return null;
        }).when(targetSpy).startReplication(any());
        doNothing().when(targetSpy).onDone();

        // start replication of this shard the first time.
        serviceSpy.startReplication(targetSpy);

        // wait for the new checkpoint to arrive, before the listener completes.
        latch.await(30, TimeUnit.SECONDS);
        verify(targetSpy, times(0)).cancel(any());
        verify(serviceSpy, times(0)).startReplication(eq(replicaShard), any());
    }

    public void testOnNewCheckpointFromNewPrimaryCancelOngoingReplication() throws IOException, InterruptedException {
        // Create a spy of Target Service so that we can verify invocation of startReplication call with specific checkpoint on it.
        SegmentReplicationTargetService serviceSpy = spy(sut);
        // Create a Mockito spy of target to stub response of few method calls.
        final SegmentReplicationTarget targetSpy = spy(
            new SegmentReplicationTarget(
                replicaShard,
                replicationSource,
                mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
            )
        );

        CountDownLatch latch = new CountDownLatch(1);
        // Mocking response when startReplication is called on targetSpy we send a new checkpoint to serviceSpy and later reduce countdown
        // of latch.
        doAnswer(invocation -> {
            // short circuit loop on new checkpoint request
            doReturn(null).when(serviceSpy).startReplication(eq(replicaShard), any());
            // a new checkpoint arrives before we've completed.
            serviceSpy.onNewCheckpoint(newPrimaryCheckpoint, replicaShard);
            try {
                invocation.callRealMethod();
            } catch (CancellableThreads.ExecutionCancelledException e) {
                latch.countDown();
            }
            return null;
        }).when(targetSpy).startReplication(any());

        // start replication. This adds the target to on-ongoing replication collection
        serviceSpy.startReplication(targetSpy);
        latch.await();
        // wait for the new checkpoint to arrive, before the listener completes.
        assertEquals(CANCELLED, targetSpy.state().getStage());
        verify(targetSpy, times(1)).cancel("Cancelling stuck target after new primary");
        verify(serviceSpy, times(1)).startReplication(eq(replicaShard), any());
    }

    public void testNewCheckpointBehindCurrentCheckpoint() {
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(checkpoint, replicaShard);
        verify(spy, times(0)).startReplication(any(), any());
    }

    public void testShardNotStarted() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);
        IndexShard shard = newShard(false);
        spy.onNewCheckpoint(checkpoint, shard);
        verify(spy, times(0)).startReplication(any(), any());
        closeShards(shard);
    }

    /**
     * here we are starting a new shard in PrimaryMode and testing that we don't process a checkpoint on shard when it is in PrimaryMode.
     */
    public void testRejectCheckpointOnShardPrimaryMode() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);

        // Starting a new shard in PrimaryMode.
        IndexShard primaryShard = newStartedShard(true);
        IndexShard spyShard = spy(primaryShard);
        spy.onNewCheckpoint(aheadCheckpoint, spyShard);

        // Verify that checkpoint is not processed as shard is in PrimaryMode.
        verify(spy, times(0)).startReplication(any(), any());
        closeShards(primaryShard);
    }

    public void testAfterIndexShardStartedDoesNothingForDocrepIndex() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);
        final IndexShard indexShard = newStartedShard();
        spy.afterIndexShardStarted(indexShard);
        verify(spy, times(0)).processLatestReceivedCheckpoint(eq(replicaShard), any());
        closeShards(indexShard);
    }

    public void testAfterIndexShardStartedProcessesLatestReceivedCheckpoint() {
        sut.updateLatestReceivedCheckpoint(aheadCheckpoint, replicaShard);
        SegmentReplicationTargetService spy = spy(sut);
        doNothing().when(spy).onNewCheckpoint(any(), any());
        spy.afterIndexShardStarted(replicaShard);
        verify(spy, times(1)).processLatestReceivedCheckpoint(eq(replicaShard), any());
    }

    public void testStartReplicationListenerSuccess() throws InterruptedException {
        sut.updateLatestReceivedCheckpoint(aheadCheckpoint, replicaShard);
        SegmentReplicationTargetService spy = spy(sut);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(i -> {
            ((SegmentReplicationTargetService.SegmentReplicationListener) i.getArgument(1)).onReplicationDone(state);
            latch.countDown();
            return null;
        }).when(spy).startReplication(any(), any());
        doNothing().when(spy).updateVisibleCheckpoint(eq(0L), any());
        spy.afterIndexShardStarted(replicaShard);

        latch.await(2, TimeUnit.SECONDS);
        verify(spy, (atLeastOnce())).updateVisibleCheckpoint(eq(0L), eq(replicaShard));
    }

    public void testStartReplicationListenerFailure() throws InterruptedException {
        sut.updateLatestReceivedCheckpoint(aheadCheckpoint, replicaShard);
        SegmentReplicationTargetService spy = spy(sut);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(i -> {
            ((SegmentReplicationTargetService.SegmentReplicationListener) i.getArgument(1)).onReplicationFailure(
                state,
                new ReplicationFailedException(replicaShard, null),
                false
            );
            latch.countDown();
            return null;
        }).when(spy).startReplication(any(), any());
        doNothing().when(spy).updateVisibleCheckpoint(eq(0L), any());
        spy.afterIndexShardStarted(replicaShard);

        latch.await(2, TimeUnit.SECONDS);
        verify(spy, (never())).updateVisibleCheckpoint(eq(0L), eq(replicaShard));
    }

    public void testDoNotProcessLatestCheckpointIfItIsbehind() {
        sut.updateLatestReceivedCheckpoint(replicaShard.getLatestReplicationCheckpoint(), replicaShard);
        assertFalse(sut.processLatestReceivedCheckpoint(replicaShard, null));
    }

    public void testOnNewCheckpointInvokedOnClosedShardDoesNothing() throws IOException {
        closeShards(replicaShard);
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(aheadCheckpoint, replicaShard);
        verify(spy, times(0)).updateLatestReceivedCheckpoint(any(), any());
    }

    public void testBeforeIndexShardClosed_DoesNothingForDocRepIndex() throws IOException {
        final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final ReplicationCollection<SegmentReplicationTarget> ongoingReplications = mock(ReplicationCollection.class);
        final SegmentReplicationTargetService targetService = new SegmentReplicationTargetService(
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            sourceFactory,
            indicesService,
            clusterService,
            ongoingReplications
        );
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
        IndexShard shard = newStartedShard(false, settings);
        targetService.beforeIndexShardClosed(shard.shardId(), shard, shard.indexSettings().getSettings());
        verifyNoInteractions(ongoingReplications);
        closeShards(shard);
    }

    public void testShardRoutingChanged_DoesNothingForDocRepIndex() throws IOException {
        final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final ReplicationCollection<SegmentReplicationTarget> ongoingReplications = mock(ReplicationCollection.class);
        final SegmentReplicationTargetService targetService = new SegmentReplicationTargetService(
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            sourceFactory,
            indicesService,
            clusterService,
            ongoingReplications
        );
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
        IndexShard shard = newStartedShard(false, settings);

        targetService.shardRoutingChanged(shard, shard.routingEntry(), primaryShard.routingEntry());
        verifyNoInteractions(ongoingReplications);
        closeShards(shard);
    }

    public void testUpdateLatestReceivedCheckpoint() {
        final SegmentReplicationTargetService spy = spy(sut);
        sut.updateLatestReceivedCheckpoint(checkpoint, replicaShard);
        sut.updateLatestReceivedCheckpoint(aheadCheckpoint, replicaShard);
        assertEquals(sut.latestReceivedCheckpoint.get(replicaShard.shardId()), aheadCheckpoint);
    }

    public void testForceSegmentSyncHandler() throws Exception {
        ForceSyncRequest forceSyncRequest = new ForceSyncRequest(1L, 1L, replicaShard.shardId());
        when(indicesService.getShardOrNull(forceSyncRequest.getShardId())).thenReturn(replicaShard);
        TransportResponse response = transportService.submitRequest(
            localNode,
            SegmentReplicationTargetService.Actions.FORCE_SYNC,
            forceSyncRequest,
            TransportRequestOptions.builder().withTimeout(TRANSPORT_TIMEOUT).build(),
            EmptyTransportResponseHandler.INSTANCE_SAME
        ).txGet();
        assertEquals(TransportResponse.Empty.INSTANCE, response);
    }

    public void testForceSegmentSyncHandlerWithFailure() throws Exception {
        IndexShard spyReplicaShard = spy(replicaShard);
        ForceSyncRequest forceSyncRequest = new ForceSyncRequest(1L, 1L, replicaShard.shardId());
        when(indicesService.getShardOrNull(forceSyncRequest.getShardId())).thenReturn(spyReplicaShard);
        IOException exception = new IOException("dummy failure");
        doThrow(exception).when(spyReplicaShard).finalizeReplication(any());

        // prevent shard failure to avoid test setup assertion
        doNothing().when(spyReplicaShard).failShard(eq("replication failure"), any());
        Exception finalizeException = expectThrows(Exception.class, () -> {
            transportService.submitRequest(
                localNode,
                SegmentReplicationTargetService.Actions.FORCE_SYNC,
                forceSyncRequest,
                TransportRequestOptions.builder().withTimeout(TRANSPORT_TIMEOUT).build(),
                EmptyTransportResponseHandler.INSTANCE_SAME
            ).txGet();
        });
        Throwable nestedException = finalizeException.getCause().getCause();
        assertTrue(nestedException instanceof IOException);
        assertTrue(nestedException.getMessage().contains("dummy failure"));
    }
}
