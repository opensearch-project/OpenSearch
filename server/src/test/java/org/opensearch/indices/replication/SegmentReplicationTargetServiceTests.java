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
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.eq;
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName())
            .build();
        primaryShard = newStartedShard(true, settings);
        replicaShard = newShard(false, settings, new NRTReplicationEngineFactory());
        recoverReplica(replicaShard, primaryShard, true, getReplicationFunc(replicaShard));
        checkpoint = new ReplicationCheckpoint(replicaShard.shardId(), 0L, 0L, 0L);
        SegmentReplicationSourceFactory replicationSourceFactory = mock(SegmentReplicationSourceFactory.class);
        replicationSource = mock(SegmentReplicationSource.class);
        when(replicationSourceFactory.get(replicaShard)).thenReturn(replicationSource);

        sut = prepareForReplication(primaryShard, null);
        initialCheckpoint = replicaShard.getLatestReplicationCheckpoint();
        aheadCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm(),
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1
        );
        newPrimaryCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm() + 1,
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1
        );
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primaryShard, replicaShard);
        super.tearDown();
    }

    public void testsSuccessfulReplication_listenerCompletes() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        sut.startReplication(checkpoint, replicaShard, new SegmentReplicationTargetService.SegmentReplicationListener() {
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
                Store store,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                Assert.fail("Should not be called");
            }
        };
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
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
        verify(spy, times(0)).startReplication(any(), any(), any());
    }

    public void testShardAlreadyReplicating() throws InterruptedException {
        // Create a spy of Target Service so that we can verify invocation of startReplication call with specific checkpoint on it.
        SegmentReplicationTargetService serviceSpy = spy(sut);
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            initialCheckpoint,
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
        verify(serviceSpy, times(0)).startReplication(eq(aheadCheckpoint), eq(replicaShard), any());
    }

    public void testOnNewCheckpointFromNewPrimaryCancelOngoingReplication() throws IOException, InterruptedException {
        // Create a spy of Target Service so that we can verify invocation of startReplication call with specific checkpoint on it.
        SegmentReplicationTargetService serviceSpy = spy(sut);
        // Create a Mockito spy of target to stub response of few method calls.
        final SegmentReplicationTarget targetSpy = spy(
            new SegmentReplicationTarget(
                initialCheckpoint,
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
            doReturn(null).when(serviceSpy).startReplication(eq(newPrimaryCheckpoint), eq(replicaShard), any());
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
        verify(serviceSpy, times(1)).startReplication(eq(newPrimaryCheckpoint), eq(replicaShard), any());
    }

    public void testNewCheckpointBehindCurrentCheckpoint() {
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(checkpoint, replicaShard);
        verify(spy, times(0)).startReplication(any(), any(), any());
    }

    public void testShardNotStarted() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);
        IndexShard shard = newShard(false);
        spy.onNewCheckpoint(checkpoint, shard);
        verify(spy, times(0)).startReplication(any(), any(), any());
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
        verify(spy, times(0)).startReplication(any(), any(), any());
        closeShards(primaryShard);
    }
}
