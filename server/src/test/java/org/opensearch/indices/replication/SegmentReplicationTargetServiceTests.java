/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.eq;

public class SegmentReplicationTargetServiceTests extends IndexShardTestCase {

    private IndexShard indexShard;
    private ReplicationCheckpoint checkpoint;
    private SegmentReplicationSource replicationSource;
    private SegmentReplicationTargetService sut;

    private ReplicationCheckpoint initialCheckpoint;
    private ReplicationCheckpoint aheadCheckpoint;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
            .put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName())
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final TransportService transportService = mock(TransportService.class);
        indexShard = newStartedShard(false, settings);
        checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        SegmentReplicationSourceFactory replicationSourceFactory = mock(SegmentReplicationSourceFactory.class);
        replicationSource = mock(SegmentReplicationSource.class);
        when(replicationSourceFactory.get(indexShard)).thenReturn(replicationSource);

        sut = new SegmentReplicationTargetService(threadPool, recoverySettings, transportService, replicationSourceFactory);
        initialCheckpoint = indexShard.getLatestReplicationCheckpoint();
        aheadCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm(),
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSeqNo(),
            initialCheckpoint.getSegmentInfosVersion() + 1
        );
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(indexShard);
        super.tearDown();
    }

    public void testTargetReturnsSuccess_listenerCompletes() {
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    assertEquals(SegmentReplicationState.Stage.DONE, state.getStage());
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    Assert.fail();
                }
            }
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        doAnswer(invocation -> {
            // set up stage correctly so the transition in markAsDone succeeds on listener completion
            moveTargetToFinalStage(target);
            final ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(spy).startReplication(any());
        sut.startReplication(spy);
    }

    public void testTargetThrowsException() {
        final OpenSearchException expectedError = new OpenSearchException("Fail");
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    Assert.fail();
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    assertEquals(SegmentReplicationState.Stage.INIT, state.getStage());
                    assertEquals(expectedError, e.getCause());
                    assertTrue(sendShardFailure);
                }
            }
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        doAnswer(invocation -> {
            final ActionListener<Void> listener = invocation.getArgument(0);
            listener.onFailure(expectedError);
            return null;
        }).when(spy).startReplication(any());
        sut.startReplication(spy);
    }

    public void testAlreadyOnNewCheckpoint() {
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(indexShard.getLatestReplicationCheckpoint(), indexShard);
        verify(spy, times(0)).startReplication(any(), any(), any());
    }

    public void testShardAlreadyReplicating() throws InterruptedException {
        // Create a spy of Target Service so that we can verify invocation of startReplication call with specific checkpoint on it.
        SegmentReplicationTargetService serviceSpy = spy(sut);
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
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
            serviceSpy.onNewCheckpoint(aheadCheckpoint, indexShard);
            listener.onResponse(null);
            latch.countDown();
            return null;
        }).when(targetSpy).startReplication(any());
        doNothing().when(targetSpy).onDone();

        // start replication of this shard the first time.
        serviceSpy.startReplication(targetSpy);

        // wait for the new checkpoint to arrive, before the listener completes.
        latch.await(30, TimeUnit.SECONDS);
        verify(serviceSpy, times(0)).startReplication(eq(aheadCheckpoint), eq(indexShard), any());
    }

    public void testNewCheckpointBehindCurrentCheckpoint() {
        SegmentReplicationTargetService spy = spy(sut);
        spy.onNewCheckpoint(checkpoint, indexShard);
        verify(spy, times(0)).startReplication(any(), any(), any());
    }

    public void testShardNotStarted() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);
        IndexShard shard = newShard(false);
        spy.onNewCheckpoint(checkpoint, shard);
        verify(spy, times(0)).startReplication(any(), any(), any());
        closeShards(shard);
    }

    public void testNewCheckpoint_validationPassesAndReplicationFails() throws IOException {
        allowShardFailures();
        SegmentReplicationTargetService spy = spy(sut);
        IndexShard spyShard = spy(indexShard);
        ArgumentCaptor<SegmentReplicationTargetService.SegmentReplicationListener> captor = ArgumentCaptor.forClass(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        doNothing().when(spy).startReplication(any(), any(), any());
        spy.onNewCheckpoint(aheadCheckpoint, spyShard);
        verify(spy, times(1)).startReplication(any(), any(), captor.capture());
        SegmentReplicationTargetService.SegmentReplicationListener listener = captor.getValue();
        listener.onFailure(new SegmentReplicationState(new ReplicationLuceneIndex()), new OpenSearchException("testing"), true);
        verify(spyShard).failShard(any(), any());
        closeShard(indexShard, false);
    }

    /**
     * here we are starting a new shard in PrimaryMode and testing that we don't process a checkpoint on shard when it is in PrimaryMode.
     */
    public void testRejectCheckpointOnShardPrimaryMode() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);

        // Starting a new shard in PrimaryMode.
        IndexShard primaryShard = newStartedShard(true);
        IndexShard spyShard = spy(primaryShard);
        doNothing().when(spy).startReplication(any(), any(), any());
        spy.onNewCheckpoint(aheadCheckpoint, spyShard);

        // Verify that checkpoint is not processed as shard is in PrimaryMode.
        verify(spy, times(0)).startReplication(any(), any(), any());
        closeShards(primaryShard);
    }

    public void testReplicationOnDone() throws IOException {
        SegmentReplicationTargetService spy = spy(sut);
        IndexShard spyShard = spy(indexShard);
        ReplicationCheckpoint cp = indexShard.getLatestReplicationCheckpoint();
        ReplicationCheckpoint newCheckpoint = new ReplicationCheckpoint(
            cp.getShardId(),
            cp.getPrimaryTerm(),
            cp.getSegmentsGen(),
            cp.getSeqNo(),
            cp.getSegmentInfosVersion() + 1
        );
        ReplicationCheckpoint anotherNewCheckpoint = new ReplicationCheckpoint(
            cp.getShardId(),
            cp.getPrimaryTerm(),
            cp.getSegmentsGen(),
            cp.getSeqNo(),
            cp.getSegmentInfosVersion() + 2
        );
        ArgumentCaptor<SegmentReplicationTargetService.SegmentReplicationListener> captor = ArgumentCaptor.forClass(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        doNothing().when(spy).startReplication(any(), any(), any());
        spy.onNewCheckpoint(newCheckpoint, spyShard);
        spy.onNewCheckpoint(anotherNewCheckpoint, spyShard);
        verify(spy, times(1)).startReplication(eq(newCheckpoint), any(), captor.capture());
        verify(spy, times(1)).onNewCheckpoint(eq(anotherNewCheckpoint), any());
        SegmentReplicationTargetService.SegmentReplicationListener listener = captor.getValue();
        listener.onDone(new SegmentReplicationState(new ReplicationLuceneIndex()));
        doNothing().when(spy).onNewCheckpoint(any(), any());
        verify(spy, timeout(0).times(2)).onNewCheckpoint(eq(anotherNewCheckpoint), any());
        closeShard(indexShard, false);

    }

    public void testBeforeIndexShardClosed_CancelsOngoingReplications() {
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        sut.startReplication(spy);
        sut.beforeIndexShardClosed(indexShard.shardId(), indexShard, Settings.EMPTY);
        verify(spy, times(1)).cancel(any());
    }

    /**
     * Move the {@link SegmentReplicationTarget} object through its {@link SegmentReplicationState.Stage} values in order
     * until the final, non-terminal stage.
     */
    private void moveTargetToFinalStage(SegmentReplicationTarget target) {
        SegmentReplicationState.Stage[] stageValues = SegmentReplicationState.Stage.values();
        assertEquals(target.state().getStage(), SegmentReplicationState.Stage.INIT);
        // Skip the first two stages (DONE and INIT) and iterate until the last value
        for (int i = 2; i < stageValues.length; i++) {
            target.state().setStage(stageValues[i]);
        }
    }
}
