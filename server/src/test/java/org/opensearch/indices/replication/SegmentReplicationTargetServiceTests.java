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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SegmentReplicationTargetServiceTests extends IndexShardTestCase {

    private IndexShard indexShard;
    private ReplicationCheckpoint checkpoint;
    private SegmentReplicationSource replicationSource;
    private SegmentReplicationTargetService sut;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final TransportService transportService = mock(TransportService.class);
        indexShard = newStartedShard(false, settings);
        checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        SegmentReplicationSourceFactory replicationSourceFactory = mock(SegmentReplicationSourceFactory.class);
        replicationSource = mock(SegmentReplicationSource.class);
        when(replicationSourceFactory.get(indexShard)).thenReturn(replicationSource);

        sut = new SegmentReplicationTargetService(threadPool, recoverySettings, transportService, replicationSourceFactory);
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
            // setting stage to REPLICATING so transition in markAsDone succeeds on listener completion
            target.state().setStage(SegmentReplicationState.Stage.REPLICATING);
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
                    assertEquals(SegmentReplicationState.Stage.REPLICATING, state.getStage());
                    assertEquals(expectedError, e.getCause());
                    assertTrue(sendShardFailure);
                }
            }
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        doAnswer(invocation -> {
            // setting stage to REPLICATING so transition in markAsDone succeeds on listener completion
            target.state().setStage(SegmentReplicationState.Stage.REPLICATING);
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

    public void testShardAlreadyReplicating() {
        SegmentReplicationTargetService spy = spy(sut);
        // Create a separate target and start it so the shard is already replicating.
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
        );
        final SegmentReplicationTarget spyTarget = Mockito.spy(target);
        spy.startReplication(spyTarget);

        // a new checkpoint comes in for the same IndexShard.
        spy.onNewCheckpoint(checkpoint, indexShard);
        verify(spy, times(0)).startReplication(any(), any(), any());
        spyTarget.markAsDone();
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
        ReplicationCheckpoint cp = indexShard.getLatestReplicationCheckpoint();
        ReplicationCheckpoint newCheckpoint = new ReplicationCheckpoint(
            cp.getShardId(),
            cp.getPrimaryTerm(),
            cp.getSegmentsGen(),
            cp.getSeqNo(),
            cp.getSegmentInfosVersion() + 1
        );
        ArgumentCaptor<SegmentReplicationTargetService.SegmentReplicationListener> captor = ArgumentCaptor.forClass(
            SegmentReplicationTargetService.SegmentReplicationListener.class
        );
        doNothing().when(spy).startReplication(any(), any(), any());
        spy.onNewCheckpoint(newCheckpoint, spyShard);
        verify(spy, times(1)).startReplication(any(), any(), captor.capture());
        SegmentReplicationTargetService.SegmentReplicationListener listener = captor.getValue();
        listener.onFailure(new SegmentReplicationState(new ReplicationLuceneIndex()), new OpenSearchException("testing"), true);
        verify(spyShard).failShard(any(), any());
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
}
