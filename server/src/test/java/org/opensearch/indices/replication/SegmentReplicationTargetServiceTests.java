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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

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
        indexShard = newShard(false, settings);
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

    public void testTargetReturnsSuccess_listenerCompletes() throws IOException {
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
            final ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(spy).startReplication(any());
        sut.startReplication(spy);
        closeShards(indexShard);
    }

    public void testTargetThrowsException() throws IOException {
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
        closeShards(indexShard);
    }

    public void testBeforeIndexShardClosed_CancelsOngoingReplications() throws IOException {
        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        sut.startReplication(spy);
        sut.beforeIndexShardClosed(indexShard.shardId(), indexShard, Settings.EMPTY);
        Mockito.verify(spy, times(1)).cancel(any());
        closeShards(indexShard);
    }
}
