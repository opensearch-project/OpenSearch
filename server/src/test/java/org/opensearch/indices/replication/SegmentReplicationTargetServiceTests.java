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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SegmentReplicationTargetServiceTests extends IndexShardTestCase {

    public void testTargetReturnsSuccess_listenerCompletes() throws IOException {
        Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final TransportService transportService = mock(TransportService.class);
        final IndexShard indexShard = newShard(false, settings);
        ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        SegmentReplicationSource replicationSource = mock(SegmentReplicationSource.class);

        SegmentReplicationTargetService sut = new SegmentReplicationTargetService(
            threadPool,
            recoverySettings,
            transportService,
            replicationSource
        );

        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {}

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    Assert.fail();
                }
            }
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        doAnswer(invocation -> {
            spy.markAsDone();
            return null;
        }).when(spy).startReplication();
        sut.startReplication(spy);
        closeShards(indexShard);
    }

    public void testTargetThrowsException() throws IOException {
        Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final TransportService transportService = mock(TransportService.class);
        final IndexShard indexShard = newShard(false, settings);
        ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        SegmentReplicationSource replicationSource = mock(SegmentReplicationSource.class);

        SegmentReplicationTargetService sut = new SegmentReplicationTargetService(
            threadPool,
            recoverySettings,
            transportService,
            replicationSource
        );

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
                    assertTrue(sendShardFailure);
                }
            }
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        doAnswer(invocation -> {
            spy.fail(new OpenSearchException("Test fail"), true);
            return null;
        }).when(spy).startReplication();
        sut.startReplication(spy);
        closeShards(indexShard);
    }

    public void testBeforeIndexShardClosed_CancelsOngoingReplications() throws IOException {
        Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final TransportService transportService = mock(TransportService.class);
        final IndexShard indexShard = newShard(false, settings);
        ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        SegmentReplicationSource replicationSource = mock(SegmentReplicationSource.class);

        SegmentReplicationTargetService sut = new SegmentReplicationTargetService(
            threadPool,
            recoverySettings,
            transportService,
            replicationSource
        );

        final SegmentReplicationTarget target = new SegmentReplicationTarget(
            checkpoint,
            indexShard,
            replicationSource,
            mock(SegmentReplicationTargetService.SegmentReplicationListener.class)
        );
        final SegmentReplicationTarget spy = Mockito.spy(target);
        sut.startReplication(spy);
        sut.beforeIndexShardClosed(indexShard.shardId(), indexShard, settings);
        Mockito.verify(spy, times(1)).cancel(any());
        closeShards(indexShard);
    }
}
