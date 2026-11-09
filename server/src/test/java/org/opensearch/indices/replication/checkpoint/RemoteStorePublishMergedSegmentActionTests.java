/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStorePublishMergedSegmentActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;
    private MockLogAppender mockAppender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class));
        mockAppender.start();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        mockAppender.close();
        super.tearDown();
    }

    public void testPublishMergedSegment() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        ShardRouting shardRouting = mock(ShardRouting.class);
        AllocationId allocationId = mock(AllocationId.class);
        RecoveryState recoveryState = mock(RecoveryState.class);
        RecoverySettings recoverySettings = mock(RecoverySettings.class);
        when(recoverySettings.getMergedSegmentReplicationTimeout()).thenReturn(new TimeValue(1000));
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(allocationId.getId()).thenReturn("1");
        when(recoveryState.getTargetNode()).thenReturn(clusterService.localNode());
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.getPendingPrimaryTerm()).thenReturn(1L);
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        when(indexShard.getRecoverySettings()).thenReturn(recoverySettings);
        when(indexShard.store()).thenReturn(mock(Store.class));

        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final MergedSegmentCheckpoint checkpoint = new MergedSegmentCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            "_1"
        );

        action.publish(indexShard, checkpoint);
    }

    public void testPublishMergedSegmentWithNoTimeLeftAfterUpload() {
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "warning about timeout during upload of merged segments to remote",
                RemoteStorePublishMergedSegmentAction.class.getCanonicalName(),
                Level.WARN,
                "Unable to confirm upload of merged segment * to remote store. Timeout of *ms exceeded. Skipping pre-copy."
            )
        );
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        ShardRouting shardRouting = mock(ShardRouting.class);
        AllocationId allocationId = mock(AllocationId.class);
        RecoveryState recoveryState = mock(RecoveryState.class);
        RecoverySettings recoverySettings = mock(RecoverySettings.class);
        when(recoverySettings.getMergedSegmentReplicationTimeout()).thenReturn(TimeValue.MINUS_ONE);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(allocationId.getId()).thenReturn("1");
        when(recoveryState.getTargetNode()).thenReturn(clusterService.localNode());
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.getPendingPrimaryTerm()).thenReturn(1L);
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        when(indexShard.getRecoverySettings()).thenReturn(recoverySettings);
        when(indexShard.store()).thenReturn(mock(Store.class));

        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final MergedSegmentCheckpoint checkpoint = new MergedSegmentCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            "_1"
        );
        try {
            Loggers.addAppender(LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class), mockAppender);
            action.publish(indexShard, checkpoint);
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class), mockAppender);
            mockAppender.close();
        }
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testPublishMergedSegmentActionOnPrimary() throws InterruptedException {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final RemoteStoreMergedSegmentCheckpoint checkpoint = new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(indexShard.shardId(), 1, 1, 1111, Codec.getDefault().getName(), Collections.emptyMap(), "_1"),
            Map.of("_1", "_1__uuid")
        );
        final RemoteStorePublishMergedSegmentRequest request = new RemoteStorePublishMergedSegmentRequest(checkpoint);

        CountDownLatch latch = new CountDownLatch(1);
        action.shardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            // we should forward the request containing the current publish checkpoint to the replica
            assertThat(result.replicaRequest(), sameInstance(request));
            latch.countDown();
        }));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    public void testPublishMergedSegmentActionOnReplica() throws IOException {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);
        final ShardId shardId = new ShardId(index, id);
        final RemoteSegmentStoreDirectory remoteDirectory = new RemoteSegmentStoreDirectory(
            mock(RemoteDirectory.class),
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            threadPool,
            shardId,
            new HashMap<>()
        );
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(
                true,
                Settings.builder()
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT")
                    .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                    .build()
            )
        );
        when(indexShard.getRemoteDirectory()).thenReturn(remoteDirectory);
        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final RemoteStoreMergedSegmentCheckpoint checkpoint = createCheckpoint(indexShard);

        final RemoteStorePublishMergedSegmentRequest request = new RemoteStorePublishMergedSegmentRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.shardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();

        // onNewMergedSegmentCheckpoint should be called on shard with checkpoint request
        verify(mockTargetService, times(1)).onNewMergedSegmentCheckpoint(checkpoint, indexShard);

        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());

    }

    public void testPublishMergedSegmentActionOnReplicaWithMismatchedShardId() throws IOException, IllegalAccessException {
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "warning about shard id mismatch",
                RemoteStorePublishMergedSegmentAction.class.getCanonicalName(),
                Level.WARN,
                "Received merged segment checkpoint for shard [test-index][5] on replica shard [index]*, ignoring checkpoint"
            )
        );
        final IndicesService indicesService = mock(IndicesService.class);
        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);
        final ShardId shardId = new ShardId(index, id);
        final RemoteSegmentStoreDirectory remoteDirectory = new RemoteSegmentStoreDirectory(
            mock(RemoteDirectory.class),
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            threadPool,
            shardId,
            new HashMap<>()
        );
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(
                true,
                Settings.builder()
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT")
                    .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                    .build()
            )
        );
        when(indexShard.getRemoteDirectory()).thenReturn(remoteDirectory);
        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final RemoteStoreMergedSegmentCheckpoint checkpoint = new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(
                new ShardId(new Index("test-index", "uuid"), 5),
                1,
                1,
                1111,
                Codec.getDefault().getName(),
                Collections.emptyMap(),
                "_1"
            ),
            Map.of("_1", "_1__uuid")
        );

        final RemoteStorePublishMergedSegmentRequest request = new RemoteStorePublishMergedSegmentRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        try {
            Loggers.addAppender(LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class), mockAppender);
            action.shardOperationOnReplica(request, indexShard, listener);
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class), mockAppender);
            mockAppender.close();
        }

        final TransportReplicationAction.ReplicaResult result = listener.actionGet();

        // onNewMergedSegmentCheckpoint should be NOT called on shard with checkpoint request
        verify(mockTargetService, times(0)).onNewMergedSegmentCheckpoint(checkpoint, indexShard);

        // we do not expect a failure
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());

    }

    public void testPublishMergedSegmentActionOnDocrepReplicaDuringMigration() throws IOException {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        final SegmentReplicationTargetService mockTargetService = mock(SegmentReplicationTargetService.class);

        final RemoteStorePublishMergedSegmentAction action = new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mockTargetService
        );

        final RemoteStoreMergedSegmentCheckpoint checkpoint = createCheckpoint(indexShard);

        final RemoteStorePublishMergedSegmentRequest request = new RemoteStorePublishMergedSegmentRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.shardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();
        // no interaction with SegmentReplicationTargetService object
        verify(mockTargetService, never()).onNewMergedSegmentCheckpoint(any(), any());
        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());
    }

    public void testGetReplicationModeWithRemoteTranslog() {
        final RemoteStorePublishMergedSegmentAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.mergedSegmentTransferTracker()).thenReturn(new MergedSegmentTransferTracker());
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    private RemoteStorePublishMergedSegmentAction createAction() {
        return new RemoteStorePublishMergedSegmentAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            mock(IndicesService.class),
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet()),
            mock(SegmentReplicationTargetService.class)
        );
    }

    private RemoteStoreMergedSegmentCheckpoint createCheckpoint(IndexShard indexShard) {
        return new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(indexShard.shardId(), 1, 1, 1111, Codec.getDefault().getName(), Collections.emptyMap(), "_1"),
            Map.of("_1", "_1__uuid")
        );
    }
}
