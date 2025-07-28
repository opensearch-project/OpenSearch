/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.lucene.codecs.Codec;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PublishReferencedSegmentsActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

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
    }

    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testPublishReferencedSegments() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
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

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );

        action.publish(indexShard, checkpoint);
    }

    public void testPublishReferencedSegmentsOnPrimary() throws Exception {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );
        final PublishReferencedSegmentsRequest request = new PublishReferencedSegmentsRequest(checkpoint);
        final CountDownLatch latch = new CountDownLatch(1);
        action.shardOperationOnPrimary(request, indexShard, new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
            // we should forward the request containing the current publish checkpoint to the replica
            assertThat(result.replicaRequest(), sameInstance(request));
        }), latch));
        latch.await();
    }

    public void testPublishReferencedSegmentsActionOnReplica() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(false, Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "SEGMENT").build())
        );

        final PublishReferencedSegmentsAction action = new PublishReferencedSegmentsAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );

        final ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            indexShard.shardId(),
            1,
            1,
            1111,
            Codec.getDefault().getName(),
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );

        final PublishReferencedSegmentsRequest request = new PublishReferencedSegmentsRequest(checkpoint);

        final PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.shardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();

        // cleanupRedundantPendingMergeSegment should be called on replica shard
        verify(indexShard, times(1)).cleanupRedundantPendingMergeSegment(checkpoint);

        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());
    }
}
