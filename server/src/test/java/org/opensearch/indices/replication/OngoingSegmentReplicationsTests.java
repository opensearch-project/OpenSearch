/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.junit.Assert;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardNotStartedException;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class OngoingSegmentReplicationsTests extends IndexShardTestCase {

    private final IndicesService mockIndicesService = mock(IndicesService.class);
    private ReplicationCheckpoint testCheckpoint;
    private DiscoveryNode primaryDiscoveryNode;
    private DiscoveryNode replicaDiscoveryNode;
    private IndexShard primary;
    private IndexShard replica;

    private GetSegmentFilesRequest getSegmentFilesRequest;

    final Settings settings = Settings.builder()
        .put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName())
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();
    final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primary = newStartedShard(true, settings);
        replica = newShard(false, settings, new NRTReplicationEngineFactory());
        recoverReplica(replica, primary, true);
        replicaDiscoveryNode = replica.recoveryState().getTargetNode();
        primaryDiscoveryNode = replica.recoveryState().getSourceNode();

        ShardId testShardId = primary.shardId();

        // This mirrors the creation of the ReplicationCheckpoint inside CopyState
        testCheckpoint = new ReplicationCheckpoint(
            testShardId,
            primary.getOperationPrimaryTerm(),
            0L,
            primary.getProcessedLocalCheckpoint(),
            0L
        );
        IndexService mockIndexService = mock(IndexService.class);
        when(mockIndicesService.indexService(testShardId.getIndex())).thenReturn(mockIndexService);
        when(mockIndexService.getShard(testShardId.id())).thenReturn(primary);

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(primary, replica);
        super.tearDown();
    }

    public void testPrepareAndSendSegments() throws IOException {
        indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        primary.refresh("Test");
        OngoingSegmentReplications replications = spy(new OngoingSegmentReplications(mockIndicesService, recoverySettings));
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        final FileChunkWriter segmentSegmentFileChunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> {
            listener.onResponse(null);
        };
        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(1, replications.size());
        assertEquals(2, copyState.refCount());

        getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            new ArrayList<>(copyState.getMetadataMap().values()),
            testCheckpoint
        );

        replications.startSegmentCopy(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                assertEquals(copyState.getMetadataMap().size(), getSegmentFilesResponse.files.size());
                assertEquals(1, copyState.refCount());
                assertEquals(0, replications.size());
                replications.cancel(copyState.getShard(), "Closing");
                assertEquals(0, copyState.refCount());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected failure", e);
                Assert.fail("Unexpected failure from startSegmentCopy listener: " + e);
            }
        });
    }

    public void testCancelReplication() throws IOException {
        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            primaryDiscoveryNode,
            testCheckpoint
        );
        final FileChunkWriter segmentSegmentFileChunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> {
            // this shouldn't be called in this test.
            Assert.fail();
        };
        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(1, replications.size());

        replications.cancelReplication(primaryDiscoveryNode);
        assertEquals(1, copyState.refCount());
        assertEquals(0, replications.size());
        replications.cancel(copyState.getShard(), "Closing");
        assertEquals(0, copyState.refCount());
    }

    public void testCancelReplication_AfterSendFilesStarts() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        // add a doc and refresh so primary has more than one segment.
        indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        primary.refresh("Test");
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            primaryDiscoveryNode,
            testCheckpoint
        );
        final FileChunkWriter segmentSegmentFileChunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> {
            // cancel the replication as soon as the writer starts sending files.
            replications.cancel(replica.routingEntry().allocationId().getId(), "Test");
        };
        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(1, replications.size());
        getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            new ArrayList<>(copyState.getMetadataMap().values()),
            testCheckpoint
        );
        replications.startSegmentCopy(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                Assert.fail("Expected onFailure to be invoked.");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(CancellableThreads.ExecutionCancelledException.class, e.getClass());
                assertEquals(1, copyState.refCount());
                assertEquals(0, replications.size());
                latch.countDown();
            }
        });
        latch.await(2, TimeUnit.SECONDS);
        assertEquals("listener should have resolved with failure", 0, latch.getCount());
        replications.cancel(primary, "Closing");
        assertEquals(0, copyState.refCount());
    }

    public void testMultipleReplicasUseSameCheckpoint() throws IOException {
        IndexShard secondReplica = newShard(primary.shardId(), false);
        recoverReplica(secondReplica, primary, true);

        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            primaryDiscoveryNode,
            testCheckpoint
        );
        final FileChunkWriter segmentSegmentFileChunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> {
            // this shouldn't be called in this test.
            Assert.fail();
        };

        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(2, copyState.refCount());

        final CheckpointInfoRequest secondRequest = new CheckpointInfoRequest(
            1L,
            secondReplica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        final GatedCloseable<CopyState> closeableTwo = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(secondRequest, segmentSegmentFileChunkWriter, closeableTwo);
        assertEquals(3, copyState.refCount());
        assertEquals(2, replications.size());

        replications.cancelReplication(primaryDiscoveryNode);
        replications.cancelReplication(replicaDiscoveryNode);
        assertEquals(1, copyState.refCount());
        assertEquals(0, replications.size());
        closeShards(secondReplica);
        replications.cancel(copyState.getShard(), "Closing");
        assertEquals(0, copyState.refCount());
    }

    public void testStartCopyWithoutPrepareStep() {
        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        final ActionListener<GetSegmentFilesResponse> listener = spy(new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                assertTrue(getSegmentFilesResponse.files.isEmpty());
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail();
            }
        });

        getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            Collections.emptyList(),
            testCheckpoint
        );

        replications.startSegmentCopy(getSegmentFilesRequest, listener);
        verify(listener, times(1)).onResponse(any());
    }

    public void testShardAlreadyReplicatingToNode() {
        OngoingSegmentReplications replications = spy(new OngoingSegmentReplications(mockIndicesService, recoverySettings));
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        final FileChunkWriter segmentSegmentFileChunkWriter = (fileMetadata, position, content, lastChunk, totalTranslogOps, listener) -> {
            listener.onResponse(null);
        };
        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        assertThrows(
            OpenSearchException.class,
            () -> replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable)
        );
    }

    public void testThrowShardNotStartedExceptionWhenNoCopyState() throws IOException {
        closeShards(primary);
        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        primary = newShard(true, settings);
        System.out.println(primary.state());
        replications.setCopyState(primary);
        assertThrows(IndexShardNotStartedException.class, () -> replications.getCopyState(primary.shardId()));
        closeShards(primary);
    }

    public void testStartReplicationWithNoFilesToFetch() throws IOException {
        // create a replications object and request a checkpoint.
        OngoingSegmentReplications replications = spy(new OngoingSegmentReplications(mockIndicesService, recoverySettings));
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        // mock the FileChunkWriter so we can assert its ever called.
        final FileChunkWriter segmentSegmentFileChunkWriter = mock(FileChunkWriter.class);
        // Prepare for replication step - and ensure copyState is added to cache.
        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter, copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(1, replications.size());
        assertEquals(2, copyState.refCount());

        getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            Collections.emptyList(),
            testCheckpoint
        );

        // invoke startSegmentCopy and assert our fileChunkWriter is never invoked.
        replications.startSegmentCopy(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                assertEquals(Collections.emptyList(), getSegmentFilesResponse.files);
                assertEquals(1, copyState.refCount());
                verifyNoInteractions(segmentSegmentFileChunkWriter);
                replications.cancel(copyState.getShard(), "Closing");
                assertEquals(0, copyState.refCount());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected failure", e);
                Assert.fail();
            }
        });
    }

    public void testCancelAllReplicationsForShard() throws IOException {
        // This tests when primary has multiple ongoing replications.
        IndexShard replica_2 = newShard(primary.shardId(), false);
        recoverReplica(replica_2, primary, true);

        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        final CheckpointInfoRequest request = new CheckpointInfoRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            primaryDiscoveryNode,
            testCheckpoint
        );

        replications.setCopyState(primary);
        final GatedCloseable<CopyState> copyStateGatedCloseable = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(request, mock(FileChunkWriter.class), copyStateGatedCloseable);
        final CopyState copyState = copyStateGatedCloseable.get();
        assertEquals(2, copyState.refCount());

        final CheckpointInfoRequest secondRequest = new CheckpointInfoRequest(
            1L,
            replica_2.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        final GatedCloseable<CopyState> closeableTwo = replications.getCopyState(primary.shardId());
        replications.prepareForReplication(secondRequest, mock(FileChunkWriter.class), closeableTwo);

        assertEquals(3, copyState.refCount());
        assertEquals(2, replications.size());

        // cancel the primary's ongoing replications.
        replications.cancel(primary, "Test");
        assertEquals(0, copyState.refCount());
        assertEquals(0, replications.size());
        closeShards(replica_2);
    }

    public void testCopyStateRefCounts() throws IOException {
        OngoingSegmentReplications replications = new OngoingSegmentReplications(mockIndicesService, recoverySettings);
        replications.setCopyState(primary);
        final Map<ShardId, CopyState> copyStateMap = replications.getCopyStateMap();
        assertEquals(1, copyStateMap.size());
        final CopyState copyState = copyStateMap.get(primary.shardId());
        assertEquals("CopyState is snapshotted until released", 1, copyState.refCount());
        final GatedCloseable<CopyState> closeable = replications.getCopyState(primary.shardId());
        assertEquals("CopyState refCount is increased when accessed", 2, copyState.refCount());
        final GatedCloseable<CopyState> closeable_2 = replications.getCopyState(primary.shardId());
        assertEquals("CopyState refCount is increased when accessed", 3, copyState.refCount());

        // index a random doc and refresh.
        indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        primary.refresh("test");

        replications.setCopyState(primary);
        final CopyState copyState_2 = copyStateMap.get(primary.shardId());
        assertEquals(1, copyState_2.refCount());
        assertEquals("CopyState not in map remains refCounted until closed", 2, copyState.refCount());
        final GatedCloseable<CopyState> closeable_3 = replications.getCopyState(primary.shardId());
        assertEquals("CopyState refCount is increased when accessed", 2, copyState_2.refCount());
        assertEquals(copyState_2, closeable_3.get());

        closeable.close();
        closeable_2.close();
        assertEquals(0, copyState.refCount());

        closeable_3.close();
        assertEquals(1, copyState_2.refCount());

        replications.cancel(primary, "Closing");
        assertEquals(0, copyState_2.refCount());
    }
}
