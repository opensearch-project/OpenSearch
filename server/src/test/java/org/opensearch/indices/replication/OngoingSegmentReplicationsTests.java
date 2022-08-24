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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

    final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
    final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primary = newStartedShard(true);
        replica = newShard(primary.shardId(), false);
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
        final CopyState copyState = replications.prepareForReplication(request, segmentSegmentFileChunkWriter);
        assertTrue(replications.isInCopyStateMap(request.getCheckpoint()));
        assertEquals(1, replications.size());
        assertEquals(1, copyState.refCount());

        getSegmentFilesRequest = new GetSegmentFilesRequest(
            1L,
            replica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            new ArrayList<>(copyState.getMetadataSnapshot().asMap().values()),
            testCheckpoint
        );

        final Collection<StoreFileMetadata> expectedFiles = List.copyOf(primary.store().getMetadata().asMap().values());
        replications.startSegmentCopy(getSegmentFilesRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse getSegmentFilesResponse) {
                assertEquals(1, getSegmentFilesResponse.files.size());
                assertEquals(1, expectedFiles.size());
                assertTrue(expectedFiles.stream().findFirst().get().isSame(getSegmentFilesResponse.files.get(0)));
                assertEquals(0, copyState.refCount());
                assertFalse(replications.isInCopyStateMap(request.getCheckpoint()));
                assertEquals(0, replications.size());
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
        final CopyState copyState = replications.prepareForReplication(request, segmentSegmentFileChunkWriter);
        assertEquals(1, replications.size());
        assertEquals(1, replications.cachedCopyStateSize());

        replications.cancelReplication(primaryDiscoveryNode);
        assertEquals(0, copyState.refCount());
        assertEquals(0, replications.size());
        assertEquals(0, replications.cachedCopyStateSize());
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

        final CopyState copyState = replications.prepareForReplication(request, segmentSegmentFileChunkWriter);
        assertEquals(1, copyState.refCount());

        final CheckpointInfoRequest secondRequest = new CheckpointInfoRequest(
            1L,
            secondReplica.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        replications.prepareForReplication(secondRequest, segmentSegmentFileChunkWriter);

        assertEquals(2, copyState.refCount());
        assertEquals(2, replications.size());
        assertEquals(1, replications.cachedCopyStateSize());

        replications.cancelReplication(primaryDiscoveryNode);
        replications.cancelReplication(replicaDiscoveryNode);
        assertEquals(0, copyState.refCount());
        assertEquals(0, replications.size());
        assertEquals(0, replications.cachedCopyStateSize());
        closeShards(secondReplica);
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

    public void testShardAlreadyReplicatingToNode() throws IOException {
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
        replications.prepareForReplication(request, segmentSegmentFileChunkWriter);
        assertThrows(OpenSearchException.class, () -> { replications.prepareForReplication(request, segmentSegmentFileChunkWriter); });
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
        final CopyState copyState = replications.prepareForReplication(request, segmentSegmentFileChunkWriter);
        assertTrue(replications.isInCopyStateMap(request.getCheckpoint()));
        assertEquals(1, replications.size());
        assertEquals(1, copyState.refCount());

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
                assertEquals(0, copyState.refCount());
                assertFalse(replications.isInCopyStateMap(request.getCheckpoint()));
                verifyNoInteractions(segmentSegmentFileChunkWriter);
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

        final CopyState copyState = replications.prepareForReplication(request, mock(FileChunkWriter.class));
        assertEquals(1, copyState.refCount());

        final CheckpointInfoRequest secondRequest = new CheckpointInfoRequest(
            1L,
            replica_2.routingEntry().allocationId().getId(),
            replicaDiscoveryNode,
            testCheckpoint
        );
        replications.prepareForReplication(secondRequest, mock(FileChunkWriter.class));

        assertEquals(2, copyState.refCount());
        assertEquals(2, replications.size());
        assertEquals(1, replications.cachedCopyStateSize());

        // cancel the primary's ongoing replications.
        replications.cancel(primary, "Test");
        assertEquals(0, copyState.refCount());
        assertEquals(0, replications.size());
        assertEquals(0, replications.cachedCopyStateSize());
        closeShards(replica_2);
    }
}
