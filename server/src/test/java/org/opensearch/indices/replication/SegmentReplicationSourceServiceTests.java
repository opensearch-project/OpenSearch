/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.codecs.Codec;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyStateTests;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;

public class SegmentReplicationSourceServiceTests extends OpenSearchTestCase {

    private ReplicationCheckpoint testCheckpoint;
    private TestThreadPool testThreadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private SegmentReplicationSourceService segmentReplicationSourceService;
    private OngoingSegmentReplications ongoingSegmentReplications;
    private IndexShard mockIndexShard;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // setup mocks
        mockIndexShard = CopyStateTests.createMockIndexShard();
        ShardId testShardId = mockIndexShard.shardId();
        IndicesService mockIndicesService = mock(IndicesService.class);
        IndexService mockIndexService = mock(IndexService.class);
        when(mockIndicesService.iterator()).thenReturn(List.of(mockIndexService).iterator());
        when(mockIndicesService.indexServiceSafe(testShardId.getIndex())).thenReturn(mockIndexService);
        when(mockIndexService.getShard(testShardId.id())).thenReturn(mockIndexShard);
        when(mockIndexService.iterator()).thenReturn(List.of(mockIndexShard).iterator());
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder().put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(mockIndexService.getIndexSettings()).thenReturn(indexSettings);
        final ShardRouting routing = mock(ShardRouting.class);
        when(routing.primary()).thenReturn(true);
        when(mockIndexShard.routingEntry()).thenReturn(routing);
        when(mockIndexShard.isPrimaryMode()).thenReturn(true);
        final ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
        when(mockIndexShard.getReplicationGroup()).thenReturn(replicationGroup);
        when(replicationGroup.getInSyncAllocationIds()).thenReturn(Collections.emptySet());
        // This mirrors the creation of the ReplicationCheckpoint inside CopyState
        testCheckpoint = new ReplicationCheckpoint(
            testShardId,
            mockIndexShard.getOperationPrimaryTerm(),
            0L,
            0L,
            Codec.getDefault().getName()
        );
        testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        CapturingTransport transport = new CapturingTransport();
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
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

        final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);

        ongoingSegmentReplications = spy(new OngoingSegmentReplications(mockIndicesService, recoverySettings));
        segmentReplicationSourceService = new SegmentReplicationSourceService(
            mockIndicesService,
            transportService,
            recoverySettings,
            ongoingSegmentReplications
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        testThreadPool = null;
        super.tearDown();
    }

    public void testGetSegmentFiles() {
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        executeGetSegmentFiles(request, new ActionListener<>() {
            @Override
            public void onResponse(GetSegmentFilesResponse response) {
                assertEquals(0, response.files.size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testUpdateVisibleCheckpoint() {
        UpdateVisibleCheckpointRequest request = new UpdateVisibleCheckpointRequest(
            0L,
            "",
            mockIndexShard.shardId(),
            localNode,
            testCheckpoint
        );
        executeUpdateVisibleCheckpoint(request, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {
                assertTrue(TransportResponse.Empty.INSTANCE.equals(transportResponse));
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testCheckpointInfo() {
        executeGetCheckpointInfo(new ActionListener<>() {
            @Override
            public void onResponse(CheckpointInfoResponse response) {
                assertEquals(testCheckpoint, response.getCheckpoint());
                assertNotNull(response.getInfosBytes());
                assertEquals(1, response.getMetadataMap().size());
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected exception: " + e);
            }
        });
    }

    public void testPrimaryClearsOutOfSyncIds() {
        final ClusterChangedEvent mock = mock(ClusterChangedEvent.class);
        when(mock.routingTableChanged()).thenReturn(true);
        segmentReplicationSourceService.clusterChanged(mock);
        verify(ongoingSegmentReplications, times(1)).clearOutOfSyncIds(any(), any());
    }

    private void executeGetCheckpointInfo(ActionListener<CheckpointInfoResponse> listener) {
        final CheckpointInfoRequest request = new CheckpointInfoRequest(1L, "testAllocationId", localNode, testCheckpoint);
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO,
            request,
            new TransportResponseHandler<CheckpointInfoResponse>() {
                @Override
                public void handleResponse(CheckpointInfoResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public CheckpointInfoResponse read(StreamInput in) throws IOException {
                    return new CheckpointInfoResponse(in);
                }
            }
        );
    }

    private void executeGetSegmentFiles(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES,
            request,
            new TransportResponseHandler<GetSegmentFilesResponse>() {
                @Override
                public void handleResponse(GetSegmentFilesResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public GetSegmentFilesResponse read(StreamInput in) throws IOException {
                    return new GetSegmentFilesResponse(in);
                }
            }
        );
    }

    private void executeUpdateVisibleCheckpoint(UpdateVisibleCheckpointRequest request, ActionListener<TransportResponse> listener) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                UpdateVisibleCheckpointRequest newRequest = new UpdateVisibleCheckpointRequest(in);
                assertTrue(newRequest.getCheckpoint().equals(request.getCheckpoint()));
                assertTrue(newRequest.getTargetAllocationId().equals(request.getTargetAllocationId()));
            }
        } catch (IOException e) {
            fail("Failed to parse UpdateVisibleCheckpointRequest " + e);
        }

        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.UPDATE_VISIBLE_CHECKPOINT,
            request,
            new TransportResponseHandler<>() {
                @Override
                public void handleResponse(TransportResponse response) {
                    listener.onResponse(TransportResponse.Empty.INSTANCE);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public CheckpointInfoResponse read(StreamInput in) throws IOException {
                    return new CheckpointInfoResponse(in);
                }
            }
        );
    }
}
