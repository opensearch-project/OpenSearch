/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyStateTests;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentReplicationSourceServiceTests extends OpenSearchTestCase {

    private ShardId testShardId;
    private ReplicationCheckpoint testCheckpoint;
    private IndicesService mockIndicesService;
    private IndexService mockIndexService;
    private IndexShard mockIndexShard;
    private TestThreadPool testThreadPool;
    private CapturingTransport transport;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private SegmentReplicationSourceService segmentReplicationSourceService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // setup mocks
        mockIndexShard = CopyStateTests.createMockIndexShard();
        testShardId = mockIndexShard.shardId();
        mockIndicesService = mock(IndicesService.class);
        mockIndexService = mock(IndexService.class);
        when(mockIndicesService.indexService(testShardId.getIndex())).thenReturn(mockIndexService);
        when(mockIndexService.getShard(testShardId.id())).thenReturn(mockIndexShard);

        // This mirrors the creation of the ReplicationCheckpoint inside CopyState
        testCheckpoint = new ReplicationCheckpoint(
            testShardId,
            mockIndexShard.getOperationPrimaryTerm(),
            0L,
            mockIndexShard.getProcessedLocalCheckpoint(),
            0L
        );
        testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        transport = new CapturingTransport();
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
        segmentReplicationSourceService = new SegmentReplicationSourceService(transportService, mockIndicesService);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        testThreadPool = null;
        super.tearDown();
    }

    public void testGetSegmentFiles_EmptyResponse() {
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            1,
            "allocationId",
            localNode,
            Collections.emptyList(),
            testCheckpoint
        );
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES,
            request,
            new TransportResponseHandler<GetSegmentFilesResponse>() {
                @Override
                public void handleResponse(GetSegmentFilesResponse response) {
                    assertEquals(0, response.files.size());
                }

                @Override
                public void handleException(TransportException e) {
                    fail("unexpected exception: " + e);
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

    public void testCheckpointInfo() {
        final CheckpointInfoRequest request = new CheckpointInfoRequest(1L, "testAllocationId", localNode, testCheckpoint);
        transportService.sendRequest(
            localNode,
            SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO,
            request,
            new TransportResponseHandler<CheckpointInfoResponse>() {
                @Override
                public void handleResponse(CheckpointInfoResponse response) {
                    assertEquals(testCheckpoint, response.getCheckpoint());
                    assertNotNull(response.getInfosBytes());
                    // CopyStateTests sets up one pending delete file and one committed segments file
                    assertEquals(1, response.getPendingDeleteFiles().size());
                    assertEquals(1, response.getSnapshot().size());
                }

                @Override
                public void handleException(TransportException e) {
                    fail("unexpected exception: " + e);
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
