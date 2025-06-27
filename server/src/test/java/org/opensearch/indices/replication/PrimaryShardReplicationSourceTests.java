/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.MergeSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class PrimaryShardReplicationSourceTests extends IndexShardTestCase {

    private static final long PRIMARY_TERM = 1L;
    private static final long SEGMENTS_GEN = 2L;
    private static final long SEQ_NO = 3L;
    private static final long VERSION = 4L;
    private static final long REPLICATION_ID = 123L;

    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private PrimaryShardReplicationSource replicationSource;
    private IndexShard indexShard;
    private DiscoveryNode sourceNode;

    private RecoverySettings recoverySettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        transport = new CapturingTransport();
        sourceNode = newDiscoveryNode("sourceNode");
        final DiscoveryNode localNode = newDiscoveryNode("localNode");
        clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode);
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

        indexShard = newStartedShard(true);

        this.recoverySettings = recoverySettings;
        replicationSource = new PrimaryShardReplicationSource(
            localNode,
            indexShard.routingEntry().allocationId().toString(),
            transportService,
            recoverySettings,
            sourceNode
        );
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(transportService, clusterService, transport);
        closeShards(indexShard);
        super.tearDown();
    }

    public void testGetCheckpointMetadata() {
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            PRIMARY_TERM,
            SEGMENTS_GEN,
            VERSION,
            Codec.getDefault().getName()
        );
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, mock(ActionListener.class));
        CapturingTransport.CapturedRequest[] requestList = transport.getCapturedRequestsAndClear();
        assertEquals(1, requestList.length);
        CapturingTransport.CapturedRequest capturedRequest = requestList[0];
        assertEquals(SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO, capturedRequest.action);
        assertEquals(sourceNode, capturedRequest.node);
        assertTrue(capturedRequest.request instanceof CheckpointInfoRequest);
    }

    public void testGetSegmentFiles() {
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            PRIMARY_TERM,
            SEGMENTS_GEN,
            VERSION,
            Codec.getDefault().getName()
        );
        StoreFileMetadata testMetadata = new StoreFileMetadata("testFile", 1L, "checksum", Version.LATEST);
        replicationSource.getSegmentFiles(
            REPLICATION_ID,
            checkpoint,
            Arrays.asList(testMetadata),
            mock(IndexShard.class),
            (fileName, bytesRecovered) -> {},
            mock(ActionListener.class)
        );
        CapturingTransport.CapturedRequest[] requestList = transport.getCapturedRequestsAndClear();
        assertEquals(1, requestList.length);
        CapturingTransport.CapturedRequest capturedRequest = requestList[0];
        assertEquals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES, capturedRequest.action);
        assertEquals(sourceNode, capturedRequest.node);
        assertTrue(capturedRequest.request instanceof GetSegmentFilesRequest);
    }

    public void testGetMergedSegmentFiles() {
        StoreFileMetadata testMetadata = new StoreFileMetadata("testFile", 1L, "checksum", Version.LATEST);
        final ReplicationCheckpoint checkpoint = new MergeSegmentCheckpoint(
            indexShard.shardId(),
            PRIMARY_TERM,
            1,
            Codec.getDefault().getName(),
            Map.of("testFile", testMetadata),
            "_0"
        );
        replicationSource.getMergedSegmentFiles(
            REPLICATION_ID,
            checkpoint,
            Arrays.asList(testMetadata),
            mock(IndexShard.class),
            (fileName, bytesRecovered) -> {},
            mock(ActionListener.class)
        );
        CapturingTransport.CapturedRequest[] requestList = transport.getCapturedRequestsAndClear();
        assertEquals(1, requestList.length);
        CapturingTransport.CapturedRequest capturedRequest = requestList[0];
        assertEquals(SegmentReplicationSourceService.Actions.GET_MERGED_SEGMENT_FILES, capturedRequest.action);
        assertEquals(sourceNode, capturedRequest.node);
        assertTrue(capturedRequest.request instanceof GetSegmentFilesRequest);
    }

    /**
     * This test verifies the transport request timeout value for fetching the segment files.
     */
    public void testTransportTimeoutForGetSegmentFilesAction() {
        long fileSize = (long) (Math.pow(10, 9));
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            PRIMARY_TERM,
            SEGMENTS_GEN,
            VERSION,
            Codec.getDefault().getName()
        );
        StoreFileMetadata testMetadata = new StoreFileMetadata("testFile", fileSize, "checksum", Version.LATEST);
        replicationSource.getSegmentFiles(
            REPLICATION_ID,
            checkpoint,
            Arrays.asList(testMetadata),
            mock(IndexShard.class),
            (fileName, bytesRecovered) -> {},
            mock(ActionListener.class)
        );
        CapturingTransport.CapturedRequest[] requestList = transport.getCapturedRequestsAndClear();
        assertEquals(1, requestList.length);
        CapturingTransport.CapturedRequest capturedRequest = requestList[0];
        assertEquals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES, capturedRequest.action);
        assertEquals(sourceNode, capturedRequest.node);
        assertEquals(recoverySettings.internalActionLongTimeout(), capturedRequest.options.timeout());
    }

    /**
     * This test verifies the transport request timeout value for fetching the merged segment files.
     */
    public void testTransportTimeoutForGetMergedSegmentFilesAction() {
        long fileSize = (long) (Math.pow(10, 9));
        StoreFileMetadata testMetadata = new StoreFileMetadata("testFile", fileSize, "checksum", Version.LATEST);
        final ReplicationCheckpoint checkpoint = new MergeSegmentCheckpoint(
            indexShard.shardId(),
            PRIMARY_TERM,
            1,
            Codec.getDefault().getName(),
            Map.of("testFile", testMetadata),
            "_0"
        );
        replicationSource.getMergedSegmentFiles(
            REPLICATION_ID,
            checkpoint,
            Arrays.asList(testMetadata),
            mock(IndexShard.class),
            (fileName, bytesRecovered) -> {},
            mock(ActionListener.class)
        );
        CapturingTransport.CapturedRequest[] requestList = transport.getCapturedRequestsAndClear();
        assertEquals(1, requestList.length);
        CapturingTransport.CapturedRequest capturedRequest = requestList[0];
        assertEquals(SegmentReplicationSourceService.Actions.GET_MERGED_SEGMENT_FILES, capturedRequest.action);
        assertEquals(sourceNode, capturedRequest.node);
        assertEquals(recoverySettings.getMergedSegmentReplicationTimeout(), capturedRequest.options.timeout());
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            org.opensearch.Version.CURRENT
        );
    }
}
