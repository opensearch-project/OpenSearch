/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.transport.TransportService;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class PeerReplicationSourceTests extends IndexShardTestCase {

    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private PeerReplicationSource peerReplicationSource;
    private IndexShard indexShard;
    private DiscoveryNode localNode;
    private DiscoveryNode sourceNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder().put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName()).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        transport = new CapturingTransport();
        localNode = newDiscoveryNode("localNode");
        sourceNode = newDiscoveryNode("sourceNode");
        clusterService = createClusterService(threadPool, localNode);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        indexShard = newStartedShard(true);

        peerReplicationSource = new PeerReplicationSource(
            transportService,
            recoverySettings,
            sourceNode,
            localNode,
            indexShard.routingEntry().allocationId().toString()
        );
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(transportService, clusterService, transport);
        closeShards(indexShard);
        super.tearDown();
    }

    public void testGetCheckpointMetadata_invokesTransportService() {
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        final long replicationId = 1L;
        peerReplicationSource.getCheckpointMetadata(replicationId, checkpoint, mock(ActionListener.class));
        CapturingTransport.CapturedRequest[] capturedRequests1 = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(1));
        CapturingTransport.CapturedRequest request = capturedRequests1[0];
        assertEquals(sourceNode, request.node);
        assertEquals(SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO, request.action);
    }

    public void testGetFiles_invokesTransportService() {
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(indexShard.shardId(), 0L, 0L, 0L, 0L);
        final long replicationId = 1L;
        peerReplicationSource.getSegmentFiles(
            replicationId,
            checkpoint,
            Collections.emptyList(),
            mock(Store.class),
            mock(ActionListener.class)
        );
        CapturingTransport.CapturedRequest[] capturedRequests1 = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(1));
        CapturingTransport.CapturedRequest request = capturedRequests1[0];
        assertEquals(sourceNode, request.node);
        assertEquals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES, request.action);
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }
}
