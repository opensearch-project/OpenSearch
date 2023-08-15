/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentReplicationTargetPendingCheckpointTests extends IndexShardTestCase {

    private IndexShard replicaShard;
    private IndexShard primaryShard;
    private ReplicationCheckpoint checkpoint;
    private SegmentReplicationTargetService sut;
    private ReplicationCheckpoint aheadCheckpoint;

    private ReplicationCheckpoint newPrimaryCheckpoint;

    private TransportService transportService;
    private TestThreadPool testThreadPool;
    private DiscoveryNode localNode;

    private IndicesService indicesService;

    private SegmentReplicationState state;
    private ReplicationCheckpoint initialCheckpoint;

    private ShardId replicaId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("node.name", SegmentReplicationTargetServiceTests.class.getSimpleName())
            .build();
        primaryShard = newStartedShard(true, settings);
        String primaryCodec = primaryShard.getLatestReplicationCheckpoint().getCodec();
        replicaShard = newShard(false, settings, new NRTReplicationEngineFactory());
        replicaId = replicaShard.shardId();
        recoverReplica(replicaShard, primaryShard, true, getReplicationFunc(replicaShard));
        checkpoint = new ReplicationCheckpoint(
            replicaShard.shardId(),
            0L,
            0L,
            0L,
            replicaShard.getLatestReplicationCheckpoint().getCodec()
        );

        testThreadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        CapturingTransport transport = new CapturingTransport();
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

        indicesService = mock(IndicesService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterState.routingTable()).thenReturn(mockRoutingTable);
        when(mockRoutingTable.shardRoutingTable(any())).thenReturn(primaryShard.getReplicationGroup().getRoutingTable());

        when(clusterState.nodes()).thenReturn(DiscoveryNodes.builder().add(localNode).build());
        sut = prepareForReplication(primaryShard, replicaShard, transportService, indicesService, clusterService);
        initialCheckpoint = primaryShard.getLatestReplicationCheckpoint();
        aheadCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm(),
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1,
            primaryCodec
        );
        newPrimaryCheckpoint = new ReplicationCheckpoint(
            initialCheckpoint.getShardId(),
            initialCheckpoint.getPrimaryTerm() + 1,
            initialCheckpoint.getSegmentsGen(),
            initialCheckpoint.getSegmentInfosVersion() + 1,
            primaryCodec
        );

        state = new SegmentReplicationState(
            replicaShard.routingEntry(),
            new ReplicationLuceneIndex(),
            0L,
            "",
            new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT)
        );
    }

    public void testAddNewPendingCheckpoints() throws InterruptedException {
        TimeValue maxLimit = TimeValue.timeValueMillis(100);
        SegmentReplicationPendingCheckpoints pendingCheckpoints = sut.pendingCheckpoints;
        pendingCheckpoints.setMaxAllowedReplicationTime(maxLimit);

        assertNull(pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, initialCheckpoint);
        assertEquals(initialCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, aheadCheckpoint);
        assertEquals(aheadCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, newPrimaryCheckpoint);
        assertEquals(newPrimaryCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));
        assertEquals(3, pendingCheckpoints.checkpointsTracker.get(replicaId).size());

        Thread.sleep(maxLimit.millis());

        List<ShardId> shardsToFail = pendingCheckpoints.getStaleShardsToFail();
        assertEquals(1, shardsToFail.size());
        assertEquals(replicaId, shardsToFail.get(0));
    }

    public void testUpdatePendingCheckpoints() {
        SegmentReplicationPendingCheckpoints pendingCheckpoints = sut.pendingCheckpoints;

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, initialCheckpoint);
        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, aheadCheckpoint);
        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, newPrimaryCheckpoint);
        pendingCheckpoints.updateCheckpointProcessed(replicaId, initialCheckpoint);
        pendingCheckpoints.updateCheckpointProcessed(replicaId, aheadCheckpoint);
        sut.asyncFailStaleReplicaTask.runInternal();

        assertEquals(newPrimaryCheckpoint, pendingCheckpoints.checkpointsTracker.get(replicaId).get(0).v1());
    }

    public void testUpdateLatestPendingCheckpoints() {
        SegmentReplicationPendingCheckpoints pendingCheckpoints = sut.pendingCheckpoints;

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, initialCheckpoint);
        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, aheadCheckpoint);
        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, newPrimaryCheckpoint);
        pendingCheckpoints.updateCheckpointProcessed(replicaId, newPrimaryCheckpoint);

        assertEquals(0, pendingCheckpoints.checkpointsTracker.get(replicaId).size());
    }

    public void testAddNewPendingCheckpointsOutOfOrder() {
        SegmentReplicationPendingCheckpoints pendingCheckpoints = sut.pendingCheckpoints;

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, initialCheckpoint);
        assertEquals(initialCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, newPrimaryCheckpoint);
        assertEquals(newPrimaryCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        expectThrows(AssertionError.class, () -> pendingCheckpoints.addNewReceivedCheckpoint(replicaId, aheadCheckpoint));
    }

    public void testPendingCheckpointsShardRemoved() {
        SegmentReplicationPendingCheckpoints pendingCheckpoints = sut.pendingCheckpoints;

        pendingCheckpoints.addNewReceivedCheckpoint(replicaId, initialCheckpoint);
        assertEquals(initialCheckpoint, pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));

        pendingCheckpoints.remove(replicaId);
        assertNull(pendingCheckpoints.getLatestReplicationCheckpoint(replicaId));
    }

}
