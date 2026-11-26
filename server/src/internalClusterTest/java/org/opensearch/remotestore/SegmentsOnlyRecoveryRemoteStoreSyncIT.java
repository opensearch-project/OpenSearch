/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Verifies that a file-based recovery waits when the remote segment store is behind the primary's safe commit. A copy
 * with a local translog is rebuilt from remote segments when it takes over, so it must not become in-sync before the
 * remote segment store contains the commit sent by peer recovery.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentsOnlyRecoveryRemoteStoreSyncIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX = "idx";
    private static final int UPLOADED = 100;
    private static final int NOT_UPLOADED = 20;

    public void testNewReplicaThenPrimaryStops() throws Exception {
        String source = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX, 0);
        leaveRemoteStoreBehindTheCommit(source);

        String target = internalCluster().startDataOnlyNode();
        setReplicas(1);
        assertRecoveryWaitsForRemoteStore();
        resumeUploads();
        ensureGreen(INDEX);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(source));
        ensureYellowAndNoInitializingShards(INDEX);
        assertEveryDocumentOn(target);
    }

    public void testNewReplicaThenOnlyItsNodeReturns() throws Exception {
        String source = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX, 0, delayedAllocation());
        leaveRemoteStoreBehindTheCommit(source);

        String target = internalCluster().startDataOnlyNode();
        setReplicas(1);
        assertRecoveryWaitsForRemoteStore();
        resumeUploads();
        ensureGreen(INDEX);

        internalCluster().restartNode(target, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(source));
                return super.onNodeStopped(nodeName);
            }
        });
        ensureYellowAndNoInitializingShards(INDEX);
        assertEquals(target, primaryNodeName());
        assertEveryDocumentOn(target);
    }

    public void testPrimaryRelocationWaitsForRemoteStore() throws Exception {
        String source = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX, 0);
        leaveRemoteStoreBehindTheCommit(source);

        String target = internalCluster().startDataOnlyNode();
        assertAcked(client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX, 0, source, target)).get());
        assertRelocationWaitsForRemoteStore();
        resumeUploads();
        ensureGreenAndNoRelocatingShards();

        assertEquals(target, primaryNodeName());
        assertEveryDocumentOn(target);
    }

    public void testReplicaReturnsWithoutRetentionLeaseThenPrimaryStops() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX, 1, delayedAllocation());
        String source = primaryNodeName();
        String target = replicaNodeName();
        ShardRouting replicaRouting = clusterService().state().routingTable().index(INDEX).shard(0).replicaShards().get(0);

        internalCluster().restartNode(target, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                leaveRemoteStoreBehindTheCommit(source);
                PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                shardOn(source, INDEX).removeRetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaRouting), future);
                future.get();
                return super.onNodeStopped(nodeName);
            }
        });
        assertRecoveryWaitsForRemoteStore();
        resumeUploads();
        ensureGreen(INDEX);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(source));
        ensureYellowAndNoInitializingShards(INDEX);
        assertEveryDocumentOn(target);
    }

    public void testReplicaRelocationThenPrimaryStops() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX, 1);
        String source = primaryNodeName();
        String oldReplicaNode = replicaNodeName();
        leaveRemoteStoreBehindTheCommit(source);

        String target = internalCluster().startDataOnlyNode();
        assertAcked(client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX, 0, oldReplicaNode, target)).get());
        assertRelocationWaitsForRemoteStore();
        resumeUploads();
        ensureGreenAndNoRelocatingShards();

        assertEquals(target, replicaNodeName());
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(source));
        ensureYellowAndNoInitializingShards(INDEX);
        assertEquals(target, primaryNodeName());
        assertEveryDocumentOn(target);
    }

    private void leaveRemoteStoreBehindTheCommit(String primaryNode) throws Exception {
        indexDocs(INDEX, 0, UPLOADED);
        flush(INDEX);
        IndexShard primary = shardOn(primaryNode, INDEX);
        assertBusy(() -> assertTrue(primary.isRemoteSegmentStoreInSync()));

        failUploads(true);
        indexDocs(INDEX, UPLOADED, NOT_UPLOADED);
        flush(INDEX);
        assertFalse("the remote segment store must be behind the commit", primary.isRemoteSegmentStoreInSync());
    }

    private void assertRecoveryWaitsForRemoteStore() {
        assertTrue(
            "the copy became active while the remote segment store was behind",
            client().admin()
                .cluster()
                .prepareHealth(INDEX)
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(5))
                .get()
                .isTimedOut()
        );
    }

    private void assertRelocationWaitsForRemoteStore() {
        assertTrue(
            "the relocation completed while the remote segment store was behind",
            client().admin()
                .cluster()
                .prepareHealth(INDEX)
                .setWaitForNoRelocatingShards(true)
                .setTimeout(TimeValue.timeValueSeconds(5))
                .get()
                .isTimedOut()
        );
    }

    private void resumeUploads() {
        failUploads(false);
    }

    private void assertEveryDocumentOn(String node) throws Exception {
        assertBusy(() -> {
            refresh(INDEX);
            assertHitCount(
                client(node).prepareSearch(INDEX).setPreference("_only_local").setSize(0).setTrackTotalHits(true).get(),
                UPLOADED + NOT_UPLOADED
            );
        });
    }

    private void failUploads(boolean fail) {
        createRepository(
            SEGMENT_REPOSITORY_NAME,
            ReloadableFsRepository.TYPE,
            Settings.builder()
                .put("location", segmentRepoPath)
                .put(ReloadableFsRepository.REPOSITORIES_FAILRATE_SETTING.getKey(), fail ? 100 : 0)
        );
    }

    private Settings delayedAllocation() {
        return Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "12h").build();
    }

    private void setReplicas(int replicas) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas))
        );
    }

    private void ensureGreenAndNoRelocatingShards() {
        assertFalse(
            client().admin().cluster().prepareHealth(INDEX).setWaitForGreenStatus().setWaitForNoRelocatingShards(true).get().isTimedOut()
        );
    }

    private String primaryNodeName() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return state.nodes().get(state.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId()).getName();
    }

    private String replicaNodeName() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return state.nodes().get(state.routingTable().index(INDEX).shard(0).replicaShards().get(0).currentNodeId()).getName();
    }
}
