/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.coordination.LeaderChecker;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.disruption.NetworkDisruption;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Recovery of a {@code segments_only} shard: reopening one, rebuilding one from a peer, relocating one, promoting a
 * replica over a lost primary, and restarting the cluster underneath it. These are the paths where the mode's split
 * durability matters most, because the segments come from the remote store while the operations that have not
 * reached a segment yet exist only in the local translog.
 *
 * <p>Each test leaves operations unflushed where the scenario allows it, since a flushed shard would recover from
 * its Lucene commit alone and never exercise that split.
 */
public class SegmentsOnlyRecoveryIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-recovery-idx";

    /**
     * Closing and reopening an index. {@code ReadOnlyEngine#isClosedRemoteIndex} skips the check that the maximum
     * sequence number equals the global checkpoint for anything on a remote node, which includes segments_only.
     */
    public void testCloseAndReopenWithUnflushedOps() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 30);

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME).setWaitForActiveShards(ActiveShardCount.DEFAULT));
        assertAcked(client().admin().indices().prepareOpen(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));
    }

    /**
     * Restarting only the replica node with unflushed operations outstanding. The replica keeps a local translog
     * but cannot replay it, because a segment replication replica runs a read only engine with no IndexWriter, so
     * peer recovery has to bring the operations back instead.
     */
    public void testReplicaNodeRestartWithUnflushedOps() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 40);

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting replicaRouting = state.routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0);
        String replicaNode = state.nodes().get(replicaRouting.currentNodeId()).getName();

        internalCluster().restartNode(replicaNode);
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));

        // Both copies must agree on the whole operation history, otherwise the replica silently lost some.
        assertBusy(() -> {
            for (String node : internalCluster().getDataNodeNames()) {
                IndexService service = internalCluster().getInstance(IndicesService.class, node).indexService(resolveIndex(INDEX_NAME));
                if (service == null) {
                    continue;
                }
                assertEquals("operations must be present on " + node, docs - 1, service.getShard(0).seqNoStats().getMaxSeqNo());
            }
        });
    }

    /**
     * Growing the index to a replica after the operations were already written and never flushed, so the replica
     * has to be built entirely by a peer recovery rather than by replication of live writes.
     */
    public void testReplicaAddedAfterUnflushedWrites() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        int docs = indexDocs(INDEX_NAME, 0, 40);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));
    }

    /** Primary relocation while operations are outstanding and unflushed. */
    public void testPrimaryRelocationWithUnflushedOps() throws Exception {
        List<String> dataNodes = startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        int docs = indexDocs(INDEX_NAME, 0, 40);

        String from = primaryNodeName(INDEX_NAME);
        String to = dataNodes.stream().filter(node -> node.equals(from) == false).findFirst().orElseThrow();
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, 0, from, to)).get();
        ensureGreen(INDEX_NAME);
        assertEquals(to, primaryNodeName(INDEX_NAME));

        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));
    }

    /**
     * A force merge rewrites the segment set, so a restart afterwards has to reconcile it with a translog that was
     * never uploaded.
     */
    public void testForceMergeThenRestartWithUnflushedOps() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        indexDocs(INDEX_NAME, 0, 30);
        flush(INDEX_NAME);
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        int docs = indexDocs(INDEX_NAME, 30, 30);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        String primaryNode = primaryNodeName(INDEX_NAME);
        refresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs);
    }

    /** Deletes and updates, which write different translog operation types than a plain index does. */
    public void testDeletesAndUpdatesSurviveARestart() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        indexDocs(INDEX_NAME, 0, 40);
        flush(INDEX_NAME);
        for (int i = 0; i < 10; i++) {
            client().prepareDelete(INDEX_NAME, Integer.toString(i)).get();
        }
        for (int i = 10; i < 20; i++) {
            client().prepareUpdate(INDEX_NAME, Integer.toString(i)).setDoc("field", "updated" + i).get();
        }

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);
        refresh(INDEX_NAME);

        String primaryNode = primaryNodeName(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 30);
        for (int i = 0; i < 10; i++) {
            assertFalse("deleted doc " + i + " came back", client().prepareGet(INDEX_NAME, Integer.toString(i)).get().isExists());
        }
        for (int i = 10; i < 20; i++) {
            assertEquals(
                "updated doc " + i + " lost its update",
                "updated" + i,
                client().prepareGet(INDEX_NAME, Integer.toString(i)).get().getSourceAsMap().get("field")
            );
        }
    }

    /**
     * Killing the primary must not lose acknowledged writes, since the replica holds them in its own translog.
     */
    public void testReplicaPromotionRetainsAcknowledgedWrites() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = randomIntBetween(10, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String primaryNode = state.nodes().get(state.routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId()).getName();
        internalCluster().stopRandomNode(settings -> primaryNode.equals(settings.get("node.name")));

        ensureYellowAndNoInitializingShards(INDEX_NAME);

        // The promoted replica replays its local translog into Lucene, so the writes survive even though some of them
        // may only become searchable after the next refresh.
        assertBusy(() -> {
            refresh(INDEX_NAME);
            assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);
        });
    }

    /**
     * The same promotion, but with writes still arriving as the primary dies. Operations acknowledged after the last
     * upload exist only in a translog, so a replica behind on segments keeps them only if it received and persisted
     * them itself. A quiescent kill cannot show that, because a refresh has already carried everything remotely.
     */
    public void testFailoverWhileIndexingRetainsAcknowledgedWrites() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        final int killAfter = randomIntBetween(20, 40);
        final int target = killAfter + randomIntBetween(20, 40);
        final AtomicInteger acknowledged = new AtomicInteger();
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch readyToKill = new CountDownLatch(1);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);

        Thread writer = new Thread(() -> {
            while (stop.get() == false && acknowledged.get() < target && System.nanoTime() < deadline) {
                // The id is the count so far rather than generated, so that a write refused on the way back after
                // the primary applied it is overwritten by the retry instead of counted twice.
                String id = Integer.toString(acknowledged.get());
                try {
                    client().prepareIndex(INDEX_NAME).setId(id).setSource("field", id).get();
                } catch (Exception e) {
                    // The primary is being replaced, so a write can be refused outright. Only acknowledged writes
                    // are asserted on, and the loop keeps going to reach the post-failover count.
                    continue;
                }
                if (acknowledged.incrementAndGet() == killAfter) {
                    readyToKill.countDown();
                }
            }
        });
        writer.start();

        try {
            assertTrue("the writer must reach the kill point", readyToKill.await(60, TimeUnit.SECONDS));

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            String primary = state.nodes().get(state.routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId()).getName();
            internalCluster().stopRandomNode(settings -> primary.equals(settings.get("node.name")));

            writer.join();
            assertEquals("the writer must keep writing through the failover", target, acknowledged.get());
        } finally {
            stop.set(true);
            writer.join();
        }

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        assertBusy(() -> {
            refresh(INDEX_NAME);
            assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), target);
        });
    }

    /**
     * Operations that have been acknowledged but not yet flushed exist only in the local translog, because the remote
     * segment store holds segments and never the operation stream. A primary must therefore recover them from its own
     * translog across a restart rather than rebuilding its commit from the remote store, which would silently drop
     * them. A replica is present so that the primary is restored into a replication group.
     */
    public void testUnflushedWritesSurviveAFullClusterRestart() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        // No refresh or flush, so nothing reaches a Lucene commit and the translog is the only record of these writes.
        int docs = randomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // The primary is the copy that had to replay the translog, so pinning the search to it is what proves no
        // acknowledged operation was dropped. The replica reaches the same count only once the primary's recovered
        // segments have been uploaded and a replication checkpoint processed, which is why it is polled separately.
        String primaryNode = primaryNodeName(INDEX_NAME);
        refresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));
    }

    /**
     * {@code _remotestore/_restore} rebuilds a shard from what the remote store holds. For this mode that is the
     * segments and nothing else, so the restore comes back at the last segment that was uploaded and the
     * operations after it are gone with the disk that held them. That is a real loss, bounded by the refresh
     * interval, and still the better of the two outcomes: the alternative when the only copy is gone is all of it.
     */
    public void testRemoteStoreRestoreRecoversUploadedSegments() throws Exception {
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        int docs = indexDocs(INDEX_NAME, 0, 30);
        // Only what reaches a segment reaches the remote store, so this is what the restore can recover.
        refresh(INDEX_NAME);
        flush(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));

        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        assertBusy(() -> {
            refresh(INDEX_NAME);
            assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);
        });

        // The restored shard is a working shard, not just a readable one, and it is still segments_only.
        IndexShard restored = shardOn(internalCluster().getDataNodeNames().iterator().next(), INDEX_NAME);
        assertTrue("the restored shard must still be remote segment backed", restored.indexSettings().isRemoteStoreEnabled());
        assertFalse("the restored shard must still keep its translog locally", restored.indexSettings().isTranslogDurableInRemoteStore());
        int afterRestore = indexDocs(INDEX_NAME, docs, 10);
        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), afterRestore));
    }

    /**
     * A segments_only primary that is partitioned away is replaced by its replica, and when it rejoins it has to
     * take the new primary's history rather than the one on its own disk. The inverse of
     * {@link #testUnflushedWritesSurviveAFullClusterRestart}: a primary replays its local translog, and the moment
     * it stops being the primary it has to stop. Stopping the node outright cannot reach this, because nothing
     * comes back to be reconciled.
     */
    public void testStalePrimaryTakesNewHistoryOnRejoin() throws Exception {
        // The cluster manager has to notice the isolated node quickly, otherwise the replica is never promoted and
        // there is no second history for the old primary to come back to.
        Settings fastFailureDetection = Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "20s")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 4)
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .build();
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(fastFailureDetection);
        internalCluster().startDataOnlyNodes(2, fastFailureDetection);
        ensureStableCluster(3);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 20);
        refresh(INDEX_NAME);
        String stalePrimaryNode = primaryNodeName(INDEX_NAME);
        String survivingNode = replicaNodeName(INDEX_NAME);
        for (String node : List.of(stalePrimaryNode, survivingNode)) {
            assertBusy(
                () -> assertHitCount(client(node).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs),
                60,
                TimeUnit.SECONDS
            );
        }

        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Stream.of(clusterManagerNode, survivingNode).collect(Collectors.toCollection(HashSet::new)),
                Collections.singleton(stalePrimaryNode)
            ),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(partition);
        partition.startDisrupting();
        ensureStableCluster(2, clusterManagerNode);

        // The replica takes over, and the history moves on without the isolated copy.
        assertBusy(
            () -> assertTrue(
                client(clusterManagerNode).admin()
                    .cluster()
                    .prepareState()
                    .get()
                    .getState()
                    .routingTable()
                    .index(INDEX_NAME)
                    .shard(0)
                    .primaryShard()
                    .active()
            ),
            60,
            TimeUnit.SECONDS
        );
        int survivingDocs = 10;
        for (int i = 0; i < survivingDocs; i++) {
            client(clusterManagerNode).prepareIndex(INDEX_NAME).setId("kept-" + i).setSource("field", "kept").get();
        }
        client(clusterManagerNode).admin().indices().prepareRefresh(INDEX_NAME).get();
        int expected = docs + survivingDocs;
        assertHitCount(client(survivingNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expected);

        // Asked of the cluster manager, and with room: the isolated node has a shard to recover as it comes back,
        // so it takes longer to settle than a node that was partitioned while idle.
        partition.stopDisrupting();
        ensureStableCluster(3, TimeValue.timeValueSeconds(120), false, clusterManagerNode);
        internalCluster().clearDisruptionScheme();
        ensureGreen(INDEX_NAME);

        // Both copies have to hold the history the cluster settled on. More than that would be the old primary
        // replaying operations of its own; fewer would be an acknowledged operation lost.
        assertBusy(() -> {
            refresh(INDEX_NAME);
            for (String node : internalCluster().getDataNodeNames()) {
                assertHitCount(client(node).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expected);
            }
        });
    }
}
