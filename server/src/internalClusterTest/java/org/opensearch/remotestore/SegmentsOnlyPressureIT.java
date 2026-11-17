/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.checkpoint.PublishCheckpointAction;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.SegmentReplicationPressureService.MAX_INDEXING_CHECKPOINTS;
import static org.opensearch.index.SegmentReplicationPressureService.MAX_REPLICATION_TIME_BACKPRESSURE_SETTING;
import static org.opensearch.index.SegmentReplicationPressureService.SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Segment replication backpressure for {@code segments_only} indices. A replica that stops applying checkpoints has
 * to make the primary reject writes here exactly as it does in any other segment replication cluster, because the lag
 * is measured between the primary and the replica's visible checkpoint and does not depend on where the translog lives.
 * <p>
 * This deliberately does not extend {@link org.opensearch.index.SegmentReplicationPressureIT}. That suite induces lag
 * by parking the replica's sending thread inside a transport interceptor, and the thread it parks is a refresh thread
 * holding a node wide lock. A {@code segments_only} replica keeps its translog locally, so unlike a fully remote backed
 * replica it still has to serve replicated refreshes on that same pool, and parking there deadlocks the refresh the
 * test itself is waiting on. Refusing the checkpoint at the replica's request handler starves the replica just as
 * effectively without holding any thread.
 */
public class SegmentsOnlyPressureIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-pressure-idx";
    private static final int MAX_CHECKPOINTS_BEHIND = 2;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(MAX_REPLICATION_TIME_BACKPRESSURE_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(MAX_INDEXING_CHECKPOINTS.getKey(), MAX_CHECKPOINTS_BEHIND)
            .build();
    }

    /**
     * A replica that falls past both the checkpoint and the time limit has to be counted as stale, which is what makes
     * the primary refuse writes and then accept them again once the replica is caught up.
     */
    public void testWritesRejectedWhileReplicaIsBehind() throws Exception {
        startClusterAndIndex();
        IndexShard primary = shardOn(nodeHolding(true), INDEX_NAME);
        MockTransportService replicaTransport = transportOn(nodeHolding(false));

        // Refuse the checkpoint rather than delaying it, so the replica is starved for as long as the rule is
        // installed while every thread on both nodes stays free.
        replicaTransport.addRequestHandlingBehavior(
            PublishCheckpointAction.ACTION_NAME + TransportReplicationAction.REPLICA_ACTION_SUFFIX,
            (handler, request, channel, task) -> {
                throw new RemoteTransportException("checkpoint withheld from the replica", new OpenSearchRejectedExecutionException());
            }
        );

        int docs = 0;
        for (int i = 0; i <= MAX_CHECKPOINTS_BEHIND; i++) {
            docs = indexDocs(INDEX_NAME, docs, 5);
            refresh(INDEX_NAME);
        }
        assertBusy(() -> assertThat(checkpointsBehind(primary), greaterThan((long) MAX_CHECKPOINTS_BEHIND)), 30, TimeUnit.SECONDS);

        // The time limit is only breached once the lag timer has been running longer than it, so a write is retried
        // until it is refused rather than asserted on once.
        assertBusy(
            () -> expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> client().prepareIndex(INDEX_NAME).setId("rejected").setSource("field", "value").get()
            ),
            30,
            TimeUnit.SECONDS
        );

        SegmentReplicationStatsResponse stats = client().admin()
            .indices()
            .prepareSegmentReplicationStats(INDEX_NAME)
            .setDetailed(true)
            .execute()
            .actionGet();
        assertThat(
            "the refusal must be attributed to the shard",
            stats.getReplicationStats().get(INDEX_NAME).get(0).getRejectedRequestCount(),
            greaterThan(0L)
        );

        replicaTransport.clearAllRules();
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertEquals("the replica must catch up once checkpoints flow again", 0, checkpointsBehind(primary)),
            60,
            TimeUnit.SECONDS
        );
        client().prepareIndex(INDEX_NAME).setId("accepted").setSource("field", "value").get();
    }

    /** A replica that keeps up must never be treated as stale, so that the refusal above cannot be vacuous. */
    public void testWritesAreAcceptedWhileTheReplicaKeepsUp() throws Exception {
        startClusterAndIndex();
        IndexShard primary = shardOn(nodeHolding(true), INDEX_NAME);

        int docs = 0;
        for (int i = 0; i <= MAX_CHECKPOINTS_BEHIND * 2; i++) {
            docs = indexDocs(INDEX_NAME, docs, 5);
            refresh(INDEX_NAME);
        }
        assertBusy(() -> assertEquals(0, checkpointsBehind(primary)));
        client().prepareIndex(INDEX_NAME).setId("accepted").setSource("field", "value").get();
    }

    private void startClusterAndIndex() {
        startCluster(2);
        // Refreshes are driven by the test so that the number of checkpoints the replica is behind is known.
        createSegmentsOnlyIndex(INDEX_NAME, 1, Settings.builder().put("index.refresh_interval", -1).build());
    }

    /** The name of the node holding the primary copy of shard 0, or the replica copy when {@code primary} is false. */
    private String nodeHolding(boolean primary) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.routingTable().index(INDEX_NAME).shard(0);
        return state.nodes().get((primary ? shard.primaryShard() : shard.replicaShards().get(0)).currentNodeId()).getName();
    }

    private MockTransportService transportOn(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }

    private long checkpointsBehind(IndexShard primary) {
        Set<SegmentReplicationShardStats> stats = primary.getReplicationStatsForTrackedReplicas();
        assertEquals("the sole replica must be tracked", 1, stats.size());
        return stats.iterator().next().getCheckpointsBehindCount();
    }
}
