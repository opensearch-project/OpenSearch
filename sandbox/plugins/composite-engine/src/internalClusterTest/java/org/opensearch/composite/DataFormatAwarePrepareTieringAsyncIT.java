/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.storage.action.tiering.PrepareTieringAction;
import org.opensearch.storage.action.tiering.PrepareTieringRequest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end integration test for the asynchronous pre-tiering sync ({@link PrepareTieringAction}).
 *
 * <p>This exercises both async mechanisms introduced for DFA tiering preparation, on a real
 * multi-shard, multi-node cluster:
 * <ol>
 *   <li><b>Broadcast async dispatch.</b> {@code TransportPrepareTieringAction} is a
 *       {@code TransportBroadcastByNodeAction} with {@code isAsyncShardOperation() == true}. It fans
 *       out to every node holding a primary shard, runs each shard's prepare on the GENERIC pool, and
 *       aggregates the per-shard results into a single {@link BroadcastResponse}. With one primary per
 *       node, a green response with all shards successful proves the parallel dispatch + aggregation
 *       worked across nodes.</li>
 *   <li><b>Non-blocking merge drain.</b> Each shard freezes its engine and registers an
 *       {@code onMergesDrained} listener instead of blocking a thread. The action only flushes and
 *       uploads once merges have drained. After a successful prepare, every primary must therefore
 *       report zero active and zero pending merges.</li>
 * </ol>
 *
 * <p>Note: an IT cannot directly assert "no thread blocked"; the proof is functional — the prepare
 * completes successfully across all shards/nodes, the merge counts are drained to zero, and no data
 * is lost across the freeze + flush + remote sync.
 */
@ThreadLeakFilters(filters = {
    DataFormatAwareReadonlyEngineBaseIT.CleanerThreadFilter.class,
    AbstractCompositeEngineIT.ParquetNativeThreadFilter.class })
public class DataFormatAwarePrepareTieringAsyncIT extends DataFormatAwareReadonlyEngineBaseIT {

    private static final String ASYNC_INDEX = "dfa-prepare-async-idx";
    private static final int NUM_SHARDS = 3;
    private static final int INDEX_BATCHES = 5;
    private static final int DOCS_PER_BATCH = 40;

    public void testPrepareTieringRunsAsyncAcrossShardsAndDrainsMerges() throws Exception {
        // One cluster-manager plus NUM_SHARDS data nodes so the (0-replica) primaries spread out and
        // the prepare broadcast genuinely fans out to multiple nodes.
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(NUM_SHARDS);

        Settings settings = Settings.builder().put(dfaIndexSettings(0)).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).build();
        client().admin().indices().prepareCreate(ASYNC_INDEX).setSettings(settings).get();
        ensureGreen(ASYNC_INDEX);

        try {
            // Index in several refreshed batches so each shard accumulates multiple segments, giving the
            // merge scheduler candidates to work on (so the merge-drain path has something to drain).
            int id = 0;
            for (int batch = 0; batch < INDEX_BATCHES; batch++) {
                for (int i = 0; i < DOCS_PER_BATCH; i++) {
                    client().prepareIndex(ASYNC_INDEX).setSource("field_text", "value_" + id, "field_number", (long) id).get();
                    id++;
                }
                client().admin().indices().prepareRefresh(ASYNC_INDEX).get();
            }
            final int totalDocs = id;
            client().admin().indices().prepareFlush(ASYNC_INDEX).setForce(true).get();

            // Sanity: the primaries really are spread across more than one node, so the broadcast fans out.
            assertTrue("primaries should be spread across multiple nodes", distinctPrimaryNodeCount(ASYNC_INDEX) > 1);

            // Data integrity baseline (search is unsupported on DFA indices, so use _stats doc count).
            assertEquals("all docs indexed before prepare", (long) totalDocs, primariesDocCount(ASYNC_INDEX));

            // Execute the real async prepare broadcast.
            PrepareTieringRequest request = new PrepareTieringRequest(ASYNC_INDEX);
            request.timeout(TimeValue.timeValueSeconds(90));
            BroadcastResponse response = client().execute(PrepareTieringAction.INSTANCE, request).actionGet();

            // Broadcast async aggregation: every primary shard prepared successfully, none failed.
            assertEquals("all primary shards targeted", NUM_SHARDS, response.getTotalShards());
            assertEquals("no shard should fail prepare", 0, response.getFailedShards());
            assertEquals("all shards should prepare successfully", NUM_SHARDS, response.getSuccessfulShards());

            // Merge-drain: after a successful prepare each primary must have fully drained its merges.
            for (int shardId = 0; shardId < NUM_SHARDS; shardId++) {
                IndexShard primary = primaryShard(ASYNC_INDEX, shardId);
                assertEquals("shard [" + shardId + "] active merges should be drained", 0, primary.getActiveMergeCount());
                assertFalse("shard [" + shardId + "] no pending merges should remain", primary.hasPendingMerges());
            }

            // Data integrity after prepare: a successful prepare flushes and verifies zero uncommitted
            // translog ops on every shard (verifyNoUncommittedOps) — a shard that lost or left data
            // uncommitted would have surfaced as a failed shard above, which we asserted is zero.
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(ASYNC_INDEX)).actionGet();
        }
    }

    /**
     * Calling prepare twice on the same index must succeed both times. The second invocation
     * exercises the already-drained inline-fire branch on every primary (no merges to drain, no
     * uncommitted ops, engine already frozen) — production's idempotent freeze + zero-uncommitted
     * verification + force-flush guard must all be no-ops on the second call rather than throw.
     * Regression test for the freeze CAS, the {@code force=true}/{@code "prepare_tiering"} flush+refresh
     * bypasses, and {@code verifyNoUncommittedOps}.
     */
    public void testPrepareTieringIsIdempotentAcrossInvocations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(NUM_SHARDS);

        Settings settings = Settings.builder().put(dfaIndexSettings(0)).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).build();
        client().admin().indices().prepareCreate(ASYNC_INDEX).setSettings(settings).get();
        ensureGreen(ASYNC_INDEX);

        try {
            int id = 0;
            for (int batch = 0; batch < INDEX_BATCHES; batch++) {
                for (int i = 0; i < DOCS_PER_BATCH; i++) {
                    client().prepareIndex(ASYNC_INDEX).setSource("field_text", "value_" + id, "field_number", (long) id).get();
                    id++;
                }
                client().admin().indices().prepareRefresh(ASYNC_INDEX).get();
            }
            client().admin().indices().prepareFlush(ASYNC_INDEX).setForce(true).get();

            PrepareTieringRequest first = new PrepareTieringRequest(ASYNC_INDEX);
            first.timeout(TimeValue.timeValueSeconds(90));
            BroadcastResponse firstResponse = client().execute(PrepareTieringAction.INSTANCE, first).actionGet();
            assertEquals("first prepare: all shards targeted", NUM_SHARDS, firstResponse.getTotalShards());
            assertEquals("first prepare: no shard should fail", 0, firstResponse.getFailedShards());
            assertEquals("first prepare: all shards succeed", NUM_SHARDS, firstResponse.getSuccessfulShards());

            // Second prepare on an already-prepared (frozen, drained, flushed) index must be a no-op
            // success on every shard. This exercises the already-drained inline-fire path and the
            // idempotent freeze.
            PrepareTieringRequest second = new PrepareTieringRequest(ASYNC_INDEX);
            second.timeout(TimeValue.timeValueSeconds(90));
            BroadcastResponse secondResponse = client().execute(PrepareTieringAction.INSTANCE, second).actionGet();
            assertEquals("second prepare: all shards targeted", NUM_SHARDS, secondResponse.getTotalShards());
            assertEquals("second prepare: no shard should fail", 0, secondResponse.getFailedShards());
            assertEquals("second prepare: all shards succeed", NUM_SHARDS, secondResponse.getSuccessfulShards());

            // Merge counts must still be drained after the redundant prepare.
            for (int shardId = 0; shardId < NUM_SHARDS; shardId++) {
                IndexShard primary = primaryShard(ASYNC_INDEX, shardId);
                assertEquals("shard [" + shardId + "] active merges drained after re-prepare", 0, primary.getActiveMergeCount());
                assertFalse("shard [" + shardId + "] no pending merges remain after re-prepare", primary.hasPendingMerges());
            }
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(ASYNC_INDEX)).actionGet();
        }
    }

    /** Primary doc count via the _stats API (standard DFA-safe approach — search is unsupported on DFA indices). */
    private long primariesDocCount(String index) {
        return client().admin()
            .indices()
            .prepareStats(index)
            .clear()
            .setDocs(true)
            .get()
            .getIndex(index)
            .getPrimaries()
            .getDocs()
            .getCount();
    }

    /** Number of distinct nodes hosting a primary shard of the given index. */
    private int distinctPrimaryNodeCount(String index) {
        ClusterState state = getClusterState();
        Set<String> nodeIds = new HashSet<>();
        for (int shardId = 0; shardId < NUM_SHARDS; shardId++) {
            ShardRouting primary = state.routingTable().index(index).shard(shardId).primaryShard();
            if (primary != null && primary.assignedToNode()) {
                nodeIds.add(primary.currentNodeId());
            }
        }
        return nodeIds.size();
    }

    /** Resolves the live primary {@link IndexShard} for a given shard id. */
    private IndexShard primaryShard(String index, int shardId) {
        ClusterState state = getClusterState();
        ShardRouting primary = state.routingTable().index(index).shard(shardId).primaryShard();
        String nodeName = state.nodes().get(primary.currentNodeId()).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        return indicesService.indexServiceSafe(resolveIndex(index)).getShard(shardId);
    }

    /**
     * Triggers a force merge and immediately calls prepare tiering back-to-back. Whether the force
     * merge is still in-flight when prepare runs depends on timing — the test verifies that in either
     * case:
     * <ul>
     *   <li>Prepare succeeds (no shard failures)</li>
     *   <li>All merges (background and force) are drained after prepare completes</li>
     *   <li>Replicas are in sync with the primary (checkpoints behind == 0)</li>
     * </ul>
     * This guards against the race condition where forceMerge was invisible to onMergesDrained,
     * causing tiering to proceed while a force merge was still running.
     */
    public void testPrepareTieringAfterForceMerge_MergesDrainedAndReplicasInSync() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);

        Settings settings = Settings.builder().put(dfaIndexSettings(1)).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        client().admin().indices().prepareCreate(ASYNC_INDEX).setSettings(settings).get();
        ensureGreen(ASYNC_INDEX);

        try {
            // Index in many batches to create multiple segments worth merging
            int id = 0;
            for (int batch = 0; batch < INDEX_BATCHES * 2; batch++) {
                for (int i = 0; i < DOCS_PER_BATCH; i++) {
                    client().prepareIndex(ASYNC_INDEX).setSource("field_text", "value_" + id, "field_number", (long) id).get();
                    id++;
                }
                client().admin().indices().prepareRefresh(ASYNC_INDEX).get();
            }
            final int totalDocs = id;
            client().admin().indices().prepareFlush(ASYNC_INDEX).setForce(true).get();

            // Trigger force merge (non-blocking) and immediately call prepare tiering back-to-back.
            // Whether the merge is still in-flight or already done when prepare runs is timing-dependent —
            // both outcomes must result in a successful prepare with drained merges.
            client().admin().indices().prepareForceMerge(ASYNC_INDEX).setMaxNumSegments(1).setFlush(false).execute();

            PrepareTieringRequest request = new PrepareTieringRequest(ASYNC_INDEX);
            request.timeout(TimeValue.timeValueSeconds(90));
            BroadcastResponse response = client().execute(PrepareTieringAction.INSTANCE, request).actionGet();

            // Prepare must succeed — regardless of whether force merge was still running or already done
            assertEquals("all shards targeted", 1, response.getTotalShards());
            assertEquals("no shard should fail prepare", 0, response.getFailedShards());
            assertEquals("shard should prepare successfully", 1, response.getSuccessfulShards());

            // After prepare completes, all merges must be drained
            IndexShard primary = primaryShard(ASYNC_INDEX, 0);
            assertEquals("active merges should be drained after prepare", 0, primary.getActiveMergeCount());
            assertFalse("no pending merges should remain after prepare", primary.hasPendingMerges());

            // Replicas must be in sync — checkpoints behind should be 0
            assertBusy(() -> {
                var replicationStats = primary.getReplicationStatsForTrackedReplicas();
                for (var stat : replicationStats) {
                    assertEquals("replica should be in sync after prepare (checkpoints behind)", 0, stat.getCheckpointsBehindCount());
                }
            }, 30, TimeUnit.SECONDS);

            // Data integrity: doc count should be preserved
            assertEquals("all docs should be present", (long) totalDocs, primariesDocCount(ASYNC_INDEX));

            // After prepare, replica should have the same segment layout as primary (in sync)
            long primarySegments = client().admin()
                .indices()
                .prepareStats(ASYNC_INDEX)
                .clear()
                .setSegments(true)
                .get()
                .getIndex(ASYNC_INDEX)
                .getPrimaries()
                .getSegments()
                .getCount();

            long totalSegments = client().admin()
                .indices()
                .prepareStats(ASYNC_INDEX)
                .clear()
                .setSegments(true)
                .get()
                .getIndex(ASYNC_INDEX)
                .getTotal()
                .getSegments()
                .getCount();

            // 1P+1R: total should be exactly 2x primary (replica mirrors primary)
            assertEquals(
                "replica segment count should match primary after prepare",
                primarySegments * 2,
                totalSegments
            );
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(ASYNC_INDEX)).actionGet();
        }
    }
}
