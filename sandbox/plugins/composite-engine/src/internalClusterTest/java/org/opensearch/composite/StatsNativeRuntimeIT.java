/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for the parquet native_runtime stats block at the node-level endpoint.
 * Verifies that rayon + tokio runtime metrics are collected and surfaced after a merge
 * operation runs, but never appear at shard level or in cluster-aggregate /{idx}/_stats.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class StatsNativeRuntimeIT extends BaseStatsIT {

    /**
     * After a force_merge runs, the parquet node-stats endpoint must include a
     * non-empty native_runtime block with rayon + tokio metrics.
     */
    @SuppressWarnings("unchecked")
    public void testNativeRuntimeStatsAppearAtNodeLevel() throws Exception {
        String idx = "native-runtime-idx";
        createCompositeIndex(idx, true);

        for (int batch = 0; batch < 5; batch++) {
            indexDocs(idx, 40, batch * 40);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            Map<String, Object> resp = parquetNodeStats("");
            assertTrue("response must have nodes block", resp.containsKey("nodes"));
            Map<String, Object> nodes = (Map<String, Object>) resp.get("nodes");
            assertFalse("nodes block must not be empty", nodes.isEmpty());

            Map<String, Object> nodeStats = (Map<String, Object>) nodes.values().iterator().next();
            assertTrue("node entry must contain native_runtime block", nodeStats.containsKey("native_runtime"));

            Map<String, Object> nativeRuntime = (Map<String, Object>) nodeStats.get("native_runtime");
            assertTrue("native_runtime must contain parquet_merge block", nativeRuntime.containsKey("parquet_merge"));
            assertTrue("native_runtime must contain parquet_io block", nativeRuntime.containsKey("parquet_io"));

            Map<String, Object> rayon = (Map<String, Object>) nativeRuntime.get("parquet_merge");
            assertTrue("rayon configured_threads must be > 0 after merge", ((Number) rayon.get("configured_threads")).longValue() > 0);
            assertTrue(
                "rayon merge_tasks_submitted must be >= 1 after force_merge",
                ((Number) rayon.get("merge_tasks_submitted")).longValue() >= 1
            );
            assertTrue(
                "rayon merge_tasks_completed must be >= 1 after successful merge",
                ((Number) rayon.get("merge_tasks_completed")).longValue() >= 1
            );
            assertEquals(
                "rayon merge_tasks_panicked must be 0 in happy path",
                0L,
                ((Number) rayon.get("merge_tasks_panicked")).longValue()
            );
            // — new rayon counters + derived fields
            assertTrue("rayon merge_tasks_started must be >= 1", ((Number) rayon.get("merge_tasks_started")).longValue() >= 1);
            assertEquals("rayon merge_tasks_failed must be 0 in happy path", 0L, ((Number) rayon.get("merge_tasks_failed")).longValue());
            assertNotNull("rayon merge_tasks_queue_depth derived field present", rayon.get("merge_tasks_queue_depth"));
            assertNotNull("rayon merge_tasks_in_flight derived field present", rayon.get("merge_tasks_in_flight"));
            assertTrue("rayon merge_tasks_queue_depth must be >= 0", ((Number) rayon.get("merge_tasks_queue_depth")).longValue() >= 0);
            assertEquals(
                "rayon merge_tasks_in_flight must be 0 after merges complete",
                0L,
                ((Number) rayon.get("merge_tasks_in_flight")).longValue()
            );

            Map<String, Object> tokio = (Map<String, Object>) nativeRuntime.get("parquet_io");
            assertTrue(
                "tokio num_workers must be > 0 after IO runtime is initialized",
                ((Number) tokio.get("num_workers")).longValue() > 0
            );
            // — new tokio fields
            assertNotNull("tokio blocking_queue_depth field present", tokio.get("blocking_queue_depth"));
            assertNotNull("tokio local_queue_depth_total field present", tokio.get("local_queue_depth_total"));
            assertNotNull("tokio polls_count_total field present", tokio.get("polls_count_total"));
            assertNotNull("tokio overflow_count_total field present", tokio.get("overflow_count_total"));
            assertTrue("tokio polls_count_total must be > 0 after merge IO", ((Number) tokio.get("polls_count_total")).longValue() > 0);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * The native_runtime block must NOT appear in the cluster-aggregate /{idx}/_stats response.
     */
    @SuppressWarnings("unchecked")
    public void testNativeRuntimeAbsentAtIndexLevel() throws Exception {
        String idx = "native-runtime-absent-idx";
        createCompositeIndex(idx, true);
        for (int batch = 0; batch < 3; batch++) {
            indexDocs(idx, 30, batch * 30);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            Map<String, Object> idxResp = parquetIndexStats(idx);
            Map<String, Object> indices = (Map<String, Object>) idxResp.get("indices");
            Map<String, Object> indexBlock = (Map<String, Object>) indices.get(idx);
            assertFalse("index-level response must NOT have native_runtime block", indexBlock.containsKey("native_runtime"));
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * The native_runtime block must NOT appear in shard-level entries either.
     */
    @SuppressWarnings("unchecked")
    public void testNativeRuntimeAbsentAtShardLevel() throws Exception {
        String idx = "native-runtime-shard-idx";
        createCompositeIndex(idx, true);
        for (int batch = 0; batch < 3; batch++) {
            indexDocs(idx, 30, batch * 30);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            Map<String, Object> shardResp = parquetIndexStats(idx, "level", "shards");
            Map<String, Object> indices = (Map<String, Object>) shardResp.get("indices");
            Map<String, Object> indexBlock = (Map<String, Object>) indices.get(idx);
            assertTrue("index block must contain shards", indexBlock.containsKey("shards"));
            Map<String, Object> shards = (Map<String, Object>) indexBlock.get("shards");
            for (Map.Entry<String, Object> e : shards.entrySet()) {
                java.util.List<Map<String, Object>> copies = (java.util.List<Map<String, Object>>) e.getValue();
                for (Map<String, Object> copy : copies) {
                    assertFalse("shard-level entry must NOT have native_runtime block", copy.containsKey("native_runtime"));
                }
            }
        }, 10, TimeUnit.SECONDS);
    }
}
