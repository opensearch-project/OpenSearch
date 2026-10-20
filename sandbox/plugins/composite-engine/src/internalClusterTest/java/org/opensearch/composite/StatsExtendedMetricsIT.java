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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for stats counters added on top of the baseline set: Validates the new counters
 * behave correctly at shard, index, and node-aggregate scopes:
 *
 * <ul>
 *   <li>lucene {@code commit.commit_failures}</li>
 *   <li>parquet {@code merge.flush_and_sort_chunk_total} / {@code _time_millis}</li>
 *   <li>parquet {@code merge.row_id_mapping_max} (max-of-maxes when aggregated)</li>
 *   <li>parquet {@code native_runtime.parquet_merge.merge_tasks_started / merge_tasks_failed}</li>
 *   <li>parquet {@code native_runtime.parquet_merge.merge_tasks_queue_depth / merge_tasks_in_flight} (derived)</li>
 *   <li>parquet {@code native_runtime.parquet_io.blocking_queue_depth / local_queue_depth_total}</li>
 *   <li>parquet {@code native_runtime.parquet_io.polls_count_total / overflow_count_total}</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class StatsExtendedMetricsIT extends BaseStatsIT {

    // ===================================================================
    // Lucene commit_failures — always 0 in happy path
    // ===================================================================

    public void testLuceneCommitFailuresIsZeroInHappyPath() throws Exception {
        String idx = "pr2-commit-failures-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 50, 0);
        refreshIndex(idx);
        forceMerge(idx, 1);

        assertBusy(() -> {
            // Index level
            Map<String, Object> resp = luceneIndexStats(idx);
            assertCounterPresent("lucene commit_failures field must be present", resp, "indices." + idx + ".commit.commit_failures");
            assertCounter("lucene commit_failures should be 0 in happy path", resp, "indices." + idx + ".commit.commit_failures", 0L);
            assertCounterAtLeast(
                "lucene commit_total should be >= 1 after force_merge",
                resp,
                "indices." + idx + ".commit.commit_total",
                1L
            );
        }, 30, TimeUnit.SECONDS);
    }

    // ===================================================================
    // Parquet flush_and_sort_chunk + row_id_mapping_max — per-shard
    // ===================================================================

    @SuppressWarnings("unchecked")
    public void testFlushAndSortChunkAndRowIdMappingMaxAtShardLevel() throws Exception {
        String idx = "pr2-fchunk-shard-idx";
        createCompositeIndex(idx, true);

        // Multiple refreshes → multiple parquet files → force_merge has real work to do.
        int totalDocs = 0;
        for (int batch = 0; batch < 4; batch++) {
            indexDocs(idx, 50, batch * 50);
            refreshIndex(idx);
            totalDocs += 50;
        }
        forceMerge(idx, 1);
        final int expectedRows = totalDocs;

        assertBusy(() -> {
            // Shard level — counters live on the actual primary shard
            Map<String, Object> shardResp = parquetIndexStats(idx, "level", "shards");
            Map<String, Object> indices = (Map<String, Object>) shardResp.get("indices");
            Map<String, Object> indexBlock = (Map<String, Object>) indices.get(idx);
            Map<String, Object> shards = (Map<String, Object>) indexBlock.get("shards");
            assertFalse("shards block must not be empty", shards.isEmpty());

            for (Map.Entry<String, Object> entry : shards.entrySet()) {
                List<Map<String, Object>> copies = (List<Map<String, Object>>) entry.getValue();
                Map<String, Object> primary = null;
                for (Map<String, Object> copy : copies) {
                    Map<String, Object> routing = (Map<String, Object>) copy.get("routing");
                    if (routing != null && Boolean.TRUE.equals(routing.get("primary"))) {
                        primary = copy;
                        break;
                    }
                }
                assertNotNull("each shard entry must have a primary copy", primary);

                Map<String, Object> merge = (Map<String, Object>) primary.get("merge");
                assertNotNull("primary shard must have merge block", merge);

                long chunkTotal = ((Number) merge.get("flush_and_sort_chunk_total")).longValue();
                long chunkTime = ((Number) merge.get("flush_and_sort_chunk_time_millis")).longValue();
                long rowIdMax = ((Number) merge.get("row_id_mapping_max")).longValue();

                assertTrue("flush_and_sort_chunk_total must be >= 1 after force_merge: " + chunkTotal, chunkTotal >= 1);
                assertTrue("flush_and_sort_chunk_time_millis must be >= 0: " + chunkTime, chunkTime >= 0);
                // row_id_mapping_max equals the row count of the largest merge output for this shard.
                // After a force-merge to 1 segment, that's at most the shard's total rows.
                assertTrue("row_id_mapping_max must be > 0 after force_merge: " + rowIdMax, rowIdMax > 0);
                assertTrue(
                    "row_id_mapping_max [" + rowIdMax + "] must not exceed total rows [" + expectedRows + "]",
                    rowIdMax <= expectedRows
                );
            }
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testFlushAndSortChunkAggregatesAtIndexLevel() throws Exception {
        String idx = "pr2-fchunk-idx-agg";
        createCompositeIndex(idx, true);

        for (int batch = 0; batch < 3; batch++) {
            indexDocs(idx, 40, batch * 40);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            // Sum of per-shard chunk_total values (level=shards)
            Map<String, Object> shardResp = parquetIndexStats(idx, "level", "shards");
            Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) shardResp.get("indices")).get(
                idx
            )).get("shards");
            long chunkTotalSum = 0;
            long chunkTimeSum = 0;
            long rowIdMaxAcross = 0;
            for (Map.Entry<String, Object> entry : shards.entrySet()) {
                List<Map<String, Object>> copies = (List<Map<String, Object>>) entry.getValue();
                for (Map<String, Object> copy : copies) {
                    Map<String, Object> routing = (Map<String, Object>) copy.get("routing");
                    if (routing == null || !Boolean.TRUE.equals(routing.get("primary"))) continue;
                    Map<String, Object> merge = (Map<String, Object>) copy.get("merge");
                    chunkTotalSum += ((Number) merge.get("flush_and_sort_chunk_total")).longValue();
                    chunkTimeSum += ((Number) merge.get("flush_and_sort_chunk_time_millis")).longValue();
                    rowIdMaxAcross = Math.max(rowIdMaxAcross, ((Number) merge.get("row_id_mapping_max")).longValue());
                }
            }

            // Index-level aggregate (default: no level=shards)
            Map<String, Object> idxResp = parquetIndexStats(idx);
            assertCounter(
                "index-level flush_and_sort_chunk_total = sum of primary shards",
                idxResp,
                "indices." + idx + ".merge.flush_and_sort_chunk_total",
                chunkTotalSum
            );
            assertCounter(
                "index-level flush_and_sort_chunk_time_millis = sum of primary shards",
                idxResp,
                "indices." + idx + ".merge.flush_and_sort_chunk_time_millis",
                chunkTimeSum
            );
            assertCounter(
                "index-level row_id_mapping_max = max-of-maxes (NOT sum)",
                idxResp,
                "indices." + idx + ".merge.row_id_mapping_max",
                rowIdMaxAcross
            );
        }, 30, TimeUnit.SECONDS);
    }

    // ===================================================================
    // Rayon native_runtime — node-level only
    // ===================================================================

    @SuppressWarnings("unchecked")
    public void testParquetMergeRayonStatsAfterMerge() throws Exception {
        String idx = "pr2-rayon-idx";
        createCompositeIndex(idx, true);

        for (int batch = 0; batch < 4; batch++) {
            indexDocs(idx, 40, batch * 40);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            Map<String, Object> resp = parquetNodeStats("");
            Map<String, Object> nodes = (Map<String, Object>) resp.get("nodes");
            assertFalse("nodes block must not be empty", nodes.isEmpty());

            Map<String, Object> nodeStats = (Map<String, Object>) nodes.values().iterator().next();
            Map<String, Object> nativeRuntime = (Map<String, Object>) nodeStats.get("native_runtime");
            assertNotNull("native_runtime block must be present", nativeRuntime);

            Map<String, Object> rayon = (Map<String, Object>) nativeRuntime.get("parquet_merge");
            assertNotNull("parquet_merge block must be present", rayon);

            long submitted = ((Number) rayon.get("merge_tasks_submitted")).longValue();
            long started = ((Number) rayon.get("merge_tasks_started")).longValue();
            long completed = ((Number) rayon.get("merge_tasks_completed")).longValue();
            long failed = ((Number) rayon.get("merge_tasks_failed")).longValue();
            long panicked = ((Number) rayon.get("merge_tasks_panicked")).longValue();
            long queueDepth = ((Number) rayon.get("merge_tasks_queue_depth")).longValue();
            long inFlight = ((Number) rayon.get("merge_tasks_in_flight")).longValue();

            assertTrue("merge_tasks_submitted must be >= 1: " + submitted, submitted >= 1);
            assertTrue("merge_tasks_started must be >= 1: " + started, started >= 1);
            assertTrue("merge_tasks_completed must be >= 1: " + completed, completed >= 1);
            assertEquals("merge_tasks_failed must be 0 in happy path", 0L, failed);
            assertEquals("merge_tasks_panicked must be 0 in happy path", 0L, panicked);

            // Derived: queue_depth = max(0, submitted - started). After merges complete, should be 0.
            assertEquals("queue_depth derivation: max(0, submitted - started)", Math.max(0L, submitted - started), queueDepth);
            assertEquals(
                "in_flight derivation: max(0, started - completed - failed - panicked)",
                Math.max(0L, started - completed - failed - panicked),
                inFlight
            );
            // After force_merge has settled, no merges in flight.
            assertEquals("in_flight should be 0 after merges complete", 0L, inFlight);
        }, 30, TimeUnit.SECONDS);
    }

    // ===================================================================
    // Tokio parquet_io — new fields present + non-negative
    // ===================================================================

    @SuppressWarnings("unchecked")
    public void testParquetIoNewTokioFieldsPresent() throws Exception {
        String idx = "pr2-tokio-idx";
        createCompositeIndex(idx, true);
        for (int batch = 0; batch < 3; batch++) {
            indexDocs(idx, 30, batch * 30);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            Map<String, Object> resp = parquetNodeStats("");
            Map<String, Object> nodes = (Map<String, Object>) resp.get("nodes");
            Map<String, Object> nodeStats = (Map<String, Object>) nodes.values().iterator().next();
            Map<String, Object> tokio = (Map<String, Object>) ((Map<String, Object>) nodeStats.get("native_runtime")).get("parquet_io");
            assertNotNull("parquet_io block must be present", tokio);

            // All fields present (existing + new from Task 5).
            for (String field : new String[] {
                "num_workers",
                "num_blocking_threads",
                "active_tasks",
                "global_queue_depth",
                "blocking_queue_depth",
                "local_queue_depth_total",
                "polls_count_total",
                "overflow_count_total",
                "spawned_tasks_total",
                "workers_busy_millis_total" }) {
                assertNotNull("parquet_io must contain field: " + field, tokio.get(field));
            }

            long workers = ((Number) tokio.get("num_workers")).longValue();
            long polls = ((Number) tokio.get("polls_count_total")).longValue();
            long busyMillis = ((Number) tokio.get("workers_busy_millis_total")).longValue();

            assertTrue("num_workers must be > 0 once tokio runtime is initialized: " + workers, workers > 0);
            // After IO activity (merge writes via spawn_blocking), the runtime polls tasks.
            assertTrue("polls_count_total must be > 0 after merge IO: " + polls, polls > 0);
            assertTrue("workers_busy_millis_total must be >= 0: " + busyMillis, busyMillis >= 0);

            // Snapshot fields must be >= 0 (current depth, not cumulative).
            assertTrue("blocking_queue_depth must be >= 0", ((Number) tokio.get("blocking_queue_depth")).longValue() >= 0);
            assertTrue("local_queue_depth_total must be >= 0", ((Number) tokio.get("local_queue_depth_total")).longValue() >= 0);
            assertTrue("overflow_count_total must be >= 0", ((Number) tokio.get("overflow_count_total")).longValue() >= 0);
        }, 30, TimeUnit.SECONDS);
    }

    // ===================================================================
    // Negative tests — fields gated correctly
    // ===================================================================

    @SuppressWarnings("unchecked")
    public void testFlushAndSortChunkPresentAtAllShardIndexLevels() throws Exception {
        // Sanity: per-shard fields must appear at every level (shard / index / node-aggregate).
        // Distinct from row_id_mapping_max which uses max-of-maxes aggregation.
        String idx = "pr2-fields-everywhere-idx";
        createCompositeIndex(idx, true);
        for (int batch = 0; batch < 2; batch++) {
            indexDocs(idx, 30, batch * 30);
            refreshIndex(idx);
        }
        forceMerge(idx, 1);

        assertBusy(() -> {
            // Index-level aggregate
            Map<String, Object> idxResp = parquetIndexStats(idx);
            assertCounterPresent(
                "flush_and_sort_chunk_total at index level",
                idxResp,
                "indices." + idx + ".merge.flush_and_sort_chunk_total"
            );
            assertCounterPresent(
                "flush_and_sort_chunk_time_millis at index level",
                idxResp,
                "indices." + idx + ".merge.flush_and_sort_chunk_time_millis"
            );
            assertCounterPresent("row_id_mapping_max at index level", idxResp, "indices." + idx + ".merge.row_id_mapping_max");

            // Shard level
            Map<String, Object> shardResp = parquetIndexStats(idx, "level", "shards");
            Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) shardResp.get("indices")).get(
                idx
            )).get("shards");
            for (Map.Entry<String, Object> entry : shards.entrySet()) {
                List<Map<String, Object>> copies = (List<Map<String, Object>>) entry.getValue();
                for (Map<String, Object> copy : copies) {
                    Map<String, Object> merge = (Map<String, Object>) copy.get("merge");
                    assertNotNull("merge block exists on shard copy", merge);
                    assertNotNull("flush_and_sort_chunk_total exists on shard", merge.get("flush_and_sort_chunk_total"));
                    assertNotNull("flush_and_sort_chunk_time_millis exists on shard", merge.get("flush_and_sort_chunk_time_millis"));
                    assertNotNull("row_id_mapping_max exists on shard", merge.get("row_id_mapping_max"));
                }
            }

            // Node level — also has these per-shard counters summed
            Map<String, Object> nodeResp = parquetNodeStats("");
            Map<String, Object> nodes = (Map<String, Object>) nodeResp.get("nodes");
            for (Object n : nodes.values()) {
                Map<String, Object> nodeStats = (Map<String, Object>) n;
                Map<String, Object> merge = (Map<String, Object>) nodeStats.get("merge");
                if (merge != null) {
                    assertNotNull("flush_and_sort_chunk_total on node", merge.get("flush_and_sort_chunk_total"));
                    assertNotNull("flush_and_sort_chunk_time_millis on node", merge.get("flush_and_sort_chunk_time_millis"));
                    assertNotNull("row_id_mapping_max on node", merge.get("row_id_mapping_max"));
                }
            }
        }, 30, TimeUnit.SECONDS);
    }
}
