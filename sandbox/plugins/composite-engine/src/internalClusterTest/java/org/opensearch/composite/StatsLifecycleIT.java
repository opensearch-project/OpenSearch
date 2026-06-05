/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for stats API lifecycle behavior on a single-node, single-shard topology.
 * Tests counter accuracy across indexing, refresh, flush, merge stages and REST contract negatives.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class StatsLifecycleIT extends BaseStatsIT {

    /**
     * Flagship accuracy test: walks through 7 stages of index lifecycle and asserts
     * counters increment correctly for both parquet and lucene formats.
     */
    public void testStatsLifecycle_SingleShard() throws Exception {
        createCompositeIndex("lifecycle-idx", true);

        // Stage 0 — fresh index
        Map<String, Object> r0 = parquetIndexStats("lifecycle-idx");
        assertCounter("stage 0 docs", r0, "indices.lifecycle-idx.indexing.docs_indexed_total", 0L);
        assertCounter("stage 0 native_write", r0, "indices.lifecycle-idx.native_write.native_write_total", 0L);
        Map<String, Object> l0 = luceneIndexStats("lifecycle-idx");
        assertCounter("stage 0 lucene refresh", l0, "indices.lifecycle-idx.refresh.refresh_total", 0L);

        // Stage 1 — index 100 docs (no refresh yet)
        indexDocs("lifecycle-idx", 100, 0);
        Map<String, Object> r1 = parquetIndexStats("lifecycle-idx");
        assertCounter("stage 1 parquet docs", r1, "indices.lifecycle-idx.indexing.docs_indexed_total", 100L);
        assertCounterAtLeast("stage 1 parquet index_time", r1, "indices.lifecycle-idx.indexing.index_time_millis", 0L);
        // Note: ParquetShardStats does NOT have docs_indexed_failures — only lucene does
        Map<String, Object> l1 = luceneIndexStats("lifecycle-idx");
        assertCounter("stage 1 lucene docs", l1, "indices.lifecycle-idx.indexing.docs_indexed_total", 100L);
        assertCounter("stage 1 lucene refresh_total", l1, "indices.lifecycle-idx.refresh.refresh_total", 0L);

        // Stage 2 — refresh
        refreshIndex("lifecycle-idx");
        Map<String, Object> r2 = parquetIndexStats("lifecycle-idx");
        assertCounterAtLeast("stage 2 native_write", r2, "indices.lifecycle-idx.native_write.native_write_total", 1L);
        assertCounterAtLeast("stage 2 native_finalize", r2, "indices.lifecycle-idx.native_write.native_finalize_total", 1L);
        Map<String, Object> l2 = luceneIndexStats("lifecycle-idx");
        assertCounterAtLeast("stage 2 lucene refresh_total", l2, "indices.lifecycle-idx.refresh.refresh_total", 1L);
        assertCounterAtLeast("stage 2 lucene refresh_time", l2, "indices.lifecycle-idx.refresh.refresh_time_millis", 0L);

        // Stage 3 — flush
        flushIndex("lifecycle-idx");
        Map<String, Object> l3 = luceneIndexStats("lifecycle-idx");
        assertCounterAtLeast("stage 3 lucene flush", l3, "indices.lifecycle-idx.flush.flush_total", 1L);
        assertCounterAtLeast("stage 3 lucene commit", l3, "indices.lifecycle-idx.commit.commit_total", 1L);
        // commit_failures must stay 0 in the happy path.
        assertCounter("stage 3 lucene commit_failures", l3, "indices.lifecycle-idx.commit.commit_failures", 0L);

        // Stage 4 — index 4 more batches with refresh between each, to produce 5 parquet files total.
        // Multiple parquet files are needed for force_merge to actually invoke a parquet merge:
        // policy doesn't merge fewer than 2 files, and we want headroom above the threshold.
        for (int batch = 1; batch <= 4; batch++) {
            indexDocs("lifecycle-idx", 40, 100 + (batch - 1) * 40);
            refreshIndex("lifecycle-idx");
        }
        Map<String, Object> r4 = parquetIndexStats("lifecycle-idx");
        assertCounter("stage 4 parquet docs accumulate", r4, "indices.lifecycle-idx.indexing.docs_indexed_total", 260L);
        Map<String, Object> l4 = luceneIndexStats("lifecycle-idx");
        assertCounter("stage 4 lucene docs accumulate", l4, "indices.lifecycle-idx.indexing.docs_indexed_total", 260L);
        assertCounterAtLeast("stage 4 lucene refresh count", l4, "indices.lifecycle-idx.refresh.refresh_total", 5L);

        // Stage 5 — force_merge to 1 segment. Parquet merges asynchronously off the force_merge call,
        // so wrap merge-counter assertions in assertBusy to wait for the native merge to finalize.
        forceMerge("lifecycle-idx", 1);
        assertBusy(() -> {
            Map<String, Object> r5 = parquetIndexStats("lifecycle-idx");
            assertCounterAtLeast("stage 5 parquet merge_total", r5, "indices.lifecycle-idx.merge.merge_total", 1L);
            assertCounterAtLeast("stage 5 parquet merge_input_files", r5, "indices.lifecycle-idx.merge.merge_input_files_total", 2L);
            assertCounter("stage 5 parquet merge_failures", r5, "indices.lifecycle-idx.merge.merge_failures", 0L);
            // per-shard merge metrics — must populate after force_merge runs at least one merge.
            assertCounterAtLeast(
                "stage 5 parquet flush_and_sort_chunk_total",
                r5,
                "indices.lifecycle-idx.merge.flush_and_sort_chunk_total",
                1L
            );
            assertCounterAtLeast(
                "stage 5 parquet flush_and_sort_chunk_time_millis",
                r5,
                "indices.lifecycle-idx.merge.flush_and_sort_chunk_time_millis",
                0L
            );
            // row_id_mapping_max equals the largest merge output's row count for this shard;
            // after merging 5 batches of 40 docs (=260 total) on a single-shard index, max is 260.
            assertCounter(
                "stage 5 parquet row_id_mapping_max equals merged row count",
                r5,
                "indices.lifecycle-idx.merge.row_id_mapping_max",
                260L
            );
            Map<String, Object> l5 = luceneIndexStats("lifecycle-idx");
            // Lucene merger fires per shard during composite force_merge — CompositeMerger
            // dispatches to both the primary (parquet) and secondary (lucene) mergers. We
            // expect merge_total >= number_of_primary_shards (1 here, single-shard test).
            // flush.flush_force_merge_time_millis also records time spent in the internal
            // force-merge during flush.
            assertCounterAtLeast(
                "stage 5 lucene merge_total >= 1 (composite force_merge dispatches to lucene merger too)",
                l5,
                "indices.lifecycle-idx.merge.merge_total",
                1L
            );
            assertCounterAtLeast(
                "stage 5 lucene flush_force_merge_time_millis",
                l5,
                "indices.lifecycle-idx.flush.flush_force_merge_time_millis",
                1L
            );
            assertCounter("stage 5 lucene merge_failures", l5, "indices.lifecycle-idx.merge.merge_failures", 0L);
        }, 30, java.util.concurrent.TimeUnit.SECONDS);

        // Stage 6 — final cross-format consistency
        Map<String, Object> rf = parquetIndexStats("lifecycle-idx");
        Map<String, Object> lf = luceneIndexStats("lifecycle-idx");
        long parquetDocs = getCounter(rf, "indices.lifecycle-idx.indexing.docs_indexed_total");
        long luceneDocs = getCounter(lf, "indices.lifecycle-idx.indexing.docs_indexed_total");
        assertEquals("cross-format doc count must match", parquetDocs, luceneDocs);
        assertEquals(260L, parquetDocs);
        // All failure counters zero in happy path
        assertCounter("happy parquet native_write_failures", rf, "indices.lifecycle-idx.native_write.native_write_failures", 0L);
        assertCounter("happy parquet merge_failures", rf, "indices.lifecycle-idx.merge.merge_failures", 0L);
        assertCounter("happy lucene merge_failures", lf, "indices.lifecycle-idx.merge.merge_failures", 0L);
    }

    /**
     * Verifies that stats for multiple indices are isolated — querying one index
     * returns only that index's counters.
     */
    public void testMultipleIndicesIsolation() throws Exception {
        createCompositeIndex("idx1", true);
        createCompositeIndex("idx2", true);
        createCompositeIndex("idx3", true);

        indexDocs("idx1", 10, 0);
        indexDocs("idx2", 20, 0);
        indexDocs("idx3", 30, 0);
        refreshIndex("idx1");
        refreshIndex("idx2");
        refreshIndex("idx3");

        assertCounter("parquet idx1", parquetIndexStats("idx1"), "indices.idx1.indexing.docs_indexed_total", 10L);
        assertCounter("parquet idx2", parquetIndexStats("idx2"), "indices.idx2.indexing.docs_indexed_total", 20L);
        assertCounter("parquet idx3", parquetIndexStats("idx3"), "indices.idx3.indexing.docs_indexed_total", 30L);
        assertCounter("lucene idx1", luceneIndexStats("idx1"), "indices.idx1.indexing.docs_indexed_total", 10L);
        assertCounter("lucene idx2", luceneIndexStats("idx2"), "indices.idx2.indexing.docs_indexed_total", 20L);
        assertCounter("lucene idx3", luceneIndexStats("idx3"), "indices.idx3.indexing.docs_indexed_total", 30L);
    }

    /**
     * Tests REST contract negative cases: unknown format, wrong HTTP method,
     * nonexistent index, response shape, and content-type.
     */
    public void testRESTContractNegatives() throws Exception {
        createCompositeIndex("contract-idx", true);
        indexDocs("contract-idx", 5, 0);
        refreshIndex("contract-idx");

        // 1. Unknown format -> 400
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/orc/contract-idx/_stats"));
            fail("expected 400 for unknown format");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }

        // 2. Wrong method POST -> 405
        try {
            getRestClient().performRequest(new Request("POST", "/_plugins/parquet/contract-idx/_stats"));
            fail("expected 405 for POST");
        } catch (ResponseException e) {
            assertEquals(405, e.getResponse().getStatusLine().getStatusCode());
        }

        // 3. Nonexistent index -> 4xx
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/no-such-idx/_stats"));
            fail("expected 4xx for nonexistent index");
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            assertTrue("expected 4xx, got " + code, code >= 400 && code < 500);
        }

        // 4. Verify a successful all-shards query returns proper structure.
        // (We use a wildcard pattern; bare /_plugins/parquet/_stats is not a registered route.)
        Response wildcardResponse = getRaw("/_plugins/parquet/*/_stats");
        Map<String, Object> wildcardBody = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            org.opensearch.common.xcontent.json.JsonXContent.jsonXContent,
            wildcardResponse.getEntity().getContent(),
            true
        );
        assertTrue("response must have 'indices' key", wildcardBody.containsKey("indices"));

        // 5. Content-Type assertion
        Response successResponse = getRaw("/_plugins/parquet/contract-idx/_stats");
        String contentType = successResponse.getEntity().getContentType();
        assertTrue("Content-Type must be application/json, got: " + contentType, contentType.startsWith("application/json"));
    }

    /**
     * Verifies that parquet primary and lucene secondary see the same operations.
     */
    public void testCrossFormatConsistency() throws Exception {
        createCompositeIndex("cross-format-idx", true);
        indexDocs("cross-format-idx", 50, 0);
        refreshIndex("cross-format-idx");
        flushIndex("cross-format-idx");

        Map<String, Object> p = parquetIndexStats("cross-format-idx");
        Map<String, Object> l = luceneIndexStats("cross-format-idx");

        long parquetDocs = getCounter(p, "indices.cross-format-idx.indexing.docs_indexed_total");
        long luceneDocs = getCounter(l, "indices.cross-format-idx.indexing.docs_indexed_total");
        assertEquals("both formats must observe identical doc count", parquetDocs, luceneDocs);
        assertEquals(50L, parquetDocs);

        // Refresh fires native_finalize on parquet AND refresh_total on lucene.
        assertCounterAtLeast("parquet finalize after refresh", p, "indices.cross-format-idx.native_write.native_finalize_total", 1L);
        assertCounterAtLeast("lucene refresh after refresh", l, "indices.cross-format-idx.refresh.refresh_total", 1L);

        // Flush fires lucene flush+commit; parquet does not have a flush counter (it finalizes per refresh).
        assertCounterAtLeast("lucene flush after flush", l, "indices.cross-format-idx.flush.flush_total", 1L);
        assertCounterAtLeast("lucene commit after flush", l, "indices.cross-format-idx.commit.commit_total", 1L);
    }

    /**
     * Verifies counters increment correctly across multiple write batches — no resets, no drift.
     */
    public void testCountersAccumulateAcrossOperations() throws Exception {
        createCompositeIndex("accumulate-idx", true);

        // Batch 1: 30 docs, refresh, capture v1
        indexDocs("accumulate-idx", 30, 0);
        refreshIndex("accumulate-idx");
        long v1Parquet = getCounter(parquetIndexStats("accumulate-idx"), "indices.accumulate-idx.indexing.docs_indexed_total");
        long v1Lucene = getCounter(luceneIndexStats("accumulate-idx"), "indices.accumulate-idx.indexing.docs_indexed_total");
        long v1Refresh = getCounter(luceneIndexStats("accumulate-idx"), "indices.accumulate-idx.refresh.refresh_total");
        assertEquals(30L, v1Parquet);
        assertEquals(30L, v1Lucene);
        assertTrue("refresh_total must be at least 1 after batch 1", v1Refresh >= 1L);

        // Batch 2: 70 more docs, refresh, capture v2
        indexDocs("accumulate-idx", 70, 30);
        refreshIndex("accumulate-idx");
        long v2Parquet = getCounter(parquetIndexStats("accumulate-idx"), "indices.accumulate-idx.indexing.docs_indexed_total");
        long v2Lucene = getCounter(luceneIndexStats("accumulate-idx"), "indices.accumulate-idx.indexing.docs_indexed_total");
        long v2Refresh = getCounter(luceneIndexStats("accumulate-idx"), "indices.accumulate-idx.refresh.refresh_total");

        // Doc counters must be exactly v1 + 70.
        assertEquals("parquet docs accumulate exactly", v1Parquet + 70L, v2Parquet);
        assertEquals("lucene docs accumulate exactly", v1Lucene + 70L, v2Lucene);
        // Refresh count must increase strictly (we issued at least one more refresh).
        assertTrue("refresh count must strictly increase: v1=" + v1Refresh + ", v2=" + v2Refresh, v2Refresh > v1Refresh);
    }

    /**
     * Verifies a freshly-created index with no operations has all counters at zero.
     */
    public void testEmptyIndexAllCountersZero() throws Exception {
        createCompositeIndex("empty-idx", true);

        Map<String, Object> p = parquetIndexStats("empty-idx");
        Map<String, Object> l = luceneIndexStats("empty-idx");

        // Every parquet counter that's been wired must be zero, not absent (catches tracker-not-registered bugs).
        assertCounter("parquet docs zero", p, "indices.empty-idx.indexing.docs_indexed_total", 0L);
        assertCounter("parquet index_time zero", p, "indices.empty-idx.indexing.index_time_millis", 0L);
        assertCounter("parquet native_write_total zero", p, "indices.empty-idx.native_write.native_write_total", 0L);
        assertCounter("parquet native_finalize_total zero", p, "indices.empty-idx.native_write.native_finalize_total", 0L);
        assertCounter("parquet merge_total zero", p, "indices.empty-idx.merge.merge_total", 0L);
        assertCounter("parquet vsr_rotations_total zero", p, "indices.empty-idx.vsr.vsr_rotations_total", 0L);

        // Same for lucene. Note: index creation may trigger an initial commit, so commit_total may be 0 or 1.
        assertCounter("lucene docs zero", l, "indices.empty-idx.indexing.docs_indexed_total", 0L);
        assertCounter("lucene refresh_total zero", l, "indices.empty-idx.refresh.refresh_total", 0L);
        assertCounter("lucene flush_total zero", l, "indices.empty-idx.flush.flush_total", 0L);
        assertCounter("lucene merge_total zero", l, "indices.empty-idx.merge.merge_total", 0L);
        // commit_total may be 0 or 1 — index creation can trigger an initial Lucene commit.
        long commitTotal = getCounter(l, "indices.empty-idx.commit.commit_total");
        assertTrue("lucene commit_total must be 0 or 1 for empty index, got " + commitTotal, commitTotal >= 0L && commitTotal <= 1L);
    }

    /**
     * Verifies that stats counters reset to 0 when an index is deleted and recreated with the
     * same name. Catches tracker leaks in the per-format provider registry.
     */
    public void testCountersResetOnIndexDelete() throws Exception {
        String idx = "reset-on-delete-idx";
        createCompositeIndex(idx, true);
        indexDocs(idx, 100, 0);
        refreshIndex(idx);

        // Pre-condition: counters reflect the indexed docs.
        assertCounter("pre-delete parquet docs", parquetIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total", 100L);
        assertCounter("pre-delete lucene docs", luceneIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total", 100L);

        // Delete and recreate same name.
        client().admin().indices().prepareDelete(idx).get();
        createCompositeIndex(idx, true);

        // Post-condition: counters must be 0 — no carryover from the deleted index's tracker.
        assertCounter("post-recreate parquet docs zero", parquetIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total", 0L);
        assertCounter("post-recreate lucene docs zero", luceneIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total", 0L);
        assertCounter(
            "post-recreate parquet native_write zero",
            parquetIndexStats(idx),
            "indices." + idx + ".native_write.native_write_total",
            0L
        );
        assertCounter("post-recreate lucene refresh zero", luceneIndexStats(idx), "indices." + idx + ".refresh.refresh_total", 0L);
    }

    /**
     * Race-condition safety: 4 threads index 250 docs each concurrently. After all threads
     * complete, docs_indexed_total must be exactly 1000.
     *
     * <p>A separate querier thread polls the stats endpoint at ~1ms intervals while indexing
     * is in flight and asserts the docs_indexed_total counter never goes backwards. This is a
     * best-effort check: a transient backwards-jump that resolves within a poll window could
     * still slip through. The primary correctness invariant is the final-count assertion at
     * the end of the test.
     */
    public void testConcurrentIndexingAccuracy() throws Exception {
        String idx = "concurrent-idx";
        createCompositeIndex(idx, true);

        final int threadCount = 4;
        final int docsPerThread = 250;
        final int totalDocs = threadCount * docsPerThread;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        int docId = threadId * docsPerThread + i;
                        client().prepareIndex(idx).setId(String.valueOf(docId)).setSource("name", "doc_" + docId, "value", docId).get();
                    }
                } catch (Exception e) {
                    throw new RuntimeException("thread " + threadId + " failed", e);
                }
                return null;
            }));
        }

        // Concurrent stats queries while indexing — best-effort monotonicity check at ~1ms cadence.
        // Single querier thread: read-then-update of `lastSeen` is safe without compare-and-swap
        // because there is no other writer.
        AtomicLong lastSeen = new AtomicLong(0);
        AtomicBoolean keepQuerying = new AtomicBoolean(true);
        AtomicReference<Throwable> queryError = new AtomicReference<>();
        Thread querier = new Thread(() -> {
            while (keepQuerying.get()) {
                try {
                    Map<String, Object> r = parquetIndexStats(idx);
                    long current = getCounter(r, "indices." + idx + ".indexing.docs_indexed_total");
                    long prev = lastSeen.get();
                    if (current < prev) {
                        throw new AssertionError("counter went backwards: prev=" + prev + ", current=" + current);
                    }
                    lastSeen.set(Math.max(prev, current));
                    Thread.sleep(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    queryError.set(t);
                    return;
                }
            }
        });
        querier.start();

        // Release indexer threads.
        start.countDown();
        for (Future<?> f : futures) {
            f.get(90, TimeUnit.SECONDS);
        }
        pool.shutdown();
        keepQuerying.set(false);
        querier.join(5000);

        // Verify no querier errors.
        if (queryError.get() != null) {
            throw new AssertionError("querier thread saw an error", queryError.get());
        }

        // Final accuracy check.
        refreshIndex(idx);
        assertCounter(
            "final docs_indexed_total must be exactly " + totalDocs,
            parquetIndexStats(idx),
            "indices." + idx + ".indexing.docs_indexed_total",
            (long) totalDocs
        );
        assertCounter(
            "lucene final must match",
            luceneIndexStats(idx),
            "indices." + idx + ".indexing.docs_indexed_total",
            (long) totalDocs
        );
    }

    /**
     * Verifies that a plain Lucene index (no composite engine) returns 200 with effectively-empty stats.
     */
    public void testNonCompositeIndexReturnsEmpty() throws Exception {
        // Create index WITHOUT composite engine — plain Lucene only.
        client().admin()
            .indices()
            .prepareCreate("plain-idx")
            .setSettings(
                org.opensearch.common.settings.Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .setMapping("name", "type=keyword")
            .get();
        ensureGreen("plain-idx");

        // Index some docs — these go to plain Lucene, not DFA.
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("plain-idx").setId(String.valueOf(i)).setSource("name", "doc_" + i).get();
        }
        refreshIndex("plain-idx");

        // Both stats endpoints must return HTTP 200 with no parquet/lucene tracker data for this index.
        Map<String, Object> p = parquetIndexStats("plain-idx");
        Map<String, Object> l = luceneIndexStats("plain-idx");

        // Response must contain the indices block. The plain-idx entry must NOT have any DFA
        // counters: with no tracker registered, FormatStatsResponse aggregation produces an
        // empty object for the index, so docs_indexed_total is absent (getCounter returns -1L).
        // Accepting -1 OR 0 makes the assertion robust to future shape changes (e.g., emitting
        // explicit zeros rather than omitting fields).
        long parquetDocs = getCounter(p, "indices.plain-idx.indexing.docs_indexed_total");
        long luceneDocs = getCounter(l, "indices.plain-idx.indexing.docs_indexed_total");
        assertTrue("parquet docs must be absent (-1) or 0 for non-composite index, got " + parquetDocs, parquetDocs <= 0L);
        assertTrue("lucene docs must be absent (-1) or 0 for non-composite index, got " + luceneDocs, luceneDocs <= 0L);

        // The response must NOT have a server error.
        assertTrue("response must contain 'indices' key", p.containsKey("indices"));
        assertTrue("response must contain 'indices' key", l.containsKey("indices"));
    }
}
