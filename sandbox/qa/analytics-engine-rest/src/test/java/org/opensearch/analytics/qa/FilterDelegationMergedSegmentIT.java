/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduces the filter-delegation failure observed on the big5 PPL workload:
 *
 * <pre>
 *   StreamException[errorCode=INTERNAL, ... FfmSegmentCollector::create(provider=1,
 *   writer_generation=N, doc_range=[0,M)): createCollector(...) failed: -1]
 * </pre>
 *
 * <p>Root cause (pre-#21845): {@code FilterTreeCallbacks} routed FFM upcalls through a
 * <b>global</b> {@code AtomicReference} handle. Under <b>concurrent</b> indexed-path
 * queries it was overwritten by the last query to start, so one query's
 * {@code createCollector(writer_generation=N)} could land on another query's
 * {@code LuceneFilterDelegationHandle} whose {@code generationToSegmentName} has no entry
 * for {@code N} — returning {@code -1}. #21845 threads a per-query {@code context_id} so
 * each query gets isolated bindings.
 *
 * <p>Why serial single-index delegation tests (incl. {@link FilterDelegationIT}) pass:
 * <ul>
 *   <li>The race needs <em>concurrent</em> indexed-path queries with <em>different</em>
 *       segment generations, so cross-routing hits a missing generation.</li>
 *   <li>The composite engine also merges-on-refresh under
 *       {@code index.composite.merge_on_refresh_max_size} (default 10MB); small tests
 *       collapse to a single aligned segment. We set it to 0 so each flushed batch is its
 *       own segment/generation.</li>
 * </ul>
 *
 * <p>{@link #testConcurrentDelegationAcrossTwoIndices} fires delegated queries against two
 * differently-segmented indices from many threads at once. On pre-#21845 code this throws
 * the {@code createCollector ... -1} StreamException; with #21845 present it passes.
 */
public class FilterDelegationMergedSegmentIT extends AnalyticsRestTestCase {

    private static final String INDEX_A = "filter_delegation_a";
    private static final String INDEX_B = "filter_delegation_b";

    /** Multiple distinct flushed segments (no merge) — serial multi-segment delegation. */
    public void testMatchDelegationAcrossMultipleSegments() throws Exception {
        createCompositeIndex(INDEX_A);
        indexFlushedBatch(INDEX_A, "hello world", 5, 10);
        indexFlushedBatch(INDEX_A, "goodbye world", 3, 10);
        indexFlushedBatch(INDEX_A, "hello there", 5, 10);
        assertMatchHelloCountIs(INDEX_A, 20L);
    }

    /** Same data, then force-merged to a single segment — serial merged-segment delegation. */
    public void testMatchDelegationAfterForceMerge() throws Exception {
        createCompositeIndex(INDEX_A);
        indexFlushedBatch(INDEX_A, "hello world", 5, 10);
        indexFlushedBatch(INDEX_A, "goodbye world", 3, 10);
        indexFlushedBatch(INDEX_A, "hello there", 5, 10);
        forceMergeToOneSegment(INDEX_A);
        assertMatchHelloCountIs(INDEX_A, 20L);
    }

    /**
     * Concurrent delegated queries across two indices with different segment generations.
     * This is the faithful repro of the pre-#21845 global-handle race: cross-routed
     * {@code createCollector} upcalls hit a generation the other query's handle doesn't
     * know, returning -1 (HTTP 500). With #21845 present, all queries succeed.
     */
    public void testConcurrentDelegationAcrossTwoIndices() throws Exception {
        // Index A: 6 "hello" segments (count of match=hello -> 60). Index B: 6 "goodbye"
        // segments + different gen layout so a cross-routed generation is absent.
        createCompositeIndex(INDEX_A);
        createCompositeIndex(INDEX_B);
        for (int i = 0; i < 6; i++) {
            indexFlushedBatch(INDEX_A, "hello world", 5, 10);
            indexFlushedBatch(INDEX_B, "goodbye world", 3, 10);
            indexFlushedBatch(INDEX_B, "farewell world", 3, 10); // B gets 2x segments -> distinct gens
        }

        int threads = 8, perThread = 15;
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final String idx = (t % 2 == 0) ? INDEX_A : INDEX_B;
            Thread w = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread && failure.get() == null; i++) {
                        executePplViaShim("source = " + idx + " | where match(message, 'world') | stats count() as c");
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers.add(w);
            w.start();
        }
        start.countDown(); // release all threads together to maximize overlap
        for (Thread w : workers) {
            w.join();
        }
        if (failure.get() != null) {
            throw new AssertionError("concurrent delegated query failed (filter-delegation race)", failure.get());
        }
    }

    private void assertMatchHelloCountIs(String index, long expected) throws Exception {
        String ppl = "source = " + index + " | where match(message, 'hello') | stats count() as c";
        Map<String, Object> result = executePplViaShim(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());
        assertEquals("count of match(message,'hello') docs", expected, ((Number) rows.get(0).get(0)).longValue());
    }

    private void createCompositeIndex(String index) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\","
            // Disable inline merge-on-refresh so each flushed batch stays its own segment /
            // writer_generation — the multi-segment topology the failure needs.
            + "  \"index.composite.merge_on_refresh_max_size\": \"0\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"tag\": { \"type\": \"keyword\" },"
            + "    \"value\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index " + index);
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /** Bulk-index {@code count} docs then force a flush so each batch lands in its own segment. */
    private void indexFlushedBatch(String index, String message, int value, int count) throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < count; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"").append(message).append("\", \"tag\": \"t\", \"value\": ").append(value).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        assertOkAndParse(client().performRequest(bulkRequest), "bulk [" + message + "]");
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }

    private void forceMergeToOneSegment(String index) throws Exception {
        Request forceMerge = new Request("POST", "/" + index + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        forceMerge.addParameter("flush", "true");
        client().performRequest(forceMerge);
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    }
}
