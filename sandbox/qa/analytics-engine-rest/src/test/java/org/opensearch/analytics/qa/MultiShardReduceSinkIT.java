/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reproduction for the multi-shard coordinator-reduce failures: the reduce/exchange sink is
 * built with a Substrait schema that disagrees with the shard fragments' Arrow schema
 * ({@code Field '$f1' ... Int32 ... table schema ... Int64}), so any query that needs a
 * coordinator-side merge of 2+ shard streams breaks.
 *
 * <p><b>Observed symptoms (all verified on {@code main} @ {@code 3570f22f064}, single node,
 * composite parquet+lucene index — single-shard controls all pass):</b>
 * <ol>
 *   <li><b>Sorted queries return 0 rows, deterministically.</b> The QTF/late-materialization
 *       stage starts ({@code LMStage CREATED}), the coordinator reduce fails, all rows are
 *       lost, and the query still returns HTTP 200 with an empty result — silent data loss.
 *       Via DSL {@code _search} the server log shows
 *       {@code RuntimeException: Failed to create exchange sink for stageId=1} caused by the
 *       Substrait Int32/Int64 mismatch ({@code ReduceStageExecutionFactory.createExecution} →
 *       {@code DatafusionReduceSink.<init>} → {@code NativeBridge.executeLocalPlan}); via the
 *       PPL frontend the same empty result appears with no error logged at all.</li>
 *   <li><b>Aggregations fail with HTTP 500</b> via DSL {@code _search}, with the same
 *       exchange-sink error. (Additionally observed manually on a single-node dev cluster:
 *       the same aggregation via PPL returns HTTP 200 with numbers that vary run to run —
 *       the reduce merges an inconsistent subset of shard streams. That variant is not
 *       asserted here because it does not manifest deterministically in this harness.)</li>
 * </ol>
 *
 * <p>2 shards are sufficient (the reduce sink is only constructed when 2+ shard streams need
 * merging, which is why single-shard testing never sees this), and ~20k documents are plenty —
 * the failure is at plan-build time, not data-volume dependent. Frontend-independent:
 * reproduces via DSL and PPL alike.
 *
 * @opensearch.internal
 */
public class MultiShardReduceSinkIT extends AnalyticsRestTestCase {

    private static final String INDEX_1SHARD = "reduce_sink_1shard";
    private static final String INDEX_2SHARD = "reduce_sink_2shard";
    private static final int TOTAL_DOCS = 20_000;
    private static final int BULK_BATCH = 5_000;
    private static final int BRANDS = 5;

    private static boolean seeded = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (seeded) {
            return;
        }
        createIndex(INDEX_1SHARD, 1);
        createIndex(INDEX_2SHARD, 2);
        seedDocs(INDEX_1SHARD);
        seedDocs(INDEX_2SHARD);
        seeded = true;
    }

    /**
     * Symptom 1: a plain sorted query on the 2-shard index returns 0 rows (HTTP 200 — silent
     * loss). The identical query on the 1-shard index returns the requested 10 rows.
     */
    public void testSortedQueryReturnsRowsOnMultiShardIndex() throws Exception {
        List<Object> oneShard = datarows(executePplViaShim("source=" + INDEX_1SHARD + " | sort - price | head 10"));
        assertEquals("1-shard control must return the page", 10, oneShard.size());

        List<Object> twoShard = datarows(executePplViaShim("source=" + INDEX_2SHARD + " | sort - price | head 10"));
        assertEquals("2-shard sorted query must return the page (currently returns 0 rows)", 10, twoShard.size());
    }

    /**
     * Symptom 2: the same aggregation through the DSL `_search` surface fails outright with
     * HTTP 500 ({@code Failed to create exchange sink for stageId=1}, caused by the Substrait
     * Int32/Int64 schema mismatch) instead of returning buckets.
     */
    public void testDslAggregationSucceedsOnMultiShardIndex() throws Exception {
        Request search = new Request("POST", "/" + INDEX_2SHARD + "/_search");
        search.setJsonEntity(
            "{\"size\":0,\"aggs\":{\"by_brand\":{\"terms\":{\"field\":\"brand\"},"
                + "\"aggs\":{\"avg_price\":{\"avg\":{\"field\":\"price\"}}}}}}"
        );
        try {
            Response response = client().performRequest(search);
            assertEquals("multi-shard DSL aggregation must succeed", 200, response.getStatusLine().getStatusCode());
        } catch (ResponseException e) {
            fail(
                "multi-shard DSL aggregation returned "
                    + e.getResponse().getStatusLine().getStatusCode()
                    + " (expected 200); server log shows 'Failed to create exchange sink for stageId=1"
                    + " ... Substrait ... Int32 ... Int64': "
                    + e.getMessage()
            );
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<Object> datarows(Map<String, Object> pplResponse) {
        List<Object> rows = (List<Object>) pplResponse.get("datarows");
        assertNotNull("datarows missing in " + pplResponse.keySet(), rows);
        return rows;
    }

    private void createIndex(String name, int shards) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {
            // index may not exist yet
        }
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": " + shards + ","
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": \"lucene\""
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"brand\": { \"type\": \"keyword\" },"
                + "    \"price\": { \"type\": \"integer\" }"
                + "  }"
                + "}"
                + "}"
        );
        Response response = client().performRequest(create);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /** Identical deterministic documents in both indexes: brand round-robin, price cycles 0..9999. */
    private void seedDocs(String index) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < TOTAL_DOCS; i++) {
            bulk.append("{\"index\":{}}\n")
                .append("{\"brand\":\"brand-")
                .append(i % BRANDS)
                .append("\",\"price\":")
                .append(i % 10_000)
                .append("}\n");
            if ((i + 1) % BULK_BATCH == 0) {
                sendBulk(index, bulk.toString());
                bulk.setLength(0);
            }
        }
        if (bulk.length() > 0) {
            sendBulk(index, bulk.toString());
        }
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        Request flush = new Request("POST", "/" + index + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);
    }

    private void sendBulk(String index, String body) throws IOException {
        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity(body);
        Response response = client().performRequest(bulk);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
