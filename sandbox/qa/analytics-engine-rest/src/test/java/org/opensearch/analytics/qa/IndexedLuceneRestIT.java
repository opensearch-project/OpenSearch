/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration test verifying the Lucene indexed table + Parquet DataFusion
 * query path produces correct results.
 *
 * Flow: PPL query via /_analytics/ppl
 *   -> DefaultPlanExecutor detects search.indexed_query.enabled=true
 *   -> converts RelNode to substrait bytes
 *   -> opens Lucene NRT reader, builds Lucene query from cluster settings
 *   -> calls SearchExecEngine.executeIndexedQuery (IndexedTableProvider in Rust)
 *   -> consumes stream via bridge.consumeStream
 *   -> returns results
 */
public class IndexedLuceneRestIT extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(IndexedLuceneRestIT.class);
    private static final String INDEX = "hits_indexed";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private void createIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception e) {
            // ignore
        }
        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity("{\n"
            + "  \"settings\": {\n"
            + "    \"index.number_of_shards\": 1,\n"
            + "    \"index.number_of_replicas\": 0,\n"
            + "    \"optimized.enabled\": true,\n"
            + "    \"index.composite.secondary_data_formats\": [\"Lucene\"]\n"
            + "  },\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"AdvEngineID\": {\"type\": \"short\"},\n"
            + "      \"Age\": {\"type\": \"short\"},\n"
            + "      \"CounterID\": {\"type\": \"integer\"},\n"
            + "      \"RegionID\": {\"type\": \"integer\"},\n"
            + "      \"SearchPhrase\": {\"type\": \"keyword\"},\n"
            + "      \"URL\": {\"type\": \"keyword\"},\n"
            + "      \"UserID\": {\"type\": \"long\"},\n"
            + "      \"Title\": {\"type\": \"keyword\"}\n"
            + "    }\n"
            + "  }\n"
            + "}");
        client().performRequest(req);
    }

    private void bulkInsert() throws IOException {
        StringBuilder bulk = new StringBuilder();
        String action = "{\"index\":{\"_index\":\"" + INDEX + "\"}}\n";

        // Doc 1: URL contains "google" (via google.com)
        bulk.append(action);
        bulk.append("{\"AdvEngineID\":2,\"Age\":25,\"CounterID\":62,\"RegionID\":229,"
            + "\"SearchPhrase\":\"clickbench test\","
            + "\"URL\":\"http://www.google.com/search?q=test\","
            + "\"UserID\":100000001,\"Title\":\"Test Page\"}\n");

        // Doc 2: URL does NOT contain "google"
        bulk.append(action);
        bulk.append("{\"AdvEngineID\":0,\"Age\":30,\"CounterID\":62,\"RegionID\":1,"
            + "\"SearchPhrase\":\"\","
            + "\"URL\":\"http://example.com/mobile\","
            + "\"UserID\":100000002,\"Title\":\"Mobile Page\"}\n");

        // Doc 3: URL contains "google" (via google.com)
        bulk.append(action);
        bulk.append("{\"AdvEngineID\":0,\"Age\":35,\"CounterID\":62,\"RegionID\":229,"
            + "\"SearchPhrase\":\"opensearch analytics\","
            + "\"URL\":\"http://mail.google.com/inbox\","
            + "\"UserID\":435090932899640449,\"Title\":\"Google Mail\"}\n");

        // Doc 4: URL does NOT contain "google"
        bulk.append(action);
        bulk.append("{\"AdvEngineID\":0,\"Age\":28,\"CounterID\":62,\"RegionID\":50,"
            + "\"SearchPhrase\":\"\","
            + "\"URL\":\"http://example.com/analytics\","
            + "\"UserID\":100000004,\"Title\":\"Analytics\"}\n");

        // Doc 5: URL does NOT contain "google"
        bulk.append(action);
        bulk.append("{\"AdvEngineID\":3,\"Age\":45,\"CounterID\":62,\"RegionID\":229,"
            + "\"SearchPhrase\":\"best search engine\","
            + "\"URL\":\"http://example.jp/search\","
            + "\"UserID\":100000007,\"Title\":\"Search Results\"}\n");

        Request bulkReq = new Request("POST", "/" + INDEX + "/_bulk");
        bulkReq.setJsonEntity(bulk.toString());
        bulkReq.addParameter("refresh", "true");
        bulkReq.setOptions(bulkReq.getOptions().toBuilder()
            .addHeader("Content-Type", "application/x-ndjson")
            .build());
        Response resp = client().performRequest(bulkReq);
        assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    private void setIndexedQuery(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\": {\"search.indexed_query.enabled\": " + enabled + "}}");
        client().performRequest(req);
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> runPPL(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + ppl.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}");
        Response resp = client().performRequest(req);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        Map<String, Object> map = entityAsMap(resp);
        return (List<List<Object>>) map.get("rows");
    }

    /**
     * Verifies the indexed Lucene+Parquet path produces correct results.
     *
     * The Lucene query is auto-derived from the PPL filter predicates:
     *   where URL = 'x'   -> TermQuery(URL, x)
     *   where AdvEngineID != 0 -> BooleanQuery(MatchAll, NOT TermQuery)
     *   no filter          -> MatchAllDocsQuery
     * The substrait plan handles the full aggregation/projection.
     */
    public void testIndexedLuceneQueryPath() throws Exception {
        createIndex();
        bulkInsert();

        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);

        // 1. Baseline via vanilla path
        List<List<Object>> baselineCount = runPPL("source=" + INDEX + " | stats count()");
        assertEquals(1, baselineCount.size());
        assertEquals("Baseline COUNT(*)", 5L, ((Number) baselineCount.get(0).get(0)).longValue());

        setIndexedQuery(true);
        try {
            // 2. No filter -> MatchAllDocsQuery -> all 5 docs
            List<List<Object>> allCount = runPPL("source=" + INDEX + " | stats count()");
            assertEquals(1, allCount.size());
            assertEquals("COUNT(*) no filter", 5L, ((Number) allCount.get(0).get(0)).longValue());
            logger.info("PASS: no-filter COUNT(*) = 5");

            // 3. Term query: where Title = 'Analytics' -> doc 4 only
            List<List<Object>> termCount = runPPL("source=" + INDEX + " | where Title = 'Analytics' | stats count()");
            assertEquals(1, termCount.size());
            assertEquals("COUNT where Title='Analytics'", 1L, ((Number) termCount.get(0).get(0)).longValue());
            logger.info("PASS: term query Title='Analytics' COUNT = 1");

            // 4. Term query with aggregation: where Title = 'Analytics' -> doc 4 (Age=28)
            List<List<Object>> termSum = runPPL("source=" + INDEX + " | where Title = 'Analytics' | stats sum(Age)");
            assertEquals(1, termSum.size());
            assertEquals("SUM(Age) where Title='Analytics'", 28L, ((Number) termSum.get(0).get(0)).longValue());
            logger.info("PASS: term query SUM(Age) = 28");

            // 5. != filter: where AdvEngineID != 0 -> docs 1 (AdvEngineID=2) and 5 (AdvEngineID=3)
            List<List<Object>> neqCount = runPPL("source=" + INDEX + " | where AdvEngineID!=0 | stats count()");
            assertEquals(1, neqCount.size());
            assertEquals("COUNT where AdvEngineID!=0", 2L, ((Number) neqCount.get(0).get(0)).longValue());
            logger.info("PASS: != filter COUNT = 2");

            // 6. MAX through indexed path on all docs
            List<List<Object>> maxResult = runPPL("source=" + INDEX + " | stats max(UserID)");
            assertEquals(1, maxResult.size());
            assertEquals("MAX(UserID)", 435090932899640449L, ((Number) maxResult.get(0).get(0)).longValue());
            logger.info("PASS: MAX(UserID) = 435090932899640449");

            // 7. DISTINCT_COUNT
            List<List<Object>> dcResult = runPPL("source=" + INDEX + " | stats distinct_count(RegionID)");
            assertEquals(1, dcResult.size());
            assertEquals("DISTINCT_COUNT(RegionID)", 3L, ((Number) dcResult.get(0).get(0)).longValue());
            logger.info("PASS: DISTINCT_COUNT(RegionID) = 3");

            logger.info("All indexed Lucene+Parquet query assertions PASSED");
        } finally {
            setIndexedQuery(false);
        }
    }
}
