/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for SQL/PPL {@code LIKE} delegation to Lucene (prefix + wildcard) and the
 * per-backend delegation block-list. Asserts correct counts for each LIKE shape, then blocks LIKE
 * via {@code analytics.delegation.lucene.blocked_predicates} and asserts identical counts (delegation
 * off ⇒ DataFusion path) — same results, different execution path. Also checks a non-{@code lucene}
 * namespace is rejected.
 */
public class LikeDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "like_delegation_e2e";
    private static final String BLOCK_KEY = "analytics.delegation.lucene.blocked_predicates";

    public void testLikeDelegationCorrectnessAndBlockListParity() throws Exception {
        createIndex();
        indexDocs();

        // Ground truth over the corpus (see indexDocs): str0 ∈
        //   "apple" ×4, "applesauce" ×3, "banana" ×2, "cherry pie" ×1 (a multi-token value).
        // SQL LIKE is a whole-value match over the raw keyword (DataFusion semantics, which the
        // delegated Lucene query must reproduce). The values are chosen so each pattern is unambiguous.
        assertLikeCount("xyz%", 0);          // nothing starts with xyz
        assertLikeCount("apple%", 7);        // prefix: "apple"×4 + "applesauce"×3
        assertLikeCount("%sauce%", 3);       // contains: only "applesauce"×3
        assertLikeCount("banan_", 2);        // underscore: "banana"×2 ("banan" + one char)
        assertLikeCount("%pie", 1);          // suffix on a multi-token value: "cherry pie"×1
        // Case-insensitivity: PPL like() is ILIKE — uppercase pattern still matches lowercase data.
        assertPplLikeCount("APPLE%", 7);

        // Now block LIKE delegation to Lucene cluster-wide; results must be identical (DataFusion path).
        setBlockedPredicates("\"LIKE\"");
        try {
            assertLikeCount("apple%", 7);
            assertLikeCount("%sauce%", 3);
            assertLikeCount("banan_", 2);
            assertPplLikeCount("APPLE%", 7);
        } finally {
            setBlockedPredicates(""); // clear
        }
    }

    /**
     * LIKE on a {@code text} field that has a {@code .keyword} subfield: the wildcard is delegated
     * against {@code msg.keyword} (raw value), so it matches the whole stored string exactly as
     * DataFusion's native LIKE would. msg ∈ "Service alpha started"×7, "Service beta stopped"×2,
     * "Gateway timeout error"×1.
     */
    public void testLikeOnTextFieldWithKeywordSubfield() throws Exception {
        createIndex();
        indexDocs();

        assertMsgLikeCount("Service%", 9);   // both "Service ..." families: 7 + 2
        assertMsgLikeCount("%timeout%", 1);  // only "Gateway timeout error"
        assertMsgLikeCount("%alpha%", 7);    // only the alpha family
    }

    /** The block-list only accepts the {@code lucene} namespace (the sole FILTER-delegation acceptor). */
    public void testBlockListRejectsNonLuceneBackend() throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.delegation.datafusion.blocked_predicates\": [\"LIKE\"]}}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(req));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }

    // ---- helpers ----

    private void assertLikeCount(String pattern, long expected) throws Exception {
        String ppl = "source = " + INDEX_NAME + " | where str0 LIKE '" + pattern + "' | stats count() as c";
        assertCount(ppl, expected, "str0 LIKE '" + pattern + "'");
    }

    private void assertMsgLikeCount(String pattern, long expected) throws Exception {
        String ppl = "source = " + INDEX_NAME + " | where msg LIKE '" + pattern + "' | stats count() as c";
        assertCount(ppl, expected, "msg LIKE '" + pattern + "'");
    }

    private void assertPplLikeCount(String pattern, long expected) throws Exception {
        String ppl = "source = " + INDEX_NAME + " | where like(str0, '" + pattern + "') | stats count() as c";
        assertCount(ppl, expected, "like(str0, '" + pattern + "')");
    }

    @SuppressWarnings("unchecked")
    private void assertCount(String ppl, long expected, String label) throws Exception {
        Map<String, Object> result = executePplViaShim(ppl);
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null for " + label, rows);
        assertEquals("count agg must return 1 row for " + label, 1, rows.size());
        Number c = (Number) rows.get(0).get(0);
        assertEquals("count for " + label, expected, c.longValue());
    }

    private void setBlockedPredicates(String jsonListContents) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"" + BLOCK_KEY + "\": [" + jsonListContents + "]}}");
        client().performRequest(req);
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"str0\": { \"type\": \"keyword\" },"
            + "    \"msg\": { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\" } } }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexDocs() throws Exception {
        StringBuilder bulk = new StringBuilder();
        // str0 (keyword) value paired with a msg (text+keyword-subfield) value per doc.
        appendDocs(bulk, "apple", "Service alpha started", 4);
        appendDocs(bulk, "applesauce", "Service alpha started", 3);
        appendDocs(bulk, "banana", "Service beta stopped", 2);
        appendDocs(bulk, "cherry pie", "Gateway timeout error", 1);

        Request bulkReq = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk.toString());
        assertOkAndParse(client().performRequest(bulkReq), "Bulk index");
    }

    private static void appendDocs(StringBuilder bulk, String str0, String msg, int count) {
        for (int i = 0; i < count; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"str0\": \"").append(str0).append("\", \"msg\": \"").append(msg).append("\"}\n");
        }
    }
}
