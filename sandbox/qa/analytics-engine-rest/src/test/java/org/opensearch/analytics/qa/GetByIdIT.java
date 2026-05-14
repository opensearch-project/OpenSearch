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

import java.util.Map;

/**
 * End-to-end integration coverage for the non-Lucene get-by-id path, wired through
 * {@code DataFusionPlugin} implementing {@code DocumentLookupProvider}.
 *
 * <p>Requires the same cluster shape as {@code ParquetDataFusionIT}: lucene +
 * analytics-backend-datafusion + parquet-data-format + composite-engine, with
 * {@code index.pluggable.dataformat=composite} and {@code composite.primary_data_format=parquet}.
 */
public class GetByIdIT extends AnalyticsRestTestCase {

    /**
     * Happy path: create, index three docs with explicit ids, refresh, fetch each and confirm the
     * source round-trips.
     */
    public void testGetByIdReturnsSource() throws Exception {
        final String index = "get_by_id_basic";
        deleteIfExists(index);
        createParquetIndex(index);

        String bulk = String.join(
            "\n",
            "{\"index\": {\"_id\": \"1\"}}",
            "{\"name\": \"alice\", \"age\": 30}",
            "{\"index\": {\"_id\": \"2\"}}",
            "{\"name\": \"bob\", \"age\": 25}",
            "{\"index\": {\"_id\": \"3\"}}",
            "{\"name\": \"carol\", \"age\": 35}",
            ""
        );
        bulkIndex(index, bulk);

        assertGet(index, "1", "alice", 30L);
        assertGet(index, "2", "bob", 25L);
        assertGet(index, "3", "carol", 35L);
    }

    /** Miss path: a non-existent id comes back 404 with {@code "found": false}. */
    public void testGetByIdMissingReturnsNotFound() throws Exception {
        final String index = "get_by_id_missing";
        deleteIfExists(index);
        createParquetIndex(index);
        bulkIndex(index, "{\"index\": {\"_id\": \"1\"}}\n{\"name\": \"alice\"}\n");

        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", "/" + index + "/_doc/missing")));
        assertEquals(404, ex.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> body = entityAsMap(ex.getResponse());
        assertEquals(Boolean.FALSE, body.get("found"));
    }

    /**
     * Per-segment writer_generation resolution: index two batches with explicit refreshes so they
     * land in separate segments, then fetch one id from each batch.
     */
    public void testGetByIdAcrossMultipleSegments() throws Exception {
        final String index = "get_by_id_multi_segment";
        deleteIfExists(index);
        createParquetIndex(index);

        bulkIndex(index, "{\"index\": {\"_id\": \"a\"}}\n{\"name\": \"alice\"}\n{\"index\": {\"_id\": \"b\"}}\n{\"name\": \"bob\"}\n");
        bulkIndex(index, "{\"index\": {\"_id\": \"c\"}}\n{\"name\": \"carol\"}\n{\"index\": {\"_id\": \"d\"}}\n{\"name\": \"dave\"}\n");

        assertGetName(index, "a", "alice");
        assertGetName(index, "b", "bob");
        assertGetName(index, "c", "carol");
        assertGetName(index, "d", "dave");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private void deleteIfExists(String index) {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {
            // not present; fine
        }
    }

    private void createParquetIndex(String index) throws Exception {
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            // Lucene secondary is required by the non-Lucene get-by-id path: the service
            // resolves the _id term via the sibling Lucene DirectoryReader.
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";
        Request req = new Request("PUT", "/" + index);
        req.setJsonEntity(body);
        Map<String, Object> created = assertOkAndParse(client().performRequest(req), "Create index " + index);
        assertEquals(true, created.get("acknowledged"));
        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkIndex(String index, String bulkBody) throws Exception {
        Request req = new Request("POST", "/" + index + "/_bulk");
        req.setJsonEntity(bulkBody);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> resp = assertOkAndParse(client().performRequest(req), "Bulk index into " + index);
        assertEquals("bulk errors=false", false, resp.get("errors"));
    }

    @SuppressWarnings("unchecked")
    private void assertGet(String index, String id, String expectedName, long expectedAge) throws Exception {
        Response resp = client().performRequest(new Request("GET", "/" + index + "/_doc/" + id));
        Map<String, Object> body = assertOkAndParse(resp, "GET " + index + "/_doc/" + id);
        assertEquals(id, body.get("_id"));
        assertEquals(Boolean.TRUE, body.get("found"));
        Map<String, Object> source = (Map<String, Object>) body.get("_source");
        assertNotNull("_source missing on get response for id=" + id, source);
        assertEquals(expectedName, source.get("name"));
        Object ageObj = source.get("age");
        assertNotNull("age missing for id=" + id, ageObj);
        assertEquals(expectedAge, ((Number) ageObj).longValue());
    }

    @SuppressWarnings("unchecked")
    private void assertGetName(String index, String id, String expectedName) throws Exception {
        Response resp = client().performRequest(new Request("GET", "/" + index + "/_doc/" + id));
        Map<String, Object> body = assertOkAndParse(resp, "GET " + index + "/_doc/" + id);
        assertEquals(id, body.get("_id"));
        assertEquals(Boolean.TRUE, body.get("found"));
        Map<String, Object> source = (Map<String, Object>) body.get("_source");
        assertNotNull("_source missing on get response for id=" + id, source);
        assertEquals(expectedName, source.get("name"));
    }
}
