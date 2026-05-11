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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration test for local recovery of the DataFormatAwareEngine via index close/reopen.
 *
 * <p>Exercises the CatalogSnapshotManager fix that notifies reader managers about
 * committed snapshots on engine startup, ensuring search queries work immediately
 * after a restart without requiring a translog replay or explicit refresh.
 *
 * <p>Uses a composite index with parquet primary + lucene secondary to validate
 * recovery across both format engines.
 */
public class LocalRecoveryIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "local_recovery_test";
    private static boolean indexProvisioned = false;

    private void ensureIndexProvisioned() throws IOException {
        if (indexProvisioned) {
            return;
        }

        // Delete if exists from a previous run
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception e) {
            // index may not exist
        }

        // Create index with parquet primary + lucene secondary
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" },"
            + "    \"score\": { \"type\": \"double\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> createResponse = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals("Index creation must be acknowledged", true, createResponse.get("acknowledged"));

        // Wait for green
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "30s");
        client().performRequest(healthRequest);

        // Bulk index documents
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {\"_id\": \"1\"}}\n{\"name\": \"alice\", \"age\": 30, \"score\": 95.5}\n");
        bulk.append("{\"index\": {\"_id\": \"2\"}}\n{\"name\": \"bob\", \"age\": 25, \"score\": 88.0}\n");
        bulk.append("{\"index\": {\"_id\": \"3\"}}\n{\"name\": \"carol\", \"age\": 35, \"score\": 92.3}\n");
        bulk.append("{\"index\": {\"_id\": \"4\"}}\n{\"name\": \"dave\", \"age\": 28, \"score\": 76.8}\n");
        bulk.append("{\"index\": {\"_id\": \"5\"}}\n{\"name\": \"eve\", \"age\": 32, \"score\": 91.0}\n");

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "Bulk index");
        assertEquals("Bulk must have no errors", false, bulkResponse.get("errors"));

        // Flush to commit data to disk
        Request flushRequest = new Request("POST", "/" + INDEX_NAME + "/_flush");
        flushRequest.addParameter("force", "true");
        client().performRequest(flushRequest);

        indexProvisioned = true;
    }

    /**
     * Provisions data, queries it, restarts the engine (close/reopen), then re-queries
     * and asserts the results are identical. This is the core validation that committed
     * catalog snapshots are correctly restored on engine startup.
     */
    public void testQueryResultsIdenticalAfterEngineRestart() throws IOException {
        ensureIndexProvisioned();

        // Query before restart: sort by age ascending
        String ppl = "source=" + INDEX_NAME + " | sort age | fields name, age, score";
        List<List<Object>> beforeRows = executePplRows(ppl);
        assertEquals("Should have 5 rows before restart", 5, beforeRows.size());

        // Close the index — engine shuts down
        Response closeResponse = client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_close"));
        assertEquals(200, closeResponse.getStatusLine().getStatusCode());

        // Reopen the index — engine starts up with local recovery from committed data
        Response openResponse = client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_open"));
        assertEquals(200, openResponse.getStatusLine().getStatusCode());

        // Wait for recovery
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);

        // Query after restart — must return identical results
        List<List<Object>> afterRows = executePplRows(ppl);
        assertEquals("Row count must match after restart", beforeRows.size(), afterRows.size());
        for (int i = 0; i < beforeRows.size(); i++) {
            assertEquals(
                "Row " + i + " must be identical after restart: before=" + beforeRows.get(i) + " after=" + afterRows.get(i),
                beforeRows.get(i).size(),
                afterRows.get(i).size()
            );
            for (int j = 0; j < beforeRows.get(i).size(); j++) {
                assertCellEquals(
                    "Mismatch at row " + i + " col " + j,
                    beforeRows.get(i).get(j),
                    afterRows.get(i).get(j)
                );
            }
        }
    }

    public void testQueryResultsIdenticalAfterForceMergeAndRestart() throws IOException {
        String indexName = "local_recovery_forcemerge_test";

        // Delete if exists from a previous run
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception e) {
            // index may not exist
        }

        // Create index with parquet primary + lucene secondary
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" },"
            + "    \"score\": { \"type\": \"double\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(body);
        Map<String, Object> createResponse = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals("Index creation must be acknowledged", true, createResponse.get("acknowledged"));

        // Wait for green
        Request healthRequest = new Request("GET", "/_cluster/health/" + indexName);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "30s");
        client().performRequest(healthRequest);

        // Ingest documents in multiple batches with flush between each to create multiple segments
        int docId = 1;
        int numBatches = 5;
        int docsPerBatch = 10;
        for (int batch = 0; batch < numBatches; batch++) {
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < docsPerBatch; i++) {
                bulk.append("{\"index\": {\"_id\": \"").append(docId).append("\"}}\n");
                bulk.append("{\"name\": \"user_")
                    .append(docId)
                    .append("\", \"age\": ")
                    .append(20 + (docId % 40))
                    .append(", \"score\": ")
                    .append(50.0 + (docId % 50))
                    .append("}\n");
                docId++;
            }

            Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            bulkRequest.addParameter("refresh", "true");
            bulkRequest.setOptions(
                bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
            );
            Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "Bulk batch " + batch);
            assertEquals("Bulk batch " + batch + " must have no errors", false, bulkResponse.get("errors"));

            // Flush after each batch to create separate segments
            Request flushRequest = new Request("POST", "/" + indexName + "/_flush");
            flushRequest.addParameter("force", "true");
            client().performRequest(flushRequest);
        }

        int totalDocs = numBatches * docsPerBatch;

        // Force merge to a single segment
        Request forceMergeRequest = new Request("POST", "/" + indexName + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        client().performRequest(forceMergeRequest);

        // Flush after force merge to persist the merged state
        Request flushRequest = new Request("POST", "/" + indexName + "/_flush");
        flushRequest.addParameter("force", "true");
        client().performRequest(flushRequest);

        // Query before restart
        String ppl = "source=" + indexName + " | sort age | fields name, age, score";
        List<List<Object>> beforeRows = executePplRows(ppl);
        assertEquals("Should have " + totalDocs + " rows before restart", totalDocs, beforeRows.size());

        // Close the index — engine shuts down
        Response closeResponse = client().performRequest(new Request("POST", "/" + indexName + "/_close"));
        assertEquals(200, closeResponse.getStatusLine().getStatusCode());

        // Reopen the index — engine starts up with local recovery from committed data
        Response openResponse = client().performRequest(new Request("POST", "/" + indexName + "/_open"));
        assertEquals(200, openResponse.getStatusLine().getStatusCode());

        // Wait for recovery
        healthRequest = new Request("GET", "/_cluster/health/" + indexName);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);

        // Query after restart — must return identical results
        List<List<Object>> afterRows = executePplRows(ppl);
        assertEquals("Row count must match after restart", beforeRows.size(), afterRows.size());
        for (int i = 0; i < beforeRows.size(); i++) {
            assertEquals(
                "Row " + i + " must be identical after restart: before=" + beforeRows.get(i) + " after=" + afterRows.get(i),
                beforeRows.get(i).size(),
                afterRows.get(i).size()
            );
            for (int j = 0; j < beforeRows.get(i).size(); j++) {
                assertCellEquals(
                    "Mismatch at row " + i + " col " + j,
                    beforeRows.get(i).get(j),
                    afterRows.get(i).get(j)
                );
            }
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private List<List<Object>> executePplRows(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> parsed = assertOkAndParse(response, "PPL: " + ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) parsed.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        return rows;
    }

    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }
}
