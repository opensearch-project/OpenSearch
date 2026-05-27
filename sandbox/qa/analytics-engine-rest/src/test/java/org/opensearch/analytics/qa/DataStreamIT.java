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
 * End-to-end IT for data stream support on the analytics-engine route. A data stream is a single
 * logical name fronting a chain of rolled-over backing indices ({@code .ds-<name>-<date>-<gen>}).
 * The planner must fan out across the backings exactly like the alias path — schema-compatibility
 * still applies in case a backing's mapping was manually amended after rollover.
 */
public class DataStreamIT extends AnalyticsRestTestCase {

    private static final String STREAM = "logs_ds";
    private static final String TEMPLATE = "logs_ds_template";

    /**
     * Happy path: indexing into a data stream auto-creates one backing index; a single rollover
     * creates a second. The planner must fan out across both — a missing fan-out would only see
     * the latest generation's rows.
     */
    public void testDataStreamFansOutAcrossRolledOverBackings() throws IOException {
        ensureCleanup();
        createDataStreamTemplate();
        bulkIntoStream(STREAM, """
            {"@timestamp":"2026-05-01T00:00:00Z","v":1}
            {"@timestamp":"2026-05-01T00:00:01Z","v":2}
            """);
        rolloverStream(STREAM);
        bulkIntoStream(STREAM, """
            {"@timestamp":"2026-05-02T00:00:00Z","v":3}
            {"@timestamp":"2026-05-02T00:00:01Z","v":4}
            """);

        long count = singleCount("source=" + STREAM + " | stats count() as c");
        assertEquals("data stream must fan out across both backings: 2 + 2", 4L, count);

        // Sanity: aggregation over the v column also fans out. Sum is 1+2+3+4 = 10.
        long sum = singleLongAgg("source=" + STREAM + " | stats sum(v) as s");
        assertEquals("sum must aggregate across backings", 10L, sum);
    }

    /**
     * If a backing index's mapping has been manually amended such that a field type conflicts
     * with another generation, planning must fail at resolution with a schema-incompatible error.
     * Same contract as alias-spanning-mismatched-mappings, just exercised through a data stream.
     */
    public void testDataStreamRejectsConflictingBackingMappings() throws IOException {
        ensureCleanup();
        createDataStreamTemplate();
        bulkIntoStream(STREAM, """
            {"@timestamp":"2026-05-01T00:00:00Z","v":1}
            """);
        rolloverStream(STREAM);
        // Diverge generation 2's mapping: PUT a `v` field as keyword (template says long).
        // The update-mapping API on a backing index will reject incompatible adds, so we drift on
        // a *new* field instead: present in gen 2 only, with a type that conflicts with one we'll
        // add to gen 1. This still trips validateSchemaCompatibility because the field appears in
        // multiple backings with divergent types.
        Request gen2Map = new Request(
            "PUT",
            "/.ds-" + STREAM + "-000002/_mapping?write_index_only=false"
        );
        gen2Map.setJsonEntity("{\"properties\":{\"divergent\":{\"type\":\"keyword\"}}}");
        client().performRequest(gen2Map);
        Request gen1Map = new Request(
            "PUT",
            "/.ds-" + STREAM + "-000001/_mapping?write_index_only=false"
        );
        gen1Map.setJsonEntity("{\"properties\":{\"divergent\":{\"type\":\"long\"}}}");
        client().performRequest(gen1Map);

        String error = executePplExpectingFailure("source=" + STREAM + " | fields divergent");
        assertContains(error, "incompatible field types");
        assertContains(error, "divergent");
    }

    // ── setup helpers ────────────────────────────────────────────────────

    /** Removes any leftover data stream + template from a prior run. */
    private void ensureCleanup() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_data_stream/" + STREAM));
        } catch (ResponseException ignored) {}
        try {
            client().performRequest(new Request("DELETE", "/_index_template/" + TEMPLATE));
        } catch (ResponseException ignored) {}
    }

    /**
     * Creates an index template that backs the data stream. Maps {@code @timestamp} as the
     * required timestamp field and {@code v} as a long. Backings inherit the composite/parquet
     * dataformat settings so the scan goes through the analytics-engine native path.
     */
    private void createDataStreamTemplate() throws IOException {
        Request put = new Request("PUT", "/_index_template/" + TEMPLATE);
        put.setJsonEntity(
            "{\"index_patterns\":[\"" + STREAM + "\"],"
                + "\"data_stream\":{},"
                + "\"template\":{"
                + "\"settings\":{"
                + "\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.number_of_shards\":1,"
                + "\"index.number_of_replicas\":0"
                + "},"
                + "\"mappings\":{\"properties\":{"
                + "\"@timestamp\":{\"type\":\"date\"},"
                + "\"v\":{\"type\":\"long\"}"
                + "}}"
                + "}}"
        );
        client().performRequest(put);
    }

    private void rolloverStream(String name) throws IOException {
        Request rollover = new Request("POST", "/" + name + "/_rollover");
        client().performRequest(rollover);
    }

    private void bulkIntoStream(String name, String ndjsonDocs) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (String doc : ndjsonDocs.split("\n")) {
            if (doc.isBlank()) continue;
            bulk.append("{\"create\":{\"_index\":\"").append(name).append("\"}}\n");
            bulk.append(doc).append("\n");
        }
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "bulk " + name);
        assertEquals("bulk into " + name + " had errors", false, response.get("errors"));
    }

    // ── query helpers (mirrored from AliasIT) ────────────────────────────

    private long singleCount(String ppl) throws IOException {
        return singleLongAgg(ppl);
    }

    private long singleLongAgg(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("missing 'rows' for: " + ppl, rows);
        assertEquals("single row expected: " + ppl, 1, rows.size());
        Object cell = rows.get(0).get(0);
        assertTrue("expected numeric: " + cell, cell instanceof Number);
        return ((Number) cell).longValue();
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    private String executePplExpectingFailure(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            fail("Expected failure but got: " + assertOkAndParse(response, ppl));
            return ""; // unreachable
        } catch (ResponseException re) {
            try (var is = re.getResponse().getEntity().getContent()) {
                return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue("expected to contain [" + needle + "] but was: " + haystack, haystack.contains(needle));
    }
}
