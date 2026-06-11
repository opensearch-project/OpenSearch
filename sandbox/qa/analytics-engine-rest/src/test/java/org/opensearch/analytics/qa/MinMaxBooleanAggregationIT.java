/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for PPL {@code min}/{@code max} over a {@code boolean} column on the
 * analytics-engine route, against the production {@code /_plugins/_ppl} surface.
 *
 * <p>Regression guard for the boolean min/max binding. Substrait's stock
 * {@code functions_arithmetic.yaml} only defines min/max for {@code i8..fp64}, and
 * {@code opensearch_aggregate_functions.yaml} previously extended them only to {@code string}.
 * Without a boolean overload, {@code stats min(bool_field)} fails on the DataFusion route with
 * {@code "Unable to find binding for call MIN($0)"} even though DataFusion's native min/max
 * support {@code Boolean}. The added boolean overload lets isthmus bind the call.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.MinMaxBooleanAggregationIT" -Dsandbox.enabled=true}
 */
public class MinMaxBooleanAggregationIT extends AnalyticsRestTestCase {

    private static final String INDEX = "minmax_boolean_e2e";

    /**
     * Single source of truth. {@code enabled} mixes true/false across rows (so min=false,
     * max=true); {@code version} is a string (for the mixed-type min/max(string) case);
     * {@code grp} groups rows for the grouped variant.
     */
    private static final List<Doc> DOCS = List.of(
        new Doc("g0", false, "java"),
        new Doc("g0", true, "python"),
        new Doc("g0", false, "go"),
        new Doc("g1", true, "rust"),
        new Doc("g1", true, "java")
    );

    private record Doc(String grp, boolean enabled, String version) {}

    private boolean indexReady = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (indexReady == false) {
            createParquetIndex();
            ingest();
            indexReady = true;
        }
    }

    /** {@code min(bool)} / {@code max(bool)} over all rows — min=false, max=true. */
    public void testMinMaxBooleanUngrouped() throws IOException {
        List<Object> row = singleRow("source=" + INDEX + " | stats min(enabled) as min_e, max(enabled) as max_e");
        assertEquals("min(enabled) over all rows", Boolean.FALSE, row.get(0));
        assertEquals("max(enabled) over all rows", Boolean.TRUE, row.get(1));
    }

    /**
     * Grouped {@code min(bool)} / {@code max(bool)} by a keyword. g0 has both true and false
     * (min=false,max=true); g1 is all-true (min=true,max=true).
     */
    public void testMinMaxBooleanGrouped() throws IOException {
        Map<String, Object> result = executePpl(
            "source=" + INDEX + " | stats min(enabled) as min_e, max(enabled) as max_e by grp"
        );
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> schema = (List<Map<String, Object>>) result.get("schema");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("missing datarows", rows);
        assertEquals("two groups", 2, rows.size());

        int grpCol = colIndex(schema, "grp");
        int minCol = colIndex(schema, "min_e");
        int maxCol = colIndex(schema, "max_e");

        for (List<Object> row : rows) {
            String grp = (String) row.get(grpCol);
            if ("g0".equals(grp)) {
                assertEquals("g0 min", Boolean.FALSE, row.get(minCol));
                assertEquals("g0 max", Boolean.TRUE, row.get(maxCol));
            } else if ("g1".equals(grp)) {
                assertEquals("g1 min (all true)", Boolean.TRUE, row.get(minCol));
                assertEquals("g1 max (all true)", Boolean.TRUE, row.get(maxCol));
            } else {
                fail("unexpected group: " + grp);
            }
        }
    }

    /**
     * Mixed-type aggregation: min/max over both a boolean and a string column in one stats.
     * Mirrors the cross-type binding (string overload already existed; boolean is the new one).
     * version min="go", max="rust"; enabled min=false, max=true.
     */
    public void testMinMaxMixedBooleanAndString() throws IOException {
        List<Object> row = singleRow(
            "source=" + INDEX
                + " | stats min(version) as min_v, max(version) as max_v, min(enabled) as min_e, max(enabled) as max_e"
        );
        assertEquals("min(version)", "go", row.get(0));
        assertEquals("max(version)", "rust", row.get(1));
        assertEquals("min(enabled)", Boolean.FALSE, row.get(2));
        assertEquals("max(enabled)", Boolean.TRUE, row.get(3));
    }

    private static int colIndex(List<Map<String, Object>> schema, String name) {
        for (int i = 0; i < schema.size(); i++) {
            if (name.equals(schema.get(i).get("name"))) {
                return i;
            }
        }
        throw new AssertionError("column not found in schema: " + name);
    }

    @SuppressWarnings("unchecked")
    private List<Object> singleRow(String ppl) throws IOException {
        Map<String, Object> result = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("missing datarows for: " + ppl, rows);
        assertEquals("expected exactly one summary row for: " + ppl, 1, rows.size());
        return rows.get(0);
    }

    private void createParquetIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
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
            + "    \"grp\": { \"type\": \"keyword\" },"
            + "    \"enabled\": { \"type\": \"boolean\" },"
            + "    \"version\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";
        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void ingest() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (Doc d : DOCS) {
            bulk.append("{\"index\":{\"_index\":\"").append(INDEX).append("\"}}\n");
            bulk.append("{\"grp\":\"").append(d.grp()).append("\",\"enabled\":").append(d.enabled())
                .append(",\"version\":\"").append(d.version()).append("\"}\n");
        }
        Request req = new Request("POST", "/_bulk");
        req.addParameter("refresh", "true");
        req.setJsonEntity(bulk.toString());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk ingest");
        assertEquals("bulk must not report errors", Boolean.FALSE, response.get("errors"));
    }
}
