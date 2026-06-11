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
 * End-to-end coverage for PPL {@code distinct_count_approx} / {@code dc} aggregation on the
 * analytics-engine route, against the production {@code /_plugins/_ppl} surface.
 *
 * <p>Regression guard for the two-stage binding that {@code distinct_count_approx} requires on
 * the DataFusion backend. The PPL marker reaches the engine still named
 * {@code DISTINCT_COUNT_APPROX} (sql#5525 only renames the substrait-emission op, a later
 * stage), so without engine-side handling the query fails first in the planner with
 * {@code "No enum constant ...AggregateFunction.DISTINCT_COUNT_APPROX"} and then in substrait
 * with {@code "Unable to find binding for call DISTINCT_COUNT_APPROX($1)"}. The engine maps the
 * marker name to the {@code APPROX_COUNT_DISTINCT} enum constant / stock operator, which binds
 * to DataFusion's native {@code approx_distinct}.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.DistinctCountApproxAggregationIT" -Dsandbox.enabled=true}
 */
public class DistinctCountApproxAggregationIT extends AnalyticsRestTestCase {

    private static final String INDEX = "dc_approx_e2e";

    /**
     * Single source of truth for every assertion's oracle. {@code gender} groups the rows;
     * {@code state} is the distinct-counted column. F has 3 distinct states (CA, CA, NY, TX -> 3);
     * M has 4 distinct states (WA, OR, OR, NV, ID -> 4).
     */
    private static final List<Doc> DOCS = List.of(
        new Doc("F", "CA"),
        new Doc("F", "CA"),
        new Doc("F", "NY"),
        new Doc("F", "TX"),
        new Doc("M", "WA"),
        new Doc("M", "OR"),
        new Doc("M", "OR"),
        new Doc("M", "NV"),
        new Doc("M", "ID")
    );

    private record Doc(String gender, String state) {}

    private boolean indexReady = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (indexReady == false) {
            createParquetIndex();
            ingest();
            indexReady = true;
        }
    }

    /** Grouped {@code distinct_count_approx(state) by gender} — the canonical failing shape. */
    public void testDistinctCountApproxGrouped() throws IOException {
        Map<String, List<Object>> byGender = approxCountByGender("distinct_count_approx(state)");
        assertEquals("F distinct states", 3L, ((Number) byGender.get("F").get(0)).longValue());
        assertEquals("M distinct states", 4L, ((Number) byGender.get("M").get(0)).longValue());
    }

    /** Aliased form {@code distinct_count_approx(state) as dca by gender}. */
    public void testDistinctCountApproxGroupedWithAlias() throws IOException {
        Map<String, List<Object>> byGender = approxCountByGender("distinct_count_approx(state) as dca");
        assertEquals("F distinct states", 3L, ((Number) byGender.get("F").get(0)).longValue());
        assertEquals("M distinct states", 4L, ((Number) byGender.get("M").get(0)).longValue());
    }

    /** Short {@code dc(...)} spelling — the same PPL marker via the {@code dc} alias. */
    public void testDcShorthandGrouped() throws IOException {
        Map<String, List<Object>> byGender = approxCountByGender("dc(state) as dca");
        assertEquals("F distinct states", 3L, ((Number) byGender.get("F").get(0)).longValue());
        assertEquals("M distinct states", 4L, ((Number) byGender.get("M").get(0)).longValue());
    }

    /** Ungrouped {@code distinct_count_approx(state)} over all rows — 7 distinct states total. */
    public void testDistinctCountApproxUngrouped() throws IOException {
        Map<String, Object> result = executePpl("source=" + INDEX + " | stats distinct_count_approx(state)");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("missing datarows", rows);
        assertEquals("one summary row", 1, rows.size());
        assertEquals("7 distinct states overall", 7L, ((Number) rows.get(0).get(0)).longValue());
    }

    /**
     * Run {@code source=INDEX | stats <agg> by gender} and return a map keyed by the gender cell,
     * each value being the full data row (so the caller reads the aggregate cell positionally).
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<Object>> approxCountByGender(String agg) throws IOException {
        Map<String, Object> result = executePpl("source=" + INDEX + " | stats " + agg + " by gender");
        List<Map<String, Object>> schema = (List<Map<String, Object>>) result.get("schema");
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("missing schema", schema);
        assertNotNull("missing datarows", rows);
        assertEquals("two gender groups", 2, rows.size());

        int genderCol = -1;
        for (int i = 0; i < schema.size(); i++) {
            if ("gender".equals(schema.get(i).get("name"))) {
                genderCol = i;
            }
        }
        assertTrue("gender column present in schema", genderCol >= 0);
        int aggCol = genderCol == 0 ? 1 : 0;

        java.util.Map<String, List<Object>> out = new java.util.HashMap<>();
        for (List<Object> row : rows) {
            out.put((String) row.get(genderCol), List.of(row.get(aggCol)));
        }
        assertTrue("both genders present", out.containsKey("F") && out.containsKey("M"));
        return out;
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
            + "    \"gender\": { \"type\": \"keyword\" },"
            + "    \"state\": { \"type\": \"keyword\" }"
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
            bulk.append("{\"gender\":\"").append(d.gender()).append("\",\"state\":\"").append(d.state()).append("\"}\n");
        }
        Request req = new Request("POST", "/_bulk");
        req.addParameter("refresh", "true");
        req.setJsonEntity(bulk.toString());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk ingest");
        assertEquals("bulk must not report errors", Boolean.FALSE, response.get("errors"));
    }
}
