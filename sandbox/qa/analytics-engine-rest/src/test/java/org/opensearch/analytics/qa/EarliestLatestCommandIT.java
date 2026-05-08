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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * End-to-end integration test for the scalar PPL boolean predicates
 * {@code where earliest(expr, ts)} and {@code where latest(expr, ts)} on the
 * analytics-engine route.
 *
 * <p>Pipeline exercised end to end:
 * <pre>
 *   PPL parser → Calcite RexCall (operator name "EARLIEST" / "LATEST")
 *     → analytics-engine PlannerImpl Phase 0:
 *       EarliestLatestAdapter.foldRelativeTimePredicates
 *         (parses the literal first arg via the relative-time DSL, replaces the
 *          call with a {@code ts >= LITERAL} / {@code ts <= LITERAL} comparison)
 *     → standard OpenSearchFilterRule + DataFusion comparison execution
 *     → coordinator → PPL response
 * </pre>
 *
 * <p>The fold is required because substrait-java has no mapping for the
 * SQL-plugin {@code EARLIEST}/{@code LATEST} UDFs and {@code OpenSearchFilterRule}
 * would otherwise throw {@code "Unrecognized filter operator [EARLIEST / OTHER_FUNCTION]"}.
 */
public class EarliestLatestCommandIT extends AnalyticsRestTestCase {

    private static final String INDEX = "earliest_latest_scalar_e2e";
    private static final int NUM_SHARDS = 1;

    /** Five rows spanning 2023-01-01..2023-01-05 on {@code @timestamp}. */
    private static final String[][] ROWS = new String[][] {
        {"server1", "ERROR", "Database connection failed", "2023-01-01"},
        {"server2", "INFO",  "Service started",            "2023-01-02"},
        {"server1", "WARN",  "High memory usage",          "2023-01-03"},
        {"server3", "ERROR", "Disk space low",             "2023-01-04"},
        {"server2", "INFO",  "Backup completed",           "2023-01-05"},
    };

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) return;
        createParquetBackedIndex();
        indexRows();
        dataProvisioned = true;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Scalar {@code where earliest(absoluteLiteral, @timestamp)} keeps rows whose
     * timestamp is at-or-after the literal. With the literal at 2023-01-03, the
     * three rows on 2023-01-03..05 survive.
     *
     * <p>This proves the fold pipeline: PPL → Calcite EARLIEST UDF →
     * {@code EarliestLatestAdapter.foldRelativeTimePredicates} →
     * {@code @timestamp >= TIMESTAMP_LITERAL} → DataFusion native comparison.
     * If the fold didn't run, the analytics-engine filter rule would reject the
     * unknown {@code EARLIEST} operator and the query would 500.
     */
    public void testWhereEarliestWithAbsoluteLiteralKeepsLaterRows() throws IOException {
        assertRowsAnyOrder(
            "source = " + INDEX + " | where earliest('2023-01-03', `@timestamp`) | stats count() as cnt",
            row(3)
        );
    }

    /**
     * Scalar {@code where latest(absoluteLiteral, @timestamp)} keeps rows whose
     * timestamp is at-or-before the literal. Literal at 2023-01-03 → rows on
     * 2023-01-01..03 qualify (3 rows).
     */
    public void testWhereLatestWithAbsoluteLiteralKeepsEarlierRows() throws IOException {
        assertRowsAnyOrder(
            "source = " + INDEX + " | where latest('2023-01-03', `@timestamp`) | stats count() as cnt",
            row(3)
        );
    }

    /**
     * Scalar {@code where earliest('-100y', @timestamp)} — a relative-time
     * expression. With a 100-year lookback every row of the fixture qualifies, so
     * this asserts that the relative-time DSL parser is wired up end-to-end (no
     * fall-back to letting the EARLIEST UDF reach the filter rule unrewritten,
     * which would 500).
     */
    public void testWhereEarliestWithRelativeTimeExpression() throws IOException {
        assertRowsAnyOrder(
            "source = " + INDEX + " | where earliest('-100y', `@timestamp`) | stats count() as cnt",
            row(5)
        );
    }

    // ── index provisioning ────────────────────────────────────────────────────

    private void createParquetBackedIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + NUM_SHARDS + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"server\":    { \"type\": \"keyword\" },"
            + "    \"level\":     { \"type\": \"keyword\" },"
            + "    \"message\":   { \"type\": \"keyword\" },"
            + "    \"@timestamp\":{ \"type\": \"date\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexRows() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < ROWS.length; i++) {
            String[] r = ROWS[i];
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{")
                .append("\"server\":\"").append(r[0]).append("\",")
                .append("\"level\":\"").append(r[1]).append("\",")
                .append("\"message\":\"").append(r[2]).append("\",")
                .append("\"@timestamp\":\"").append(r[3]).append("\"")
                .append("}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Multiset comparison — group iteration order across stages is not
     * guaranteed.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsAnyOrder(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        List<String> expectedSorted = Arrays.stream(expected).map(EarliestLatestCommandIT::normalizeRow).sorted().toList();
        List<String> actualSorted = actualRows.stream().map(EarliestLatestCommandIT::normalizeRow).sorted().toList();
        assertEquals("Row multisets differ for query: " + ppl, expectedSorted, actualSorted);
    }

    private static String normalizeRow(List<Object> row) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) sb.append('|');
            sb.append(row.get(i) == null ? "<NULL>" : row.get(i).toString());
        }
        return sb.append(']').toString();
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
