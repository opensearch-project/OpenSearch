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
 * End-to-end coverage for PPL scalar
 * <a href="https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/datetime/">{@code earliest}
 * and {@code latest}</a> filter predicates on the analytics-engine route.
 *
 * <p>Pipeline exercised:
 * <pre>
 *   PPL parser → Calcite RexCall (operator name "EARLIEST" / "LATEST")
 *     → BackendPlanAdapter resolves ScalarFunction.EARLIEST / LATEST →
 *       EarliestLatestAdapter rewrites the call to
 *       {@code ts >=|<= [TIMESTAMP_LITERAL | DATETIME_PLUS(now(), INTERVAL) | date_trunc(unit, now())]}
 *     → Substrait emit (now(), INTERVAL_DAY/MONTH, date_trunc resolve via the
 *       sandbox extension catalog) → DataFusion {@code SimplifyExpressions}
 *       folds {@code now()} against {@code query_execution_start_time} → native
 *       timestamp comparison → coordinator → PPL response.
 * </pre>
 *
 * <p>Fixture: shared {@code calcs} dataset (17 rows, all in 2004). The
 * {@code datetime0} column spans {@code 2004-07-04T22:49:28Z} (key08, earliest)
 * to {@code 2004-08-02T07:59:23Z} (key02, latest).
 */
public class EarliestLatestCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    // ── Absolute literal — emits TIMESTAMP_LITERAL on RHS ───────────────────

    /**
     * {@code where earliest(absolute_literal, ts)} keeps rows with {@code ts >=}
     * the literal. Of 17 rows in calcs, 11 have {@code datetime0 >= 2004-07-15}.
     */
    public void testEarliestAbsoluteLiteralCount() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where earliest('2004-07-15 00:00:00', `datetime0`) | stats count() as cnt",
            11L
        );
    }

    /**
     * {@code where latest(absolute_literal, ts)} keeps rows with {@code ts <=}
     * the literal. 6 rows have {@code datetime0 <= 2004-07-15}.
     */
    public void testLatestAbsoluteLiteralCount() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where latest('2004-07-15 00:00:00', `datetime0`) | stats count() as cnt",
            6L
        );
    }

    /**
     * Specific row keep: {@code earliest(literal, ts)} on a literal that splits
     * a single key out. With {@code 2004-07-26T00:00:00Z} as the threshold, only
     * rows with {@code datetime0 >= that} survive (key01, key02, key04, key05,
     * key06, key09, key11, key14, key16) — exercises the predicate selectivity
     * end-to-end, not just the count.
     */
    public void testEarliestAbsoluteLiteralSelectsSpecificRows() throws IOException {
        // Verify against an exact key that's just above the threshold.
        assertFirstRowLong(
            oneRow("key01") + "| where earliest('2004-07-26 00:00:00', `datetime0`) | stats count() as cnt",
            1L
        );
        // And one just below — should be filtered out, leaving zero rows that pass through stats.
        assertFirstRowLong(
            oneRow("key00") + "| where earliest('2004-07-26 00:00:00', `datetime0`) | stats count() as cnt",
            0L
        );
    }

    // ── Pure offset — emits DATETIME_PLUS(now(), INTERVAL) ──────────────────

    /**
     * {@code earliest('-100y', ts)} — relative offset 100 years back. Every row
     * of the calcs fixture sits in 2004, so a 100-year lookback keeps all 17.
     * This proves the offset path lowers to a {@code now() ± INTERVAL} shape
     * that DataFusion's optimizer folds at engine plan time.
     */
    public void testEarliestRelativeOffsetKeepsAllRows() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where earliest('-100y', `datetime0`) | stats count() as cnt",
            17L
        );
    }

    /**
     * {@code latest('+100y', ts)} — symmetric: 100 years forward keeps every
     * row that's in the past relative to "now".
     */
    public void testLatestRelativeOffsetKeepsAllRows() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where latest('+100y', `datetime0`) | stats count() as cnt",
            17L
        );
    }

    /**
     * {@code latest('-1y', ts)} — 1-year-ago threshold. All calcs rows are in
     * 2004 so they're all older than 1 year; the predicate should keep all 17.
     */
    public void testLatestRelativeOneYearKeepsAllPastRows() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where latest('-1y', `datetime0`) | stats count() as cnt",
            17L
        );
    }

    // ── 'now' literal — emits bare now() ────────────────────────────────────

    /**
     * {@code latest('now', ts)} — the literal "now" lowers to a bare
     * {@code now()} on the RHS; the predicate becomes {@code ts <= now()}. All
     * calcs rows are in 2004 so they all qualify.
     */
    public void testLatestNowKeepsAllPastRows() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where latest('now', `datetime0`) | stats count() as cnt",
            17L
        );
    }

    // ── helpers (mirror DateTimeScalarFunctionsIT / ConditionalFunctionsIT) ─

    private void assertFirstRowLong(String ppl, long expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).longValue());
    }

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
