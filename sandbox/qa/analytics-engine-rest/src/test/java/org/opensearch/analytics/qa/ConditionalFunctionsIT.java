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
 * End-to-end coverage for PPL
 * <a href="https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/condition/">conditional
 * functions</a>.
 *
 * <p>Currently covers scalar {@code where earliest(literal, ts)} /
 * {@code where latest(literal, ts)} only — these route through
 * {@code EarliestLatestAdapter} on the analytics-backend-datafusion plugin,
 * which rewrites the call into a {@code now()}-symbolic comparison (or a
 * TIMESTAMP literal for absolute inputs). DataFusion's
 * {@code SimplifyExpressions} then folds {@code now()} against the engine's
 * {@code query_execution_start_time}.
 *
 * <p>Other conditional functions ({@code case}, {@code if}, {@code ifnull},
 * {@code coalesce}, {@code nullif}, {@code isempty}, {@code isblank},
 * {@code regexp_match}, {@code is null} / {@code is not null} /
 * {@code ispresent}) are covered by PR #21643 — once that lands the test
 * cases append into this file.
 *
 * <p>Fixture: shared {@code calcs} dataset (17 rows). The {@code datetime0}
 * column spans {@code 2004-07-04T22:49:28Z} (key08) to
 * {@code 2004-08-02T07:59:23Z} (key02).
 */
public class ConditionalFunctionsIT extends AnalyticsRestTestCase {

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

    // ── earliest / latest ───────────────────────────────────────────────────
    //
    // Pipeline:
    //   PPL parser → Calcite RexCall (operator name "EARLIEST" / "LATEST")
    //     → BackendPlanAdapter resolves ScalarFunction.EARLIEST / LATEST →
    //       EarliestLatestAdapter rewrites the call to
    //         ts >=|<= [TIMESTAMP_LITERAL | DATETIME_PLUS(now(), INTERVAL) | date_trunc(unit, now())]
    //     → Substrait emit (now(), INTERVAL_DAY/MONTH, date_trunc resolve via
    //       the sandbox extension catalog) → DataFusion SimplifyExpressions
    //       folds now() against query_execution_start_time → native timestamp
    //       comparison.

    /**
     * {@code where earliest(absolute_literal, ts)} keeps rows with {@code ts >=}
     * the literal. 11 of 17 calcs rows have {@code datetime0 >= 2004-07-15}.
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
     * Per-row predicate selectivity for {@code earliest(literal, ts)}: a key
     * just above the threshold survives (1 row), a key just below is filtered
     * out (0 rows).
     */
    public void testEarliestAbsoluteLiteralSelectsSpecificRows() throws IOException {
        assertFirstRowLong(
            oneRow("key01") + "| where earliest('2004-07-26 00:00:00', `datetime0`) | stats count() as cnt",
            1L
        );
        assertFirstRowLong(
            oneRow("key00") + "| where earliest('2004-07-26 00:00:00', `datetime0`) | stats count() as cnt",
            0L
        );
    }

    /**
     * {@code earliest('-100y', ts)} — relative offset 100 years back. All calcs
     * rows are in 2004, so a 100-year lookback keeps all 17. Exercises the
     * pure-offset path: {@code DATETIME_PLUS(now(), INTERVAL_MONTH(-1200))}
     * which DataFusion folds at engine plan time.
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
     * 2004 so they're older than 1 year; the predicate keeps all 17.
     */
    public void testLatestRelativeOneYearKeepsAllPastRows() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | where latest('-1y', `datetime0`) | stats count() as cnt",
            17L
        );
    }

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

    // ── helpers ─────────────────────────────────────────────────────────────

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
