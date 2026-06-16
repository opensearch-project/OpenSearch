/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

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
 * <ul>
 *   <li><b>Native structural (no adapter)</b> — {@code case ... when ... end}, {@code if}
 *       (aliased to CASE by PPL), {@code ifnull} (aliased to COALESCE by PPL), {@code nullif}
 *       (lowered to {@code CASE(EQUALS(a,b), NULL, a)} by PPL), {@code regexp_match}
 *       (aliased to REGEXP_CONTAINS). The backend sees standard Substrait primitives and
 *       DataFusion's substrait consumer handles each natively.</li>
 *   <li><b>Predicate-shape in project</b> — {@code x IS NULL} / {@code x IS NOT NULL} /
 *       {@code ispresent(x)} (PPL aliases the last to IS_NOT_NULL).</li>
 *   <li><b>Datafusion-side adapter</b> — scalar {@code where earliest(literal, ts)} /
 *       {@code where latest(literal, ts)} are rewritten by {@code EarliestLatestAdapter}
 *       into a {@code now()}-symbolic comparison (or a TIMESTAMP literal for absolute
 *       inputs). DataFusion's {@code SimplifyExpressions} then folds {@code now()}
 *       against the engine's {@code query_execution_start_time}.</li>
 * </ul>
 *
 * <p>Fixture columns from the {@code calcs} dataset (per row keyed by {@code key}):
 * <ul>
 *   <li>{@code int0}: nullable BIGINT — 6 null rows (key01, key02, key03, key07, key08, key12) out
 *       of 17.</li>
 *   <li>{@code num0}: nullable DOUBLE — nulls on key07, key09–key16.</li>
 *   <li>{@code str0}: never null across the 17 rows; one of {@code FURNITURE},
 *       {@code OFFICE SUPPLIES}, or {@code TECHNOLOGY}.</li>
 *   <li>{@code str2}: nullable VARCHAR — null on key03, key06, key12, key16. Non-null value
 *       {@code "one"} on key00.</li>
 *   <li>{@code datetime0}: non-null TIMESTAMP — spans
 *       {@code 2004-07-04T22:49:28Z} (key08) to {@code 2004-08-02T07:59:23Z} (key02).</li>
 * </ul>
 */
public class ConditionalFunctionsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    // ── case ────────────────────────────────────────────────────────────────

    /** Basic two-branch CASE with else — exercises the structural IfThen shape. */
    public void testCaseTwoBranches() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = case(str2 = 'one', 'match' else 'nomatch') | fields v",
            "match"
        );
    }

    /** Multi-branch CASE with fall-through to the final default arm. */
    public void testCaseMultiBranchDefault() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = case(str2 = 'two', 'a', str2 = 'three', 'b' else 'else') | fields v",
            "else"
        );
    }

    /** CASE without an else clause returns NULL on fall-through. */
    public void testCaseNoElseFallsThroughToNull() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00") + "| eval v = case(str2 = 'two', 'a', str2 = 'three', 'b') | fields v"
        );
        assertNull("case without else on no-match should be NULL but was " + cell, cell);
    }

    // ── if ──────────────────────────────────────────────────────────────────

    /** {@code if(cond, t, f)} — registers this to {@code SqlStdOperatorTable.CASE}. */
    public void testIfBranchesOnBoolean() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = if(str2 = 'one', 'yes', 'no') | fields v", "yes");
        assertFirstRowString(oneRow("key00") + "| eval v = if(str2 = 'two', 'yes', 'no') | fields v", "no");
    }

    /**
     * {@code if(true, a, b)} and {@code if(false, a, b)} — constant-condition shape.
     */
    public void testIfConstantConditions() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = if(true, 'first', 'second') | fields v", "first");
        assertFirstRowString(oneRow("key00") + "| eval v = if(false, 'first', 'second') | fields v", "second");
    }

    // ── ifnull ──────────────────────────────────────────────────────────────

    /**
     * {@code ifnull(a, b)} — PPL aliases to stock {@code SqlStdOperatorTable.COALESCE}.
     */
    public void testIfnullReplacesNull() throws IOException {
        // int0 is NULL for key02; ifnull should substitute the default.
        Object cell = firstRowFirstCell(oneRow("key02") + "| eval v = ifnull(int0, -1) | fields v");
        assertTrue("ifnull result should be numeric, got: " + cell, cell instanceof Number);
        assertEquals(-1L, ((Number) cell).longValue());
    }

    /** Non-null input passes through. */
    public void testIfnullPassesNonNullThrough() throws IOException {
        // int0 = 1 on key00.
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = ifnull(int0, -1) | fields v");
        assertTrue(cell instanceof Number);
        assertEquals(1L, ((Number) cell).longValue());
    }

    /**
     * {@code ifnull(x, x)} with a null column — both arguments resolve to NULL, so the result is
     * NULL.
     */
    public void testIfnullBothArgsNullProducesNull() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key02") + "| eval v = ifnull(int0, int0) | fields v");
        assertNull("ifnull(null, null) should be NULL but was " + cell, cell);
    }

    /**
     * {@code ifnull(a, b)} where both operands are non-null literals — first wins, second never
     * consulted.
     */
    public void testIfnullTwoNonNullLiteralsReturnsFirst() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = ifnull(200, int0) | fields v");
        assertTrue(cell instanceof Number);
        assertEquals(200L, ((Number) cell).longValue());
    }

    // ── x IS NULL / IS NOT NULL / ispresent (project side) ───────────────────

    /** {@code x IS NULL} in an eval projection. */
    public void testIsNullInProject() throws IOException {
        assertFirstRowBoolean(oneRow("key02") + "| eval v = int0 is null | fields v", true);
        assertFirstRowBoolean(oneRow("key00") + "| eval v = int0 is null | fields v", false);
    }

    /** {@code x IS NOT NULL} in an eval projection. */
    public void testIsNotNullInProject() throws IOException {
        assertFirstRowBoolean(oneRow("key02") + "| eval v = int0 is not null | fields v", false);
        assertFirstRowBoolean(oneRow("key00") + "| eval v = int0 is not null | fields v", true);
    }

    /** {@code ispresent(x)} — PPL aliases to {@code IS_NOT_NULL}. */
    public void testIspresent() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval v = ispresent(int0) | fields v", true);
        assertFirstRowBoolean(oneRow("key02") + "| eval v = ispresent(int0) | fields v", false);
    }

    // ── nullif ──────────────────────────────────────────────────────────────

    /**
     * PPL lowers {@code nullif(a, b)} to {@code CASE WHEN a = b THEN NULL ELSE a END}.
     */
    public void testNullifMatching() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = nullif(str2, 'one') | fields v");
        assertNull("nullif('one', 'one') should be NULL, got: " + cell, cell);
    }

    public void testNullifNonMatching() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = nullif(str2, 'two') | fields v", "one");
    }

    public void testNullifSameColumnProducesNull() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = nullif(str2, str2) | fields v");
        assertNull("nullif(x, x) should be NULL but was " + cell, cell);
    }

    // ── coalesce ────────────────────────────────────────────────────────────

    /**
     * {@code coalesce(a, b, ...)} — returns the first non-null argument
     */
    public void testCoalesceReturnsFirstNonNull() throws IOException {
        // int0 is NULL on key02; coalesce(int0, 42) → 42.
        Object cell = firstRowFirstCell(oneRow("key02") + "| eval v = coalesce(int0, 42) | fields v");
        assertTrue("coalesce result should be numeric, got: " + cell, cell instanceof Number);
        assertEquals(42L, ((Number) cell).longValue());
    }

    /** Non-null first operand passes through. */
    public void testCoalescePassesNonNullThrough() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = coalesce(int0, 99) | fields v");
        assertTrue(cell instanceof Number);
        assertEquals(1L, ((Number) cell).longValue());
    }

    public void testCoalesceThreeArgs() throws IOException {
        // key07: int0=null, num0=null → coalesce falls through to 7.
        Object cell = firstRowFirstCell(oneRow("key07") + "| eval v = coalesce(int0, num0, 7) | fields v");
        assertTrue(cell instanceof Number);
        assertEquals(7.0, ((Number) cell).doubleValue(), 1e-9);
    }

    /**
     * Mixed-type coalesce with VARCHAR + numeric.
     *
     * <p>key00 has str2='one', int0=1 — so coalesce(str2, int0, 'fallback') resolves to 'one'.
     * The real coercion exercises on key12 (str2=null, int0=null), where the third literal
     * 'fallback' wins.
     */
    public void testCoalesceMixedTypes() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = coalesce(str2, int0, 'fallback') | fields v",
            "one"
        );
        assertFirstRowString(
            oneRow("key12") + "| eval v = coalesce(str2, int0, 'fallback') | fields v",
            "fallback"
        );
    }

    // ── isempty ─────────────────────────────────────────────────────────────

    /**
     * {@code isempty(x)} — lowered to {@code OR(IS_NULL(x), IS_EMPTY(x))}; the adapter
     * rewrites the IS_EMPTY arm to {@code CHAR_LENGTH(x) = 0}.
     */
    public void testIsemptyOnNonEmptyString() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval v = isempty(str2) | fields v", false);
    }

    public void testIsemptyOnEmptyLiteral() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval s = '' | eval v = isempty(s) | fields v", true);
    }

    public void testIsemptyOnNullInput() throws IOException {
        // str2 is NULL on key12.
        assertFirstRowBoolean(oneRow("key12") + "| eval v = isempty(str2) | fields v", true);
    }

    // ── isblank ─────────────────────────────────────────────────────────────

    /**
     * {@code isblank(x)} — lowered to {@code OR(IS_NULL(x), IS_EMPTY(TRIM(x)))}. With
     * the adapter, the IS_EMPTY(TRIM(x)) arm becomes {@code CHAR_LENGTH(TRIM(x)) = 0}.
     */
    public void testIsblankOnWhitespaceOnly() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval s = '   ' | eval v = isblank(s) | fields v", true);
    }

    public void testIsblankOnNonEmpty() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval v = isblank(str2) | fields v", false);
    }

    public void testIsblankOnNullInput() throws IOException {
        // str2 is NULL on key12.
        assertFirstRowBoolean(oneRow("key12") + "| eval v = isblank(str2) | fields v", true);
    }

    // ── regexp_match ────────────────────────────────────────────────────────

    /**
     * {@code regexp_match(x, pattern)}.
     */
    public void testRegexpMatchMatches() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval v = regexp_match(str2, 'on.') | fields v", true);
    }

    public void testRegexpMatchNoMatch() throws IOException {
        assertFirstRowBoolean(oneRow("key00") + "| eval v = regexp_match(str2, '^z') | fields v", false);
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
    @AwaitsFix(bugUrl = "Real opensearch-sql plugin: \"Failed to start streaming fragment on [calcs][0]\" - head|where|where residual filter is mis-routed through the indexed executor (ffm.rs routes on the always-present __row_id__ column). Needs the ffm routing + countFilters<=1 fix (engine/rust fix, separate PR).")
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

    /**
     * Regression: {@code earliest} inside an {@code eval} (Project context) over a non-null
     * TIMESTAMP column. The {@code datetime0} column is non-null, and a folded TIMESTAMP literal
     * is also non-null, so the comparison's inferred return type is {@code BOOLEAN NOT NULL}
     * — but the {@code earliest} call was declared nullable BOOLEAN. Without the type-pinning
     * cast in {@code EarliestLatestAdapter}, the enclosing Project's cached rowType (nullable
     * BOOLEAN) trips Calcite's {@code BOOLEAN vs BOOLEAN NOT NULL} assertion at execution.
     * All 17 calcs rows have {@code datetime0 >= 1900-01-01}, so the predicate is true everywhere.
     */
    public void testEarliestInProjectOverNotNullTimestampDoesNotCrash() throws IOException {
        assertFirstRowLong(
            "source = " + DATASET.indexName + " | eval is_after = earliest('1900-01-01 00:00:00', `datetime0`)"
                + " | where is_after | stats count() as cnt",
            17L
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private void assertFirstRowBoolean(String ppl, boolean expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null boolean result for query [" + ppl + "]", cell);
        assertTrue("Expected boolean result for query [" + ppl + "] but got: " + cell, cell instanceof Boolean);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private void assertFirstRowLong(String ppl, long expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).longValue());
    }

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }

}
