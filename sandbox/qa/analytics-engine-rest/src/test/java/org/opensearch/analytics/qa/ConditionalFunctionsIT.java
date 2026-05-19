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

import org.junit.Ignore;

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
 * </ul>
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
