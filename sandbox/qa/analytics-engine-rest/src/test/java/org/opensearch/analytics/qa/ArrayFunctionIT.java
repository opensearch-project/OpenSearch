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
 * End-to-end coverage for the PPL array-construction and multivalue (mv*)
 * functions on the analytics-engine route (PPL → CalciteRelNodeVisitor →
 * Substrait → DataFusion). Mirrors the SQL plugin's
 * {@code CalciteArrayFunctionIT} one-test-method-to-one for the subset of tests
 * the analytics-engine path supports today.
 *
 * <p>Function surface exercised:
 * <ul>
 *   <li>{@code array(...)} → DataFusion {@code make_array} via
 *       {@link org.opensearch.be.datafusion.MakeArrayAdapter}.</li>
 *   <li>{@code array_length} → DataFusion native {@code array_length}.</li>
 *   <li>{@code mvindex(arr, from, to)} (range form) → DataFusion {@code array_slice}
 *       via {@link org.opensearch.be.datafusion.ArraySliceAdapter} (BIGINT index
 *       coerce + 0-based-{@code (start, length)} → 1-based-{@code (start, end)}).</li>
 *   <li>{@code mvindex(arr, N)} (single-element form) → DataFusion {@code array_element}
 *       via {@link org.opensearch.be.datafusion.ArrayElementAdapter}.</li>
 *   <li>{@code mvdedup(arr)} → DataFusion native {@code array_distinct}.</li>
 *   <li>{@code mvjoin(arr, sep)} → DataFusion {@code array_to_string} via
 *       {@link org.opensearch.be.datafusion.ArrayToStringAdapter}.</li>
 *   <li>{@code mvzip(left, right [, sep])} → custom Rust UDF {@code udf::mvzip}.</li>
 *   <li>{@code mvfind(arr, regex)} → custom Rust UDF {@code udf::mvfind}.</li>
 *   <li>{@code split(str, delim)} (returns array) → DataFusion {@code string_to_array}.</li>
 * </ul>
 *
 * <p>The {@code calcs} dataset is used as a scan target; most tests build literal
 * arrays inside {@code eval} so the field types don't matter — what matters is
 * that the source is a parquet-backed index the analytics-engine planner can
 * scan.
 *
 * <p>Tests for lambda-based functions ({@code transform}, {@code mvmap},
 * {@code reduce}, {@code forall}, {@code exists}, {@code filter}) are
 * intentionally absent: substrait extension YAML doesn't support declaring
 * {@code func<…>} lambda-typed arguments, so those don't ship through the
 * analytics-engine route in this PR. Empty-array tests are also absent —
 * {@code array()} defaults to {@code ARRAY[UNKNOWN]} which substrait can't
 * encode without the SQL companion {@code #5421} default to {@code VARCHAR}.
 */
public class ArrayFunctionIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** Base query template: pin to one row so every assertion runs against a single result row. */
    private String oneRow() {
        return "source=" + DATASET.indexName + " | head 1 ";
    }

    // ── array(...) constructor ──────────────────────────────────────────────

    /** Mixed-numeric literal array — exercises the BigDecimal → Double row-codec
     *  promotion (without it, decimal cells truncate to integers). */
    public void testArray() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = array(1, -1.5, 2, 1.0) | fields result",
            Arrays.asList(1.0, -1.5, 2.0, 1.0));
    }

    /** Mixed int+string literal array — Calcite widens to {@code ARRAY<VARCHAR>}
     *  via {@code ArrayFunctionImpl.internalCast}. */
    public void testArrayWithString() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = array(1, 'demo') | fields result",
            Arrays.asList("1", "demo"));
    }

    // ── array_length ────────────────────────────────────────────────────────

    public void testArrayLength() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval arr = array(1, -1.5, 2, 1.0) | eval len = array_length(arr) | fields len",
            4.0);
    }

    // ── mvindex range (array_slice) ─────────────────────────────────────────

    /** {@code mvindex(arr, 1, 3)} — 0-based-(start, length) → DataFusion 1-based-(start, end inclusive)
     *  via {@link org.opensearch.be.datafusion.ArraySliceAdapter}. Without the rewrite the result
     *  would be {@code [1, 2, 3]} instead of the expected {@code [2, 3, 4]}. */
    public void testMvindexRangePositive() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array(1, 2, 3, 4, 5) | eval result = mvindex(arr, 1, 3) | fields result",
            Arrays.asList(2, 3, 4));
    }

    /** Negative indices — DataFusion's array_slice supports them natively. */
    public void testMvindexRangeNegative() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array(1, 2, 3, 4, 5) | eval result = mvindex(arr, -3, -1) | fields result",
            Arrays.asList(3, 4, 5));
    }

    public void testMvindexRangeFirstThree() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array(10, 20, 30, 40, 50) | eval result = mvindex(arr, 0, 2) | fields result",
            Arrays.asList(10, 20, 30));
    }

    // ── mvindex single (array_element) ──────────────────────────────────────

    /** {@code mvindex(arr, N)} with a single index — PPL emits Calcite's
     *  {@code SqlStdOperatorTable.ITEM} which {@link org.opensearch.be.datafusion.ArrayElementAdapter}
     *  renames to DataFusion {@code array_element} with a BIGINT-coerced 1-based index. */
    public void testMvindexSingleElementPositive() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval arr = array(10, 20, 30) | eval result = mvindex(arr, 1) | fields result",
            20.0);
    }

    public void testMvindexSingleElementNegative() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval arr = array(10, 20, 30) | eval result = mvindex(arr, -1) | fields result",
            30.0);
    }

    // ── mvdedup (array_distinct) ────────────────────────────────────────────

    public void testMvdedupWithDuplicates() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array(1, 2, 2, 3, 3, 3) | eval result = mvdedup(arr) | fields result",
            Arrays.asList(1, 2, 3));
    }

    public void testMvdedupWithStrings() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array('a', 'b', 'a', 'c', 'b') | eval result = mvdedup(arr) | fields result",
            Arrays.asList("a", "b", "c"));
    }

    public void testMvdedupAllDuplicates() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval arr = array(7, 7, 7) | eval result = mvdedup(arr) | fields result",
            Arrays.asList(7));
    }

    // ── mvjoin (array_to_string) ────────────────────────────────────────────

    public void testMvjoinWithStringArray() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval result = mvjoin(array('a', 'b', 'c'), ',') | fields result",
            "a,b,c");
    }

    public void testMvjoinWithStringifiedNumbers() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval result = mvjoin(array('1', '2', '3'), ' | ') | fields result",
            "1 | 2 | 3");
    }

    public void testMvjoinWithSpecialDelimiters() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval result = mvjoin(array('x', 'y'), '-->') | fields result",
            "x-->y");
    }

    // ── mvzip (Rust UDF) ────────────────────────────────────────────────────

    public void testMvzipBasic() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = mvzip(array('a', 'b', 'c'), array('1', '2', '3')) | fields result",
            Arrays.asList("a,1", "b,2", "c,3"));
    }

    public void testMvzipWithCustomDelimiter() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = mvzip(array('a', 'b'), array('1', '2'), '-') | fields result",
            Arrays.asList("a-1", "b-2"));
    }

    public void testMvzipNested() throws IOException {
        assertFirstRowList(
            oneRow()
                + "| eval r = mvzip(mvzip(array('a','b'), array('1','2')), array('x','y')) | fields r",
            Arrays.asList("a,1,x", "b,2,y"));
    }

    // ── mvfind (Rust UDF) ───────────────────────────────────────────────────

    /** Returns the 0-based index of the first array element matching the regex. */
    public void testMvfindWithMatch() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = mvfind(array('apple', 'banana', 'cherry'), 'ban.*') | fields result",
            1.0);
    }

    public void testMvfindWithNoMatch() throws IOException {
        assertFirstRowNull(
            oneRow() + "| eval result = mvfind(array('apple', 'banana'), 'zzz') | fields result");
    }

    /** Dynamic regex — exercises the {@code SqlLibraryOperators.CONCAT_FUNCTION} → substrait
     *  {@code concat} Sig bridge added in this PR. Without that bridge the call fails substrait
     *  conversion with {@code Unable to convert call CONCAT(string, string)}. */
    public void testMvfindWithDynamicRegex() throws IOException {
        assertFirstRowDouble(
            oneRow()
                + "| eval result = mvfind(array('apple', 'banana', 'cherry'), concat('ban', '.*')) | fields result",
            1.0);
    }

    // ── split (returns array of strings) ─────────────────────────────────

    public void testSplitWithSemicolonDelimiter() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = split('a;b;c', ';') | fields result",
            Arrays.asList("a", "b", "c"));
    }

    public void testSplitWithMultiCharDelimiter() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = split('a::b::c', '::') | fields result",
            Arrays.asList("a", "b", "c"));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /** Numeric-tolerant list comparison — Jackson parses JSON numbers as
     *  Integer/Long/Double interchangeably, so equality on cross-type numbers
     *  fails even when values match. Compare via {@link Double#compare} on
     *  numeric pairs and {@link Object#equals} otherwise. */
    private void assertFirstRowList(String ppl, List<?> expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null array result for query [" + ppl + "]", cell);
        assertTrue(
            "Expected list result for query [" + ppl + "] but got: " + cell + " (" + cell.getClass() + ")",
            cell instanceof List);
        List<?> actual = (List<?>) cell;
        assertEquals(
            "Length mismatch for query [" + ppl + "]: expected " + expected + " but got " + actual,
            expected.size(),
            actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertCellEquals(expected.get(i), actual.get(i));
        }
    }

    private void assertFirstRowDouble(String ppl, double expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).doubleValue(), 1e-9);
    }

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private void assertFirstRowNull(String ppl) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNull("Expected null result for query [" + ppl + "] but got: " + cell, cell);
    }

    private static void assertCellEquals(Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            assertEquals(
                "Numeric value mismatch",
                ((Number) expected).doubleValue(),
                ((Number) actual).doubleValue(),
                1e-9);
            return;
        }
        assertEquals(expected, actual);
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
