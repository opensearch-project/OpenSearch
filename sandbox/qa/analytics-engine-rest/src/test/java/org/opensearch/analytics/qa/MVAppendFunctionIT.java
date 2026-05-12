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
 * End-to-end coverage for PPL {@code mvappend(arg1, arg2, …)} on the
 * analytics-engine route. Mirrors the SQL plugin's
 * {@code CalciteMVAppendFunctionIT} one-test-method-to-one for the subset of
 * tests that pass on the analytics-engine path.
 *
 * <p>{@code mvappend} flattens an arbitrary mix of scalar and array operands
 * into a single array, dropping null elements. Onboarded as a custom Rust UDF
 * ({@code udf::mvappend}) registered at session-context creation; the Java
 * adapter ({@link org.opensearch.be.datafusion.MvappendAdapter}) reshapes scalar
 * operands into singleton {@code make_array} calls so substrait's variadic-{@code any1}
 * shape sees a uniform {@code list[componentType]} across every position.
 *
 * <p>Tests covering genuinely heterogeneous mvappend signatures
 * ({@code mvappend(1, 'text', 2.5)}, {@code mvappend(age, 'years', 'old')},
 * {@code mvappend('test', nullif(1,1), 2)}) are absent because Calcite legitimately
 * widens those to {@code ARRAY[ANY]} — substrait can't encode {@code ANY}, and
 * Arrow's Union arrays aren't operated on by {@code datafusion-functions-array}.
 * Empty-array operand tests are also absent — the empty {@code array()} default
 * surfaces as {@code ARRAY[UNKNOWN]}/{@code ARRAY[VARCHAR]} in the column ref,
 * which type-inference can't reach back through the project chain to ignore.
 *
 * <p>The {@code testMvappendInWhereClause} variant (filter predicate on an
 * ARRAY field) is also absent because the analytics-engine planner's filter
 * rule rejects {@code EQUALS} on an ARRAY field without walking into the
 * predicate tree — that's a separate planner refactor tracked under #21554's
 * "What's left" section.
 */
public class MVAppendFunctionIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow() {
        return "source=" + DATASET.indexName + " | head 1 ";
    }

    // ── uniform-typed scalar variadic ───────────────────────────────────────

    public void testMvappendWithMultipleElements() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = mvappend(1, 2, 3) | fields result",
            Arrays.asList(1, 2, 3));
    }

    public void testMvappendWithSingleElement() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = mvappend(42) | fields result",
            Arrays.asList(42));
    }

    public void testMvappendWithStringValues() throws IOException {
        assertFirstRowList(
            oneRow() + "| eval result = mvappend('hello', 'world') | fields result",
            Arrays.asList("hello", "world"));
    }

    // ── array operands (uniform element type) ───────────────────────────────

    public void testMvappendWithArrayFlattening() throws IOException {
        assertFirstRowList(
            oneRow()
                + "| eval arr1 = array(1, 2), arr2 = array(3, 4), result = mvappend(arr1, arr2) | fields result",
            Arrays.asList(1, 2, 3, 4));
    }

    public void testMvappendWithNestedArrays() throws IOException {
        assertFirstRowList(
            oneRow()
                + "| eval arr1 = array('a', 'b'), arr2 = array('c'), arr3 = array('d', 'e'),"
                + " result = mvappend(arr1, arr2, arr3) | fields result",
            Arrays.asList("a", "b", "c", "d", "e"));
    }

    // ── field references ────────────────────────────────────────────────────

    /** Two VARCHAR field references → uniform {@code ARRAY[VARCHAR]}. Anchored
     *  to a specific row by filtering on {@code key} so the assertion is
     *  deterministic. */
    public void testMvappendWithRealFields() throws IOException {
        assertFirstRowList(
            "source=" + DATASET.indexName
                + " | where key='key00' | head 1 | eval result = mvappend(str0, str1) | fields result",
            // calcs row key00: str0='FURNITURE', str1='CLAMP ON LAMPS'
            Arrays.asList("FURNITURE", "CLAMP ON LAMPS"));
    }

    // ── tests gated on SQL companion #5424 ──────────────────────────────────
    // The following SQL-side tests are intentionally absent until
    // opensearch-project/sql#5424 (the {@code MVAppendFunctionImpl} widening
    // via {@code leastRestrictive} + DECIMAL → DOUBLE promotion + operand
    // pre-cast in {@code MVAppendImplementor}) is merged and republished as
    // {@code unified-query-core:3.7.0.0-SNAPSHOT}. Without it, these collapse
    // to {@code ARRAY[ANY]} which substrait can't encode:
    //
    //   testMvappendWithMixedArrayAndScalar — array(1,2), 3, 4 (nullability bridge)
    //   testMvappendWithNumericArrays       — array(1.5,2.5), array(3.5), 4.5 (nullability bridge)
    //   testMvappendWithIntAndDouble        — 1, 2.5 (DECIMAL → DOUBLE promotion + pre-cast)
    //   testMvappendWithComplexExpression   — array(int0), array(int0*2), int0+10 (nullability bridge)
    //
    // Add them back once #5424 lands. Their SQL-side counterparts are verified
    // in CalciteMVAppendFunctionIT against the analytics-engine route.

    // ── helpers ─────────────────────────────────────────────────────────────

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
