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
 *  (comparison / arithmetic / logical / concat) routed through the analytics-engine PPL path to DataFusion.
 *
 * <p>Each test exercises one operator on the {@code calcs} dataset in both a filter
 * ({@code where}) and a project ({@code eval}) position where applicable. Per-operator
 * inputs are hand-picked so that filter row counts and eval cell values are small and
 * stable under the dataset's current 17 rows.
 *
 * <p>Covers: {@code =, !=, <, <=, >, >=, and, or, not, in, between (via >= AND <=),
 * like, +, -, *, /, %, concat (||)}. XOR is the PPL {@code xor} function which
 * lowers to {@code NOT_EQUALS} on booleans — validated in {@link #testXorViaNotEquals()}.
 * ILIKE is deliberately omitted: Substrait's default extension catalog does not declare
 * an {@code ilike} function, so Isthmus cannot serialize it to the shape DataFusion's
 * Rust substrait consumer expects; see the Group F tracker for status.
 */
public class OperatorCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Comparisons (filter-side) ───────────────────────────────────────────────

    public void testEqualsFilter() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | where int0 = 8", 3);
    }

    public void testNotEqualsFilter() throws IOException {
        // 17 total rows. int0 = 8 matches 3 rows → 14 != 8 rows (nulls excluded by the operator).
        assertRowCount("source=" + DATASET.indexName + " | where int0 != 8 | fields int0", 8);
    }

    public void testLessThanFilter() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | where int0 < 4 | fields int0", 2);
    }

    public void testLessThanOrEqualFilter() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | where int0 <= 4 | fields int0", 5);
    }

    public void testGreaterThanFilter() throws IOException {
        // int0 distribution in calcs: 1,3,4,4,4,7,8,8,8,10,11 (+6 nulls). int0 > 8 → 10,11.
        assertRowCount("source=" + DATASET.indexName + " | where int0 > 8 | fields int0", 2);
    }

    public void testGreaterThanOrEqualFilter() throws IOException {
        // int0 >= 8 → 8,8,8,10,11
        assertRowCount("source=" + DATASET.indexName + " | where int0 >= 8 | fields int0", 5);
    }

    // ── IN / BETWEEN (Sarg fold) ───────────────────────────────────────────────

    public void testInListFilter() throws IOException {
        // IN folds to SEARCH(Sarg[...]); SargAdapter expands before substrait.
        assertRowCount("source=" + DATASET.indexName + " | where int0 in (1, 8) | fields int0", 4);
    }

    public void testBetweenAsRangeFilter() throws IOException {
        // PPL's between desugars to `>= AND <=`; Calcite folds contiguous ranges into a Sarg.
        assertRowCount(
            "source=" + DATASET.indexName + " | where int0 >= 4 and int0 <= 8 | fields int0",
            7
        );
    }

    // ── LIKE ─────────────────────────────────────────────────────────────────

    public void testLikeFilter() throws IOException {
        // PPL's `like(field, pattern)` emits SqlLibraryOperators.ILIKE (PPL treats like as
        // case-insensitive by default). Isthmus serializes ILIKE via the custom `ilike`
        // extension declared in opensearch_scalar_functions.yaml; DataFusion's substrait
        // consumer routes it to a case-insensitive LikeExpr.
        // Pattern "%e%" matches every str2 containing an 'e'.
        // str2 values: one,two,three,five,six,eight,nine,ten,eleven,twelve,fourteen,fifteen,sixteen
        // Contains 'e': one(yes),three(yes),five(yes),eight(yes),nine(yes),ten(yes),eleven(yes),
        // twelve(yes),fourteen(yes),fifteen(yes),sixteen(yes) → 11 rows (two,six exclude).
        assertRowCount("source=" + DATASET.indexName + " | where like(str2, '%e%') | fields str2", 11);
    }

    public void testLikeFilterIsCaseInsensitive() throws IOException {
        // Guards against regression to the previous ILIKE→LIKE rewrite that silently dropped
        // case-insensitivity. str0 values are all uppercase ("FURNITURE", "OFFICE SUPPLIES",
        // "TECHNOLOGY"); a lowercase pattern would match 0 rows under case-sensitive LIKE.
        // Under PPL's case-insensitive `like` (→ substrait `ilike`) it matches both FURNITURE rows.
        assertRowCount("source=" + DATASET.indexName + " | where like(str0, '%furniture%') | fields str0", 2);
    }

    // ── Logical (filter-side) ──────────────────────────────────────────────────

    public void testLogicalAndFilter() throws IOException {
        // int0 > 4 AND int0 < 10 → 7,8,8,8 = 4 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where int0 > 4 and int0 < 10 | fields int0",
            4
        );
    }

    public void testLogicalOrFilter() throws IOException {
        assertRowCount(
            "source=" + DATASET.indexName + " | where int0 = 1 or int0 = 10 | fields int0",
            2
        );
    }

    public void testLogicalNotFilter() throws IOException {
        // NOT in PPL — `where not (x > y)` syntax. Negates the inner predicate structurally.
        // int0 values: 1,3,4,4,4,7,8,8,8,10,11 (+6 nulls). NOT (int0 > 4) keeps 1,3,4,4,4 = 5 rows
        // (SQL three-valued logic excludes NULLs — Calcite's NOT on a NULL stays NULL, which is
        // truthy-equivalent to false for filtering).
        assertRowCount(
            "source=" + DATASET.indexName + " | where not (int0 > 4) | fields int0",
            5
        );
    }

    // ── XOR (PPL xor → NOT_EQUALS on BOOLEAN) ──────────────────────────────────

    public void testXorViaNotEquals() throws IOException {
        // PPL's XOR is an infix boolean operator: `a XOR b`. It lowers to `a != b` on booleans
        // (PPLFuncImpTable maps XOR → SqlStdOperatorTable.NOT_EQUALS with BOOLEAN type checker),
        // so the same not_equal Substrait extension that powers `!=` handles this. Rows survive
        // the filter only when bool0 and bool1 differ.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where bool0 xor bool1 | fields bool0, bool1"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("xor query returned no rows block", rows);
        // The calcs dataset contains rows where bool0 != bool1; assert the filter surfaces them.
        assertTrue("xor should return at least 1 row, got " + rows.size(), !rows.isEmpty());
        for (List<Object> row : rows) {
            assertFalse("bool0 xor bool1 row has equal values: " + row, row.get(0).equals(row.get(1)));
        }
    }

    // ── Arithmetic (project-side via eval + filter for verification) ───────────

    public void testArithmeticPlusInEval() throws IOException {
        // num0=12.3, num1=8.42 → sum=20.72. Select one row by key to keep expected values stable.
        assertSingleRowField(
            "source=" + DATASET.indexName + " | where key = 'key00' | eval s = num0 + num1 | fields s",
            20.72
        );
    }

    public void testArithmeticMinusInEval() throws IOException {
        assertSingleRowField(
            "source=" + DATASET.indexName + " | where key = 'key00' | eval d = num0 - num1 | fields d",
            3.88
        );
    }

    public void testArithmeticTimesInEval() throws IOException {
        // 12.3 * 8.42 = 103.566
        assertSingleRowField(
            "source=" + DATASET.indexName + " | where key = 'key00' | eval p = num0 * num1 | fields p",
            103.566
        );
    }

    // DIVIDE / MOD / CONCAT: PPL emits custom UDFs rather than the SqlStdOperatorTable entries
    // that Isthmus's default SCALAR_SIGS covers. {@link StdOperatorRewriteAdapter} rewrites them
    // to the standard Calcite operators before substrait serialisation so the default extension
    // catalog's {@code divide} / {@code modulus} / {@code concat} entries resolve.

    public void testArithmeticDivideInEval() throws IOException {
        // 12.3 / 8.42 ≈ 1.4608 — StdOperatorRewriteAdapter maps PPL DIVIDE UDF to
        // SqlStdOperatorTable.DIVIDE, which Isthmus serialises via substrait `divide`.
        assertSingleRowApprox(
            "source=" + DATASET.indexName + " | where key = 'key00' | eval q = num0 / num1 | fields q",
            1.4608,
            1e-3
        );
    }

    public void testArithmeticModInEval() throws IOException {
        // int3=8 for key00; 8 % 3 = 2 — MOD adapter → SqlStdOperatorTable.MOD → substrait `modulus`.
        assertSingleRowField(
            "source=" + DATASET.indexName + " | where key = 'key00' | eval r = int3 % 3 | fields r",
            2
        );
    }

    // ── Project-side comparisons: eval boolean result, filter by it ───────────

    public void testEqualsInEvalProjection() throws IOException {
        // eval produces a boolean, filter selects rows where it's true.
        assertRowCount(
            "source=" + DATASET.indexName + " | eval m = (int0 = 8) | where m = true | fields int0",
            3
        );
    }

    public void testAndInEvalProjection() throws IOException {
        assertRowCount(
            "source=" + DATASET.indexName + " | eval m = (int0 > 4) and (int0 < 10) | where m = true | fields int0",
            4
        );
    }

    public void testOrInEvalProjection() throws IOException {
        assertRowCount(
            "source=" + DATASET.indexName + " | eval m = (int0 = 1) or (int0 = 10) | where m = true | fields int0",
            2
        );
    }

    public void testNotInEvalProjection() throws IOException {
        assertRowCount(
            "source=" + DATASET.indexName + " | eval m = not (int0 > 4) | where m = true | fields int0",
            5
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private void assertRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Row count mismatch for query: " + ppl, expected, rows.size());
    }

    private void assertSingleRowField(String ppl, Object expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Expected exactly 1 row for query: " + ppl, 1, rows.size());
        Object actual = rows.get(0).get(0);
        assertCellEquals("Cell value mismatch for query: " + ppl, expected, actual);
    }

    private void assertSingleRowApprox(String ppl, double expected, double tolerance) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Expected exactly 1 row for query: " + ppl, 1, rows.size());
        Object actual = rows.get(0).get(0);
        assertNotNull("Cell is null for query: " + ppl, actual);
        double actualD = ((Number) actual).doubleValue();
        if (Math.abs(actualD - expected) > tolerance) {
            fail("Expected ~" + expected + " (tolerance " + tolerance + ") but got " + actualD + " for query: " + ppl);
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Numeric-tolerant cell comparison: Integer/Long/Double arriving from JSON parsing
     * may differ by concrete boxed type even when numerically equal.
     */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                // Fall back to tolerance for floating-point arithmetic residue.
                if (Math.abs(e - a) > 1e-9) {
                    fail(message + ": expected <" + expected + "> but was <" + actual + ">");
                }
            }
            return;
        }
        assertEquals(message, expected, actual);
    }

    // Suppress the "unused" warning — Arrays.toString is retained for debug parity with
    // other QA ITs in this package that dump row arrays on assertion failures.
    @SuppressWarnings("unused")
    private static String debugRows(List<List<Object>> rows) {
        return Arrays.toString(rows.toArray());
    }
}
