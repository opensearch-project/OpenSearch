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
 * Self-contained integration test for PPL {@code sort} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteSortCommandIT} / {@code CalcitePPLSortIT}. {@code sort} lowers
 * to {@code LogicalSort}; the asc / desc / nulls-first / nulls-last variants set the
 * collation field on the same RelNode. Push-down sort by an expression (`sort abs(num0)`)
 * lifts the expression into a {@code LogicalProject} child of the sort, which is what
 * exercises the new project-side capabilities for {@link org.opensearch.analytics.spi.ScalarFunction#ABS}
 * and {@link org.opensearch.analytics.spi.ScalarFunction#SUBSTRING} added in this PR.
 */
public class SortCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static final Dataset DATASET_MULTI = new Dataset("calcs", "calcs_multi_sort");

    private static boolean dataProvisioned = false;
    private static boolean multiProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** Provision a multi-shard calcs index for tests that need to exercise the multi-shard
     *  sort/project/head planner path. Kept separate from {@link #DATASET} so the abs/substring
     *  runtime-flake tests that only pass at single-shard aren't destabilized. */
    private void ensureMultiShardProvisioned() throws IOException {
        if (multiProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET_MULTI, 3);
            multiProvisioned = true;
        }
    }

    // ── plain field sort ───────────────────────────────────────────────────────

    public void testSortAscByInt() throws IOException {
        // int0 across the 17 calcs rows: [1, null, null, null, 7, 3, 8, null, null, 8, 4, 10,
        // null, 4, 11, 4, 8] — 6 nulls and 11 integers. Default sort is ASC nulls-first.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | sort int0 | fields int0",
            row((Object) null), row((Object) null), row((Object) null),
            row((Object) null), row((Object) null), row((Object) null),
            row(1), row(3), row(4), row(4), row(4), row(7), row(8), row(8), row(8), row(10), row(11)
        );
    }

    public void testSortDescByInt() throws IOException {
        // DESC nulls-last (the analytics path follows Calcite's default DESC = NULLS LAST).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | sort -int0 | fields int0",
            row(11), row(10), row(8), row(8), row(8), row(7), row(4), row(4), row(4),
            row(3), row(1),
            row((Object) null), row((Object) null), row((Object) null),
            row((Object) null), row((Object) null), row((Object) null)
        );
    }

    // ── push-down sort by scalar expression — exercises ABS / SUBSTRING capabilities ──

    public void testSortByAbsExpression() throws IOException {
        // `abs(num0)` lowers to ABS($N) inside a LogicalProject child of the sort. Without
        // ABS in STANDARD_PROJECT_OPS, the analytics planner rejects the projection with
        // "No backend supports scalar function [ABS] among [datafusion]".
        //
        // Calcs num0: [12.3, -12.3, 15.7, -15.7, 3.5, -3.5, 0, null, 10, null x8] — 9 nulls
        // and 8 non-nulls. abs(num0) preserves null and yields {0, 3.5, 3.5, 10, 12.3, 12.3,
        // 15.7, 15.7} for the non-null tail. Sorted ASC nulls-first puts the 9 nulls first.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eval n = abs(num0) | sort n | fields n | head 9"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count", 9, rows.size());
        for (int i = 0; i < 9; i++) {
            assertNull("Row " + i + " should be null", rows.get(i).get(0));
        }
    }

    public void testSortByAbsTakesNonNullsFromTail() throws IOException {
        // Skip past the 9 nulls and verify the 8 non-null abs values appear in ASC order.
        Map<String, Object> response = executePpl(
            "source="
                + DATASET.indexName
                + " | eval n = abs(num0) | sort n | fields n | head 8 from 9"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count after 9 nulls", 8, rows.size());
        double[] expectedSorted = { 0, 3.5, 3.5, 10, 12.3, 12.3, 15.7, 15.7 };
        for (int i = 0; i < expectedSorted.length; i++) {
            Object v = rows.get(i).get(0);
            assertNotNull("Row " + i + " unexpectedly null", v);
            assertEquals(
                "abs(num0) sorted value at row " + i,
                expectedSorted[i],
                ((Number) v).doubleValue(),
                1e-9
            );
        }
    }

    /**
     * Sort → Project → head pipeline. Exercises the exact shape flagged as a planner
     * landmine in {@code OpenSearchDistributionTraitDef.convert()}: a collated Sort
     * under a LIMIT with a narrowing Project in between, over a multi-shard-ish scan.
     * The planner has to place an ER under the collated Sort (concat gather + global
     * sort) and leave the outer LIMIT without an additional ER — if Volcano ever
     * explores a SINGLETON→RANDOM scatter path in the resulting RelSets, convert()
     * throws "HASH/RANGE exchange not yet implemented [toTrait=RANDOM]".
     *
     * <p>Asserts top-3 int0 values from calcs: [null, null, null] (6 nulls total,
     * default ASC nulls-first).
     */
    public void testSortThenProjectThenHead() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName + " | sort int0 | fields str0, int0 | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 returns 3 rows", 3, rows.size());
        // ASC nulls-first over calcs int0 ([1, null×3, 7, 3, 8, null×2, 8, 4, 10,
        // null, 4, 11, 4, 8]): top 3 are all null.
        for (int i = 0; i < 3; i++) {
            assertEquals("Row " + i + " has 2 columns", 2, rows.get(i).size());
            assertNull("Top-3 nulls-first: int0 at row " + i + " should be null", rows.get(i).get(1));
        }
    }

    public void testSortBySubstringExpression() throws IOException {
        // `substring(str2, 1, 3)` lowers to SUBSTRING($N, 1, 3) inside a LogicalProject child of
        // the sort. Without SUBSTRING in STANDARD_PROJECT_OPS, the planner rejects it with
        // "No backend supports scalar function [SUBSTRING] among [datafusion]".
        //
        // Calcs str2 first 3 chars (where non-null): one, two, thr, fiv, six, eig, nin, ten,
        // ele, twe, fou, fif, six. Sort ASC nulls-first puts the 4 nulls first.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eval s = substring(str2, 1, 3) | sort s | fields s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count == calcs row count", 17, rows.size());
        // First 4 rows must be nulls (4 null str2 values in calcs).
        for (int i = 0; i < 4; i++) {
            assertNull("Expected null at row " + i + " (sorted ASC nulls-first)", rows.get(i).get(0));
        }
        // The remaining 13 must be sorted alphabetically.
        for (int i = 5; i < rows.size(); i++) {
            String prev = (String) rows.get(i - 1).get(0);
            String curr = (String) rows.get(i).get(0);
            assertNotNull("Non-null after null block", curr);
            assertTrue(
                "Sort order violation at row " + i + ": " + prev + " > " + curr,
                prev.compareTo(curr) <= 0
            );
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    /** Numeric-tolerant cell comparator (Jackson returns Integer/Long/Double interchangeably). */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
