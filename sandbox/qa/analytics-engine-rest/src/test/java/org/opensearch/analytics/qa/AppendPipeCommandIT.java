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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code appendpipe} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalcitePPLAppendPipeCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without cross-plugin
 * dependencies. Each test sends a PPL query through {@code POST /_analytics/ppl} (exposed
 * by the {@code test-ppl-frontend} plugin), which runs the same {@code UnifiedQueryPlanner}
 * → {@code CalciteRelNodeVisitor} → Substrait → DataFusion pipeline as the SQL plugin's
 * force-routed analytics path.
 *
 * <p>{@code appendpipe} differs from {@code append} (covered by {@link AppendCommandIT}):
 * {@code appendpipe [pipeline]} duplicates the current intermediate result, applies the
 * inline {@code [pipeline]} to the duplicate, and appends the duplicate's output to the
 * original. {@code append [search]} runs an entirely separate sub-query and unions its
 * output. Both lower to a Calcite {@code LogicalUnion} but the upper-stage shape differs
 * because {@code appendpipe} reuses the original's row stream as its input rather than
 * starting a fresh {@code source=...}.
 *
 * <p>Provisions the {@code calcs} dataset once. {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class AppendPipeCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── duplicate + inline sort, then head ──────────────────────────────────────

    public void testAppendPipeSort() throws IOException {
        // Branch: stats sum(int0) by str0 → 3 rows (FURNITURE=1, OFFICE SUPPLIES=18, TECHNOLOGY=49).
        // Outer `sort str0` pins the original to alphabetical order. `appendpipe [sort -sum_int0_by_str0]`
        // duplicates the 3 rows and re-sorts them descending, then appends. `head 5` keeps the first
        // 5 of the 6 total rows: original 3 + first 2 of the descending duplicate.
        assertRows(
            "source="
                + DATASET.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | appendpipe [ sort -sum_int0_by_str0 ]"
                + " | head 5",
            row(1, "FURNITURE"),
            row(18, "OFFICE SUPPLIES"),
            row(49, "TECHNOLOGY"),
            row(49, "TECHNOLOGY"),
            row(18, "OFFICE SUPPLIES")
        );
    }

    // ── duplicate + inline stats producing a smaller schema (merged column) ─────

    public void testAppendPipeWithMergedColumn() throws IOException {
        // Outer stats: sum(int0) by str0 → 3 rows. `appendpipe [stats sum(sum) as sum]` runs an inner
        // stats over the duplicate, collapsing it to a single row carrying only the `sum` column.
        // Schema unification keeps both the original branch's `str0` and the inner branch's
        // `sum` column; the inner row is null-padded for the missing `str0`. The two branches
        // arrive at the coordinator's union in non-deterministic order (each is its own data-node
        // stage), so compare as a multiset rather than positionally.
        assertRowsAnyOrder(
            "source="
                + DATASET.indexName
                + " | stats sum(int0) as sum by str0 | sort str0"
                + " | appendpipe [ stats sum(sum) as sum ]",
            row(1, "FURNITURE"),
            row(18, "OFFICE SUPPLIES"),
            row(49, "TECHNOLOGY"),
            row(68, null)
        );
    }

    // ── duplicate + inline cast that clashes with the original's column type ───

    public void testAppendPipeWithConflictTypeColumn() {
        // Branch 1 produces `sum` as BIGINT (sum over int0). The inner pipeline of
        // `appendpipe [eval sum = cast(sum as double)]` rewrites the same-named column to
        // DOUBLE. SchemaUnifier refuses to merge the diverging types and surfaces a
        // planner-side validation error before execution.
        assertErrorContains(
            "source="
                + DATASET.indexName
                + " | stats sum(int0) as sum by str0 | sort str0"
                + " | appendpipe [ eval sum = cast(sum as double) ]"
                + " | head 5",
            "due to incompatible types"
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Multiset comparison — branch ordering at the coordinator's Union is non-deterministic.
     * Used by {@link #testAppendPipeWithMergedColumn} where the original-branch stats output
     * (3 rows) and the inner-branch collapsed-sum (1 row) can arrive in either order.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsAnyOrder(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        java.util.List<List<Object>> remaining = new java.util.ArrayList<>(actualRows);
        outer:
        for (List<Object> want : expected) {
            for (int i = 0; i < remaining.size(); i++) {
                if (rowsEqual(want, remaining.get(i))) {
                    remaining.remove(i);
                    continue outer;
                }
            }
            fail("Expected row not found for query: " + ppl + " — missing: " + want + " in actual: " + actualRows);
        }
    }

    private static boolean rowsEqual(List<Object> a, List<Object> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            Object ax = a.get(i);
            Object bx = b.get(i);
            if (ax == null || bx == null) {
                if (ax != bx) return false;
                continue;
            }
            if (ax instanceof Number && bx instanceof Number) {
                if (Double.compare(((Number) ax).doubleValue(), ((Number) bx).doubleValue()) != 0) return false;
                continue;
            }
            if (!ax.equals(bx)) return false;
        }
        return true;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
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

    private void assertErrorContains(String ppl, String expectedSubstring) {
        try {
            Map<String, Object> response = executePpl(ppl);
            fail("Expected query to fail with [" + expectedSubstring + "] but got response: " + response);
        } catch (ResponseException e) {
            String body;
            try {
                body = org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(e.getResponse()).toString();
            } catch (IOException ioe) {
                body = e.getMessage();
            }
            assertTrue(
                "Expected response body to contain [" + expectedSubstring + "] but was: " + body,
                body.contains(expectedSubstring)
            );
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

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
}
