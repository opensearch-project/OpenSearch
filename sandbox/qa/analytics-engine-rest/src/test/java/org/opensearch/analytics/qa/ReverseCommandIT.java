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
 * Self-contained integration test for PPL {@code reverse} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteReverseCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without
 * cross-plugin dependencies on the SQL plugin.
 *
 * <p>{@code reverse} is plan-time only: {@code CalciteRelNodeVisitor.visitReverse} either
 *
 * <ul>
 *   <li>finds an existing {@code LogicalSort} via {@code RelMetadataQuery.collations()}
 *       (or by backtracking through filter/project nodes) and reverses its collation;
 *   <li>or, if the row type has an {@code @timestamp} field, sorts {@code DESC} on it;
 *   <li>or, otherwise, no-ops.
 * </ul>
 *
 * The output is always a {@code LogicalSort} with reversed direction (or a passthrough)
 * — no new operators, no new scalar functions, no aggregates. That means the analytics
 * route needs zero new wiring to support it: the existing {@code EngineCapability.SORT}
 * registration in {@code DataFusionAnalyticsBackendPlugin} is enough.
 *
 * <p>This IT pins the shapes that go through the analytics path end-to-end: simple
 * {@code sort + reverse}, {@code sort + reverse + head} (two-Sort-stack which exercises
 * {@code attachFragmentOnTop} for the limit-aware path), and {@code sort + reverse +
 * reverse} (double-reverse rebuilding the original sort). Reverse-after-aggregate (no-op)
 * and reverse-after-eval (where collation propagates through projections) are also
 * covered.
 *
 * <p>Out of scope (failure modes documented in the upstream IT):
 *
 * <ul>
 *   <li>{@code testStreamstats*} — streamstats lowers to window functions (ROW_NUMBER /
 *       windowed COUNT / windowed SUM) which the analytics path does not yet wire.
 *   <li>{@code testTimechart*} — depends on {@code SPAN} time-bucketing scalar (separate
 *       out-of-scope bucket).
 *   <li>{@code testReverseWithTimestampField} — TIMESTAMP rendering across paths.
 * </ul>
 *
 * Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}.
 */
public class ReverseCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── basic sort + reverse — Sort with collation flipped in-place ─────────────

    public void testReverseAfterSort() throws IOException {
        // Calcs int0 ASC nulls-first: [null × 6, 1, 3, 4, 4, 4, 7, 8, 8, 8, 10, 11].
        // After reverse, the collation flips to DESC nulls-last (Calcite's reverseCollation
        // also flips null direction to keep semantics symmetric).
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | where isnotnull(int0) | sort int0 | reverse | fields int0",
            row(11), row(10), row(8), row(8), row(8), row(7), row(4), row(4), row(4), row(3), row(1)
        );
    }

    // ── double-reverse — Sort restored to original direction ───────────────────

    public void testDoubleReverseRestoresOriginalSort() throws IOException {
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | where isnotnull(int0) | sort int0 | reverse | reverse | fields int0",
            row(1), row(3), row(4), row(4), row(4), row(7), row(8), row(8), row(8), row(10), row(11)
        );
    }

    // ── reverse + head — limit-aware: reverse adds a separate Sort on top ──────

    public void testReverseWithHead() throws IOException {
        // visitReverse detects the inner Sort has fetch=null in the pure-collation case, so
        // it replaces the Sort in-place. After `| head 3`, a Sort(fetch=3) sits on top of
        // the reversed Sort. Top three values from int0 DESC: 11, 10, 8.
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | where isnotnull(int0) | sort int0 | reverse | head 3 | fields int0",
            row(11), row(10), row(8)
        );
    }

    // ── reverse with descending sort — flips back to ascending ─────────────────

    public void testReverseWithDescendingSort() throws IOException {
        // Flipped DESC + reverse → ASC. Lowest three are 1, 3, 4.
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | where isnotnull(int0) | sort -int0 | reverse | head 3 | fields int0",
            row(1), row(3), row(4)
        );
    }

    // ── reverse traverses through filter/project to find the upstream sort ─────

    public void testReverseAfterFilterFindsUpstreamSort() throws IOException {
        // Backtracking case: `sort | where | reverse` — reverse walks past the Filter to find
        // the LogicalSort and reverses its direction. PlanUtils.insertReversedSortInTree
        // rebuilds the tree with the reversed Sort below the Filter.
        // Filter int0 >= 4 keeps {4 ×3, 7, 8 ×3, 10, 11} = 9 rows; reversed sort gives 11,
        // 10, 8 first.
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | sort int0 | where int0 >= 4 | reverse | head 3 | fields int0",
            row(11), row(10), row(8)
        );
    }

    public void testReverseAfterEvalFindsUpstreamSort() throws IOException {
        // Same backtracking, but through an eval-introduced Project. Sort first by int0 ASC,
        // then eval doubled = int0 * 2, then reverse. Backtrack walks past Project to find
        // the Sort, reverses it, and the doubled column propagates through.
        assertRowsInOrder(
            "source=" + DATASET.indexName
                + " | where isnotnull(int0) | sort int0 | eval doubled = int0 * 2 | reverse | head 3"
                + " | fields int0, doubled",
            row(11, 22), row(10, 20), row(8, 16)
        );
    }

    // ── reverse after aggregation — no-op when collation is destroyed ──────────

    public void testReverseAfterAggregationIsNoOp() throws IOException {
        // Aggregation destroys input collation, so `reverse` finds no collation and falls
        // back to the @timestamp branch, which doesn't apply (calcs has no @timestamp), so
        // it's a no-op. Aggregation row order isn't pinned, so compare as a multiset.
        assertRowsAnyOrder(
            "source=" + DATASET.indexName + " | stats count by str0 | reverse",
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    // ── reverse after explicit post-aggregate sort — works through the sort ────

    public void testReverseAfterAggregationWithSort() throws IOException {
        // Sort after aggregation establishes a fresh collation; reverse flips it.
        assertRowsInOrder(
            "source=" + DATASET.indexName + " | stats count by str0 | sort str0 | reverse",
            row(9L, "TECHNOLOGY"),
            row(6L, "OFFICE SUPPLIES"),
            row(2L, "FURNITURE")
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsInOrder(String ppl, List<Object>... expected) throws IOException {
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
