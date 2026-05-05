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
 * Self-contained integration test for PPL {@code append} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalcitePPLAppendCommandIT} from the {@code opensearch-project/sql}
 * repository so that the analytics-engine path can be verified inside core without
 * cross-plugin dependencies on the SQL plugin. Each test sends a PPL query through
 * {@code POST /_analytics/ppl} (exposed by the {@code test-ppl-frontend} plugin), which
 * runs the same {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} → Substrait
 * → DataFusion pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Covers the Append surface forms that exercise:
 * <ul>
 *   <li>two stats branches sorted + truncated by {@code head N}</li>
 *   <li>cross-index union (a second copy of the same dataset under a different index name)</li>
 *   <li>shared output column name across branches (no auto-rename)</li>
 *   <li>{@code | append [ ]} with several empty-subsearch shapes that all collapse to
 *       the first branch (bare brackets, inner stats with no source, nested
 *       {@code | append [ ]}, inner {@code | lookup})</li>
 *   <li>{@code | append [ | <inner|cross|left|semi> join … ]} — empty-left-side joins
 *       that also collapse to the first branch</li>
 *   <li>{@code | append [ … | <right|full> join … ]} — joins where the right side
 *       contributes additional rows under the merged schema even though the left
 *       side is empty</li>
 *   <li>type-incompatibility error path raised in {@code SchemaUnifier}</li>
 * </ul>
 *
 * <p>Provisions the {@code calcs} dataset twice — once into the {@code calcs} index and
 * once into {@code calcs_alt} — so {@code testAppendDifferentIndex} can union across
 * indices without pulling in a second dataset. Both indices are parquet-backed via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps them across test methods.
 */
public class AppendCommandIT extends AnalyticsRestTestCase {

    private static final Dataset CALCS = new Dataset("calcs", "calcs");
    private static final Dataset CALCS_ALT = new Dataset("calcs", "calcs_alt");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision both calcs indices on first invocation. Must be called inside a
     * test method (not {@code setUp()}) — {@code OpenSearchRestTestCase}'s static
     * {@code client()} is not initialized until after {@code @BeforeClass} but is
     * reliably available inside test bodies.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), CALCS);
            DatasetProvisioner.provision(client(), CALCS_ALT);
            dataProvisioned = true;
        }
    }

    // ── two stats branches → sort → head ────────────────────────────────────────

    public void testAppend() throws IOException {
        // Branch 1: sum(int0) grouped by str0 (3 rows). Branch 2: sum(int1) grouped
        // by str3 (2 rows). Union all + head 5 keeps every row, but the order
        // between the two child-stage streams isn't deterministic, so compare as a
        // multiset.
        assertRowsAnyOrder(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ source="
                + CALCS.indexName
                + " | stats sum(int1) as sum_int1_by_str3 by str3 | sort sum_int1_by_str3 ]"
                + " | head 5",
            row(1, "FURNITURE", null, null),
            row(18, "OFFICE SUPPLIES", null, null),
            row(49, "TECHNOLOGY", null, null),
            row(null, null, -14, null),
            row(null, null, -8, "e")
        );
    }

    // ── cross-index union ───────────────────────────────────────────────────────

    public void testAppendDifferentIndex() throws IOException {
        // Branch 1: calcs grouped by str0 (3 rows). Branch 2: calcs_alt total sum(int1)
        // (1 row). Each branch is its own data-node stage on its own shard set; the two
        // streams arrive at the coordinator's Union in non-deterministic order.
        assertRowsAnyOrder(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum by str0 | sort str0"
                + " | append [ source="
                + CALCS_ALT.indexName
                + " | stats sum(int1) as alt_sum_int1 ]",
            row(1, "FURNITURE", null),
            row(18, "OFFICE SUPPLIES", null),
            row(49, "TECHNOLOGY", null),
            row(null, null, -22)
        );
    }

    // ── shared output column name across branches (no auto-rename) ──────────────

    public void testAppendWithMergedColumn() throws IOException {
        // Both branches produce a column named "sum"; SchemaUnifier merges the column
        // by name. Group columns differ (str0 vs str3) so each row populates one and
        // leaves the other null. Inter-branch order is non-deterministic; head 5 keeps
        // every row (3 + 2 = 5) so multiset comparison is exact.
        assertRowsAnyOrder(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum by str0 | sort str0"
                + " | append [ source="
                + CALCS.indexName
                + " | stats sum(int0) as sum by str3 | sort sum ]"
                + " | head 5",
            row(1, "FURNITURE", null),
            row(18, "OFFICE SUPPLIES", null),
            row(49, "TECHNOLOGY", null),
            row(32, null, null),
            row(36, null, "e")
        );
    }

    // ── empty subsearch — collapses to first branch ────────────────────────────

    public void testAppendEmptySearchCommandBareBrackets() throws IOException {
        // `| append [ ]` — fully empty subsearch.
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ ]"
        );
    }

    public void testAppendEmptySearchCommandStatsWithoutSource() throws IOException {
        // `| append [ | stats ... ]` — subsearch starts with a pipe, so the implicit
        // source is the empty Values relation; the inner stats produces no rows.
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | stats sum(int1) as alt_sum by bool0 ]"
        );
    }

    public void testAppendEmptySearchCommandNestedAppend() throws IOException {
        // Nested empty append inside a where-only subsearch.
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | where int0 > 5 | append [ ] ]"
        );
    }

    public void testAppendEmptySearchCommandLookup() throws IOException {
        // `| append [ | where … | lookup INDEX field as alias ]` — lookup against an
        // empty implicit source. EmptySourcePropagateVisitor collapses the subsearch
        // to LogicalValues(empty), which OpenSearchUnionRule then drops.
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | where int0 > 5 | lookup "
                + CALCS.indexName
                + " str0 as istr0 ]"
        );
    }

    // ── empty subsearch with join (5 join types; 4 collapse to first branch) ───

    public void testAppendEmptySearchWithInnerJoin() throws IOException {
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | join left=L right=R on L.str0 = R.str0 "
                + CALCS.indexName
                + " ]"
        );
    }

    public void testAppendEmptySearchWithCrossJoin() throws IOException {
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | cross join left=L right=R on L.str0 = R.str0 "
                + CALCS.indexName
                + " ]"
        );
    }

    public void testAppendEmptySearchWithLeftJoin() throws IOException {
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | left join left=L right=R on L.str0 = R.str0 "
                + CALCS.indexName
                + " ]"
        );
    }

    public void testAppendEmptySearchWithSemiJoin() throws IOException {
        assertEmptyAppendOnlyFirstBranch(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | semi join left=L right=R on L.str0 = R.str0 "
                + CALCS.indexName
                + " ]"
        );
    }

    // ── empty subsearch with right/full join — adds rows from the right side ───

    public void testAppendEmptySearchWithRightJoin() throws IOException {
        // RIGHT JOIN of (empty filtered subset, real subquery) → still emits every
        // right-side row with NULL on the left columns. The append therefore yields
        // the first branch's rows plus the right-subquery's rows under the merged
        // schema (sum_int0_by_str0 / str0 / cnt). Inter-branch order is non-deterministic.
        assertRowsAnyOrder(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | where str0 = 'OFFICE SUPPLIES' | right join on str0 = str0 [source="
                + CALCS.indexName
                + " | stats count() as cnt by str0 | sort str0 ] ]",
            row(1, "FURNITURE", null),
            row(18, "OFFICE SUPPLIES", null),
            row(49, "TECHNOLOGY", null),
            row(null, "FURNITURE", 2),
            row(null, "OFFICE SUPPLIES", 6),
            row(null, "TECHNOLOGY", 9)
        );
    }

    public void testAppendEmptySearchWithFullJoin() throws IOException {
        // Same shape as right join — the empty left side has no rows to match, so
        // FULL JOIN reduces to RIGHT JOIN here. Inter-branch order is non-deterministic.
        assertRowsAnyOrder(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum_int0_by_str0 by str0 | sort str0"
                + " | append [ | where str0 = 'OFFICE SUPPLIES' | full join on str0 = str0 [source="
                + CALCS.indexName
                + " | stats count() as cnt by str0 | sort str0 ] ]",
            row(1, "FURNITURE", null),
            row(18, "OFFICE SUPPLIES", null),
            row(49, "TECHNOLOGY", null),
            row(null, "FURNITURE", 2),
            row(null, "OFFICE SUPPLIES", 6),
            row(null, "TECHNOLOGY", 9)
        );
    }

    // ── type-incompatibility error raised in SchemaUnifier ─────────────────────

    public void testAppendWithConflictTypeColumn() {
        // Branch 1 produces "sum" as BIGINT; branch 2 casts "sum" to DOUBLE. Schema
        // unification refuses to merge the diverging types and surfaces a planner
        // error before execution.
        assertErrorContains(
            "source="
                + CALCS.indexName
                + " | stats sum(int0) as sum by str0 | sort str0"
                + " | append [ source="
                + CALCS.indexName
                + " | stats sum(int0) as sum by str3 | sort sum"
                + " | eval sum = cast(sum as double) ]"
                + " | head 5",
            "Unable to process column 'sum' due to incompatible types"
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Construct an expected row from positional values matching the PPL output column order. */
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * The four empty-subsearch shapes share the same expected first-branch-only output;
     * factored to keep the four test methods readable.
     */
    private void assertEmptyAppendOnlyFirstBranch(String ppl) throws IOException {
        assertRows(ppl, row(1, "FURNITURE"), row(18, "OFFICE SUPPLIES"), row(49, "TECHNOLOGY"));
    }

    /**
     * Send a PPL query to {@code POST /_analytics/ppl} and assert the response's
     * {@code rows} match the expected list element-by-element using a numeric-tolerant
     * comparator (Java JSON parsing returns Integer/Long/Double interchangeably).
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }

    /**
     * Multiset variant of {@link #assertRows} for queries whose row order is not
     * deterministic. Substrait's {@code Set} (Union) rel preserves order within a
     * single input partition but not between partitions: the two child stages of
     * an {@code | append} pipeline can stream into the coordinator sink in either
     * order depending on shard scheduling timing, so a {@code | head N} on top of
     * a Union may pick different rows across runs (or the same rows in different
     * orders).
     *
     * <p>Cell values are normalised to a canonical string form before comparison —
     * numeric types collapse to a {@code Double} so JSON-parsed
     * {@code Integer}/{@code Long}/{@code Double} compare equal across the Java side
     * even when their boxed types differ. Rows are then compared as a sorted multiset.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsAnyOrder(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        List<String> expectedNormalized = Arrays.stream(expected).map(AppendCommandIT::normalizeRow).sorted().toList();
        List<String> actualNormalized = actualRows.stream().map(AppendCommandIT::normalizeRow).sorted().toList();
        assertEquals("Row multisets differ for query: " + ppl, expectedNormalized, actualNormalized);
    }

    /** Renders one row to a stable canonical string for multiset comparison. */
    private static String normalizeRow(List<Object> row) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) sb.append('|');
            sb.append(normalizeCell(row.get(i)));
        }
        return sb.append(']').toString();
    }

    private static String normalizeCell(Object cell) {
        if (cell == null) return "<NULL>";
        if (cell instanceof Number) return Double.toString(((Number) cell).doubleValue());
        return cell.toString();
    }

    /**
     * Send a PPL query expecting the planner to reject it; assert the resulting HTTP
     * error body contains {@code expectedSubstring} (typically the validation message).
     */
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

    /** Send {@code POST /_analytics/ppl} and return the parsed JSON body. */
    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Compare two cells with numeric tolerance — JSON parsing produces
     * Integer/Long/Double values that may not match {@code .equals()} across types
     * even when numerically equal.
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
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }
}
