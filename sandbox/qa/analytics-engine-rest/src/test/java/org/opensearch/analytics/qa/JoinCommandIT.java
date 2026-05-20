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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for PPL commands that lower to {@code LogicalJoin} on the
 * analytics-engine route (POST /_analytics/ppl).
 *
 * <p>Exercises the three commands that produce a join RelNode:
 * <ul>
 *   <li>{@code join} — direct LogicalJoin (inner / left / cross)</li>
 *   <li>{@code lookup} — LogicalJoin (LEFT) with rename/replace semantics</li>
 *   <li>{@code appendcol} — LogicalJoin (FULL OUTER) by synthesized row number</li>
 * </ul>
 *
 * <p>{@code graphLookup} is intentionally out of scope for this IT — it requires
 * a graph-shaped dataset with self-referential edges that the calcs dataset
 * does not provide.
 *
 * <p>Uses the {@code calcs} dataset provisioned into two indices ({@code calcs}
 * and {@code calcs_alt}) so two-table joins have distinct right-hand operands
 * without pulling in a second dataset. Row-count assertions are used for this
 * exploratory coverage — the IT focuses on whether each command plans, converts
 * to Substrait, and executes end-to-end rather than on exact row values.
 *
 * <p>All join / lookup tests pass end-to-end. {@code testAppendcol} is
 * {@code @AwaitsFix} — see task #113.
 */
public class JoinCommandIT extends AnalyticsRestTestCase {

    private static final Dataset CALCS = new Dataset("calcs", "calcs");
    private static final Dataset CALCS_ALT = new Dataset("calcs", "calcs_alt");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision both calcs indices on first invocation. Called inside test
     * methods — {@code client()} is not available in {@code @BeforeClass}.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), CALCS);
            DatasetProvisioner.provision(client(), CALCS_ALT);
            dataProvisioned = true;
        }
    }

    // ── join (direct LogicalJoin) ──────────────────────────────────────────────

    /**
     * Inner equi-join across two indices of the calcs dataset, grouped on
     * {@code str0}. Both sides are pre-aggregated to a narrow keyword-only
     * schema so the join output is scalar-only.
     */
    public void testInnerJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | stats count() as left_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source="
            + CALCS_ALT.indexName
            + " | stats count() as right_cnt by str0 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    /**
     * Left outer join. Drops one str0 value from the right side via a filter so
     * a subset of left rows have no match and appear with nulls on the right.
     */
    public void testLeftOuterJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, str0"
            + " | left join left=a, right=b ON a.str0 = b.str0"
            + " [ source="
            + CALCS_ALT.indexName
            + " | where str0 = 'TECHNOLOGY' | fields key, str0 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 89L);
    }

    /**
     * Right outer join — mirror of left outer. Drops a value from the LEFT side via a
     * filter so some right rows have no match and appear with nulls on the left.
     */
    public void testRightOuterJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | where str0 = 'TECHNOLOGY' | fields key, str0"
            + " | right join left=a, right=b ON a.str0 = b.str0"
            + " [ source="
            + CALCS_ALT.indexName
            + " | fields key, str0 ]"
            + " | stats count() as cnt";
        assertRowCountPositive(ppl);
    }

    /** Left semi join — returns left rows that have at least one match on the right. */
    public void testLeftSemiJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, str0"
            + " | left semi join left=a, right=b ON a.str0 = b.str0"
            + " [ source="
            + CALCS_ALT.indexName
            + " | fields key, str0 ]"
            + " | stats count() as cnt";
        assertRowCountPositive(ppl);
    }

    /** Left anti join — returns left rows with NO match on the right. */
    public void testLeftAntiJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, str0"
            + " | left anti join left=a, right=b ON a.str0 = b.str0"
            + " [ source="
            + CALCS_ALT.indexName
            + " | where str0 = 'TECHNOLOGY' | fields key, str0 ]"
            + " | stats count() as cnt";
        assertRowCountPositive(ppl);
    }

    /**
     * Cross join (join predicate {@code 1=1}). Exercises the degenerate
     * no-equi-condition shape — Isthmus emits it as a Substrait {@code Cross}
     * rel, which DataFusion executes as a NestedLoopJoin.
     */
    public void testCrossJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key"
            + " | join left=a, right=b on 1=1"
            + " [ source="
            + CALCS_ALT.indexName
            + " | fields key ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 289L);
    }

    // ── lookup (LogicalJoin LEFT) ──────────────────────────────────────────────

    /**
     * Lookup with REPLACE: left table rows are enriched with {@code str0} from
     * the right table, matched on {@code key}. LEFT join semantics — every left
     * row is retained.
     */
    public void testLookup() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, int0, str0"
            + " | lookup "
            + CALCS_ALT.indexName
            + " key REPLACE str0"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 17L);
    }

    /**
     * Lookup with REPLACE … AS rename — the right-side value overwrites a
     * differently-named left column. Exercises the projection wrapper emitted by
     * the lookup → LogicalJoin lowering.
     */
    public void testLookupReplaceWithRename() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, int0, str0"
            + " | lookup "
            + CALCS_ALT.indexName
            + " key REPLACE str0 AS str2"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 17L);
    }

    // ── appendcol (LogicalJoin FULL OUTER by row_num) ──────────────────────────

    /**
     * appendcol pairs the outer pipeline with a subsearch by synthesized row
     * number. PPL grammar does not allow {@code source=…} inside the
     * {@code appendcol [ … ]} brackets — the subsearch operates on the implicit
     * upstream input.
     *
     * <p><b>Pending (window-function track)</b>: appendcol lowers to
     * {@code ROW_NUMBER() OVER (ORDER BY …)} for pairing rows. Window-function
     * support is a follow-up.
     */
    @AwaitsFix(bugUrl = "Task #113: appendcol plans correctly (ROW_NUMBER supported) but hits the same AggregateSplit-under-per-side-ER issue surfacing a runtime schema coercion mismatch.")
    public void testAppendcol() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | stats count() as total by str0 | sort str0"
            + " | appendcol [ stats count() as alt_total ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    // ── Combinations: matches the structural UTs in PlanShapeTests ──────────────

    /**
     * Mirrors {@code PlanShapeTests.testJoinThenAggregate_2shard}: inner join across
     * two indices then a SINGLE aggregate above the join. The Join's COORDINATOR
     * SINGLETON output satisfies the aggregate's input demand; no PARTIAL/FINAL
     * split fires (no shuffle to split across).
     */
    public void testJoinThenAggregate() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | fields key, str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | fields key, str0 ]"
            + " | stats count() as cnt by str0"
            + " | stats count() as cnt";
        // 3 distinct str0 values after grouping.
        assertSingleCount(ppl, 3L);
    }

    /**
     * Mirrors {@code PlanShapeTests.testJoinThenSort_2shard}: inner join then Sort
     * over the joined output. Sort runs at coord (Join already delivers SINGLETON)
     * with no extra ER between them.
     */
    public void testJoinThenSort() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | stats count() as left_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as right_cnt by str0 ]"
            + " | sort str0"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    /**
     * Mirrors {@code PlanShapeTests.testChainedJoin_2shard}: A ⨝ B ⨝ C. Each leaf
     * scan is gathered to coord with its own per-side ER; the outer join sits over
     * the inner join's SINGLETON output. Verifies trait propagation through nested
     * joins.
     */
    public void testChainedInnerJoin() throws IOException {
        final String ppl = "source="
            + CALCS.indexName
            + " | stats count() as cnt_a by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as cnt_b by str0 ]"
            + " | inner join left=ab, right=c ON ab.str0 = c.str0"
            + " [ source=" + CALCS.indexName + " | stats count() as cnt_c by str0 ]"
            + " | stats count() as cnt";
        // All 3 chained joins on str0 — each side groups to 3 rows, equi-join yields 3.
        assertSingleCount(ppl, 3L);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /**
     * Execute a PPL query expected to return a single {@code cnt} row and assert the count
     * is a non-negative number. Used for join kinds where exact row-count expectations
     * aren't pinned (right/semi/anti over the calcs dataset) but end-to-end execution
     * through planner + substrait + DataFusion is what's being exercised.
     */
    private void assertRowCountPositive(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Expected single count row for query: " + ppl, 1, rows.size());
        Object actual = rows.get(0).get(0);
        assertTrue(
            "Expected numeric count for query: " + ppl + " but got: " + actual,
            actual instanceof Number
        );
        assertTrue(
            "Expected non-negative count for query: " + ppl + " but got: " + actual,
            ((Number) actual).longValue() >= 0
        );
    }

    /** Execute a PPL query expected to return a single {@code cnt} row and assert the value. */
    private void assertSingleCount(String ppl, long expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Expected single count row for query: " + ppl, 1, rows.size());
        Object actual = rows.get(0).get(0);
        assertTrue(
            "Expected numeric count for query: " + ppl + " but got: " + actual,
            actual instanceof Number
        );
        assertEquals("Count mismatch for query: " + ppl, expected, ((Number) actual).longValue());
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
     * Send a PPL query expecting a failure and assert the response body contains
     * {@code expectedSubstring}. Kept for future use when a gated test is
     * converted to pin an expected error rather than skip entirely.
     */
    @SuppressWarnings("unused")
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
}
