/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.opensearch.cluster.ClusterState;

/**
 * Plan-shape coverage for the {@code subquery-remove} phase introduced in
 * {@link PlannerImpl#runAllOptimizations}. The phase lowers {@link RexSubQuery}s
 * (EXISTS / IN / SOME / ANY) into {@code LogicalCorrelate} via Calcite's three
 * {@code *_SUB_QUERY_TO_CORRELATE} rules, then decorrelates the result into a
 * straight join. Without it, downstream rules (e.g. {@code OpenSearchFilterRule}
 * which resolves leaf predicates through a {@code ScalarFunction} table that
 * doesn't include {@code EXISTS}) reject the plan.
 *
 * <p>Tests parse real SQL through {@link SqlPlannerTestFixture}, run the full
 * planner pipeline, and assert that no {@code RexSubQuery} remains anywhere in
 * the optimized tree — the strongest invariant the phase guarantees.
 */
public class SubQueryPlanShapeTests extends PlanShapeTestBase {

    /** Uncorrelated EXISTS: {@code SELECT * FROM test_index WHERE EXISTS (SELECT 1 FROM test_index)}. */
    public void testUncorrelatedExistsSubqueryIsLowered() {
        ClusterState parserState = SqlPlannerTestFixture.clusterStateWith("test_index", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT * FROM test_index WHERE EXISTS (SELECT 1 FROM test_index)", parserState);
        assertContainsSubQuery("Pre-condition: parsed plan must carry the EXISTS RexSubQuery", parsed);

        RelNode result = runPlanner(parsed, singleShardContext());

        assertNoSubQuery(
            "subquery-remove phase must lower every RexSubQuery (EXISTS / IN / SOME / ANY) before"
                + " marking — otherwise OpenSearchFilterRule rejects the operator at runtime with"
                + " 'Unrecognized filter operator [EXISTS / EXISTS]'.",
            result
        );
    }

    /** Correlated EXISTS: subquery references an outer-row column. */
    public void testCorrelatedExistsSubqueryIsLowered() {
        ClusterState parserState = SqlPlannerTestFixture.clusterStateWith("test_index", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql(
            "SELECT * FROM test_index t WHERE EXISTS" + " (SELECT 1 FROM test_index s WHERE s.status = t.status)",
            parserState
        );
        assertContainsSubQuery("Pre-condition: parsed plan must carry the EXISTS RexSubQuery", parsed);

        RelNode result = runPlanner(parsed, singleShardContext());

        assertNoSubQuery(
            "subquery-remove + decorrelate must convert a correlated EXISTS into a standard join"
                + " (no leftover RexSubQuery, no leftover LogicalCorrelate).",
            result
        );
    }

    /** Uncorrelated IN-list: lowered through {@code FILTER_SUB_QUERY_TO_CORRELATE} too. */
    public void testInSubqueryIsLowered() {
        ClusterState parserState = SqlPlannerTestFixture.clusterStateWith("test_index", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql(
            "SELECT * FROM test_index WHERE status IN (SELECT status FROM test_index)",
            parserState
        );
        assertContainsSubQuery("Pre-condition: parsed plan must carry the IN RexSubQuery", parsed);

        RelNode result = runPlanner(parsed, singleShardContext());

        assertNoSubQuery(
            "subquery-remove must cover IN subqueries in addition to EXISTS — both flow through"
                + " the same CoreRules.FILTER_SUB_QUERY_TO_CORRELATE rewrite.",
            result
        );
    }

    // ---- Helpers ----

    private static void assertContainsSubQuery(String message, RelNode plan) {
        if (!findSubQuery(plan)) {
            throw new AssertionError(message + "\nPlan:\n" + RelOptUtil.toString(plan));
        }
    }

    private static void assertNoSubQuery(String message, RelNode plan) {
        if (findSubQuery(plan)) {
            throw new AssertionError(message + "\nPost-optimize plan:\n" + RelOptUtil.toString(plan));
        }
    }

    private static boolean findSubQuery(RelNode plan) {
        boolean[] found = { false };
        RexShuttle rexFinder = new RexShuttle() {
            @Override
            public org.apache.calcite.rex.RexNode visitSubQuery(RexSubQuery sub) {
                found[0] = true;
                return sub;
            }
        };
        RelShuttle relWalker = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode node) {
                if (found[0]) return node;
                node.accept(rexFinder);
                return found[0] ? node : super.visit(node);
            }
        };
        plan.accept(relWalker);
        return found[0];
    }
}
