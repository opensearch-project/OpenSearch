/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link org.opensearch.analytics.planner.rules.OpenSearchJoinRule}: matches
 * inner equi-joins, produces an {@link OpenSearchJoin} with both inputs SINGLETON-converted
 * (i.e. wrapped in {@link OpenSearchExchangeReducer} by Volcano), and rejects non-inner
 * and pure non-equi joins.
 */
public class JoinRuleTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(JoinRuleTests.class);

    public void testInnerEquiJoinMatchesAndProducesOpenSearchJoin() {
        RelNode result = runJoin(JoinRelType.INNER, equiJoinCondition());

        // Volcano may wrap the SINGLETON-producing OpenSearchJoin in a redundant top-level
        // OpenSearchExchangeReducer when satisfying the root's required SINGLETON trait —
        // unwrap one layer if so.
        RelNode unwrapped = RelNodeUtils.unwrapHep(result);
        if (unwrapped instanceof OpenSearchExchangeReducer wrapper) {
            unwrapped = RelNodeUtils.unwrapHep(wrapper.getInput());
        }
        assertTrue("rule should produce OpenSearchJoin, got " + unwrapped.getClass().getSimpleName(), unwrapped instanceof OpenSearchJoin);

        OpenSearchJoin osJoin = (OpenSearchJoin) unwrapped;
        assertEquals("inner join type preserved", JoinRelType.INNER, osJoin.getJoinType());
        RelNode left = RelNodeUtils.unwrapHep(osJoin.getLeft());
        RelNode right = RelNodeUtils.unwrapHep(osJoin.getRight());
        assertTrue(
            "left input wrapped in OpenSearchExchangeReducer, got " + left.getClass().getSimpleName(),
            left instanceof OpenSearchExchangeReducer
        );
        assertTrue(
            "right input wrapped in OpenSearchExchangeReducer, got " + right.getClass().getSimpleName(),
            right instanceof OpenSearchExchangeReducer
        );
    }

    public void testLeftEquiJoinMatchesAndProducesOpenSearchJoin() {
        assertEquiJoinMatches(JoinRelType.LEFT);
    }

    public void testRightEquiJoinMatchesAndProducesOpenSearchJoin() {
        assertEquiJoinMatches(JoinRelType.RIGHT);
    }

    public void testFullOuterJoinDoesNotMatch() {
        // FULL OUTER remains out of scope for this wave: DataFusion's substrait consumer
        // historically has gaps around FULL OUTER execution, so we deliberately don't
        // widen the rule to it yet. When it's enabled, flip this to
        // {@link #assertEquiJoinMatches(JoinRelType)} and add an IT validation.
        assertNonInnerJoinDoesNotMatch(JoinRelType.FULL);
    }

    private void assertEquiJoinMatches(JoinRelType joinType) {
        RelNode result = runJoin(joinType, equiJoinCondition());

        RelNode unwrapped = RelNodeUtils.unwrapHep(result);
        if (unwrapped instanceof OpenSearchExchangeReducer wrapper) {
            unwrapped = RelNodeUtils.unwrapHep(wrapper.getInput());
        }
        assertTrue(
            "rule should produce OpenSearchJoin for " + joinType + ", got " + unwrapped.getClass().getSimpleName(),
            unwrapped instanceof OpenSearchJoin
        );

        OpenSearchJoin osJoin = (OpenSearchJoin) unwrapped;
        assertEquals(joinType + " join type preserved", joinType, osJoin.getJoinType());
        RelNode left = RelNodeUtils.unwrapHep(osJoin.getLeft());
        RelNode right = RelNodeUtils.unwrapHep(osJoin.getRight());
        assertTrue(
            "left input wrapped in OpenSearchExchangeReducer, got " + left.getClass().getSimpleName(),
            left instanceof OpenSearchExchangeReducer
        );
        assertTrue(
            "right input wrapped in OpenSearchExchangeReducer, got " + right.getClass().getSimpleName(),
            right instanceof OpenSearchExchangeReducer
        );
    }

    public void testCrossJoinMatchesAndProducesOpenSearchJoin() {
        // ON 1 = 1 — literal-only condition. Calcite folds this to condition=TRUE: empty
        // leftKeys, empty nonEquiConditions → JoinInfo.isEqui()=true. The rule must accept
        // this shape so the downstream OpenSearchAggregateRule finds a marked child. Isthmus
        // emits it as a substrait Cross rel, which DataFusion executes as NestedLoopJoin.
        RexNode trueCondition = rexBuilder.makeLiteral(true);
        RelNode result = runJoin(JoinRelType.INNER, trueCondition);

        RelNode unwrapped = RelNodeUtils.unwrapHep(result);
        if (unwrapped instanceof OpenSearchExchangeReducer wrapper) {
            unwrapped = RelNodeUtils.unwrapHep(wrapper.getInput());
        }
        assertTrue(
            "rule should produce OpenSearchJoin for cross joins, got " + unwrapped.getClass().getSimpleName(),
            unwrapped instanceof OpenSearchJoin
        );
    }

    public void testPureNonEquiJoinDoesNotMatch() {
        // left.k < right.k — no equi-condition, the rule's analyzeCondition().leftKeys is empty.
        RexNode lt = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        // Non-equi inner join — the rule doesn't match, so the LogicalJoin survives through
        // HEP marking. The downstream Volcano stage may not be able to plan it (no rule to
        // turn LogicalJoin into something with a coord-side execution path), which surfaces
        // as a planner failure. We only assert that whatever does come back is NOT an
        // OpenSearchJoin — i.e. our rule did not (incorrectly) match a pure non-equi join.
        try {
            RelNode result = runJoin(JoinRelType.INNER, lt);
            assertFalse("rule must not match pure non-equi inner joins", containsOpenSearchJoin(result));
        } catch (RuntimeException expected) {
            // Volcano can't plan a non-equi join through OpenSearch — that's fine; the
            // important thing is that our rule didn't pick it up.
        }
    }

    private void assertNonInnerJoinDoesNotMatch(JoinRelType joinType) {
        // Volcano can't produce a SINGLETON-distributed plan for a non-inner LogicalJoin
        // because no rule converts LogicalJoin → OpenSearchJoin for non-inner types and the
        // distribution trait def can't bridge from NONE convention. The planner failure is
        // the expected outcome; the contract we verify is that our rule did NOT match.
        try {
            RelNode result = runJoin(joinType, equiJoinCondition());
            assertFalse("rule must not match non-inner joins (joinType=" + joinType + ")", containsOpenSearchJoin(result));
        } catch (RuntimeException expected) {
            // Acceptable — see above.
        }
    }

    private static boolean containsOpenSearchJoin(RelNode root) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(root);
        if (unwrapped instanceof OpenSearchJoin) return true;
        for (RelNode input : unwrapped.getInputs()) {
            if (containsOpenSearchJoin(input)) return true;
        }
        return false;
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private RelNode runJoin(JoinRelType joinType, RexNode condition) {
        // Both sides use the same mocked index (the planner's table-mark step looks up cluster
        // state by qualified name, and the test fixture only mocks "test_index"). The join
        // rule we're testing doesn't care about table identity — it only inspects the join
        // shape and condition.
        PlannerContext context = buildContext("parquet", 2, Map.of("k", Map.of("type", "integer"), "v", Map.of("type", "integer")));
        RelOptTable table = mockTable("test_index", "k", "v");
        RelNode left = stubScan(table);
        RelNode right = stubScan(table);
        LogicalJoin join = LogicalJoin.create(left, right, List.of(), condition, Set.of(), joinType);

        LOGGER.info("Input join:\n{}", RelOptUtil.toString(join));
        RelNode result = runPlanner(join, context);
        LOGGER.info("Marked+CBO output:\n{}", RelOptUtil.toString(result));
        return result;
    }

    private RexNode equiJoinCondition() {
        // left.k = right.k — left field 0 = right field 0 (offset by left fieldCount=2).
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
    }
}
