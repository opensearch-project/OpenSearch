/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link org.opensearch.analytics.planner.rules.OpenSearchHashJoinRule}.
 *
 * <p>The rule is registered in {@link PlannerImpl}'s Volcano phase. These tests drive
 * {@link PlannerImpl#createPlan} with an equi-join and confirm the planner terminates (no
 * infinite rule fire) and produces an {@link OpenSearchJoin} at the root — the HASH
 * alternative is a sibling the scheduler chooses from based on cost, but termination is
 * the load-bearing property we want to pin for the Volcano trait/subset handling.
 *
 * <p>Context: an earlier version of the rule guarded against re-fire by checking
 * {@code instanceof OpenSearchShuffleExchange} on the join's inputs. Volcano wraps inputs
 * in {@code RelSubset}s which are never {@code OpenSearchShuffleExchange}, so the guard
 * was ineffective and the rule looped until the test timed out. The current rule instead
 * gates on the join node's own {@link RelDistribution.Type} trait — after it produces the
 * HASH alternative, its own distribution is HASH, and {@code matches()} returns false.
 * This test suite exists to prevent regression into the looping version.
 */
public class HashJoinRuleTests extends BasePlannerRulesTests {

    /** Equi-join terminates and roots at an OpenSearchJoin. Pins "no infinite Volcano fire." */
    public void testEquiJoinPlannerTerminates() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        int leftColCount = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftColCount)
        );
        LogicalJoin join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);

        PlannerContext context = buildContext("parquet", 10, intFields());
        RelNode result = runPlanner(join, context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Root must be OpenSearchJoin", result instanceof OpenSearchJoin);
    }

    /** Theta join must skip OpenSearchHashJoinRule entirely — rule's matches() returns false on non-equi. */
    public void testThetaJoinSkippedByHashJoinRule() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        int leftColCount = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftColCount)
        );
        LogicalJoin join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);

        PlannerContext context = buildContext("parquet", 10, intFields());
        RelNode result = runPlanner(join, context);
        assertTrue("Root must still be OpenSearchJoin for theta (falls back to non-shuffle path)", result instanceof OpenSearchJoin);
        OpenSearchJoin osJoin = (OpenSearchJoin) result;
        OpenSearchDistribution dist = osJoin.getTraitSet().getTrait(context.getDistributionTraitDef());
        assertTrue(
            "Theta join's distribution must be SINGLETON (not HASH) — the hash rule skipped it",
            dist != null && dist.getType() == RelDistribution.Type.SINGLETON
        );
    }
}
