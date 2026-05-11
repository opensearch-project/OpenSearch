/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.List;
import java.util.Set;

public class JoinStrategyAdvisorTests extends BasePlannerRulesTests {

    /** Single-index scan — no join — always COORDINATOR_CENTRIC. */
    public void testNoJoinReturnsCoordinatorCentric() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        RelNode marked = PlannerImpl.createPlan(stubScan(mockTable("test_index", "status", "size")), context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(100);
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    /** Binary equi-join on a single-shard mock → BROADCAST (shard count 1 ≤ threshold 2). */
    public void testEquiJoinSmallShardsPicksBroadcast() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        RelNode join = makeInnerEquiJoin();
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(/* broadcastMaxShards */ 2);
        JoinStrategy strategy = advisor.adviseAndTag(dag, context.getClusterState());
        assertEquals(JoinStrategy.BROADCAST, strategy);

        // Role tagging: root stage is COORDINATOR_REDUCE, one child BROADCAST_BUILD, one BROADCAST_PROBE.
        Stage root = dag.rootStage();
        assertEquals(Stage.StageRole.COORDINATOR_REDUCE, root.getRole());
        assertEquals(2, root.getChildStages().size());
        Set<Stage.StageRole> childRoles = Set.of(root.getChildStages().get(0).getRole(), root.getChildStages().get(1).getRole());
        assertTrue(
            "child roles must include BROADCAST_BUILD and BROADCAST_PROBE, got " + childRoles,
            childRoles.contains(Stage.StageRole.BROADCAST_BUILD) && childRoles.contains(Stage.StageRole.BROADCAST_PROBE)
        );
    }

    /** Binary equi-join on 10-shard mocks → HASH_SHUFFLE (10 > threshold 2). */
    public void testEquiJoinLargeShardsPicksHashShuffle() {
        PlannerContext context = buildContext("parquet", /* shardCount */ 10, intFields());
        RelNode join = makeInnerEquiJoin();
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2);
        assertEquals(JoinStrategy.HASH_SHUFFLE, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    /**
     * Codex P2 regression fix: if either join input is an aggregate / derived subquery,
     * {@link JoinStrategyAdvisor#adviseAndTag} must fall back to
     * {@link JoinStrategy#COORDINATOR_CENTRIC} — an earlier version let {@code findSoleScanIndex}
     * chase through any unary operator, which misclassified {@code Agg → Project → Scan} inputs
     * as "plain scans" and incorrectly picked HASH_SHUFFLE on large-shard aggregates.
     */
    public void testAggregateInputFallsBackToCoordinatorCentric() {
        PlannerContext context = buildContext("parquet", /* shardCount */ 10, intFields());
        // left: plain scan of test_index
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        // right: Aggregate GROUP BY column 0 (status) + COUNT(*) over test_index — derived input.
        // Output shape: [status INTEGER, count BIGINT], so right column 0 is INTEGER (group key).
        RelNode rightAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());

        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        RelNode join = LogicalJoin.create(leftScan, rightAgg, List.of(), condition, Set.of(), JoinRelType.INNER);
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2);
        assertEquals(
            "Joins over derived/aggregated inputs must fall back to COORDINATOR_CENTRIC",
            JoinStrategy.COORDINATOR_CENTRIC,
            advisor.adviseAndTag(dag, context.getClusterState())
        );
    }

    /** Theta (non-equi) join → COORDINATOR_CENTRIC regardless of shard count. */
    public void testThetaJoinAlwaysCoordinatorCentric() {
        PlannerContext context = buildContext("parquet", /* shardCount */ 10, intFields());
        RelNode join = makeThetaJoin();
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2);
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    private RelNode makeInnerEquiJoin() {
        RelNode left = stubScan(mockTable("test_index", "status", "size"));
        RelNode right = stubScan(mockTable("test_index", "status", "size"));
        int leftCols = left.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(left, right, List.of(), condition, Set.of(), JoinRelType.INNER);
    }

    private RelNode makeThetaJoin() {
        RelNode left = stubScan(mockTable("test_index", "status", "size"));
        RelNode right = stubScan(mockTable("test_index", "status", "size"));
        int leftCols = left.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(left, right, List.of(), condition, Set.of(), JoinRelType.INNER);
    }
}
