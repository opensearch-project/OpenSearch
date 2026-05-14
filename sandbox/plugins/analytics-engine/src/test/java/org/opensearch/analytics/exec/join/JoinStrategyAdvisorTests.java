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

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(100, 1_000_000L);
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    /**
     * Binary equi-join on a single-shard mock without a {@link org.opensearch.transport.client.Client}
     * → {@code StatisticsCollector} cannot fetch row counts → fail-safe HASH_SHUFFLE. The shard
     * gate passes (1 ≤ 2) but the row gate refuses because rowCount is unknown.
     *
     * <p>This is the unit-test equivalent of an environment where IndicesStats is unavailable
     * (planner-level tests don't wire a Client). End-to-end tests in {@code qa/} cover the
     * BROADCAST happy path with real row counts.
     */
    public void testEquiJoinSmallShardsRefusesBroadcastWhenRowsUnknown() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        RelNode join = makeInnerEquiJoin();
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(/* broadcastMaxShards */ 2, /* broadcastMaxRows */ 1_000_000L);
        JoinStrategy strategy = advisor.adviseAndTag(dag, context.getClusterState());
        assertEquals(JoinStrategy.HASH_SHUFFLE, strategy);

        // Role tagging only happens for BROADCAST. With rowCount=0 fail-safe, the advisor
        // returns HASH_SHUFFLE and does not retag children.
        Stage root = dag.rootStage();
        assertEquals(2, root.getChildStages().size());
    }

    /** Binary equi-join on 10-shard mocks → HASH_SHUFFLE (10 > threshold 2). */
    public void testEquiJoinLargeShardsPicksHashShuffle() {
        PlannerContext context = buildContext("parquet", /* shardCount */ 10, intFields());
        RelNode join = makeInnerEquiJoin();
        RelNode marked = PlannerImpl.createPlan(join, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2, 1_000_000L);
        assertEquals(JoinStrategy.HASH_SHUFFLE, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    /**
     * Regression: if either join input is an aggregate / derived subquery,
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

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2, 1_000_000L);
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

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(2, 1_000_000L);
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, advisor.adviseAndTag(dag, context.getClusterState()));
    }

    /**
     * Regression: queries like {@code | inner join ... | sort ... | head N} produce a Sort wrapping
     * the join at the root. The advisor must look through the wrappers — otherwise BROADCAST is
     * never picked for any user query that has a top-level `| sort` or `| head`.
     *
     * <p>Without rows wired in, the test asserts that the advisor reaches the {@code OpenSearchJoin}
     * (and lands on HASH_SHUFFLE because rowCount=0), rather than short-circuiting to
     * {@code COORDINATOR_CENTRIC} on the wrong-type root check. The unit test catches the
     * structural unwrap; the BROADCAST happy path is covered end-to-end in {@code BroadcastJoinIT}.
     */
    public void testAdvisorLooksThroughSortWrapper() {
        PlannerContext context = buildContext("parquet", /* shardCount */ 1, intFields());
        RelNode join = makeInnerEquiJoin();
        RelNode wrapped = makeSort(join, /* fetch */ 50); // Sort + LIMIT 50
        RelNode marked = PlannerImpl.createPlan(wrapped, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        JoinStrategyAdvisor advisor = new JoinStrategyAdvisor(/* maxShards */ 2, /* maxRows */ 1_000_000L);
        JoinStrategy strategy = advisor.adviseAndTag(dag, context.getClusterState());

        // Without row counts the selector returns HASH_SHUFFLE (fail-safe). The point of this
        // test is that we did NOT return COORDINATOR_CENTRIC: the advisor unwrapped the Sort,
        // found the OpenSearchJoin, and ran the strategy selector against it.
        assertNotEquals(
            "Advisor must unwrap top-level Sort and inspect the underlying join, not short-circuit to COORDINATOR_CENTRIC",
            JoinStrategy.COORDINATOR_CENTRIC,
            strategy
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, strategy);
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
