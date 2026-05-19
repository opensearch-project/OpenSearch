/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DAG-shape tests that lock in the exact {@link QueryDAG#toString()} output for representative
 * PPL shapes. String-based assertions give reviewers the full DAG (stages, fragments,
 * stageInputScan placeholders) at a glance — a regression that shuffles Sort placement,
 * misplaces an ER, or changes stage cuts shows up as a clean diff.
 *
 * <p>Structural invariants that don't depend on exact output shape (targetResolver presence,
 * exchange info propagation) stay in {@link DAGBuilderTests}.
 */
public class DAGShapeTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(DAGShapeTests.class);

    private QueryDAG buildDAG(int shardCount, RelNode logicalPlan) {
        var context = buildContext("parquet", shardCount, intFields());
        return buildDAG(context, logicalPlan);
    }

    private QueryDAG buildDAG(PlannerContext context, RelNode logicalPlan) {
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("QueryDAG:\n{}", dag);
        return dag;
    }

    /**
     * Asserts the {@link QueryDAG#toString()} matches the expected string. Normalizes
     * trailing whitespace and strips the randomized {@code queryId} so tests are stable.
     * On mismatch, the full actual output is shown so reviewers see the DAG shape.
     */
    private static void assertDagShape(String expected, QueryDAG dag) {
        String actual = stripQueryId(dag.toString());
        String normalizedExpected = normalizeLines(expected);
        String normalizedActual = normalizeLines(actual);
        assertEquals("DAG shape mismatch — actual:\n" + actual, normalizedExpected, normalizedActual);
    }

    private static String stripQueryId(String dagStr) {
        return dagStr.replaceFirst("queryId=[0-9a-fA-F-]+", "queryId=<random>");
    }

    private static String normalizeLines(String s) {
        StringBuilder sb = new StringBuilder();
        for (String line : s.split("\n", -1)) {
            int end = line.length();
            while (end > 0 && (line.charAt(end - 1) == ' ' || line.charAt(end - 1) == '\t'))
                end--;
            sb.append(line, 0, end).append('\n');
        }
        while (sb.length() >= 2 && sb.charAt(sb.length() - 1) == '\n' && sb.charAt(sb.length() - 2) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    // ── Join DAG shapes ──────────────────────────────────────────────────────
    //
    // Case 1 (1-shard same table) — co-location fast path. Both inputs scan the same 1-shard
    // index; Join runs at SHARD with no per-side ER. The aggregate above demands SINGLETON,
    // gathered via one ER between Aggregate and Join → 2 stages total.
    //
    // Cases 2-4 — general path. Each input is gathered to coord via a per-side ER → 3 stages.

    public void testJoinDag_case1_singleShardSameTable() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        QueryDAG dag = buildDAG(context, buildJoinWithStatsShape("test_index", "test_index"));
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 1
                  OpenSearchAggregate(group=[{}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchStageInputScan(childStageId=[0], viableBackends=[[mock-parquet]])
                  Stage 0 exchange=SINGLETON
                    OpenSearchJoin(condition=[=($0, $2)], joinType=[left], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                      OpenSearchSort(fetch=[50000], viableBackends=[[mock-parquet]])
                        OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    public void testJoinDag_case2_multiShardSameTable() {
        PlannerContext context = buildContext("parquet", 3, intFields());
        QueryDAG dag = buildDAG(context, buildJoinWithStatsShape("test_index", "test_index"));
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 2
                  OpenSearchAggregate(group=[{}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchJoin(condition=[=($0, $2)], joinType=[left], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchStageInputScan(childStageId=[0], viableBackends=[[mock-parquet]])
                      OpenSearchSort(fetch=[50000], viableBackends=[[mock-parquet]])
                        OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                          OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                            OpenSearchStageInputScan(childStageId=[1], viableBackends=[[mock-parquet]])
                  Stage 0 exchange=SINGLETON
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  Stage 1 exchange=SINGLETON
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    public void testJoinDag_case3_singleShardDifferentTables() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("left_idx", 1, "right_idx", 1));
        QueryDAG dag = buildDAG(context, buildJoinWithStatsShape("left_idx", "right_idx"));
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 2
                  OpenSearchAggregate(group=[{}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchJoin(condition=[=($0, $2)], joinType=[left], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchStageInputScan(childStageId=[0], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchStageInputScan(childStageId=[1], viableBackends=[[mock-parquet]])
                  Stage 0 exchange=SINGLETON
                    OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  Stage 1 exchange=SINGLETON
                    OpenSearchSort(fetch=[50000], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    public void testJoinDag_case4_multiShardDifferentTables() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("left_idx", 3, "right_idx", 3));
        QueryDAG dag = buildDAG(context, buildJoinWithStatsShape("left_idx", "right_idx"));
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 2
                  OpenSearchAggregate(group=[{}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchJoin(condition=[=($0, $2)], joinType=[left], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchStageInputScan(childStageId=[0], viableBackends=[[mock-parquet]])
                      OpenSearchSort(fetch=[50000], viableBackends=[[mock-parquet]])
                        OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                          OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                            OpenSearchStageInputScan(childStageId=[1], viableBackends=[[mock-parquet]])
                  Stage 0 exchange=SINGLETON
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  Stage 1 exchange=SINGLETON
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    // ── Top-K DAG shapes ─────────────────────────────────────────────────────

    /** Multi-shard: PARTIAL on shards, FINAL at coord; Sort(fetch=2) above FINAL. */
    public void testTopKAfterStatsDag_multiShard() {
        QueryDAG dag = buildDAG(3, buildTopKAfterStats());
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 1
                  OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                      OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                            OpenSearchStageInputScan(childStageId=[0], viableBackends=[[mock-parquet]])
                  Stage 0 exchange=SINGLETON
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    /** Single-shard: data already on one node (SINGLETON(SCAN)) satisfies root's SINGLETON
     *  demand without a gather — AggregateSplit doesn't fire, no ER inserted, whole tree
     *  collapses into a single stage. */
    public void testTopKAfterStatsDag_singleShard() {
        QueryDAG dag = buildDAG(1, buildTopKAfterStats());
        assertDagShape(
            """
                QueryDAG(queryId=<random>)
                Stage 0
                  OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                      OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            dag
        );
    }

    // ── Builders ─────────────────────────────────────────────────────────────

    private RelNode buildJoinWithStatsShape(String leftTable, String rightTable) {
        RelOptTable left = mockTable(leftTable, "status", "size");
        RelOptTable right = mockTable(rightTable, "status", "size");
        RelNode leftScan = stubScan(left);
        RelNode leftProject = LogicalProject.create(
            leftScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(leftScan, 0), rexBuilder.makeInputRef(leftScan, 1)),
            List.of("status", "size")
        );
        RelNode rightScan = stubScan(right);
        RelNode rightProject = LogicalProject.create(
            rightScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(rightScan, 0), rexBuilder.makeInputRef(rightScan, 1)),
            List.of("status", "size")
        );
        RelNode rightSorted = LogicalSort.create(
            rightProject,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(50000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(leftProject, rightSorted, List.of(), cond, Set.of(), JoinRelType.LEFT);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return org.apache.calcite.rel.logical.LogicalAggregate.create(
            join,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(),
            null,
            List.of(countCall)
        );
    }

    private RelNode buildTopKAfterStats() {
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode agg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            stubScan(mockTable("test_index", "status", "size")),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countCall)
        );
        RelNode innerSwap = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "k")
        );
        RelNode innerSort = LogicalSort.create(
            innerSwap,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        return LogicalProject.create(
            innerSort,
            List.of(),
            List.of(rexBuilder.makeInputRef(innerSort, 1), rexBuilder.makeInputRef(innerSort, 0)),
            List.of("k", "cnt")
        );
    }

}
