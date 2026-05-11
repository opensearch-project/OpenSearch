/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Set;

/**
 * Integration-style tests that exercise the <b>coordinator-side pipeline</b> for a join —
 * {@link PlannerImpl#createPlan} → {@link DAGBuilder#build} → {@link PlanForker#forkAll} →
 * {@link BackendPlanAdapter#adaptAll}.
 *
 * <p>The existing {@link DAGBuilderTests} only exercises the first two steps; these tests ensure
 * that all four passes compose correctly for join queries, which exercise multi-input stage
 * shapes that aggregate/union queries don't.
 *
 * <p>The last pipeline step ({@link FragmentConversionDriver#convertAll}) is NOT driven here
 * because the mock backend in {@link BasePlannerRulesTests} doesn't ship a real
 * {@link org.opensearch.analytics.spi.FragmentConvertor} — that pass is already covered by
 * {@link FragmentConversionDriverTests} with its own recording convertor. Running actual native
 * execution (DataFusion runtime) requires spinning up a full OpenSearch cluster with the
 * arrow-flight-rpc extension — doable via {@code internalClusterTest} but too heavyweight for a
 * quick regression.
 *
 * <p>These tests double as regression anchors for the Codex findings across M0/M1/M2:
 * <ul>
 *   <li>Every marked join admits at least one backend via the post-intersection capability check.</li>
 *   <li>Plan forking narrows per-stage {@code viableBackends} to a single concrete backend.</li>
 *   <li>DAG cut shape is stable across shard counts and equi/theta join variants.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class JoinPipelineEndToEndTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(JoinPipelineEndToEndTests.class);

    /** Drive the first three coordinator-side passes and return the resulting DAG. */
    private QueryDAG buildDagThroughAdapter(int shardCount, RelNode logicalPlan) {
        PlannerContext context = buildContext("parquet", shardCount, intFields());
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        LOGGER.info("Adapted QueryDAG:\n{}", dag);
        return dag;
    }

    /**
     * INNER equi-join of two single-shard tables produces the expected coordinator-centric shape:
     * <pre>
     *   Root: OpenSearchJoin (coordinator)
     *   ├─ OpenSearchExchangeReducer → OpenSearchStageInputScan (left)
     *   └─ OpenSearchExchangeReducer → OpenSearchStageInputScan (right)
     *   Child stage 0: OpenSearchTableScan (left)
     *   Child stage 1: OpenSearchTableScan (right)
     * </pre>
     *
     * Each child stage has a plan alternative narrowed to a single backend after {@code PlanForker}.
     */
    public void testInnerEquiJoinDagShapeAndAlternatives() {
        QueryDAG dag = buildDagThroughAdapter(/* shards */ 1, makeInnerEquiJoin());

        // Root is the coordinator join stage.
        Stage root = dag.rootStage();
        assertTrue(
            "Root stage fragment must be OpenSearchJoin, got " + root.getFragment().getClass().getSimpleName(),
            root.getFragment() instanceof OpenSearchJoin
        );
        assertNull("Root stage must have no shard target (coordinator-side)", root.getTargetResolver());
        // Two child stages — one per join input.
        assertEquals("Root must have exactly 2 child stages for a binary join", 2, root.getChildStages().size());

        OpenSearchJoin rootJoin = (OpenSearchJoin) root.getFragment();
        // Root join's inputs are reducer-wrapped StageInputScans (post-DAG-cut shape).
        assertTrue(
            "Root join left input must be ExchangeReducer",
            rootJoin.getLeft() instanceof OpenSearchExchangeReducer
        );
        assertTrue(
            "Root join right input must be ExchangeReducer",
            rootJoin.getRight() instanceof OpenSearchExchangeReducer
        );
        OpenSearchExchangeReducer leftReducer = (OpenSearchExchangeReducer) rootJoin.getLeft();
        OpenSearchExchangeReducer rightReducer = (OpenSearchExchangeReducer) rootJoin.getRight();
        assertTrue(
            "Left reducer's input must be StageInputScan placeholder",
            leftReducer.getInput() instanceof OpenSearchStageInputScan
        );
        assertTrue(
            "Right reducer's input must be StageInputScan placeholder",
            rightReducer.getInput() instanceof OpenSearchStageInputScan
        );

        for (Stage child : root.getChildStages()) {
            assertNotNull("Child stage must have a shard target resolver", child.getTargetResolver());
            assertTrue(
                "Child stage fragment must be a scan",
                child.getFragment() instanceof OpenSearchTableScan
            );
            // PlanForker narrows viableBackends on plan alternatives, but Stage.fragment keeps
            // the pre-fork annotated tree (for replanning). Assertion lives on planAlternatives.
            assertFalse(
                "PlanForker must have produced at least one plan alternative per stage",
                child.getPlanAlternatives().isEmpty()
            );
            for (StagePlan alternative : child.getPlanAlternatives()) {
                assertEquals(
                    "Mock DataFusion is the only join-capable backend in the harness",
                    MockDataFusionBackend.NAME,
                    alternative.backendId()
                );
            }
        }
    }

    /**
     * Multi-shard variant: each join input is a 5-shard scan. Structural shape is identical to the
     * single-shard case — the number of shards affects target resolution at dispatch time, not the
     * DAG topology. This test pins that invariant so future shard-count-dependent changes don't
     * silently alter the coordinator-side pipeline.
     */
    public void testInnerEquiJoinMultiShardKeepsSameDagShape() {
        QueryDAG dag = buildDagThroughAdapter(/* shards */ 5, makeInnerEquiJoin());

        Stage root = dag.rootStage();
        assertTrue(root.getFragment() instanceof OpenSearchJoin);
        assertEquals(2, root.getChildStages().size());
        for (Stage child : root.getChildStages()) {
            assertNotNull(child.getTargetResolver());
            assertTrue(child.getFragment() instanceof OpenSearchTableScan);
            assertFalse(child.getPlanAlternatives().isEmpty());
            for (StagePlan alternative : child.getPlanAlternatives()) {
                assertEquals(MockDataFusionBackend.NAME, alternative.backendId());
            }
        }
    }

    /**
     * Theta (non-equi) join still reaches end-to-end conversion: the planner admits it via the
     * mock backend's join capability and the DAG/fragment-conversion passes treat it the same
     * as an equi-join at this layer — the difference is that MPP shuffle/broadcast alternatives
     * are never registered for theta joins (handled in {@code OpenSearchHashJoinRule}).
     */
    public void testThetaJoinReachesBackendAdapter() {
        QueryDAG dag = buildDagThroughAdapter(/* shards */ 3, makeThetaJoin());

        Stage root = dag.rootStage();
        assertTrue(root.getFragment() instanceof OpenSearchJoin);
        assertEquals(JoinRelType.INNER, ((OpenSearchJoin) root.getFragment()).getJoinType());
        assertEquals(2, root.getChildStages().size());
        for (Stage child : root.getChildStages()) {
            assertFalse(child.getPlanAlternatives().isEmpty());
            for (StagePlan alternative : child.getPlanAlternatives()) {
                assertEquals(
                    "Theta join's plan alternative must still resolve to the join-capable backend",
                    MockDataFusionBackend.NAME,
                    alternative.backendId()
                );
            }
        }
    }

    // ---- Query builders (local to these pipeline tests) ----

    private LogicalJoin makeInnerEquiJoin() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
    }

    private LogicalJoin makeThetaJoin() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
    }
}
