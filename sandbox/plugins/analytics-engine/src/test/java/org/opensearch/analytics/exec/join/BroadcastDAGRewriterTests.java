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
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link BroadcastDAGRewriter}: given an advisor-tagged M0 coordinator-centric DAG,
 * the rewriter produces the broadcast-shape DAG and re-runs the plan pipeline cleanly.
 */
public class BroadcastDAGRewriterTests extends BasePlannerRulesTests {

    private QueryDAG taggedDag(int shardCount, LogicalJoin logicalJoin, Stage.StageRole leftRole, Stage.StageRole rightRole) {
        PlannerContext context = buildContext("parquet", shardCount, intFields());
        RelNode cboOutput = runPlanner(logicalJoin, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        // Simulate JoinStrategyAdvisor.tagBroadcastRoles — the rewriter requires BROADCAST_BUILD
        // and BROADCAST_PROBE tags on the two child stages.
        Stage root = dag.rootStage();
        assertEquals("test fixture assumes binary join → 2 child stages", 2, root.getChildStages().size());
        root.getChildStages().get(0).setRole(leftRole);
        root.getChildStages().get(1).setRole(rightRole);
        return dag;
    }

    public void testRewriteProducesBroadcastShape() {
        PlannerContext context = buildContext("parquet", 3, intFields());
        QueryDAG inputDag = taggedDag(3, makeInnerEquiJoin(), Stage.StageRole.BROADCAST_BUILD, Stage.StageRole.BROADCAST_PROBE);

        QueryDAG rewritten = BroadcastDAGRewriter.rewrite(inputDag, context.getCapabilityRegistry(), mockClusterService());

        // Root: ExchangeReducer over StageInputScan, no target resolver, has a sink provider.
        Stage newRoot = rewritten.rootStage();
        assertEquals(Stage.StageRole.COORDINATOR_REDUCE, newRoot.getRole());
        assertNull("root stage must have no target resolver", newRoot.getTargetResolver());
        assertNotNull("root stage must have sink provider", newRoot.getExchangeSinkProvider());
        assertTrue(
            "root fragment must be ExchangeReducer, got " + newRoot.getFragment().getClass().getSimpleName(),
            newRoot.getFragment() instanceof OpenSearchExchangeReducer
        );
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) newRoot.getFragment();
        assertTrue(
            "reducer input must be StageInputScan pointing at probe stage",
            reducer.getInput() instanceof OpenSearchStageInputScan
        );

        // Probe stage: OpenSearchJoin over (original probe scan, OpenSearchBroadcastScan).
        assertEquals("root has exactly one child — the probe stage", 1, newRoot.getChildStages().size());
        Stage probe = newRoot.getChildStages().get(0);
        assertEquals(Stage.StageRole.BROADCAST_PROBE, probe.getRole());
        assertNotNull("probe stage must carry a shard target resolver over the probe index", probe.getTargetResolver());
        assertTrue(
            "probe fragment must be OpenSearchJoin, got " + probe.getFragment().getClass().getSimpleName(),
            probe.getFragment() instanceof OpenSearchJoin
        );
        OpenSearchJoin probeJoin = (OpenSearchJoin) probe.getFragment();
        // Build was tagged as the LEFT child → probeJoin's left input should be the broadcast scan.
        assertTrue(
            "probe join left input must be OpenSearchBroadcastScan (build was tagged LEFT)",
            probeJoin.getLeft() instanceof OpenSearchBroadcastScan
        );
        assertTrue(
            "probe join right input must be the probe OpenSearchTableScan",
            probeJoin.getRight() instanceof OpenSearchTableScan
        );
        OpenSearchBroadcastScan bc = (OpenSearchBroadcastScan) probeJoin.getLeft();
        assertTrue("namedInputId must start with broadcast-", bc.getNamedInputId().startsWith("broadcast-"));

        // Build stage: plain scan, BROADCAST_BUILD role, shard target resolver.
        assertEquals("probe has exactly one child — the build stage", 1, probe.getChildStages().size());
        Stage build = probe.getChildStages().get(0);
        assertEquals(Stage.StageRole.BROADCAST_BUILD, build.getRole());
        assertNotNull("build stage must carry a shard target resolver", build.getTargetResolver());
        assertTrue(
            "build fragment must be an OpenSearchTableScan, got " + build.getFragment().getClass().getSimpleName(),
            build.getFragment() instanceof OpenSearchTableScan
        );

        // The rewriter re-runs the plan pipeline: every stage must have a non-null plan alternative.
        assertFalse("root must have plan alternatives", newRoot.getPlanAlternatives().isEmpty());
        assertFalse("probe must have plan alternatives", probe.getPlanAlternatives().isEmpty());
        assertFalse("build must have plan alternatives", build.getPlanAlternatives().isEmpty());

        // namedInputId must equal "broadcast-<buildStageId>" — this is the contract
        // BroadcastInjectionHandler relies on.
        assertEquals("broadcast-" + build.getStageId(), bc.getNamedInputId());
    }

    public void testRewriteRespectsRightTaggedAsBuild() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        QueryDAG inputDag = taggedDag(1, makeInnerEquiJoin(), Stage.StageRole.BROADCAST_PROBE, Stage.StageRole.BROADCAST_BUILD);

        QueryDAG rewritten = BroadcastDAGRewriter.rewrite(inputDag, context.getCapabilityRegistry(), mockClusterService());

        Stage probe = rewritten.rootStage().getChildStages().get(0);
        OpenSearchJoin probeJoin = (OpenSearchJoin) probe.getFragment();
        // Build was tagged as the RIGHT child → probeJoin's RIGHT input is the broadcast scan.
        assertTrue(
            "probe join right input must be OpenSearchBroadcastScan (build was tagged RIGHT)",
            probeJoin.getRight() instanceof OpenSearchBroadcastScan
        );
        assertTrue("probe join left input must be the probe OpenSearchTableScan", probeJoin.getLeft() instanceof OpenSearchTableScan);
    }

    public void testRewriteRejectsMissingTags() {
        PlannerContext context = buildContext("parquet", 1, intFields());
        // Both children tagged as SHARD_SOURCE (the default) — advisor never ran for broadcast.
        QueryDAG inputDag = taggedDag(1, makeInnerEquiJoin(), Stage.StageRole.SHARD_SOURCE, Stage.StageRole.SHARD_SOURCE);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> BroadcastDAGRewriter.rewrite(inputDag, context.getCapabilityRegistry(), mockClusterService())
        );
        assertTrue(
            "error must name the missing tags, got: " + ex.getMessage(),
            ex.getMessage().contains("BROADCAST_BUILD") && ex.getMessage().contains("BROADCAST_PROBE")
        );
    }

    // ---- Query builders (local) ----

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
}
