/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;

/**
 * Structural invariants on the {@link QueryDAG} that don't depend on the full plan shape —
 * stage counts, targetResolver / sinkProvider presence, exchangeInfo propagation.
 *
 * <p>Exact-string DAG-shape tests that lock in the {@link QueryDAG#toString()} output live in
 * {@link DAGShapeTests} — those catch regressions in fragment placement or stage cuts via a
 * clean diff of the full tree.
 */
public class DAGBuilderTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(DAGBuilderTests.class);

    private QueryDAG buildDAG(int shardCount, RelNode logicalPlan) {
        var context = buildContext("parquet", shardCount, intFields());
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("QueryDAG:\n{}", dag);
        return dag;
    }

    private static void assertBottomUpIds(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            assertTrue("child stageId must be lower than parent", child.getStageId() < stage.getStageId());
            assertBottomUpIds(child);
        }
    }

    /**
     * Single-shard plans produce a single-stage DAG: SOURCE(SINGLETON) satisfies the root's
     * RESULT(SINGLETON) demand without an ER, so the root stage runs on the data node directly
     * and has a target resolver (no coord/child split).
     */
    public void testSingleShardProducesSingleStageDag() {
        QueryDAG scanDag = buildDAG(1, stubScan(mockTable("test_index", "status", "size")));
        assertEquals(0, scanDag.rootStage().getChildStages().size());
        assertNotNull(scanDag.rootStage().getTargetResolver());
        assertTrue(scanDag.rootStage().getFragment() instanceof OpenSearchTableScan);

        QueryDAG aggDag = buildDAG(1, makeAggregate(sumCall()));
        assertEquals(0, aggDag.rootStage().getChildStages().size());
        assertNotNull(aggDag.rootStage().getTargetResolver());
    }

    /**
     * Multi-shard scan and aggregate both produce two stages. Verifies coordinator root
     * structure (ExchangeReducer → StageInputScan, null targetResolver) and child structure
     * (TableScan leaf, non-null targetResolver, correct ExchangeInfo).
     */
    public void testTwoStageQueries() {
        QueryDAG scanDag = buildDAG(5, stubScan(mockTable("test_index", "status", "size")));
        assertBottomUpIds(scanDag.rootStage());
        assertEquals(1, scanDag.rootStage().getChildStages().size());
        assertNull(scanDag.rootStage().getTargetResolver());
        assertTrue(scanDag.rootStage().getFragment() instanceof OpenSearchExchangeReducer);
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) scanDag.rootStage().getFragment();
        assertTrue(reducer.getInput() instanceof OpenSearchStageInputScan);
        Stage scanChild = scanDag.rootStage().getChildStages().get(0);
        assertNotNull(scanChild.getTargetResolver());
        assertTrue(scanChild.getFragment() instanceof OpenSearchTableScan);

        QueryDAG aggDag = buildDAG(2, makeAggregate(sumCall()));
        assertBottomUpIds(aggDag.rootStage());
        assertEquals(1, aggDag.rootStage().getChildStages().size());
        assertNull(aggDag.rootStage().getTargetResolver());
        assertNotNull(aggDag.rootStage().getExchangeSinkProvider());
        Stage aggChild = aggDag.rootStage().getChildStages().get(0);
        assertNotNull(aggChild.getTargetResolver());
        assertNull(aggChild.getExchangeSinkProvider());
        assertNotNull(aggChild.getExchangeInfo());
        assertEquals(RelDistribution.Type.SINGLETON, aggChild.getExchangeInfo().distributionType());
    }

    /**
     * The reducer's own {@link ExchangeInfo} must flow into the cut child stage —
     * DAGBuilder must not hardcode SINGLETON. Asserts that a non-singleton ExchangeInfo
     * placed on the reducer survives the cut, which is the contract that lets
     * future shuffle/broadcast strategies work without DAGBuilder changes.
     */
    public void testReducerExchangeInfoFlowsToChildStage() {
        var context = buildContext("parquet", 2, intFields());
        RelNode logical = stubScan(mockTable("test_index", "status", "size"));
        RelNode cbo = runPlanner(logical, context);
        // For a multi-shard scan, the planner inserts an OpenSearchExchangeReducer at the root.
        OpenSearchExchangeReducer originalReducer = (OpenSearchExchangeReducer) cbo;

        ExchangeInfo customInfo = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, List.of(0));
        OpenSearchExchangeReducer customReducer = new OpenSearchExchangeReducer(
            originalReducer.getCluster(),
            originalReducer.getTraitSet(),
            originalReducer.getInput(),
            originalReducer.getViableBackends(),
            customInfo
        );

        QueryDAG dag = DAGBuilder.build(customReducer, context.getCapabilityRegistry(), mockClusterService());
        Stage child = dag.rootStage().getChildStages().get(0);
        assertEquals(customInfo, child.getExchangeInfo());
    }
}
