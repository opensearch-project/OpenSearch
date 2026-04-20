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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

/**
 * Tests for {@link DAGBuilder} — verifies correct stage structure for single-stage
 * and two-stage query shapes.
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
     * Single-shard scan and aggregate both produce one stage with a shard target resolver
     * and no exchange sink — no coordinator stage needed.
     */
    public void testSingleStageQueries() {
        QueryDAG scanDag = buildDAG(1, stubScan(mockTable("test_index", "status", "size")));
        assertEquals(0, scanDag.rootStage().getChildStages().size());
        assertNotNull(scanDag.rootStage().getTargetResolver());
        assertNull(scanDag.rootStage().getExchangeSinkProvider());

        QueryDAG aggDag = buildDAG(1, makeAggregate(sumCall()));
        assertEquals(0, aggDag.rootStage().getChildStages().size());
        assertNotNull(aggDag.rootStage().getTargetResolver());
        assertNull(aggDag.rootStage().getExchangeSinkProvider());

        // Sort(Filter(Scan)) with limit — single stage, sort-capable backend
        QueryDAG sortDag = buildDAG(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10)
        );
        assertEquals(0, sortDag.rootStage().getChildStages().size());
        assertNotNull(sortDag.rootStage().getTargetResolver());
        assertNull(sortDag.rootStage().getExchangeSinkProvider());
        assertTrue(sortDag.rootStage().getFragment() instanceof OpenSearchSort);
    }

    /**
     * Multi-shard scan and aggregate both produce two stages. Verifies coordinator root
     * structure (ExchangeReducer → StageInputScan, null targetResolver) and child structure
     * (TableScan leaf, non-null targetResolver, correct ExchangeInfo).
     */
    public void testTwoStageQueries() {
        // Multi-shard scan: pure gather, no compute at coordinator
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

        // Multi-shard aggregate: coordinator reduces partial aggregates
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
}
