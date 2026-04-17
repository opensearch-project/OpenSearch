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
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DAGBuilder} — verifies correct stage structure.
 */
public class DAGBuilderTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(DAGBuilderTests.class);

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any()))
            .thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        return clusterService;
    }

    private QueryDAG buildDAG(int shardCount, RelNode logicalPlan) {
        var context = buildContext("parquet", shardCount, intFields());
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("QueryDAG:\n{}", dag);
        return dag;
    }

    /** Asserts stage IDs are assigned bottom-up across the entire DAG. */
    private static void assertBottomUpIds(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            assertTrue("child stageId [" + child.getStageId() + "] must be lower than parent [" + stage.getStageId() + "]",
                child.getStageId() < stage.getStageId());
            assertBottomUpIds(child);
        }
    }

    /** Single-shard scan — no exchange, one stage dispatching to shards. */
    public void testSingleShardScanProducesOneStage() {
        QueryDAG dag = buildDAG(1, stubScan(mockTable("test_index", "status", "size")));

        assertEquals(0, dag.rootStage().getChildStages().size());
        assertNotNull(dag.rootStage().getTargetResolver());
        assertNull(dag.rootStage().getExchangeSinkProvider());
    }

    /** Multi-shard scan — CBO inserts ExchangeReducer at root, two stages produced. */
    public void testMultiShardScanProducesTwoStages() {
        QueryDAG dag = buildDAG(5, stubScan(mockTable("test_index", "status", "size")));

        assertBottomUpIds(dag.rootStage());
        assertEquals(1, dag.rootStage().getChildStages().size());
        assertNull("root must have null targetResolver", dag.rootStage().getTargetResolver());

        // Root fragment: ExchangeReducer → StageInputScan (pure gather, no compute)
        assertTrue("root fragment must be ExchangeReducer",
            dag.rootStage().getFragment() instanceof OpenSearchExchangeReducer);
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) dag.rootStage().getFragment();
        assertTrue("reducer input must be StageInputScan",
            reducer.getInput() instanceof OpenSearchStageInputScan);

        // Child fragment: TableScan only (no exchange operators)
        Stage child = dag.rootStage().getChildStages().get(0);
        assertNotNull("child must have targetResolver", child.getTargetResolver());
        assertTrue("child fragment must be OpenSearchTableScan",
            child.getFragment() instanceof OpenSearchTableScan);
    }

    /** Single-shard aggregate — no exchange needed, one stage. */
    public void testSingleShardAggregateProducesOneStage() {
        QueryDAG dag = buildDAG(1, makeAggregate(1, sumCall()));

        assertEquals(0, dag.rootStage().getChildStages().size());
        assertNotNull(dag.rootStage().getTargetResolver());
        assertNull(dag.rootStage().getExchangeSinkProvider());
    }

    /** Multi-shard aggregate — two stages: coordinator root + data node child. */
    public void testMultiShardAggregateProducesTwoStages() {
        QueryDAG dag = buildDAG(2, makeAggregate(2, sumCall()));

        assertBottomUpIds(dag.rootStage());
        assertEquals(1, dag.rootStage().getChildStages().size());
        assertNull("root must have null targetResolver", dag.rootStage().getTargetResolver());
        assertNotNull("root must have exchangeSinkProvider", dag.rootStage().getExchangeSinkProvider());

        Stage child = dag.rootStage().getChildStages().get(0);
        assertNotNull("child must have targetResolver", child.getTargetResolver());
        assertNull("child must have null exchangeSinkProvider", child.getExchangeSinkProvider());
        assertNotNull(child.getExchangeInfo());
        assertEquals(RelDistribution.Type.SINGLETON, child.getExchangeInfo().distributionType());
    }

    private static Map<String, Map<String, Object>> intFields() {
        return Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        );
    }
}
