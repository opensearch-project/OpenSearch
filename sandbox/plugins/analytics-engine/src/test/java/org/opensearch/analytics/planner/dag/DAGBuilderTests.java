/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.ClickBench;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.SqlPlannerTestFixture;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.cluster.ClusterState;

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

    /**
     * Literal-row source (LogicalValues / OpenSearchValues): a coordinator-local compute leaf
     * with no TableScan in the fragment. DAGBuilder must (1) skip {@code ShardTargetResolver}
     * attachment because there's no shard to target, and (2) still install a sink provider
     * because the root stage executes a backend plan locally (LOCAL_COMPUTE).
     */
    public void testValuesRootHasNoTargetResolverButHasSink() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = typeFactory.builder().add("a", intType).build();
        RexLiteral one = (RexLiteral) rexBuilder.makeLiteral(1, intType, true);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(ImmutableList.of(one));
        RelNode plan = LogicalValues.create(cluster, rowType, tuples);

        QueryDAG dag = buildDAG(2, plan);
        assertEquals("Values is leaf — no child stages", 0, dag.rootStage().getChildStages().size());
        assertTrue("root fragment must be OpenSearchValues", dag.rootStage().getFragment() instanceof OpenSearchValues);
        assertNull("no TableScan → no ShardTargetResolver", dag.rootStage().getTargetResolver());
        assertNotNull("compute leaf needs a sink provider for its local backend output", dag.rootStage().getExchangeSinkProvider());
    }

    // ── QTF (late-materialization) DAG shapes ──────────────────────────────

    /**
     * Drives SQL through the full planner so the QTF rewriter fires, then runs the result
     * through {@link DAGBuilder}. Returns the four-stage DAG documented at
     * {@link #testQtfDag_multiShardFourStages}.
     */
    private QueryDAG buildQtfDag(String sql, int shardCount) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
        PlannerContext context = new PlannerContext(
            new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new),
            state,
            false
        );
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        RelNode cbo = PlannerImpl.runAllOptimizations(parsed, context);
        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("QTF QueryDAG:\n{}", dag);
        return dag;
    }

    /**
     * Multi-shard QTF produces four stages, dataflow runs bottom-up from shards to the post-LM
     * coordinator reduce:
     * <pre>
     *   Stage 0 SHARD_FRAGMENT       (Filter? + Scan)
     *     ↓
     *   Stage 1 COORDINATOR_REDUCE   (Sort+Limit over reduce-set, ___row_id, ___ugsi)
     *     ↓   inputSinkDecorator = OrdinalAppendingSink (stamps shard ordinal as ___ugsi)
     *   Stage 2 LATE_MATERIALIZATION (wrapper rooted at StageInputScan(stage 1))
     *     ↓
     *   Stage 3 COORDINATOR_REDUCE   (post-LM ops: outer Project, etc.)  ← root
     * </pre>
     *
     * <p>Stage 3 is the post-LM compute stage, separated by {@link DAGBuilder} so the LM stage
     * itself stays a pure scatter/gather/stitch and the post-LM Project (or any future
     * Filter/Aggregate/Sort) runs on Substrait via the standard reduce path.
     */
    public void testQtfDag_multiShardFourStages() {
        QueryDAG dag = buildQtfDag("SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
        assertBottomUpIds(dag.rootStage());

        Stage postLm = dag.rootStage();
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, postLm.getExecutionType());
        assertNull("post-LM reduce carries no input decorator", postLm.getInputSinkDecorator());
        assertEquals(1, postLm.getChildStages().size());

        Stage lm = postLm.getChildStages().get(0);
        assertEquals(StageExecutionType.LATE_MATERIALIZATION, lm.getExecutionType());
        assertNotNull(
            "LM fragment must contain OpenSearchLateMaterialization wrapper",
            RelNodeUtils.findNode(lm.getFragment(), OpenSearchLateMaterialization.class)
        );
        assertEquals(1, lm.getChildStages().size());

        Stage reduce = lm.getChildStages().get(0);
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, reduce.getExecutionType());
        assertNotNull("LM cut must install OrdinalAppendingSink decorator on reducer", reduce.getInputSinkDecorator());
        assertEquals(1, reduce.getChildStages().size());

        // StageInputScan must carry ___ugsi — cutAtExchange uses the reducer's output rowType.
        OpenSearchStageInputScan reduceInputScan = RelNodeUtils.findNode(reduce.getFragment(), OpenSearchStageInputScan.class);
        assertNotNull(reduceInputScan);
        assertEquals(0, reduceInputScan.getChildStageId());
        assertTrue(
            "StageInputScan rowType must include " + OpenSearchLateMaterialization.UGSI_FIELD,
            reduceInputScan.getRowType().getFieldNames().contains(OpenSearchLateMaterialization.UGSI_FIELD)
        );

        Stage scan = reduce.getChildStages().get(0);
        assertEquals(StageExecutionType.SHARD_FRAGMENT, scan.getExecutionType());
        assertNull("scan stage carries no input decorator", scan.getInputSinkDecorator());
        assertNotNull("scan stage must have a target resolver", scan.getTargetResolver());
        assertEquals(0, scan.getChildStages().size());
    }

    /**
     * Stage 0's shard fragment must propagate {@code __row_id__} on its Scan output. The
     * rewriter narrows the Scan to {@code [belowAnchorPhysicalFields..., __row_id__]} via an
     * override rowType; this asserts that override survives DAG cuts so the converted
     * Substrait declares the helper column. Without it, Stage 1's reduce plan references
     * {@code input-0.__row_id__} but the partition exposes only the physical cols and
     * DataFusion fails the registration with "No field named __row_id__".
     */
    public void testQtfDag_stage0ScanCarriesRowIdHelper() {
        QueryDAG dag = buildQtfDag("SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
        Stage scan = dag.rootStage().getChildStages().getFirst().getChildStages().getFirst().getChildStages().getFirst();
        assertEquals(StageExecutionType.SHARD_FRAGMENT, scan.getExecutionType());

        OpenSearchTableScan tableScan = RelNodeUtils.findNode(scan.getFragment(), OpenSearchTableScan.class);
        assertNotNull("Stage 0 fragment must contain an OpenSearchTableScan", tableScan);

        List<String> scanFieldNames = tableScan.getRowType().getFieldNames();
        assertTrue(
            "Stage 0 Scan rowType must carry " + OpenSearchLateMaterialization.ROW_ID_FIELD + ", got " + scanFieldNames,
            scanFieldNames.contains(OpenSearchLateMaterialization.ROW_ID_FIELD)
        );
    }

    /**
     * Reducer stages outside QTF carry no {@code inputSinkDecorator} — confirms the decorator
     * is a QTF-only attachment, not a regression on every reduce.
     */
    public void testReducerHasNoInputSinkDecoratorForNonQtf() {
        QueryDAG dag = buildDAG(3, makeAggregate(sumCall()));
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, dag.rootStage().getExecutionType());
        assertNull(dag.rootStage().getInputSinkDecorator());
    }
}
