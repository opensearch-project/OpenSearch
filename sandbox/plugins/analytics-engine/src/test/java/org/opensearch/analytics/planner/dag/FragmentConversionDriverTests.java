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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link FragmentConversionDriver} — verifies annotations are stripped
 * and the correct {@link FragmentConvertor} method is called per leaf type,
 * across a range of query shapes.
 */
public class FragmentConversionDriverTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriverTests.class);

    private static final Set<String> OPENSEARCH_OPERATORS = Set.of(
        OpenSearchFilter.class.getSimpleName(),
        OpenSearchAggregate.class.getSimpleName(),
        OpenSearchTableScan.class.getSimpleName(),
        OpenSearchSort.class.getSimpleName(),
        OpenSearchProject.class.getSimpleName()
    );

    private static final Set<String> ANNOTATION_MARKERS = Set.of(
        AnnotatedPredicate.class.getSimpleName(),
        AggregateCallAnnotation.class.getSimpleName(),
        AnnotatedProjectExpression.class.getSimpleName()
    );

    // ---- Helpers ----

    private MockDataFusionBackend dfWithConvertor(RecordingConvertor convertor) {
        return new MockDataFusionBackend() {
            @Override
            public FragmentConvertor getFragmentConvertor() {
                return convertor;
            }
        };
    }

    private QueryDAG buildAndConvert(int shardCount, RelNode logicalPlan, RecordingConvertor convertor) {
        var df = dfWithConvertor(convertor);
        var context = buildContext("parquet", shardCount, intFields(), List.of(df));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after conversion:\n{}", dag);
        return dag;
    }

    private void assertDoesntContainOperators(RelNode fragment, Set<String> forbidden) {
        assertNotNull(fragment);
        String planStr = RelOptUtil.toString(fragment);
        LOGGER.info("Stripped fragment:\n{}", planStr);
        for (String op : forbidden)
            assertFalse("Stripped fragment must not contain [" + op + "]", planStr.contains(op));
    }

    private void assertShardScanConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertNotNull("convertedBytes must be set", plan.convertedBytes());
        assertTrue("convertShardScanFragment must be called", convertor.shardScanCalled);
        assertEquals("test_index", convertor.tableName);
        assertDoesntContainOperators(convertor.fragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.fragment, ANNOTATION_MARKERS);
    }

    private void assertCoordinatorConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertNotNull("convertedBytes must be set", plan.convertedBytes());
        assertTrue("convertCoordinatorFragment must be called", convertor.coordinatorCalled);
        // Coordinator fragment may contain ExchangeReducer/StageInputScan (infrastructure) but no operator wrappers or annotations
        assertDoesntContainOperators(convertor.coordinatorFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.coordinatorFragment, ANNOTATION_MARKERS);
    }

    // ---- Single-stage query shapes ----

    /**
     * Scan, Filter(Scan), Aggregate(Scan), Sort(Filter(Scan)) — all single-stage.
     * Verifies convertShardScanFragment is called and fragment is fully stripped.
     */
    public void testSingleStageQueryShapes() {
        RecordingConvertor scanConvertor = new RecordingConvertor();
        QueryDAG scanDag = buildAndConvert(1, stubScan(mockTable("test_index", "status", "size")), scanConvertor);
        assertTrue(scanDag.rootStage().getChildStages().isEmpty());
        assertShardScanConverted(scanConvertor, scanDag.rootStage());

        RecordingConvertor filterConvertor = new RecordingConvertor();
        QueryDAG filterDag = buildAndConvert(
            1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            filterConvertor
        );
        assertTrue(filterDag.rootStage().getChildStages().isEmpty());
        assertShardScanConverted(filterConvertor, filterDag.rootStage());

        RecordingConvertor aggConvertor = new RecordingConvertor();
        QueryDAG aggDag = buildAndConvert(1, makeAggregate(sumCall()), aggConvertor);
        assertTrue(aggDag.rootStage().getChildStages().isEmpty());
        assertShardScanConverted(aggConvertor, aggDag.rootStage());

        RecordingConvertor sortConvertor = new RecordingConvertor();
        QueryDAG sortDag = buildAndConvert(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10),
            sortConvertor
        );
        assertTrue(sortDag.rootStage().getChildStages().isEmpty());
        assertShardScanConverted(sortConvertor, sortDag.rootStage());
    }

    // ---- Composed pipeline shapes ----

    /** Aggregate(Filter(Scan)) — most common OLAP shape. */
    public void testAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeAggregate(
                makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                sumCall()
            ),
            convertor
        );
        assertShardScanConverted(convertor, dag.rootStage());
    }

    /** Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline. */
    public void testSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );
        assertShardScanConverted(convertor, dag.rootStage());
    }

    // ---- Two-stage shapes ----

    /**
     * Multi-shard Aggregate(Scan) — child calls convertShardScanFragment,
     * root calls convertCoordinatorFragment.
     */
    public void testTwoStageAggregateConversion() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(sumCall()), convertor);

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertCoordinatorConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    /**
     * Multi-shard Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline, two stages.
     */
    public void testTwoStageSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            2,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertCoordinatorConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    // ---- RecordingConvertor ----

    /** Records which convertor method was called and what was passed. */
    private static class RecordingConvertor implements FragmentConvertor {
        boolean shardScanCalled;
        boolean coordinatorCalled;
        String tableName;
        RelNode fragment;
        RelNode coordinatorFragment;

        @Override
        public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
            this.shardScanCalled = true;
            this.tableName = tableName;
            this.fragment = fragment;
            return ("shard:" + tableName).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] convertCoordinatorFragment(RelNode fragment) {
            this.coordinatorCalled = true;
            this.coordinatorFragment = fragment;
            return "coordinator".getBytes(StandardCharsets.UTF_8);
        }
    }
}
