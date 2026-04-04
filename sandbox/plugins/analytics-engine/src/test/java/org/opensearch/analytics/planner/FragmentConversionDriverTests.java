/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link FragmentConversionDriver} — verifies that resolved fragments
 * are stripped and passed to the backend's {@link FragmentConvertor}.
 */
public class FragmentConversionDriverTests extends BasePlannerRulesTests {

    /**
     * Simple filter + scan: verifies annotations are stripped and convertScanFragment
     * is called with a clean Calcite RelNode (no OpenSearch wrappers, no annotations).
     */
    public void testScanFragmentConversion() {
        Map<String, Map<String, Object>> fields = Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        );

        PlannerContext context = buildContext("parquet", 1, fields);
        RelOptTable table = mockTable("test_index",
            new String[]{"status", "size"},
            new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.INTEGER});

        RelNode filter = LogicalFilter.create(
            stubScan(table),
            makeEquals(0, SqlTypeName.INTEGER, 200)
        );

        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());

        Stage stage = dag.rootStage();
        assertFalse("Expected plan alternatives", stage.getPlanAlternatives().isEmpty());

        StagePlan plan = stage.getPlanAlternatives().getFirst();
        RecordingConvertor recorder = new RecordingConvertor();

        byte[] result = FragmentConversionDriver.convert(plan.resolvedFragment(), recorder);

        assertNotNull(result);
        assertTrue("Expected convertScanFragment to be called", recorder.scanCalled);
        assertEquals("test_index", recorder.tableName);
        assertNotNull(recorder.fragment);

        // Verify the fragment is stripped — no OpenSearch wrappers
        String planStr = RelOptUtil.toString(recorder.fragment);
        logger.info("Stripped fragment:\n{}", planStr);
        assertFalse("Should not contain OpenSearchFilter", planStr.contains("OpenSearchFilter"));
        assertFalse("Should not contain ANNOTATED_PREDICATE", planStr.contains("ANNOTATED_PREDICATE"));
        assertTrue("Should contain LogicalFilter", planStr.contains("LogicalFilter"));
        assertTrue("Should contain LogicalTableScan", planStr.contains("LogicalTableScan"));
    }

    /**
     * Aggregate + filter + scan: verifies aggregate annotations are stripped
     * and the fragment contains standard Calcite operators.
     */
    public void testAggregateFragmentConversion() {
        Map<String, Map<String, Object>> fields = Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        );

        PlannerContext context = buildContext("parquet", 1, fields);
        RelOptTable table = mockTable("test_index",
            new String[]{"status", "size"},
            new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.INTEGER});

        RelNode scan = stubScan(table);
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false, List.of(),
            List.of(1), -1, null, RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER), "total_size"
        );
        RelNode aggregate = LogicalAggregate.create(
            scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall)
        );

        RelNode cboOutput = runPlanner(aggregate, context);
        QueryDAG dag = DAGBuilder.build(cboOutput);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());

        Stage stage = dag.rootStage();
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        RecordingConvertor recorder = new RecordingConvertor();

        byte[] result = FragmentConversionDriver.convert(plan.resolvedFragment(), recorder);

        assertNotNull(result);
        assertTrue("Expected convertScanFragment to be called", recorder.scanCalled);

        String planStr = RelOptUtil.toString(recorder.fragment);
        logger.info("Stripped fragment:\n{}", planStr);
        assertFalse("Should not contain OpenSearchAggregate", planStr.contains("OpenSearchAggregate"));
        assertFalse("Should not contain AGG_CALL_ANNOTATION", planStr.contains("AGG_CALL_ANNOTATION"));
        assertTrue("Should contain LogicalAggregate", planStr.contains("LogicalAggregate"));
    }

    /** Mock FragmentConvertor that records what was called. */
    private static class RecordingConvertor implements FragmentConvertor {
        boolean scanCalled;
        boolean shuffleCalled;
        String tableName;
        RelNode fragment;

        @Override
        public byte[] convertScanFragment(String tableName, RelNode fragment) {
            this.scanCalled = true;
            this.tableName = tableName;
            this.fragment = fragment;
            return ("scan:" + tableName).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] convertShuffleReadFragment(String tableName, RelNode fragment) {
            this.shuffleCalled = true;
            this.tableName = tableName;
            this.fragment = fragment;
            return ("shuffle:" + tableName).getBytes(StandardCharsets.UTF_8);
        }
    }
}
