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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link AggregateDecompositionResolver} — verifies the four decomposition
 * cases (pass-through, function-swap, engine-native, primitive-decomp) produce correct
 * PARTIAL/FINAL rewrites with types derived from {@code AggregateFunction.intermediateFields}.
 */
public class AggregateDecompositionResolverTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(AggregateDecompositionResolverTests.class);

    // ── Test infrastructure ──

    private QueryDAG buildAndResolve(AggregateCall... aggCalls) {
        return buildAndResolve(intFields(), aggCalls);
    }

    private QueryDAG buildAndResolve(Map<String, Map<String, Object>> fields, AggregateCall... aggCalls) {
        PlannerContext context = buildContext("parquet", 2, fields);
        RelNode input = makeMultiCallAggregate(stubScan(mockTable("test_index", "status", "size")), aggCalls);
        LOGGER.info("Input:\n{}", RelOptUtil.toString(input));
        RelNode cboOutput = runPlanner(input, context);
        LOGGER.info("CBO output:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        LOGGER.info("Before resolve:\n{}", dag);
        AggregateDecompositionResolver.resolveAll(dag, context.getCapabilityRegistry());
        LOGGER.info("After resolve:\n{}", dag);
        return dag;
    }

    private OpenSearchAggregate findPartialAgg(QueryDAG dag) {
        Stage childStage = dag.rootStage().getChildStages().get(0);
        StagePlan childPlan = childStage.getPlanAlternatives().get(0);
        return findAgg(childPlan.resolvedFragment(), AggregateMode.PARTIAL);
    }

    private RelNode findParentFragment(QueryDAG dag) {
        return dag.rootStage().getPlanAlternatives().get(0).resolvedFragment();
    }

    private OpenSearchAggregate findFinalAgg(RelNode fragment) {
        return findAgg(fragment, AggregateMode.FINAL);
    }

    private static OpenSearchAggregate findAgg(RelNode node, AggregateMode mode) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == mode) {
            return agg;
        }
        for (RelNode input : node.getInputs()) {
            OpenSearchAggregate found = findAgg(input, mode);
            if (found != null) return found;
        }
        return null;
    }

    private static OpenSearchStageInputScan findStageInput(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return scan;
        }
        for (RelNode input : node.getInputs()) {
            OpenSearchStageInputScan found = findStageInput(input);
            if (found != null) return found;
        }
        return null;
    }

    // ── Tests ──

    /**
     * SUM is pass-through: PARTIAL keeps SUM, FINAL keeps SUM with arg rebound.
     * Types unchanged.
     */
    public void testPassThroughSum() {
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "s"
        );
        QueryDAG dag = buildAndResolve(sum);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull("PARTIAL aggregate must exist", partial);
        assertEquals(1, partial.getAggCallList().size());
        AggregateCall partialCall = partial.getAggCallList().get(0);
        assertEquals("SUM", partialCall.getAggregation().getName());
        assertEquals(List.of(1), partialCall.getArgList());

        RelNode parentFragment = findParentFragment(dag);
        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull("FINAL aggregate must exist", finalAgg);
        assertEquals(1, finalAgg.getAggCallList().size());
        AggregateCall finalCall = finalAgg.getAggCallList().get(0);
        assertEquals("SUM", finalCall.getAggregation().getName());
        // FINAL arg rebound to group_count + 0 = 1 (one group key at index 0)
        int groupCount = finalAgg.getGroupSet().cardinality();
        assertEquals(List.of(groupCount), finalCall.getArgList());
    }

    /**
     * COUNT(*) is function-swap: PARTIAL retyped to BIGINT, FINAL becomes SUM(count_col).
     */
    public void testFunctionSwapCount() {
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, List.of(), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT), "c"
        );
        QueryDAG dag = buildAndResolve(count);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        assertEquals(1, partial.getAggCallList().size());
        AggregateCall partialCall = partial.getAggCallList().get(0);
        // PARTIAL keeps COUNT but retyped to BIGINT (from intermediateFields Int64)
        assertEquals("COUNT", partialCall.getAggregation().getName());
        assertEquals(SqlTypeName.BIGINT, partialCall.getType().getSqlTypeName());

        RelNode parentFragment = findParentFragment(dag);
        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        assertEquals(1, finalAgg.getAggCallList().size());
        AggregateCall finalCall = finalAgg.getAggCallList().get(0);
        // FINAL becomes SUM (function-swap: COUNT → SUM)
        assertEquals("SUM", finalCall.getAggregation().getName());
        int groupCount = finalAgg.getGroupSet().cardinality();
        assertEquals(List.of(groupCount), finalCall.getArgList());
    }

    /**
     * APPROX_COUNT_DISTINCT is engine-native: exchange row type has VARBINARY,
     * FINAL keeps APPROX_COUNT_DISTINCT with arg rebound.
     */
    public void testEngineNativeDC() {
        AggregateCall dc = AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT), "d"
        );
        QueryDAG dag = buildAndResolve(dc);

        // Verify exchange row type (StageInputScan) has VARBINARY from intermediateFields
        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull("StageInputScan must exist", stageInput);
        // Row type: [group_key:INTEGER, d:VARBINARY]
        RelDataType exchangeRowType = stageInput.getRowType();
        assertEquals(2, exchangeRowType.getFieldCount());
        assertEquals(SqlTypeName.VARBINARY, exchangeRowType.getFieldList().get(1).getType().getSqlTypeName());

        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        assertEquals(1, finalAgg.getAggCallList().size());
        AggregateCall finalCall = finalAgg.getAggCallList().get(0);
        // FINAL keeps APPROX_COUNT_DISTINCT (engine-native: reducer == self)
        assertEquals("APPROX_COUNT_DISTINCT", finalCall.getAggregation().getName());
        int groupCount = finalAgg.getGroupSet().cardinality();
        assertEquals(List.of(groupCount), finalCall.getArgList());
    }

    /**
     * AVG is primitive-decomp: PARTIAL emits COUNT(x) + SUM(x);
     * Exchange row type has BIGINT + DOUBLE from intermediateFields;
     * FINAL emits SUM(cnt) + SUM(sum); Project wrapper has sum/count cast to original type.
     */
    public void testPrimitiveDecompAvg() {
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "a"
        );
        QueryDAG dag = buildAndResolve(avg);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        // AVG decomposes into 2 partial calls: SUM (count reducer) + SUM (sum reducer)
        assertEquals(2, partial.getAggCallList().size());
        assertEquals("SUM", partial.getAggCallList().get(0).getAggregation().getName());
        assertEquals("SUM", partial.getAggCallList().get(1).getAggregation().getName());

        // Verify exchange row type has correct types from intermediateFields
        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull(stageInput);
        RelDataType exchangeRowType = stageInput.getRowType();
        // [group_key:INTEGER, count:BIGINT, sum:DOUBLE]
        assertEquals(3, exchangeRowType.getFieldCount());
        assertEquals(SqlTypeName.BIGINT, exchangeRowType.getFieldList().get(1).getType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, exchangeRowType.getFieldList().get(2).getType().getSqlTypeName());

        // Should have a LogicalProject on top for the finalExpression
        assertTrue("Parent fragment should have LogicalProject for AVG",
            parentFragment instanceof LogicalProject);

        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        // FINAL has 2 SUM calls (reducers for count and sum)
        assertEquals(2, finalAgg.getAggCallList().size());
        assertEquals("SUM", finalAgg.getAggCallList().get(0).getAggregation().getName());
        assertEquals("SUM", finalAgg.getAggCallList().get(1).getAggregation().getName());
    }

    /**
     * Mixed query: avg(size), count() c, sum(x) s — all families together.
     * Verifies column positions are correct in exchange row type.
     */
    public void testMixedQ10() {
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "avg_size"
        );
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, List.of(), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT), "c"
        );
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "s"
        );
        QueryDAG dag = buildAndResolve(avg, count, sum);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        // AVG → 2 calls (SUM, SUM), COUNT → 1 call, SUM → 1 call = 4 total
        assertEquals(4, partial.getAggCallList().size());

        // Verify exchange row type from intermediateFields
        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull(stageInput);
        RelDataType exchangeRowType = stageInput.getRowType();
        // [group:INTEGER, avg_count:BIGINT, avg_sum:DOUBLE, c:BIGINT, s:INTEGER]
        assertEquals(5, exchangeRowType.getFieldCount());
        assertEquals(SqlTypeName.INTEGER, exchangeRowType.getFieldList().get(0).getType().getSqlTypeName()); // group
        assertEquals(SqlTypeName.BIGINT, exchangeRowType.getFieldList().get(1).getType().getSqlTypeName());  // avg count
        assertEquals(SqlTypeName.DOUBLE, exchangeRowType.getFieldList().get(2).getType().getSqlTypeName());  // avg sum
        assertEquals(SqlTypeName.BIGINT, exchangeRowType.getFieldList().get(3).getType().getSqlTypeName());  // count
        assertEquals(SqlTypeName.INTEGER, exchangeRowType.getFieldList().get(4).getType().getSqlTypeName()); // sum (pass-through)

        // AVG requires a Project wrapper
        assertTrue("Parent fragment should have LogicalProject for AVG",
            parentFragment instanceof LogicalProject);

        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        // FINAL: 2 SUM (for avg decomp) + 1 SUM (for count swap) + 1 SUM (pass-through) = 4
        assertEquals(4, finalAgg.getAggCallList().size());
    }

    /**
     * Group keys appear first in all row types; their types are unchanged.
     */
    public void testGroupKeysFlowThrough() {
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "s"
        );
        QueryDAG dag = buildAndResolve(sum);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        // Group key is field 0 (status)
        assertEquals(1, partial.getGroupSet().cardinality());
        assertTrue(partial.getGroupSet().get(0));

        // Row type: [group_key, agg_result]
        RelDataType partialRowType = partial.getRowType();
        assertTrue(partialRowType.getFieldCount() >= 2);
        // Group key type should be INTEGER (from the input)
        RelDataTypeField groupField = partialRowType.getFieldList().get(0);
        assertEquals(SqlTypeName.INTEGER, groupField.getType().getSqlTypeName());
    }

    /**
     * Regression guard: verify that the exchange row type (StageInputScan) uses types
     * from intermediateFields, NOT from Calcite's inferReturnType.
     *
     * For AVG(INTEGER): Calcite's SUM.inferReturnType would give INTEGER for the sum,
     * but intermediateFields says Float64 → DOUBLE. The exchange row type must use DOUBLE.
     */
    public void testNoCalciteInferReturnType() {
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG, false, List.of(1), -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "a"
        );
        QueryDAG dag = buildAndResolve(avg);

        // The exchange row type must use intermediateFields types
        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull(stageInput);
        RelDataType exchangeRowType = stageInput.getRowType();

        // The "sum" field must be DOUBLE (from intermediateFields Float64),
        // NOT INTEGER (which Calcite's SUM.inferReturnType would produce for INTEGER input)
        assertEquals("Sum exchange type must come from intermediateFields (DOUBLE), not Calcite inference",
            SqlTypeName.DOUBLE, exchangeRowType.getFieldList().get(2).getType().getSqlTypeName());

        // The "count" field must be BIGINT (from intermediateFields Int64)
        assertEquals(SqlTypeName.BIGINT, exchangeRowType.getFieldList().get(1).getType().getSqlTypeName());
    }
}
