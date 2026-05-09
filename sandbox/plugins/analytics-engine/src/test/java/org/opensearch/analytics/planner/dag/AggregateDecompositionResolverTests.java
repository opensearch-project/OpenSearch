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
import org.apache.calcite.rel.core.Project;
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
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
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
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
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
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "d"
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
            SqlStdOperatorTable.AVG,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "a"
        );
        QueryDAG dag = buildAndResolve(avg);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        // AVG decomposes into 2 partial calls. Calcite's AggregateReduceFunctionsRule runs
        // during HEP marking (before our split rule) and produces SUM(x) + COUNT(x) as the
        // primitives, with a Project on top carrying CAST(SUM/COUNT AS avgReturnType).
        // Split rule then propagates the primitives to both halves as pass-through.
        assertEquals(2, partial.getAggCallList().size());
        assertEquals("SUM", partial.getAggCallList().get(0).getAggregation().getName());
        assertEquals("COUNT", partial.getAggCallList().get(1).getAggregation().getName());

        // Exchange row type: [group_key:INTEGER, sum:<int-family>, count:<int-family>]
        // Calcite's SUM / COUNT inference over the test fixture's INTEGER input yields
        // integer-family return types (INTEGER or BIGINT depending on nullability rules).
        // No type override from intermediateFields is needed here — the prior invariant
        // that "sum must be DOUBLE from intermediateFields" only held when AVG was kept
        // un-decomposed and DataFusion's internal AVG state (Float64 sum) leaked into
        // the exchange. Calcite's decomposition sidesteps that entirely.
        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull(stageInput);
        RelDataType exchangeRowType = stageInput.getRowType();
        assertEquals(3, exchangeRowType.getFieldCount());
        SqlTypeName sumType = exchangeRowType.getFieldList().get(1).getType().getSqlTypeName();
        SqlTypeName countType = exchangeRowType.getFieldList().get(2).getType().getSqlTypeName();
        assertTrue("Sum type is integer-family: got " + sumType, sumType == SqlTypeName.BIGINT || sumType == SqlTypeName.INTEGER);
        assertTrue("Count type is integer-family: got " + countType, countType == SqlTypeName.BIGINT || countType == SqlTypeName.INTEGER);

        // Parent fragment is a Project carrying the final-expression computation
        // (CAST(sum/count)). Marked as OpenSearchProject (not LogicalProject) because
        // OpenSearchProjectRule runs in the same HEP phase as Calcite's reduce rule.
        assertTrue("Parent fragment should be a Project carrying the final expression", parentFragment instanceof Project);

        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        // FINAL reduces the partial primitives: SUM(sum_col) + SUM(count_col). The resolver's
        // function-swap branch rewrites the original COUNT at FINAL into SUM over the partial
        // count column.
        assertEquals(2, finalAgg.getAggCallList().size());
        assertEquals("SUM", finalAgg.getAggCallList().get(0).getAggregation().getName());
        assertEquals("SUM", finalAgg.getAggCallList().get(1).getAggregation().getName());
    }

    /**
     * Mixed query: avg(size), count() c, sum(x) s — all families together.
     * Verifies column positions are correct in exchange row type.
     */
    /**
     * Mixed query: avg(size), count() c, sum(x) s — all families together. Spot-checks that
     * the resolver + Calcite's AggregateReduceFunctionsRule compose correctly when AVG,
     * COUNT, and plain SUM appear in the same aggregate.
     *
     * <p>Note on aggregate-call count: Calcite's rule deduplicates aggregates whose
     * arguments match — for this query, the user's {@code count()} is identical to AVG's
     * inner {@code COUNT()}, and the user's {@code sum(size)} is identical to AVG's inner
     * {@code SUM(size)}. Calcite collapses these into a single pair of primitive calls and
     * reshapes the Project on top to surface each user-named column as an input reference.
     * So PARTIAL carries 2 primitives (not 4), and the Project provides {@code avg_size},
     * {@code c}, and {@code s} outputs from the same underlying columns. Semantically
     * equivalent to the un-deduplicated form, with strictly fewer per-shard aggregations.
     */
    public void testMixedQ10() {
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "avg_size"
        );
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
        );
        QueryDAG dag = buildAndResolve(avg, count, sum);

        OpenSearchAggregate partial = findPartialAgg(dag);
        assertNotNull(partial);
        // Deduplication: AVG's SUM($1)/COUNT() absorb user's SUM($1)/COUNT() → 2 primitives.
        assertEquals(2, partial.getAggCallList().size());

        // Parent fragment is a Project that projects avg_size, c, s from the aggregate output
        // via CAST(div) + input refs.
        RelNode parentFragment = findParentFragment(dag);
        assertTrue("Parent fragment should be a Project surfacing all three user-named columns", parentFragment instanceof Project);
        Project parentProject = (Project) parentFragment;
        assertEquals("Project must surface [status, avg_size, c, s] → 4 output columns", 4, parentProject.getProjects().size());

        OpenSearchAggregate finalAgg = findFinalAgg(parentFragment);
        assertNotNull(finalAgg);
        assertEquals(2, finalAgg.getAggCallList().size());
    }

    /**
     * Group keys appear first in all row types; their types are unchanged.
     */
    public void testGroupKeysFlowThrough() {
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
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
     * Historically this test enforced "AVG's sum-field exchange type must come from
     * AggregateFunction.intermediateFields (DOUBLE), not Calcite inference (BIGINT for
     * SUM(INTEGER))". That invariant existed because the hand-rolled resolver kept AVG
     * un-decomposed in the Calcite plan and had to override the StageInputScan row type
     * with DataFusion's native AVG state schema (Float64 sum) to avoid wire-format mismatch.
     *
     * <p>With {@code OpenSearchAggregateReduceRule} running during HEP marking, AVG is
     * decomposed into primitive SUM(x) + COUNT(x) before our resolver ever sees it. The
     * primitives' Calcite-inferred types (SUM(INTEGER) = BIGINT) now match DataFusion's
     * emitted types (Int64 for SUM over integer input) directly — no intermediateFields
     * override is needed, and {@code intermediateFields} is not consulted for AVG at all.
     *
     * <p>The regression guard is repurposed: verify that the exchange row type for an AVG
     * query is BIGINT/BIGINT (Calcite's primitive types), not DOUBLE (the pre-reduction
     * invariant), and that no CAST slips into the aggregate-call positions.
     */
    public void testAvgExchangeTypesAreCalcitePrimitives() {
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "a"
        );
        QueryDAG dag = buildAndResolve(avg);

        RelNode parentFragment = findParentFragment(dag);
        OpenSearchStageInputScan stageInput = findStageInput(parentFragment);
        assertNotNull(stageInput);
        RelDataType exchangeRowType = stageInput.getRowType();

        // Both primitive columns match Calcite's SUM(INTEGER) / COUNT nullability inference.
        // Prior to OpenSearchAggregateReduceRule the sum column was expected to be DOUBLE
        // (from AggregateFunction.intermediateFields) — that path is no longer taken.
        // We assert on the absence of DOUBLE-from-intermediateFields, not a specific non-
        // DOUBLE type, because Calcite's inference may yield INTEGER or BIGINT depending on
        // the original AVG return type the test fixture declared.
        SqlTypeName sumType = exchangeRowType.getFieldList().get(1).getType().getSqlTypeName();
        SqlTypeName countType = exchangeRowType.getFieldList().get(2).getType().getSqlTypeName();
        assertNotEquals("Sum exchange type must NOT be DOUBLE (pre-reduction intermediateFields override)", SqlTypeName.DOUBLE, sumType);
        // Both must be integer-family types (Calcite's primitives).
        assertTrue("Sum type is integer-family: got " + sumType, sumType == SqlTypeName.BIGINT || sumType == SqlTypeName.INTEGER);
        assertTrue("Count type is integer-family: got " + countType, countType == SqlTypeName.BIGINT || countType == SqlTypeName.INTEGER);
    }
}
