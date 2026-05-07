/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SortRel;

/**
 * Tests for {@link DataFusionFragmentConvertor}. Each conversion method is
 * exercised independently against a Calcite RelNode constructed in-process,
 * the returned Substrait proto bytes are decoded back into proto structures,
 * and assertions are made on proto shape — not serialized string content.
 *
 */
public class DataFusionFragmentConvertorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        // Load the Substrait extension catalog with the test classloader as TCCL —
        // mirrors the swap performed by DataFusionPlugin#loadSubstraitExtensions.
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DataFusionFragmentConvertorTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection delegationExtensions = SimpleExtension.load(List.of("/delegation_functions.yaml"));
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(delegationExtensions);
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private DataFusionFragmentConvertor newConvertor() {
        return new DataFusionFragmentConvertor(extensions);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /** Builds a nullable row type with integer columns named "A", "B", ... */
    private RelDataType rowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    /** Decodes Substrait proto bytes into a {@link Plan}. */
    private Plan decodeSubstrait(byte[] bytes) throws Exception {
        assertNotNull("convertor bytes must not be null", bytes);
        assertTrue("convertor bytes must not be empty", bytes.length > 0);
        return Plan.parseFrom(bytes);
    }

    /** Extracts the single root {@link Rel} of a Substrait {@link Plan}. */
    private Rel rootRel(Plan plan) {
        assertFalse("plan must contain at least one relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        return planRel.getRoot().getInput();
    }

    /**
     * Builds a Calcite {@code LogicalTableScan} via the convertor's own
     * {@link DataFusionFragmentConvertor.StageInputTableScan} — a minimal TableScan
     * subclass that the isthmus visitor emits as a {@link ReadRel} with a
     * one-element named-table reference.
     */
    private RelNode buildTableScan(String tableName, String... columns) {
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), tableName, rowType(columns));
    }

    private LogicalAggregate buildSumAggregate(RelNode input, int columnIndex) {
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(columnIndex),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "sum_col"
        );
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * A bare table scan converts to a {@code ReadRel} whose named table carries
     * the supplied tableName (no catalog prefix).
     */
    public void testConvertShardScanFragment_TableScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        byte[] bytes = newConvertor().convertShardScanFragment("test_index", scan);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a ReadRel", root.hasRead());
        ReadRel read = root.getRead();
        assertTrue("ReadRel must reference a named table", read.hasNamedTable());
        assertEquals(List.of("test_index"), read.getNamedTable().getNamesList());
    }

    /**
     * A {@code Filter(Scan)} fragment converts to {@code FilterRel(ReadRel)}.
     */
    public void testConvertShardScanFragment_FilterOverScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(scan, predicate);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a FilterRel", root.hasFilter());
        FilterRel filterRel = root.getFilter();
        assertTrue("FilterRel must carry a condition", filterRel.hasCondition());
        Rel inner = filterRel.getInput();
        assertTrue("Filter input must be a ReadRel", inner.hasRead());
        assertEquals(List.of("test_index"), inner.getRead().getNamedTable().getNamesList());
    }

    /**
     * Attaching a partial aggregate on top of inner bytes yields an
     * {@code AggregateRel(readRel)} with phase INITIAL_TO_INTERMEDIATE.
     */
    public void testAttachPartialAggOnTop_WrapsInner() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner bytes from a shard-scan conversion.
        RelNode scan = buildTableScan("test_index", "A");
        byte[] innerBytes = convertor.convertShardScanFragment("test_index", scan);

        // Build a bare partial-agg fragment whose input matches the inner's rowType.
        LogicalAggregate partialAgg = buildSumAggregate(scan, 0);

        byte[] combined = convertor.attachPartialAggOnTop(partialAgg, innerBytes);

        Plan plan = decodeSubstrait(combined);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel agg = root.getAggregate();
        assertFalse("aggregate must have at least one measure", agg.getMeasuresList().isEmpty());
        AggregateFunction fn = agg.getMeasures(0).getMeasure();
        assertEquals(
            "partial-agg phase must be INITIAL_TO_INTERMEDIATE",
            AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
            fn.getPhase()
        );
        // Aggregate is rewired over the inner plan's root ReadRel.
        Rel inner = agg.getInput();
        assertTrue("Aggregate input must be a ReadRel", inner.hasRead());
        assertEquals(List.of("test_index"), inner.getRead().getNamedTable().getNamesList());
    }

    /**
     * A final-agg fragment whose leaf is an {@link OpenSearchStageInputScan}
     * converts to {@code AggregateRel(ReadRel(namedTable=["input-<childStageId>"]))}.
     * The stage-input id is per-child so multi-input shapes (Union) get distinct names
     * for each registered DataFusion partition; single-input shapes still arrive at
     * the conventional {@code "input-0"} when childStageId is 0.
     */
    public void testConvertFinalAggFragment_WithStageInputScanLeaf() throws Exception {
        RelDataType stageRowType = rowType("A");
        int childStageId = 7;
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), childStageId, stageRowType, List.of("datafusion"));
        LogicalAggregate finalAgg = buildSumAggregate(stageInput, 0);

        byte[] bytes = newConvertor().convertFinalAggFragment(finalAgg);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel agg = root.getAggregate();
        assertFalse("aggregate must have at least one measure", agg.getMeasuresList().isEmpty());
        // Isthmus defaults final-mode aggregates to INITIAL_TO_RESULT.
        AggregateFunction fn = agg.getMeasures(0).getMeasure();
        assertEquals("final-agg phase must be INITIAL_TO_RESULT", AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT, fn.getPhase());
        Rel inner = agg.getInput();
        assertTrue("Aggregate input must be a ReadRel", inner.hasRead());
        assertEquals(
            "StageInputScan must be emitted as a ReadRel with the per-child stage-input id",
            List.of("input-" + childStageId),
            inner.getRead().getNamedTable().getNamesList()
        );
    }

    /**
     * Attaching a {@link LogicalSort} on top of inner bytes yields
     * {@code SortRel(<inner>)}.
     */
    public void testAttachFragmentOnTop_Sort() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner: final-agg over stage-input.
        RelDataType stageRowType = rowType("A");
        int childStageId = 3;
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), childStageId, stageRowType, List.of("datafusion"));
        LogicalAggregate finalAgg = buildSumAggregate(stageInput, 0);
        byte[] innerBytes = convertor.convertFinalAggFragment(finalAgg);

        // Contract: attachFragmentOnTop receives a childless operator. Sort requires an
        // input for row-type validation in the isthmus visitor; give it a bare placeholder
        // with the same output row type as the inner agg. The placeholder is discarded
        // during rewire (replaced with the inner plan's root).
        RelNode placeholderInput = buildTableScan("__placeholder__", "sum_col");
        LogicalSort sort = LogicalSort.create(placeholderInput, RelCollations.of(0), null, null);

        byte[] combined = convertor.attachFragmentOnTop(sort, innerBytes);

        Plan plan = decodeSubstrait(combined);
        Rel root = rootRel(plan);
        assertTrue("root must be a SortRel", root.hasSort());
        SortRel sortRel = root.getSort();
        // Sort is rewired over the inner agg.
        Rel inner = sortRel.getInput();
        assertTrue("Sort input must be an AggregateRel", inner.hasAggregate());
        Rel aggInput = inner.getAggregate().getInput();
        assertTrue("Agg input must be a ReadRel", aggInput.hasRead());
        assertEquals(List.of("input-" + childStageId), aggInput.getRead().getNamedTable().getNamesList());
    }

    /**
     * A filter containing {@code delegated_predicate(42)} converts to Substrait
     * with the placeholder preserved as a scalar function call in the FilterRel condition.
     */
    public void testConvertShardScanFragment_DelegatedPredicatePlaceholder() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode placeholder = DelegatedPredicateFunction.makeCall(rexBuilder, 42);
        RelNode filter = LogicalFilter.create(scan, placeholder);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a FilterRel", root.hasFilter());
        FilterRel filterRel = root.getFilter();
        assertTrue("FilterRel must carry a condition", filterRel.hasCondition());
        assertTrue("condition must be a scalar function", filterRel.getCondition().hasScalarFunction());
        logger.info("Substrait condition (single delegated):\n{}", filterRel.getCondition());
        Expression.ScalarFunction scalarFunc = filterRel.getCondition().getScalarFunction();
        assertFalse("scalar function must have arguments", scalarFunc.getArgumentsList().isEmpty());
        // Verify the argument is literal i32 = 42
        assertEquals(42, scalarFunc.getArguments(0).getValue().getLiteral().getI32());
    }

    /**
     * AND(A > 10, delegated_predicate(7)) — mixed native + delegated.
     * Substrait AND has two children: GT scalar function and delegated_predicate scalar function.
     */
    public void testConvertShardScanFragment_MixedNativeAndDelegated() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode nativePred = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode delegated = DelegatedPredicateFunction.makeCall(rexBuilder, 7);
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, delegated);
        RelNode filter = LogicalFilter.create(scan, andCondition);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);
        Plan plan = decodeSubstrait(bytes);
        FilterRel filterRel = rootRel(plan).getFilter();
        // Root condition is AND (scalar function with 2 args)
        assertTrue("condition must be a scalar function", filterRel.getCondition().hasScalarFunction());
        Expression.ScalarFunction andFunc = filterRel.getCondition().getScalarFunction();
        assertEquals("AND must have 2 arguments", 2, andFunc.getArgumentsCount());
        // Second arg should contain delegated_predicate with literal 7
        Expression delegatedArg = andFunc.getArguments(1).getValue();
        assertTrue("second AND arg must be a scalar function", delegatedArg.hasScalarFunction());
        assertEquals(7, delegatedArg.getScalarFunction().getArguments(0).getValue().getLiteral().getI32());
    }

    /**
     * A {@code LogicalJoin} fragment converts to {@code JoinRel} whose plan root
     * carries a {@code names} list that matches the concatenated left + right schemas —
     * DataFusion's Substrait consumer walks the output schema once per leaf field and
     * fails with "Names list must match exactly to nested schema" if the two disagree.
     */
    public void testConvertFinalAggFragment_JoinPreservesFlatNamesList() throws Exception {
        RelNode leftScan = buildTableScan("left_input", "lk", "lv");
        RelNode rightScan = buildTableScan("right_input", "rk", "rv");
        // Equi condition: left.lk = right.rk (left has 2 fields, so right.rk is index 2).
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(leftScan, 0),
            rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true), 2)
        );
        LogicalJoin join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, java.util.Set.of(), JoinRelType.INNER);

        byte[] bytes = newConvertor().convertFinalAggFragment(join);

        Plan plan = decodeSubstrait(bytes);
        RelRoot root = plan.getRelations(0).getRoot();
        assertTrue("root input must be a JoinRel", root.getInput().hasJoin());
        JoinRel joinRel = root.getInput().getJoin();
        assertTrue("JoinRel must carry a left input", joinRel.hasLeft());
        assertTrue("JoinRel must carry a right input", joinRel.hasRight());
        // Names list must match the flat concat of left + right schemas — Substrait consumer
        // walks N leaf fields and expects N names. Any mismatch surfaces as the "Names list
        // must match exactly to nested schema" error at plan decode time.
        assertEquals("names list must match join's flat output schema", List.of("lk", "lv", "rk", "rv"), root.getNamesList());
    }

    /**
     * Attaching an Aggregate fragment on top of a Join-derived inner plan must override
     * the names list to the Aggregate's single output column — not the Join's 4-column
     * names list. DataFusion's consumer walks the final plan's output schema; if we reuse
     * the inner's names, it reports "Names list must match exactly to nested schema, but
     * found N uses for M names" where N is the Aggregate's output column count and M is
     * the Join's.
     */
    public void testAttachFragmentOnTop_AggregateOverJoinShrinksNamesList() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner: Join with 4-column output.
        RelNode leftScan = buildTableScan("left_input", "lk", "lv");
        RelNode rightScan = buildTableScan("right_input", "rk", "rv");
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(leftScan, 0),
            rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true), 2)
        );
        LogicalJoin join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, java.util.Set.of(), JoinRelType.INNER);
        byte[] innerBytes = convertor.convertFinalAggFragment(join);

        // Outer: Aggregate producing a single count column.
        LogicalAggregate aggregate = buildSumAggregate(join, 0);
        byte[] combined = convertor.attachFragmentOnTop(aggregate, innerBytes);

        Plan plan = decodeSubstrait(combined);
        RelRoot root = plan.getRelations(0).getRoot();
        assertTrue("root input must be an AggregateRel after rewire", root.getInput().hasAggregate());
        // Aggregate's output row type has 1 column (`sum_col`), so names must be a single
        // element — if we'd reused inner's 4-name list, this assertion would fail with 4.
        assertEquals("names list must match aggregate's 1-column output schema", List.of("sum_col"), root.getNamesList());
    }

    /**
     * AND(A > 10, OR(delegated_predicate(1), NOT(delegated_predicate(2)))) — complex boolean tree.
     * Verifies nested AND/OR/NOT with delegation placeholders and their annotation IDs survive
     * Substrait conversion.
     */
    public void testConvertShardScanFragment_ComplexBooleanTreeWithDelegation() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode nativePred = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode delegated1 = DelegatedPredicateFunction.makeCall(rexBuilder, 1);
        RexNode delegated2 = DelegatedPredicateFunction.makeCall(rexBuilder, 2);
        RexNode notDelegated2 = rexBuilder.makeCall(SqlStdOperatorTable.NOT, delegated2);
        RexNode orClause = rexBuilder.makeCall(SqlStdOperatorTable.OR, delegated1, notDelegated2);
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, nativePred, orClause);
        RelNode filter = LogicalFilter.create(scan, andCondition);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);
        Plan plan = decodeSubstrait(bytes);
        logger.info("Substrait plan (complex boolean tree):\n{}", plan);
        FilterRel filterRel = rootRel(plan).getFilter();

        // Root: AND with 2 args
        Expression.ScalarFunction andFunc = filterRel.getCondition().getScalarFunction();
        assertEquals("AND must have 2 arguments", 2, andFunc.getArgumentsCount());

        // arg[0]: GT (native predicate) — has field ref and literal 10
        Expression gtArg = andFunc.getArguments(0).getValue();
        assertTrue("first AND arg must be a scalar function (GT)", gtArg.hasScalarFunction());
        assertEquals(10, gtArg.getScalarFunction().getArguments(1).getValue().getLiteral().getI32());

        // arg[1]: OR with 2 args
        Expression orArg = andFunc.getArguments(1).getValue();
        assertTrue("second AND arg must be a scalar function (OR)", orArg.hasScalarFunction());
        Expression.ScalarFunction orFunc = orArg.getScalarFunction();
        assertEquals("OR must have 2 arguments", 2, orFunc.getArgumentsCount());

        // OR arg[0]: delegated_predicate(1)
        Expression dp1 = orFunc.getArguments(0).getValue();
        assertTrue("OR first arg must be scalar function", dp1.hasScalarFunction());
        assertEquals(1, dp1.getScalarFunction().getArguments(0).getValue().getLiteral().getI32());

        // OR arg[1]: NOT(delegated_predicate(2))
        Expression notExpr = orFunc.getArguments(1).getValue();
        assertTrue("OR second arg must be scalar function (NOT)", notExpr.hasScalarFunction());
        Expression dp2 = notExpr.getScalarFunction().getArguments(0).getValue();
        assertTrue("NOT arg must be scalar function", dp2.hasScalarFunction());
        assertEquals(2, dp2.getScalarFunction().getArguments(0).getValue().getLiteral().getI32());
    }

}
