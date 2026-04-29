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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
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
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
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
     * converts to {@code AggregateRel(ReadRel(namedTable=[__stage_<id>_input__]))}.
     */
    public void testConvertFinalAggFragment_WithStageInputScanLeaf() throws Exception {
        RelDataType stageRowType = rowType("A");
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 7, stageRowType, List.of("datafusion"));
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
            "StageInputScan must be emitted as a ReadRel with the stage-input id",
            List.of(DatafusionReduceSink.INPUT_ID),
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
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 3, stageRowType, List.of("datafusion"));
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
        assertEquals(List.of(DatafusionReduceSink.INPUT_ID), aggInput.getRead().getNamedTable().getNamesList());
    }

}
