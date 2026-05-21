/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.spi.OpenSearchAggregateOperators;

import java.util.List;

/**
 * Unit tests for {@link OpenSearchStatsReduceRule}. Each test builds a
 * {@link LogicalAggregate} containing one or more aggregate calls, applies <i>only</i>
 * the STATS reduction rule via a minimal {@link HepPlanner}, and asserts the resulting
 * tree's structure: a {@link Project} on top of a {@link LogicalAggregate} whose
 * {@code AggregateCall}s are the expected primitive set.
 *
 * <p>Tests are intentionally rule-scoped — they do not exercise {@code PlannerImpl}
 * marking, CBO, or fragment conversion. End-to-end planner integration is covered by
 * {@code StatsAggregatePlanShapeTests}.
 */
public class OpenSearchStatsReduceRuleTests extends BasePlannerRulesTests {

    public void testSingleStatsCall_noGroupBy_decomposesIntoFourPrimitivesPlusProject() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price"));
        AggregateCall statsCall = makeStatsCall(scan, 0, "stats_price");
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        assertProjectOverAggregateWithFourPrimitives(rewritten, 0);
    }

    public void testSingleStatsCall_withGroupBy_preservesGroupSetAndPassesThroughGroupColumn() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "city", "price"));
        AggregateCall statsCall = makeStatsCall(scan, 1, "stats_price");
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        assertTrue("Top must be a Project", rewritten instanceof Project);
        Project project = (Project) rewritten;
        assertEquals(2, project.getProjects().size());
        assertEquals("city", project.getRowType().getFieldList().get(0).getName());
        assertEquals("stats_price", project.getRowType().getFieldList().get(1).getName());

        Aggregate inner = (Aggregate) project.getInput();
        assertEquals(ImmutableBitSet.of(0), inner.getGroupSet());
        // Order matters — SUM-first emission preserves typeMatchesInferred after Volcano split.
        assertEquals(4, inner.getAggCallList().size());
        assertAggCallKinds(inner, SqlKind.SUM, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT);
    }

    public void testStatsMixedWithOtherAggregate_decomposesOnlyStats() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price", "quantity"));
        AggregateCall statsCall = makeStatsCall(scan, 0, "stats_price");
        // Pre-existing AVG over quantity — must survive untouched.
        AggregateCall avgCall = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            scan,
            null,
            "avg_qty"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(statsCall, avgCall));

        RelNode rewritten = applyRule(aggregate);

        Project project = (Project) rewritten;
        assertEquals(2, project.getProjects().size());

        Aggregate inner = (Aggregate) project.getInput();
        // 4 primitives for STATS + 1 surviving AVG = 5.
        assertEquals(5, inner.getAggCallList().size());
        assertAggCallKinds(inner, SqlKind.SUM, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.AVG);
        assertSame(SqlStdOperatorTable.AVG, inner.getAggCallList().get(4).getAggregation());
    }

    public void testMultipleStatsCalls_eachExpandsIntoFourPrimitives() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price", "quantity"));
        AggregateCall stats1 = makeStatsCall(scan, 0, "stats_price");
        AggregateCall stats2 = makeStatsCall(scan, 1, "stats_qty");
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(stats1, stats2));

        RelNode rewritten = applyRule(aggregate);

        Project project = (Project) rewritten;
        assertEquals(2, project.getProjects().size());

        Aggregate inner = (Aggregate) project.getInput();
        // 4 primitives per STATS × 2 STATS = 8.
        assertEquals(8, inner.getAggCallList().size());
        assertAggCallKinds(
            inner,
            SqlKind.SUM,
            SqlKind.MIN,
            SqlKind.MAX,
            SqlKind.COUNT,
            SqlKind.SUM,
            SqlKind.MIN,
            SqlKind.MAX,
            SqlKind.COUNT
        );
        assertTrue(project.getProjects().get(0) instanceof RexCall);
        assertEquals(SqlKind.ROW, ((RexCall) project.getProjects().get(0)).getOperator().getKind());
        assertTrue(project.getProjects().get(1) instanceof RexCall);
        assertEquals(SqlKind.ROW, ((RexCall) project.getProjects().get(1)).getOperator().getKind());
    }

    public void testStatsCall_filterArgIsPropagatedToAllPrimitives() {
        // mockTable's default schema is single-column INTEGER, but FILTER needs a BOOLEAN
        // NOT NULL column — build a 2-col table where col 1 is BOOLEAN.
        StubTableScan scan2 = (StubTableScan) stubScan(
            mockTable("orders2", new String[] { "price", "in_scope" }, new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.BOOLEAN })
        );
        AggregateCall statsCall = AggregateCall.create(
            OpenSearchAggregateOperators.STATS,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            1,
            null,
            RelCollations.EMPTY,
            0,
            scan2,
            null,
            "stats_price"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan2, List.of(), ImmutableBitSet.of(), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        Aggregate inner = (Aggregate) ((Project) rewritten).getInput();
        for (AggregateCall call : inner.getAggCallList()) {
            assertEquals("filterArg must propagate to " + call.getAggregation().getName(), 1, call.filterArg);
        }
    }

    public void testRuleNoOpWhenNoStatsCallPresent() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price"));
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            scan,
            null,
            "sum_price"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        RelNode rewritten = applyRule(aggregate);

        // No STATS → rule does not fire → HEP returns the input unchanged.
        assertTrue("Plan must remain a LogicalAggregate when no STATS present", rewritten instanceof LogicalAggregate);
        assertEquals(1, ((LogicalAggregate) rewritten).getAggCallList().size());
        assertSame(SqlStdOperatorTable.SUM, ((LogicalAggregate) rewritten).getAggCallList().get(0).getAggregation());
    }

    public void testStatsStructFieldNamesMatchSpec() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price"));
        AggregateCall statsCall = makeStatsCall(scan, 0, "stats_price");
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        // Legacy OpenSearch InternalStats field order: count, min, max, avg, sum.
        Project project = (Project) rewritten;
        RelDataType statsType = project.getRowType().getFieldList().get(0).getType();
        assertTrue("STATS column must be a struct/row type", statsType.isStruct());
        assertEquals(5, statsType.getFieldList().size());
        assertEquals(OpenSearchAggregateOperators.STATS_FIELD_COUNT, statsType.getFieldList().get(0).getName());
        assertEquals(OpenSearchAggregateOperators.STATS_FIELD_MIN, statsType.getFieldList().get(1).getName());
        assertEquals(OpenSearchAggregateOperators.STATS_FIELD_MAX, statsType.getFieldList().get(2).getName());
        assertEquals(OpenSearchAggregateOperators.STATS_FIELD_AVG, statsType.getFieldList().get(3).getName());
        assertEquals(OpenSearchAggregateOperators.STATS_FIELD_SUM, statsType.getFieldList().get(4).getName());
    }

    public void testStatsStructFieldTypesForIntegerInput() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price"));
        AggregateCall statsCall = makeStatsCall(scan, 0, "stats_price");
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        Project project = (Project) rewritten;
        RelDataType statsType = project.getRowType().getFieldList().get(0).getType();
        assertEquals(SqlTypeName.BIGINT, statsType.getFieldList().get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, statsType.getFieldList().get(1).getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, statsType.getFieldList().get(2).getType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, statsType.getFieldList().get(3).getType().getSqlTypeName());
        // Calcite's AGG_SUM preserves operand type (INTEGER → INTEGER); distributed SUM
        // widening to BIGINT happens later in the planner, not in return-type inference.
        assertEquals(SqlTypeName.INTEGER, statsType.getFieldList().get(4).getType().getSqlTypeName());
    }

    public void testStatsCall_distinctIsPropagatedWhereMeaningful() {
        StubTableScan scan = (StubTableScan) stubScan(mockTable("orders", "price"));
        AggregateCall statsCall = AggregateCall.create(
            OpenSearchAggregateOperators.STATS,
            true,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            scan,
            null,
            "stats_price"
        );
        LogicalAggregate aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(statsCall));

        RelNode rewritten = applyRule(aggregate);

        Aggregate inner = (Aggregate) ((Project) rewritten).getInput();
        // Calcite normalizes DISTINCT for idempotent aggregates: MIN/MAX silently strip it
        // (since MIN(DISTINCT x) == MIN(x)). SUM and COUNT preserve DISTINCT because it
        // changes their result. The rule's job is to propagate the flag at construction time;
        // Calcite then applies its normalization. Both outcomes are correct.
        for (AggregateCall call : inner.getAggCallList()) {
            SqlKind kind = call.getAggregation().getKind();
            if (kind == SqlKind.SUM || kind == SqlKind.COUNT) {
                assertTrue("DISTINCT must propagate to " + kind, call.isDistinct());
            } else {
                assertFalse("Calcite normalizes DISTINCT off " + kind + " (idempotent)", call.isDistinct());
            }
        }
    }

    private AggregateCall makeStatsCall(RelNode input, int colIdx, String name) {
        return AggregateCall.create(
            OpenSearchAggregateOperators.STATS,
            false,
            false,
            false,
            List.of(),
            List.of(colIdx),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            input,
            null,
            name
        );
    }

    /**
     * Applies only {@link OpenSearchStatsReduceRule} to {@code root} via a minimal HEP
     * planner and returns the post-rule plan. Mirrors what {@code PlannerImpl}'s
     * {@code decomposeAggregates} phase does, scoped to a single rule for unit-level
     * isolation.
     */
    private RelNode applyRule(RelNode root) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addRuleInstance(OpenSearchStatsReduceRule.INSTANCE);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    /**
     * Asserts the rewritten plan is a {@code Project(LogicalAggregate(input))} where the
     * inner aggregate has exactly four primitive calls in the canonical SUM/MIN/MAX/COUNT
     * order and the project's stats column at {@code statsColIdx} is a struct.
     */
    private static void assertProjectOverAggregateWithFourPrimitives(RelNode rewritten, int statsColIdx) {
        assertTrue("Top must be a Project", rewritten instanceof Project);
        Project project = (Project) rewritten;
        assertTrue("Project's input must be a LogicalAggregate", project.getInput() instanceof LogicalAggregate);
        LogicalAggregate inner = (LogicalAggregate) project.getInput();
        assertEquals(4, inner.getAggCallList().size());
        assertAggCallKinds(inner, SqlKind.SUM, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT);
        RelDataType statsType = project.getRowType().getFieldList().get(statsColIdx).getType();
        assertTrue("STATS column must be a struct type", statsType.isStruct());
    }

    private static void assertAggCallKinds(Aggregate aggregate, SqlKind... expectedKinds) {
        assertEquals("aggCall count mismatch", expectedKinds.length, aggregate.getAggCallList().size());
        for (int i = 0; i < expectedKinds.length; i++) {
            assertEquals(
                "aggCall " + i + " kind mismatch",
                expectedKinds[i],
                aggregate.getAggCallList().get(i).getAggregation().getKind()
            );
        }
    }
}
