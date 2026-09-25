/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Direct tests for {@link OpenSearchAggregateConstantShiftRule}: the rule runs alone in a HEP program over
 * hand-built {@code LogicalAggregate(LogicalProject(scan))} trees shaped like the PPL lowering of
 * {@code stats sum(ResolutionWidth), sum(ResolutionWidth+1), ...}.
 */
public class OpenSearchAggregateConstantShiftRuleTests extends BasePlannerRulesTests {

    private static final int REGION = 0;
    private static final int WIDTH = 1;
    private static final int HEIGHT = 2;

    // ── positive cases ─────────────────────────────────────────────────────────

    public void testShiftedSumsCollapseToOneSumAndOneCount() {
        // stats sum(RW), sum(RW+1), sum(RW+2), ... sum(RW+9) -> SUM(x), COUNT(x) + scalar Project
        LogicalAggregate input = shiftedSumLadder(10, false);
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT);
        // The mock column is NOT NULL, so Calcite folds COUNT(x) into COUNT() (RexBuilder.addAggCall); on a
        // nullable column it stays COUNT(x). Either way the identity uses the non-null count of x.
        assertTrue(
            "COUNT reads the aggregated column or nothing (NOT NULL fold)",
            aggregate.getAggCallList().get(1).getArgList().size() <= 1
        );

        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals(
            "only the shared column survives in the scan projection: " + scanProject.getProjects(),
            1,
            scanProject.getProjects().size()
        );
        assertEquals(
            "the widest variant (the BIGINT cast) is what gets summed",
            "CAST($1):BIGINT",
            digest(scanProject.getProjects().get(0))
        );
        assertNoArithmetic(scanProject);

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("sum(ResolutionWidth+1)=[+($0, $1)]")); // |k| == 1 folds the multiply away
        assertTrue(plan, plan.contains("sum(ResolutionWidth+2)=[+($0, *(2, $1))]"));
        assertTrue(plan, plan.contains("sum(ResolutionWidth+9)=[+($0, *(9, $1))]"));
        assertRowTypePreserved(input, result);
    }

    public void testMinusAndLiteralFirstForms() {
        // sum(RW - 5) -> SUM - 5*COUNT ; sum(3 + RW) -> SUM + 3*COUNT
        TableScan scan = scan();
        LogicalProject project = project(
            scan,
            List.of(ref(scan, WIDTH), minus(castBigint(ref(scan, WIDTH)), 5), plus(3, castBigint(ref(scan, WIDTH))))
        );
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 1, "sum(ResolutionWidth - 5)", 0),
            sum(project, 2, "sum(3 + ResolutionWidth)", 0)
        );
        RelNode result = runRule(input);

        assertAggCalls(aggregateUnder(result), SqlKind.SUM, SqlKind.COUNT);
        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("sum(ResolutionWidth - 5)=[-($0, *(5, $1))]"));
        assertTrue(plan, plan.contains("sum(3 + ResolutionWidth)=[+($0, *(3, $1))]"));
        assertRowTypePreserved(input, result);
    }

    public void testGroupKeysPassThrough() {
        // stats sum(RW), sum(RW+1) by RegionID -> group key first, identity holds per group
        LogicalAggregate input = shiftedSumLadder(2, true);
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertEquals(ImmutableBitSet.of(0), aggregate.getGroupSet());
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals("group key + shared column", List.of("$0", "CAST($1):BIGINT"), digests(scanProject.getProjects()));

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("RegionID=[$0]"));
        assertTrue(plan, plan.contains("sum(ResolutionWidth+1)=[+($1, $2)]"));
        assertRowTypePreserved(input, result);
    }

    public void testUntouchedCallsAreRemappedNotDropped() {
        // stats sum(RW+1), count(), max(RH), sum(RH+2) -> count() is untouched and dedups; max(RH) is shiftable so it joins the RH
        // column (the widest variant, cast back to SMALLINT); two columns -> SUM/COUNT pairs plus one MAX
        TableScan scan = scan();
        LogicalProject project = project(
            scan,
            List.of(ref(scan, WIDTH), plus(castBigint(ref(scan, WIDTH)), 1), ref(scan, HEIGHT), plus(castBigint(ref(scan, HEIGHT)), 2))
        );
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 1, "sum(ResolutionWidth+1)", 0),
            call(SqlStdOperatorTable.COUNT, project, List.of(), "count()", 0),
            call(SqlStdOperatorTable.MAX, project, List.of(2), "max(ResolutionHeight)", 0),
            sum(project, 3, "sum(ResolutionHeight+2)", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        // sum(RW+1) registers SUM(RW), COUNT(); the query's own count() dedups onto that COUNT(); MAX(RH) over the shared
        // BIGINT column; SUM(RH) (its COUNT() dedups too): 4 accumulators for 4 outputs over two columns.
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT, SqlKind.MAX, SqlKind.SUM);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals(
            "one scan column per aggregated column, no x ± k column",
            List.of("CAST($1):BIGINT", "CAST($2):BIGINT"),
            digests(scanProject.getProjects())
        );
        assertNoArithmetic(scanProject);

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("count()=[$1]"));
        assertTrue(plan, plan.contains("max(ResolutionHeight)=[CAST($2):SMALLINT")); // widest variant summed, original type restored
        assertTrue(plan, plan.contains("sum(ResolutionWidth+1)=[+($0, $1)]"));
        assertTrue(plan, plan.contains("sum(ResolutionHeight+2)=[+($3, *(2, $1))]"));
        assertRowTypePreserved(input, result);
    }

    public void testPlainSumWithoutShiftedSiblingStaysAsIs() {
        // stats sum(RW), sum(RH+2): RW has no shifted sibling, so its SUM is re-registered unchanged (same call,
        // no COUNT partner); RH gets SUM+COUNT
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(ref(scan, WIDTH), plus(castBigint(ref(scan, HEIGHT)), 2)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 0, "sum(ResolutionWidth)", 0),
            sum(project, 1, "sum(ResolutionHeight+2)", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.SUM, SqlKind.COUNT);
        assertEquals(List.of("$1", "CAST($2):BIGINT"), digests(((LogicalProject) aggregate.getInput()).getProjects()));
        assertRowTypePreserved(input, result);
    }

    public void testSingleShiftedTermIsStillRewritten() {
        // COUNT is O(1) per batch, so even one sum(RW+7) is cheaper as SUM/COUNT than as a derived column.
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 7)));
        LogicalAggregate input = aggregate(project, ImmutableBitSet.of(), sum(project, 0, "sum(ResolutionWidth+7)", 0));
        RelNode result = runRule(input);

        assertAggCalls(aggregateUnder(result), SqlKind.SUM, SqlKind.COUNT);
        assertTrue(RelOptUtil.toString(result), RelOptUtil.toString(result).contains("sum(ResolutionWidth+7)=[+($0, *(7, $1))]"));
        assertRowTypePreserved(input, result);
    }

    public void testCountMinMaxShiftsHoistOut() {
        // stats count(RW+1), min(RW+2), max(RW - 3), sum(RW+4) -> COUNT(x), MIN(x), MAX(x), SUM(x) over one column
        TableScan scan = scan();
        RexNode column = castBigint(ref(scan, WIDTH));
        LogicalProject project = project(scan, List.of(plus(column, 1), plus(column, 2), minus(column, 3), plus(column, 4)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            call(SqlStdOperatorTable.COUNT, project, List.of(0), "count(ResolutionWidth+1)", 0),
            call(SqlStdOperatorTable.MIN, project, List.of(1), "min(ResolutionWidth+2)", 0),
            call(SqlStdOperatorTable.MAX, project, List.of(2), "max(ResolutionWidth - 3)", 0),
            sum(project, 3, "sum(ResolutionWidth+4)", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        // COUNT(x+1) -> COUNT(x) (folded to COUNT() on the NOT NULL mock column) is shared with sum(x+4)'s COUNT
        assertAggCalls(aggregate, SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX, SqlKind.SUM);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals("all four terms read the single shared column", List.of("CAST($1):BIGINT"), digests(scanProject.getProjects()));
        assertNoArithmetic(scanProject);

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("count(ResolutionWidth+1)=[$0]")); // shift-invariant: no arithmetic at all
        assertTrue(plan, plan.contains("min(ResolutionWidth+2)=[+($1, 2)]")); // MIN/MAX shift once, not per row
        assertTrue(plan, plan.contains("max(ResolutionWidth - 3)=[-($2, 3)]"));
        assertTrue(plan, plan.contains("sum(ResolutionWidth+4)=[+($3, *(4, $0))]")); // SUM reuses the COUNT registered by count()
        assertRowTypePreserved(input, result);
    }

    public void testMinMaxOnlyQueryIsRewritten() {
        // stats min(RW+5), max(RW+5): no SUM at all — MIN/MAX shifts alone justify the rewrite (one aggregated column, no COUNT)
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 5)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            call(SqlStdOperatorTable.MIN, project, List.of(0), "min(ResolutionWidth+5)", 0),
            call(SqlStdOperatorTable.MAX, project, List.of(0), "max(ResolutionWidth+5)", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.MIN, SqlKind.MAX);
        assertNoArithmetic((LogicalProject) aggregate.getInput());
        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("min(ResolutionWidth+5)=[+($0, 5)]"));
        assertTrue(plan, plan.contains("max(ResolutionWidth+5)=[+($1, 5)]"));
        assertRowTypePreserved(input, result);
    }

    public void testAvgReducedFormCollapsesCompletely() {
        // What AggregateReduceFunctionsRule leaves behind for avg(RW+1): SUM(RW+1) / COUNT(RW+1). Both terms now hoist,
        // so the derived column disappears entirely (previously COUNT(RW+1) kept it alive).
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 1)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 0, "$f0", 0),
            call(SqlStdOperatorTable.COUNT, project, List.of(0), "$f1", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT);
        assertEquals(List.of("CAST($1):BIGINT"), digests(((LogicalProject) aggregate.getInput()).getProjects()));
        assertNoArithmetic((LogicalProject) aggregate.getInput());
        assertRowTypePreserved(input, result);
    }

    public void testNullableColumnKeepsCountOfTheColumn() {
        // Production columns are nullable, so the identity must use COUNT(x) (non-null rows), never the COUNT() fold:
        // sum(x + 1) over {1, NULL, 3} is 6, i.e. SUM(x) + COUNT(x) = 4 + 2, not SUM(x) + COUNT() = 4 + 3.
        TableScan scan = stubScan(mockNullableTable("nullable", "RegionID", "ResolutionWidth"));
        LogicalProject project = project(
            scan,
            List.of(ref(scan, 1), plus(castBigint(ref(scan, 1)), 1), minus(castBigint(ref(scan, 1)), 2), plus(castBigint(ref(scan, 1)), 3))
        );
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 0, "sum(ResolutionWidth)", 0),
            sum(project, 1, "sum(ResolutionWidth+1)", 0),
            sum(project, 2, "sum(ResolutionWidth - 2)", 0),
            call(SqlStdOperatorTable.COUNT, project, List.of(3), "count(ResolutionWidth+3)", 0),
            call(SqlStdOperatorTable.MIN, project, List.of(3), "min(ResolutionWidth+3)", 0)
        );
        assertTrue("premise: the shifted argument is nullable", project.getProjects().get(1).getType().isNullable());
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT, SqlKind.MIN);
        AggregateCall count = aggregate.getAggCallList().get(1);
        assertEquals("COUNT must read the (nullable) aggregated column, not fold to COUNT()", List.of(0), count.getArgList());
        assertTrue("the aggregated column stays nullable", aggregate.getInput().getRowType().getFieldList().get(0).getType().isNullable());
        assertEquals(List.of("CAST($1):BIGINT"), digests(((LogicalProject) aggregate.getInput()).getProjects()));

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("sum(ResolutionWidth+1)=[+($0, $1)]"));
        assertTrue(plan, plan.contains("sum(ResolutionWidth - 2)=[-($0, *(2, $1))]"));
        assertTrue(plan, plan.contains("count(ResolutionWidth+3)=[$1]"));
        assertTrue(plan, plan.contains("min(ResolutionWidth+3)=[+($2, 3)]"));
        assertRowTypePreserved(input, result);
    }

    public void testNonParticipatingCallWithArgumentIsRemappedAfterPruning() {
        // stats sum(RW+1), sum(RW+2), min(d), sum(distinct RW): min(d) reads a DOUBLE column and sum(distinct RW) is
        // DISTINCT, so neither takes part. Both sit after the two x ± k columns that get pruned, so their argument
        // ordinals must be remapped — a stale ordinal would silently aggregate the wrong column.
        TableScan scan = stubScan(
            mockTable(
                "mixed",
                new String[] { "RegionID", "ResolutionWidth", "d" },
                new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.SMALLINT, SqlTypeName.DOUBLE }
            )
        );
        RexNode width = castBigint(ref(scan, WIDTH));
        LogicalProject project = project(scan, List.of(plus(width, 1), plus(width, 2), ref(scan, 2), ref(scan, WIDTH)));
        AggregateCall distinctSum = distinctSum(project, 3, "sum(distinct ResolutionWidth)");
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 0, "sum(ResolutionWidth+1)", 0),
            sum(project, 1, "sum(ResolutionWidth+2)", 0),
            call(SqlStdOperatorTable.MIN, project, List.of(2), "min(d)", 0),
            distinctSum
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT, SqlKind.MIN, SqlKind.SUM);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        List<String> columns = digests(scanProject.getProjects());
        assertNoArithmetic(scanProject);
        // every surviving call reads the column it was written against, at its new ordinal
        AggregateCall min = aggregate.getAggCallList().get(2);
        AggregateCall distinct = aggregate.getAggCallList().get(3);
        assertEquals("min(d) must still read the DOUBLE column", "$2", columns.get(min.getArgList().get(0)));
        assertEquals("sum(distinct RW) must still read the raw SMALLINT column", "$1", columns.get(distinct.getArgList().get(0)));
        assertTrue(distinct.isDistinct());
        assertEquals(
            "shifted sums read the shared BIGINT column",
            "CAST($1):BIGINT",
            columns.get(aggregate.getAggCallList().get(0).getArgList().get(0))
        );

        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("min(d)=[$2]"));
        assertTrue(plan, plan.contains("sum(distinct ResolutionWidth)=[$3]"));
        assertRowTypePreserved(input, result);
    }

    // ── through the real aggregate-decompose collection ─────────────────────────

    public void testVarianceSiblingDoesNotBlockTheShift() {
        // stats sum(x + 1), var_samp(x): the reduce rule rewrites VAR_SAMP into SUM(x*x)/SUM(x)/COUNT(x) and stacks a
        // NEW Project (x, $1, x*x) on the aggregate's Project (x, x+1). Seen alone, the shift rule then finds a bare
        // input ref for sum(x + 1) and stays silent; PROJECT_MERGE in the same collection folds the pair so it fires.
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(ref(scan, WIDTH), plus(castBigint(ref(scan, WIDTH)), 1)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 1, "sum(ResolutionWidth + 1)", 0),
            call(SqlStdOperatorTable.VAR_SAMP, project, List.of(0), "var_samp(ResolutionWidth)", 0)
        );
        RelNode result = runDecomposeCollection(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        // one SUM(x) shared by the shift and the variance, one COUNT, one SUM(x*x)
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT, SqlKind.SUM);
        assertNoArithmetic((LogicalProject) aggregate.getInput());
        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("sum(ResolutionWidth + 1)=[+($0, $1)]"));
        assertRowTypePreserved(input, result);
    }

    public void testLadderWithStddevSiblingCollapses() {
        // stats sum(x + 1), sum(x + 2), sum(x + 3), stddev_samp(y): three shifted sums share SUM(x)/COUNT(x); the
        // stddev keeps its own SUM(y), SUM(y*y), COUNT(y). No derived x ± k column survives.
        TableScan scan = scan();
        RexNode x = castBigint(ref(scan, WIDTH));
        LogicalProject project = project(scan, List.of(ref(scan, HEIGHT), plus(x, 1), plus(x, 2), plus(x, 3)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            sum(project, 1, "sum(ResolutionWidth + 1)", 0),
            sum(project, 2, "sum(ResolutionWidth + 2)", 0),
            sum(project, 3, "sum(ResolutionWidth + 3)", 0),
            call(SqlStdOperatorTable.STDDEV_SAMP, project, List.of(0), "stddev_samp(ResolutionHeight)", 0)
        );
        RelNode result = runDecomposeCollection(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        // SUM(x), SUM(y), SUM(y*y) plus ONE COUNT: on the NOT NULL mock columns COUNT(x) and COUNT(y) both fold to COUNT()
        // and dedup into a single accumulator (on nullable columns they stay COUNT(x) / COUNT(y)).
        assertAggCalls(aggregate, SqlKind.SUM, SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals("x (widened), y, y*y — nothing else", 3, scanProject.getProjects().size());
        assertNoArithmetic(scanProject);
        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("sum(ResolutionWidth + 2)=[+($"));
        assertRowTypePreserved(input, result);
    }

    // ── overflow safety for MIN / MAX (positive control: testMinMaxOnlyQueryIsRewritten, SMALLINT column, k = 5) ──

    public void testMinMaxOverBigintColumnAreNotShifted() {
        // min(big + k) over a BIGINT column: a row may wrap, and the order of wrapped values is not the order of the
        // originals, so MIN(big) + k would be wrong (min > max is possible). SUM keeps the identity under wrapping, so
        // sum(big + k) in the same stats still hoists; the MIN / MAX keep their per-row argument.
        TableScan scan = stubScan(
            mockTable("t", new String[] { "id", "big" }, new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.BIGINT })
        );
        LogicalProject project = project(scan, List.of(plus(ref(scan, 1), 5)));
        LogicalAggregate input = aggregate(
            project,
            ImmutableBitSet.of(),
            call(SqlStdOperatorTable.MIN, project, List.of(0), "min(big + 5)", 0),
            call(SqlStdOperatorTable.MAX, project, List.of(0), "max(big + 5)", 0),
            sum(project, 0, "sum(big + 5)", 0)
        );
        RelNode result = runRule(input);

        LogicalAggregate aggregate = aggregateUnder(result);
        assertAggCalls(aggregate, SqlKind.MIN, SqlKind.MAX, SqlKind.SUM, SqlKind.COUNT);
        LogicalProject scanProject = (LogicalProject) aggregate.getInput();
        assertEquals(
            "the big + 5 column survives for MIN / MAX, the raw column is added for SUM / COUNT",
            2,
            scanProject.getProjects().size()
        );
        String plan = RelOptUtil.toString(result);
        assertTrue(plan, plan.contains("min(big + 5)=[$0]"));
        assertTrue(plan, plan.contains("max(big + 5)=[$1]"));
        assertTrue(plan, plan.contains("sum(big + 5)=[+($2, *(5, $3))]"));
        assertRowTypePreserved(input, result);
    }

    public void testMinMaxOverNarrowColumnWithHugeShiftAreNotShifted() {
        // min(x + 9223372036854775000) over an INTEGER column: 2^31 + k exceeds Long.MAX_VALUE, so a row can wrap.
        TableScan scan = scan();
        RexNode huge = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            castBigint(ref(scan, WIDTH)),
            rexBuilder.makeLiteral(new BigDecimal("9223372036854775000"), typeFactory.createSqlType(SqlTypeName.BIGINT), false)
        );
        LogicalProject project = project(scan, List.of(huge));
        assertNotRewritten(
            aggregate(
                project,
                ImmutableBitSet.of(),
                call(SqlStdOperatorTable.MIN, project, List.of(0), "min(x + huge)", 0),
                call(SqlStdOperatorTable.MAX, project, List.of(0), "max(x + huge)", 0)
            )
        );
    }

    // ── known limitation: x ± k computed below another operator ─────────────────
    //
    // The operand is Aggregate over Project and only that Project's expressions are inspected. When a query computes
    // x ± k in an earlier `eval` and something sits between it and the `stats` (a Sort with or without fetch, a
    // Filter that cannot be transposed, a Join, a Union, dedup's window + filter, a subquery-derived join), the
    // Aggregate's Project holds a bare input ref and the rule stays silent. Results are unaffected; only the
    // per-row column survives. The planner deliberately does not lift Projects through those operators
    // (SORT_PROJECT_TRANSPOSE is omitted so projections keep pushing down), so this is pinned rather than fixed.
    // Workaround for users: write the arithmetic in `stats`, or put the `eval` immediately before it.

    public void testShiftBelowSortWithFetchIsNotRewritten() {
        assertNotRewrittenThroughDecompose(aggregateOverBareRefOver(sortFetch(shiftedProject(), 100)));
    }

    public void testShiftBelowSortIsNotRewritten() {
        assertNotRewrittenThroughDecompose(aggregateOverBareRefOver(sortBy(shiftedProject(), 0)));
    }

    public void testShiftBelowFilterIsNotRewritten() {
        // e.g. eval v = x + 1, r = rand() | where r >= 0 | stats sum(v): the filter cannot move below the project
        LogicalProject shifted = shiftedProject();
        RelNode filter = LogicalFilter.create(
            shifted,
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref(shifted, 0), rexBuilder.makeExactLiteral(BigDecimal.ZERO))
        );
        assertNotRewrittenThroughDecompose(aggregateOverBareRefOver(filter));
    }

    public void testShiftBelowJoinIsNotRewritten() {
        LogicalProject shifted = shiftedProject();
        TableScan right = stubScan(mockTable("right", new String[] { "id" }, new SqlTypeName[] { SqlTypeName.INTEGER }));
        RelNode join = LogicalJoin.create(
            shifted,
            right,
            List.of(),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                ref(shifted, 1),
                rexBuilder.makeInputRef(right.getRowType().getFieldList().get(0).getType(), 2)
            ),
            java.util.Set.of(),
            JoinRelType.INNER
        );
        assertNotRewrittenThroughDecompose(aggregateOverBareRefOver(join));
    }

    public void testShiftBelowUnionIsNotRewritten() {
        LogicalProject left = shiftedProject();
        LogicalProject right = shiftedProject();
        RelNode union = LogicalUnion.create(List.of(left, right), true);
        assertNotRewrittenThroughDecompose(aggregateOverBareRefOver(union));
    }

    /** {@code Project(v = CAST(RW):BIGINT + 1, RegionID)} — the eval that computes the shift. */
    private LogicalProject shiftedProject() {
        TableScan scan = scan();
        return project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 1), ref(scan, REGION)));
    }

    /** {@code Aggregate(SUM($0)) over Project(v = $0)} — the stats over a bare reference to the lower column. */
    private LogicalAggregate aggregateOverBareRefOver(RelNode input) {
        LogicalProject bare = LogicalProject.create(input, List.of(), List.of(ref(input, 0)), List.of("v"));
        return aggregate(bare, ImmutableBitSet.of(), sum(bare, 0, "sum(v)", 0));
    }

    private RelNode sortFetch(RelNode input, int fetch) {
        return LogicalSort.create(input, RelCollations.EMPTY, null, rexBuilder.makeExactLiteral(BigDecimal.valueOf(fetch)));
    }

    private RelNode sortBy(RelNode input, int field) {
        return LogicalSort.create(input, RelCollations.of(field), null, null);
    }

    /** Neither the rule alone nor the production collection may change the aggregate's call list or its input. */
    private void assertNotRewrittenThroughDecompose(LogicalAggregate input) {
        for (RelNode result : List.of(runRule(input), runDecomposeCollection(input))) {
            LogicalAggregate aggregate = result instanceof LogicalAggregate a ? a : aggregateUnder(result);
            assertEquals("call list must be untouched: " + RelOptUtil.toString(result), 1, aggregate.getAggCallList().size());
            assertEquals(SqlKind.SUM, aggregate.getAggCallList().get(0).getAggregation().getKind());
            String plan = RelOptUtil.toString(result);
            assertTrue("the per-row x + 1 column must still be computed below: " + plan, plan.contains("+(CAST($1):BIGINT"));
            assertFalse("no COUNT may have been introduced: " + plan, plan.contains("COUNT("));
        }
    }

    public void testNonDeterministicColumnIsNotRewritten() {
        // stats sum(cast(rand() * 1e6 as long) + 1), sum(cast(rand() * 1e6 as long) + 2): the two RAND() calls are
        // textually identical but independent draws. Unifying them into one column would correlate the results
        // (s2 - s1 would always equal the row count), so non-deterministic columns never take part — shifted or not.
        TableScan scan = scan();
        RexNode draw1 = randDraw();
        RexNode draw2 = randDraw();
        LogicalProject project = project(scan, List.of(plus(draw1, 1), plus(draw2, 2), draw1));
        assertNotRewritten(
            aggregate(
                project,
                ImmutableBitSet.of(),
                sum(project, 0, "sum(r + 1)", 0),
                sum(project, 1, "sum(r + 2)", 0),
                sum(project, 2, "sum(r)", 0)
            )
        );
    }

    // ── negative cases ──────────────────────────────────────────────────────────

    public void testCountStarAndDoubleMinNotRewritten() {
        // count() has no argument to shift; min(double + 1) is excluded like sum (integer-only rule)
        TableScan scan = scan();
        RexNode dbl = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.DOUBLE), ref(scan, WIDTH));
        LogicalProject project = project(scan, List.of(ref(scan, WIDTH), plus(dbl, 1)));
        assertNotRewritten(
            aggregate(
                project,
                ImmutableBitSet.of(),
                call(SqlStdOperatorTable.COUNT, project, List.of(), "count()", 0),
                call(SqlStdOperatorTable.MIN, project, List.of(1), "min(double+1)", 0)
            )
        );
    }

    public void testSumOfTwoColumnsNotRewritten() {
        TableScan scan = scan();
        LogicalProject project = project(
            scan,
            List.of(rexBuilder.makeCall(SqlStdOperatorTable.PLUS, castBigint(ref(scan, WIDTH)), castBigint(ref(scan, HEIGHT))))
        );
        LogicalAggregate input = aggregate(project, ImmutableBitSet.of(), sum(project, 0, "sum(RW+RH)", 0));
        assertNotRewritten(input);
    }

    public void testNullLiteralNotRewritten() {
        TableScan scan = scan();
        RexNode nullInt = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
        LogicalProject project = project(
            scan,
            List.of(rexBuilder.makeCall(SqlStdOperatorTable.PLUS, castBigint(ref(scan, WIDTH)), nullInt))
        );
        LogicalAggregate input = aggregate(project, ImmutableBitSet.of(), sum(project, 0, "sum(RW+null)", 0));
        assertNotRewritten(input);
    }

    public void testDoubleColumnNotRewritten() {
        // Floating-point addition is not associative: SUM(d + 1) and SUM(d) + COUNT(d) can differ, so decline.
        RelOptTable table = mockTable(
            "clickbench",
            new String[] { "RegionID", "Price" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.DOUBLE }
        );
        TableScan scan = stubScan(table);
        LogicalProject project = project(
            scan,
            List.of(rexBuilder.makeCall(SqlStdOperatorTable.PLUS, ref(scan, 1), rexBuilder.makeExactLiteral(BigDecimal.ONE)))
        );
        LogicalAggregate input = aggregate(project, ImmutableBitSet.of(), sum(project, 0, "sum(Price+1)", 0));
        assertNotRewritten(input);
    }

    public void testDistinctSumNotRewritten() {
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 1)));
        assertNotRewritten(aggregate(project, ImmutableBitSet.of(), distinctSum(project, 0, "sum(distinct RW+1)")));
    }

    public void testLiteralAggSiblingIsNotRewritten() {
        // stats sum(RW+1) next to two LITERAL_AGG pre-operand calls: LITERAL_AGG(1).equals(LITERAL_AGG(2)) is true in
        // Calcite (rexList is not part of equals), so the shared-accumulator dedup cannot be trusted with them.
        TableScan scan = scan();
        LogicalProject project = project(scan, List.of(plus(castBigint(ref(scan, WIDTH)), 1)));
        AggregateCall one = literalAgg(project, 1, "one");
        AggregateCall two = literalAgg(project, 2, "two");
        assertTrue("precondition: Calcite's AggregateCall.equals ignores rexList", one.equals(two));
        assertNotRewritten(aggregate(project, ImmutableBitSet.of(), sum(project, 0, "sum(RW+1)", 0), one, two));
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** {@code stats sum(RW), sum(RW+1), ... sum(RW+terms-1) [by RegionID]} exactly as the PPL lowering shapes it. */
    private LogicalAggregate shiftedSumLadder(int terms, boolean grouped) {
        TableScan scan = scan();
        List<RexNode> exprs = new ArrayList<>();
        exprs.add(ref(scan, REGION));
        exprs.add(ref(scan, WIDTH));
        for (int k = 1; k < terms; k++) {
            exprs.add(plus(castBigint(ref(scan, WIDTH)), k));
        }
        LogicalProject project = project(scan, exprs);
        ImmutableBitSet groupSet = grouped ? ImmutableBitSet.of(REGION) : ImmutableBitSet.of();
        List<AggregateCall> calls = new ArrayList<>();
        calls.add(sum(project, 1, "sum(ResolutionWidth)", groupSet.cardinality()));
        for (int k = 1; k < terms; k++) {
            calls.add(sum(project, 1 + k, "sum(ResolutionWidth+" + k + ")", groupSet.cardinality()));
        }
        return aggregate(project, groupSet, calls.toArray(AggregateCall[]::new));
    }

    private TableScan scan() {
        return stubScan(
            mockTable(
                "clickbench",
                new String[] { "RegionID", "ResolutionWidth", "ResolutionHeight" },
                new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.SMALLINT, SqlTypeName.SMALLINT }
            )
        );
    }

    private RexNode ref(RelNode input, int field) {
        return rexBuilder.makeInputRef(input, field);
    }

    /** {@code CAST(node):BIGINT} whose type carries the operand's nullability, as the PPL lowering produces it. */
    private RexNode castBigint(RexNode node) {
        RelDataType bigint = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            node.getType().isNullable()
        );
        return rexBuilder.makeCast(bigint, node);
    }

    /** {@code CAST(RAND() * 1000000):BIGINT}: an integer-typed argument that is a fresh draw per call site. */
    private RexNode randDraw() {
        return rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            rexBuilder.makeCall(
                SqlStdOperatorTable.MULTIPLY,
                rexBuilder.makeCall(SqlStdOperatorTable.RAND),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(1000000))
            )
        );
    }

    private RexNode plus(RexNode node, int k) {
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, node, rexBuilder.makeExactLiteral(BigDecimal.valueOf(k)));
    }

    private RexNode plus(int k, RexNode node) {
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeExactLiteral(BigDecimal.valueOf(k)), node);
    }

    private RexNode minus(RexNode node, int k) {
        return rexBuilder.makeCall(SqlStdOperatorTable.MINUS, node, rexBuilder.makeExactLiteral(BigDecimal.valueOf(k)));
    }

    /** Names plain column refs after their source field (as the PPL lowering does); derived expressions get {@code $fN}. */
    private LogicalProject project(RelNode input, List<RexNode> exprs) {
        List<String> names = new ArrayList<>(exprs.size());
        for (RexNode expr : exprs) {
            names.add(expr instanceof RexInputRef ref ? input.getRowType().getFieldNames().get(ref.getIndex()) : null);
        }
        return LogicalProject.create(input, List.of(), exprs, names);
    }

    private AggregateCall sum(RelNode input, int arg, String name, int groupCount) {
        return call(SqlStdOperatorTable.SUM, input, List.of(arg), name, groupCount);
    }

    private AggregateCall call(SqlAggFunction function, RelNode input, List<Integer> args, String name, int groupCount) {
        return call(function, false, input, args, name, groupCount);
    }

    /** {@code SUM(DISTINCT arg)}: never takes part, whatever the argument looks like. */
    /** {@code LITERAL_AGG(value)}: the literal is a pre-operand ({@code rexList}), there are no column arguments. */
    private AggregateCall literalAgg(RelNode input, int value, String name) {
        return AggregateCall.create(
            SqlInternalOperators.LITERAL_AGG,
            false,
            false,
            false,
            List.of(rexBuilder.makeExactLiteral(BigDecimal.valueOf(value))),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            input,
            null,
            name
        );
    }

    private AggregateCall distinctSum(RelNode input, int arg, String name) {
        return call(SqlStdOperatorTable.SUM, true, input, List.of(arg), name, 0);
    }

    private AggregateCall call(SqlAggFunction function, boolean distinct, RelNode input, List<Integer> args, String name, int groupCount) {
        return AggregateCall.create(
            function,
            distinct,
            false,
            false,
            List.of(),
            args,
            -1,
            null,
            RelCollations.EMPTY,
            groupCount,
            input,
            null,
            name
        );
    }

    private LogicalAggregate aggregate(RelNode input, ImmutableBitSet groupSet, AggregateCall... calls) {
        return LogicalAggregate.create(input, List.of(), groupSet, null, List.of(calls));
    }

    private RelNode runRule(RelNode input) {
        HepProgram program = new HepProgramBuilder().addRuleInstance(new OpenSearchAggregateConstantShiftRule()).build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(input);
        return planner.findBestExp();
    }

    /** The production {@code aggregate-decompose} collection: reduce + shift + PROJECT_MERGE in one fixpoint loop. */
    private RelNode runDecomposeCollection(RelNode input) {
        HepProgram program = new HepProgramBuilder().addRuleCollection(
            List.of(new OpenSearchAggregateReduceRule(), new OpenSearchAggregateConstantShiftRule(), CoreRules.PROJECT_MERGE)
        ).build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(input);
        return planner.findBestExp();
    }

    private static LogicalAggregate aggregateUnder(RelNode result) {
        assertTrue("rewritten root must be the scalar Project: " + RelOptUtil.toString(result), result instanceof LogicalProject);
        RelNode aggregate = result.getInput(0);
        assertTrue("Project must sit on the LogicalAggregate: " + RelOptUtil.toString(result), aggregate instanceof LogicalAggregate);
        return (LogicalAggregate) aggregate;
    }

    private static void assertAggCalls(LogicalAggregate aggregate, SqlKind... kinds) {
        List<SqlKind> actual = aggregate.getAggCallList().stream().map(c -> c.getAggregation().getKind()).toList();
        assertEquals(RelOptUtil.toString(aggregate), List.of(kinds), actual);
    }

    private static void assertNoArithmetic(LogicalProject project) {
        for (RexNode expr : project.getProjects()) {
            assertFalse(
                "derived x ± k column must not survive: " + expr,
                expr.getKind() == SqlKind.PLUS || expr.getKind() == SqlKind.MINUS
            );
        }
    }

    private static void assertRowTypePreserved(RelNode original, RelNode rewritten) {
        RelDataType expected = original.getRowType();
        RelDataType actual = rewritten.getRowType();
        assertEquals("output names must survive (they are the PPL response schema)", expected.getFieldNames(), actual.getFieldNames());
        assertTrue("output types must be identical: " + expected + " vs " + actual, RelOptUtil.areRowTypesEqual(expected, actual, true));
    }

    private void assertNotRewritten(LogicalAggregate input) {
        RelNode result = runRule(input);
        assertTrue("rule must not fire: " + RelOptUtil.toString(result), result instanceof LogicalAggregate);
        assertEquals(input.getAggCallList().size(), ((LogicalAggregate) result).getAggCallList().size());
    }

    /** Expression digest without the nullability suffix (the mock columns are NOT NULL, the real ones are not). */
    private static String digest(RexNode node) {
        return node.toString().replace(" NOT NULL", "");
    }

    private static List<String> digests(List<RexNode> nodes) {
        return nodes.stream().map(OpenSearchAggregateConstantShiftRuleTests::digest).toList();
    }
}
