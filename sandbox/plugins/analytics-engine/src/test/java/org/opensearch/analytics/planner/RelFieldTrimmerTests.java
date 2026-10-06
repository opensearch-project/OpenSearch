/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Unit tests for {@link PlannerImpl#trimFields} (Calcite {@link org.apache.calcite.sql2rel.RelFieldTrimmer}).
 *
 * <p>The SQL frontend ({@code SqlToRelConverter}) already emits minimal projections, so a SQL-driven
 * plan never gives the trimmer unused columns to remove — trimming is a no-op there. The PPL frontend,
 * by contrast, leaves WIDE intermediate projections (e.g. {@code fields c} over a full scan) which the
 * trimmer must shrink. To exercise that behaviour deterministically we hand-build trees with a
 * deliberately-wide intermediate {@link LogicalProject} (all table columns) under a narrow consumer,
 * run {@code trimFields}, and assert the wide Project was shrunk to only the needed columns.
 *
 * <p>RelFieldTrimmer expresses narrowing as a {@link LogicalProject} above the (full-width) scan —
 * it never mutates the {@link TableScan} rowType (verified against Calcite source). So we assert on
 * the bottom-most Project's columns.
 */
public class RelFieldTrimmerTests extends BasePlannerRulesTests {

    private static final String[] COLS = { "f0", "f1", "f2", "f3", "f4" };

    /** Wide Project (all 5 cols) under a consumer that needs only a subset → trimmer shrinks it. */
    public void testTrims_wideProjectUnderNarrowProject() {
        TableScan scan = stubScan(mockTable("t", COLS));
        // inner wide Project: passes through all 5 columns (what a frontend might leave behind).
        LogicalProject wide = allColumnsProject(scan);
        // outer Project keeps only f2 → only f2 is needed below.
        LogicalProject outer = LogicalProject.create(wide, List.of(), List.of(ref(wide, 2)), List.of("f2"));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        assertBottomProjectCols(trimmed, "f2");
    }

    /** Wide Project under a Filter(f0) + narrow output(f2): trimmer keeps only {f0, f2}. */
    public void testTrims_wideProjectUnderFilter() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RelNode filter = makeFilter(wide, makeEquals(0, SqlTypeName.INTEGER, 5)); // needs f0
        LogicalProject outer = LogicalProject.create(filter, List.of(), List.of(ref(filter, 2)), List.of("f2")); // needs f2

        RelNode trimmed = PlannerImpl.trimFields(outer);
        // f0 (filter) + f2 (output) survive; f1/f3/f4 trimmed away.
        assertBottomProjectColsAnyOrder(trimmed, "f0", "f2");
    }

    /** Wide Project under a Sort(f1) + narrow output(f2): trimmer keeps only {f1, f2}. */
    public void testTrims_wideProjectUnderSort() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RelNode sort = LogicalSort.create(
            wide,
            RelCollations.of(new RelFieldCollation(1)), // sort on f1
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalProject outer = LogicalProject.create(sort, List.of(), List.of(ref(sort, 2)), List.of("f2")); // output f2

        RelNode trimmed = PlannerImpl.trimFields(outer);
        assertBottomProjectColsAnyOrder(trimmed, "f1", "f2");
    }

    /** NEGATIVE: outer needs every column → nothing to trim; the wide Project stays all 5. */
    public void testNoTrim_allColumnsNeeded() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        // outer re-projects all 5 → all needed.
        List<RexNode> allRefs = List.of(ref(wide, 0), ref(wide, 1), ref(wide, 2), ref(wide, 3), ref(wide, 4));
        LogicalProject outer = LogicalProject.create(wide, List.of(), allRefs, List.of(COLS));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        assertBottomProjectColsAnyOrder(trimmed, COLS);
    }

    /** NEGATIVE: trimmer throws on a non-literal OFFSET → trimFields catches and returns the input. */
    public void testFallback_nonLiteralOffset_returnsInputUnchanged() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RexNode nonLiteralOffset = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode sort = LogicalSort.create(
            wide,
            RelCollations.of(new RelFieldCollation(1)),
            nonLiteralOffset,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalProject outer = LogicalProject.create(sort, List.of(), List.of(ref(sort, 2)), List.of("f2"));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        // Fallback: returns the exact same instance (untrimmed), never throws.
        assertSame("trimFields must fall back to the untrimmed input on RelFieldTrimmer failure", outer, trimmed);
    }

    /** Aggregate(SUM f3) GROUP BY f0 over wide Project: trimmer keeps only {f0, f3}. */
    public void testTrims_aggregateOverWideProject() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        AggregateCall sumF3 = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(3),
            -1,
            wide,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
        );
        LogicalAggregate agg = LogicalAggregate.create(wide, List.of(), ImmutableBitSet.of(0), null, List.of(sumF3));

        RelNode trimmed = PlannerImpl.trimFields(agg);
        assertBottomProjectColsAnyOrder(trimmed, "f0", "f3");
    }

    /** Aggregate(COUNT) GROUP BY f0 over Filter(f1) over wide Project: keeps {f0, f1}. */
    public void testTrims_aggregateOverFilterOverWideProject() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RelNode filter = makeFilter(wide, makeEquals(1, SqlTypeName.INTEGER, 7)); // needs f1
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            filter,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
        LogicalAggregate agg = LogicalAggregate.create(filter, List.of(), ImmutableBitSet.of(0), null, List.of(count)); // groups f0

        RelNode trimmed = PlannerImpl.trimFields(agg);
        assertBottomProjectColsAnyOrder(trimmed, "f0", "f1");
    }

    /** Join of two wide arms, output uses left.f0 + right.f2, join on left.f0 = right.f0: each arm
     *  trimmed to only the columns its side contributes. */
    public void testTrims_joinOverWideArms() {
        TableScan left = stubScan(mockTable("l", COLS));
        TableScan right = stubScan(mockTable("r", COLS));
        LogicalProject leftWide = allColumnsProject(left);
        LogicalProject rightWide = allColumnsProject(right);
        // join condition: left.f0 ($0) = right.f0 ($5, offset by left width 5)
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 5)
        );
        RelNode join = LogicalJoin.create(leftWide, rightWide, List.of(), cond, Set.<CorrelationId>of(), JoinRelType.INNER);
        // output: left.f0 ($0), right.f2 ($7)
        LogicalProject outer = LogicalProject.create(join, List.of(), List.of(ref(join, 0), ref(join, 7)), List.of("lf0", "rf2"));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        // Each arm narrows: left to {f0}, right to {f0 (join key), f2 (output)}.
        List<LogicalProject> projects = RelNodeUtils.findAllNodes(trimmed, LogicalProject.class);
        // bottom two projects are the trimmed arm projects.
        Set<String> allArmCols = new TreeSet<>();
        for (LogicalProject p : projects) {
            if (RelNodeUtils.unwrapHep(p.getInput()) instanceof TableScan) {
                allArmCols.addAll(p.getRowType().getFieldNames());
            }
        }
        assertEquals(
            "Join arms must be trimmed to only contributed cols:\n" + RelOptUtil.toString(trimmed),
            new TreeSet<>(List.of("f0", "f2")),
            allArmCols
        );
    }

    /** Union of two wide arms, consumer needs only f2: both arms trimmed to {f2}. */
    public void testTrims_unionOverWideArms() {
        LogicalProject arm0 = allColumnsProject(stubScan(mockTable("t", COLS)));
        LogicalProject arm1 = allColumnsProject(stubScan(mockTable("t", COLS)));
        RelNode union = LogicalUnion.create(List.of(arm0, arm1), /* all */ true);
        LogicalProject outer = LogicalProject.create(union, List.of(), List.of(ref(union, 2)), List.of("f2"));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        // every arm Project (directly above a scan) must be narrowed to {f2}.
        for (LogicalProject p : RelNodeUtils.findAllNodes(trimmed, LogicalProject.class)) {
            if (RelNodeUtils.unwrapHep(p.getInput()) instanceof TableScan) {
                assertEquals(
                    "Union arm must be trimmed to {f2}:\n" + RelOptUtil.toString(trimmed),
                    List.of("f2"),
                    p.getRowType().getFieldNames()
                );
            }
        }
    }

    /** Windowed Project (w = SUM(f3) OVER ()) over a wide Project; consumer needs only w → the wide
     *  Project is trimmed to just the window's input {f3}. */
    public void testTrims_windowOverWideProject() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RexNode over = rexBuilder.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            SqlStdOperatorTable.SUM,
            List.of(ref(wide, 3)), // SUM over f3
            ImmutableList.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
        LogicalProject windowed = LogicalProject.create(wide, List.of(), List.of(ref(wide, 0), over), List.of("f0", "w"));
        // consumer keeps only the window output w ($1).
        LogicalProject outer = LogicalProject.create(windowed, List.of(), List.of(ref(windowed, 1)), List.of("w"));

        RelNode trimmed = PlannerImpl.trimFields(outer);
        // The wide + windowed + outer Projects collapse into a single Project(w) whose window reads
        // the scan's f3 ($3) directly — the intermediate all-columns Project is eliminated.
        LogicalProject bottom = bottomMostProject(trimmed);
        assertNotNull("Expected a Project in:\n" + RelOptUtil.toString(trimmed), bottom);
        assertEquals("Window Project output:\n" + RelOptUtil.toString(trimmed), List.of("w"), bottom.getRowType().getFieldNames());
        assertTrue(
            "Window Project must sit directly on the scan (intermediate wide Project trimmed away):\n" + RelOptUtil.toString(trimmed),
            RelNodeUtils.unwrapHep(bottom.getInput()) instanceof TableScan
        );
        assertTrue(
            "Window must reference f3 ($3) off the scan:\n" + RelOptUtil.toString(trimmed),
            bottom.getProjects().get(0).toString().contains("$3")
        );
    }

    /** DEAD window: Aggregate(COUNT by f0) over a windowed Project whose window col w is never read
     *  → trimmer drops the dead window entirely (the SUM(...) OVER() disappears). Mirrors the
     *  AggregateOverWindowInProject case seen while fixing the WindowPlanShape suite. */
    public void testTrims_deadWindowRemoved() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        RexNode over = rexBuilder.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            SqlStdOperatorTable.SUM,
            List.of(ref(wide, 3)),
            ImmutableList.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
        // windowed Project exposes [f0, w]; w is the window output.
        LogicalProject windowed = LogicalProject.create(wide, List.of(), List.of(ref(wide, 0), over), List.of("f0", "w"));
        // COUNT() GROUP BY f0 — references only f0 ($0), never the window col w ($1).
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            windowed,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
        LogicalAggregate agg = LogicalAggregate.create(windowed, List.of(), ImmutableBitSet.of(0), null, List.of(count));

        RelNode trimmed = PlannerImpl.trimFields(agg);
        // The dead window is gone: no RexOver survives anywhere, and the scan-adjacent Project keeps f0 only.
        String planText = RelOptUtil.toString(trimmed);
        assertFalse("Dead window SUM(...) OVER () must be trimmed away:\n" + planText, planText.contains("OVER ("));
        assertBottomProjectColsAnyOrder(trimmed, "f0");
    }

    /**
     * Alias-only top Project (1:1 refs, renamed) over an Aggregate — the shape PPL {@code transpose}
     * leaves behind as its final RENAME over the PIVOT. RelFieldTrimmer drops such a trivial top
     * Project, which silently renames the query's output columns (e.g. {@code column}→{@code f0}).
     * {@code trimFields} must re-impose the original output names (mirrors Calcite RelRoot.project),
     * else the executor can't find the expected columns in the result batch (Q82 HTTP 500 regression).
     */
    public void testTrims_aliasOnlyTopProject_preservesOutputNames() {
        TableScan scan = stubScan(mockTable("t", COLS));
        LogicalProject wide = allColumnsProject(scan);
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            wide,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
        // Aggregate exposes [f0, c]; the alias-only Project renames them to the query's output names.
        LogicalAggregate agg = LogicalAggregate.create(wide, List.of(), ImmutableBitSet.of(0), null, List.of(count));
        LogicalProject rename = LogicalProject.create(agg, List.of(), List.of(ref(agg, 0), ref(agg, 1)), List.of("column", "total"));

        RelNode trimmed = PlannerImpl.trimFields(rename);
        // The output schema the executor sees must keep the renamed names, not the trimmer's underlying ones.
        assertEquals(
            "trimFields must preserve the original output column names:\n" + RelOptUtil.toString(trimmed),
            List.of("column", "total"),
            trimmed.getRowType().getFieldNames()
        );
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private LogicalProject allColumnsProject(RelNode input) {
        List<RexNode> refs = List.of(ref(input, 0), ref(input, 1), ref(input, 2), ref(input, 3), ref(input, 4));
        return LogicalProject.create(input, List.of(), refs, List.of(COLS));
    }

    private RexNode ref(RelNode input, int index) {
        return rexBuilder.makeInputRef(input, index);
    }

    private void assertBottomProjectCols(RelNode trimmed, String... expectedInOrder) {
        LogicalProject bottom = bottomMostProject(trimmed);
        assertNotNull("Expected a Project in:\n" + RelOptUtil.toString(trimmed), bottom);
        assertEquals(
            "Bottom Project must carry exactly these cols (in order):\n" + RelOptUtil.toString(trimmed),
            List.of(expectedInOrder),
            bottom.getRowType().getFieldNames()
        );
    }

    private void assertBottomProjectColsAnyOrder(RelNode trimmed, String... expected) {
        LogicalProject bottom = bottomMostProject(trimmed);
        assertNotNull("Expected a Project in:\n" + RelOptUtil.toString(trimmed), bottom);
        assertEquals(
            "Bottom Project must carry exactly these cols:\n" + RelOptUtil.toString(trimmed),
            new java.util.TreeSet<>(List.of(expected)),
            new java.util.TreeSet<>(bottom.getRowType().getFieldNames())
        );
    }

    private static LogicalProject bottomMostProject(RelNode root) {
        List<LogicalProject> projects = RelNodeUtils.findAllNodes(root, LogicalProject.class);
        return projects.isEmpty() ? null : projects.get(projects.size() - 1);
    }
}
