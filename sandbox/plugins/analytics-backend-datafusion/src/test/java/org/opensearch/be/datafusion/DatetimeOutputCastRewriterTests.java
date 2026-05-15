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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link DatetimeOutputCastRewriter}. Builds Calcite RelNode trees that match
 * (and don't match) the engine-output {@code CAST(<TIMESTAMP> AS VARCHAR)} shape that
 * {@code DatetimeOutputCastRule} produces, and asserts the rewriter narrows precisely
 * to direct project slots over standard {@code TIMESTAMP}.
 */
public class DatetimeOutputCastRewriterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Motivating shape: outer Project slot is exactly {@code CAST(<TIMESTAMP> AS VARCHAR)}.
     * Rewriter must replace it with a {@code TO_CHAR(<TIMESTAMP>, '%Y-%m-%d %H:%M:%S')}
     * call whose result type matches the original cast's VARCHAR type.
     */
    public void testDirectTimestampOutputCastIsRewrittenToToChar() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, tsField);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("ts_str"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        LogicalProject rewrittenProj = (LogicalProject) rewritten;
        RexNode rewrittenSlot = rewrittenProj.getProjects().get(0);

        assertTrue("Expected the slot to be rewritten into a function call", rewrittenSlot instanceof RexCall);
        RexCall call = (RexCall) rewrittenSlot;
        assertEquals("Slot operator must be Calcite's TO_CHAR", SqlLibraryOperators.TO_CHAR, call.getOperator());
        assertEquals("TO_CHAR result type must match the original CAST's VARCHAR type", varcharType, call.getType());
        assertEquals(
            "First operand must remain the original TIMESTAMP source ref",
            tsField.toString(),
            call.getOperands().get(0).toString()
        );

        RexNode formatOperand = call.getOperands().get(1);
        assertTrue("Second operand must be a literal format string", formatOperand instanceof RexLiteral);
        assertEquals(
            "Format must be PPL's space-separator timestamp pattern",
            DatetimeOutputCastRewriter.PPL_TIMESTAMP_FORMAT,
            ((RexLiteral) formatOperand).getValueAs(String.class)
        );
    }

    /**
     * When the root rel is not a {@link org.apache.calcite.rel.logical.LogicalProject}
     * (e.g. a {@code LogicalFilter} fragment), the tree must round-trip verbatim.
     * The rewriter only inspects the root project introduced by
     * {@code DatetimeOutputCastRule}; non-project roots are out of scope.
     */
    public void testCastInsideFilterPredicateIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode castInPredicate = rexBuilder.makeCast(varcharType, tsField);
        RexNode literal = rexBuilder.makeLiteral("2024-01-15 12:00:00");
        RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, castInPredicate, literal);
        RelNode filter = LogicalFilter.create(values, predicate);

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(filter);

        assertSame("Filter tree must round-trip identical when no Project slot matches", filter, rewritten);
    }

    /**
     * A {@code CAST(<TIMESTAMP> AS VARCHAR)} buried inside a CASE branch is also
     * user-authored (e.g. {@code SELECT CASE WHEN ... THEN CAST(ts AS VARCHAR) END});
     * the rewriter must leave nested casts alone.
     */
    public void testNestedCastInsideCaseIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode condition = rexBuilder.makeLiteral(true, boolType);
        RexNode innerCast = rexBuilder.makeCast(varcharType, tsField);
        RexNode elseLit = rexBuilder.makeNullLiteral(varcharType);
        RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, innerCast, elseLit);
        RelNode project = LogicalProject.create(values, List.of(), List.of(caseExpr), List.of("guarded_ts"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        LogicalProject rewrittenProj = (LogicalProject) rewritten;
        RexNode rewrittenSlot = rewrittenProj.getProjects().get(0);

        assertTrue("Outer slot must remain a CASE call", rewrittenSlot instanceof RexCall);
        assertEquals("Outer CASE operator must be unchanged", SqlStdOperatorTable.CASE, ((RexCall) rewrittenSlot).getOperator());
        // The inner CAST must round-trip verbatim — no TO_CHAR substitution inside the CASE branch.
        RexNode innerThen = ((RexCall) rewrittenSlot).getOperands().get(1);
        assertEquals(innerCast.toString(), innerThen.toString());
    }

    /**
     * DATE and TIME sources are not rewritten — Arrow's CAST kernel already produces
     * PPL's expected format for these (no calendar/no clock), and the issue scope is
     * TIMESTAMP-only. A standard non-datetime cast is also untouched as a sanity check.
     */
    public void testNonTimestampCastsAreUntouched() {
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        RelDataType timeType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
        RelDataType intType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelDataType rowType = typeFactory.builder().add("d", dateType).add("t", timeType).add("n", intType).build();
        RelNode values = LogicalValues.createEmpty(cluster, rowType);

        RexNode dateCast = rexBuilder.makeCast(varcharType, rexBuilder.makeInputRef(values, 0));
        RexNode timeCast = rexBuilder.makeCast(varcharType, rexBuilder.makeInputRef(values, 1));
        RexNode intCast = rexBuilder.makeCast(varcharType, rexBuilder.makeInputRef(values, 2));
        RelNode project = LogicalProject.create(
            values,
            List.of(),
            List.of(dateCast, timeCast, intCast),
            List.of("d_str", "t_str", "n_str"),
            Set.of()
        );

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        // No matching slots → tree must be returned identical.
        assertSame("Non-TIMESTAMP source casts must round-trip identical", project, rewritten);
    }

    /**
     * A direct slot that is NOT a CAST (e.g. a plain field reference) must round-trip
     * identical — the rewriter only matches the precise CAST shape.
     */
    public void testNonCastSlotIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RelNode project = LogicalProject.create(values, List.of(), List.of(tsField), List.of("ts"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        assertSame(project, rewritten);
    }

    /**
     * A direct {@code CAST(<TIMESTAMP> AS VARCHAR)} living inside an INNER
     * {@link LogicalProject} (i.e. not the root) must round-trip verbatim.
     * {@code DatetimeOutputCastRule} only adds a single root-level project; any
     * inner project carries user/optimizer-authored expressions whose CAST
     * shape happens to match the engine-output rule but is semantically
     * different and must be left alone.
     */
    public void testInnerProjectDirectTimestampCastIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode innerCast = rexBuilder.makeCast(varcharType, tsField);
        RelNode innerProject = LogicalProject.create(values, List.of(), List.of(innerCast), List.of("s"), Set.of());

        RexNode outerRef = rexBuilder.makeInputRef(innerProject, 0);
        RelNode outerProject = LogicalProject.create(innerProject, List.of(), List.of(outerRef), List.of("s"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(outerProject);
        LogicalProject rewrittenOuter = (LogicalProject) rewritten;
        LogicalProject rewrittenInner = (LogicalProject) rewrittenOuter.getInput();

        assertEquals(
            "Inner project's direct CAST slot must round-trip verbatim",
            innerCast.toString(),
            rewrittenInner.getProjects().get(0).toString()
        );
    }

    /**
     * {@code CAST(<TIMESTAMP> AS CHAR(n))} is user-authored — CHAR has fixed
     * length and padding semantics that {@code to_char} does not preserve.
     * {@code DatetimeOutputCastRule} only ever emits {@code AS VARCHAR}, so
     * CHAR targets must round-trip verbatim.
     */
    public void testTimestampCastToCharIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType charType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.CHAR, 32), true);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode charCast = rexBuilder.makeCast(charType, tsField);
        RelNode project = LogicalProject.create(values, List.of(), List.of(charCast), List.of("ts_char"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        assertSame("CAST(... AS CHAR(n)) must round-trip verbatim — out of rule scope", project, rewritten);
    }

    /**
     * The format string is {@code "%Y-%m-%d %H:%M:%S"} — seconds-only — to match
     * Calcite's reference output for {@code CAST(TIMESTAMP AS VARCHAR)}, which
     * truncates fractional seconds. This pins that contract: a TIMESTAMP(9)
     * source still resolves to the seconds-only format literal in the
     * rewritten {@code TO_CHAR} call.
     */
    public void testTimestampPrecisionDoesNotChangeFormat() {
        RelDataType nanoTimestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWithTimestampField(nanoTimestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, tsField);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("ts_str"), Set.of());

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(project);
        RexCall call = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);
        RexLiteral formatLit = (RexLiteral) call.getOperands().get(1);
        assertEquals(
            "TIMESTAMP(9) source must still resolve to the seconds-only format — fractional seconds are dropped",
            DatetimeOutputCastRewriter.PPL_TIMESTAMP_FORMAT,
            formatLit.getValueAs(String.class)
        );
    }

    /**
     * Production shape from the unified planner: the output Project sits directly under a
     * {@code LogicalSystemLimit} (a {@link LogicalSort} with no collation and a fixed fetch).
     * The rewriter must descend through that single Sort wrapper, rewrite the Project's
     * cast slots, and reattach the Sort on top of the rewritten Project.
     */
    public void testSortOverProjectDirectTimestampOutputCastIsRewritten() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode tsField = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, tsField);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("ts_str"), Set.of());
        RexNode fetch = rexBuilder.makeLiteral(10000, intType);
        RelNode sort = LogicalSort.create(project, RelCollations.EMPTY, null, fetch);

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(sort);

        assertTrue("Rewritten root must remain a Sort", rewritten instanceof LogicalSort);
        LogicalSort rewrittenSort = (LogicalSort) rewritten;
        assertSame("Sort fetch must round-trip identical", fetch, rewrittenSort.fetch);
        assertTrue("Sort input must remain a Project", rewrittenSort.getInput() instanceof LogicalProject);
        LogicalProject rewrittenProj = (LogicalProject) rewrittenSort.getInput();
        RexNode rewrittenSlot = rewrittenProj.getProjects().get(0);
        assertTrue("Inner project slot must be rewritten to a TO_CHAR call", rewrittenSlot instanceof RexCall);
        RexCall call = (RexCall) rewrittenSlot;
        assertEquals("Slot operator must be Calcite's TO_CHAR", SqlLibraryOperators.TO_CHAR, call.getOperator());
        assertEquals(
            "First operand must remain the original TIMESTAMP source ref",
            tsField.toString(),
            call.getOperands().get(0).toString()
        );
    }

    /**
     * If the root is a {@link LogicalSort} whose input is NOT a {@link LogicalProject}
     * (e.g. Sort directly over a scan/values), the tree must round-trip identical.
     * The rewriter only descends through the Sort when its input is a Project.
     */
    public void testSortOverNonProjectIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelNode values = singleRowWithTimestampField(timestampType);
        RexNode fetch = rexBuilder.makeLiteral(10000, intType);
        RelNode sort = LogicalSort.create(values, RelCollations.EMPTY, null, fetch);

        RelNode rewritten = DatetimeOutputCastRewriter.rewrite(sort);
        assertSame("Sort over non-Project must round-trip identical", sort, rewritten);
    }

    private RelNode singleRowWithTimestampField(RelDataType timestampType) {
        RelDataType rowType = typeFactory.builder().add("ts", timestampType).build();
        return LogicalValues.createEmpty(cluster, rowType);
    }
}
