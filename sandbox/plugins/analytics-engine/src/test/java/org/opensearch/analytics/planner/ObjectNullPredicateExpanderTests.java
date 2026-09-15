/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.MakeStructFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

/**
 * Mechanics of {@link ObjectNullPredicateExpander}. The integration coverage in
 * {@code ObjectFieldIT} proves the end result, but it cannot distinguish the conjunction from the
 * disjunction (a fixture where every row populates the object passes either way), nor see the
 * bail-outs. These pin the rewrite itself.
 */
public class ObjectNullPredicateExpanderTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelNode input;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), rexBuilder);
        // Two nullable VARCHAR leaves plus one INTEGER, standing in for a scan's leaf columns.
        RelDataType varchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType integer = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType rowType = typeFactory.builder().add("a", varchar).add("b", varchar).add("n", integer).build();
        input = LogicalValues.createEmpty(cluster, rowType);
    }

    /** {@code IS NULL} over an object becomes AND over its leaves — every leaf must be null. */
    public void testIsNullBecomesConjunctionOfLeaves() {
        String shape = expandFilter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, struct()));

        assertTrue("expected an AND over the leaves, got: " + shape, shape.contains("AND"));
        assertFalse("must not be an OR, got: " + shape, shape.contains("OR"));
        assertFalse("the struct call must be gone, got: " + shape, shape.contains("make_struct"));
        assertTrue("both leaves must be tested, got: " + shape, shape.contains("IS NULL($0)") && shape.contains("IS NULL($1)"));
    }

    /** {@code IS NOT NULL} becomes OR — any populated leaf makes the object non-null. */
    public void testIsNotNullBecomesDisjunctionOfLeaves() {
        String shape = expandFilter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, struct()));

        assertTrue("expected an OR over the leaves, got: " + shape, shape.contains("OR"));
        assertFalse("must not be an AND, got: " + shape, shape.contains("AND"));
        assertTrue("both leaves must be tested, got: " + shape, shape.contains("IS NOT NULL($0)") && shape.contains("IS NOT NULL($1)"));
    }

    /**
     * A sub-object contributes its own leaves rather than being tested as a struct — otherwise the
     * inner {@code make_struct} would be the operand of an IS NULL that is never true.
     */
    public void testNestedObjectContributesItsLeaves() {
        RexNode inner = MakeStructFunction.makeCall(
            rexBuilder,
            typeFactory.createStructType(List.of(field(1).getType()), List.of("name")),
            List.of("name"),
            List.of(field(1))
        );
        RexNode outer = MakeStructFunction.makeCall(
            rexBuilder,
            typeFactory.createStructType(List.of(field(0).getType(), inner.getType()), List.of("top", "props")),
            List.of("top", "props"),
            List.of(field(0), inner)
        );

        String shape = expandFilter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, outer));

        assertFalse("no struct call may survive, got: " + shape, shape.contains("make_struct"));
        assertTrue("outer leaf tested, got: " + shape, shape.contains("IS NULL($0)"));
        assertTrue("nested leaf tested, got: " + shape, shape.contains("IS NULL($1)"));
    }

    /** A single-leaf object collapses to that one test — composeConjunction returns it unwrapped. */
    public void testSingleLeafObjectCollapsesToOneTest() {
        RexNode single = MakeStructFunction.makeCall(
            rexBuilder,
            typeFactory.createStructType(List.of(field(0).getType()), List.of("only")),
            List.of("only"),
            List.of(field(0))
        );

        String shape = expandFilter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, single));

        assertTrue("expected the bare test, got: " + shape, shape.contains("IS NULL($0)"));
        assertFalse("no conjunction needed for one leaf, got: " + shape, shape.contains("AND"));
    }

    /** A null test on an ordinary column is untouched, and the pass reports no change. */
    public void testNullTestOnScalarColumnIsLeftAlone() {
        LogicalFilter filter = LogicalFilter.create(input, rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, field(2)));

        assertTrue("a plan with no object must be reported unchanged", ObjectNullPredicateExpander.rewrite(filter).isEmpty());
    }

    /**
     * A shapeless object is materialized as a typed NULL literal, not a {@code make_struct}, so the
     * expander declines — correctly, since such an object is always null and Calcite folds
     * {@code IS NULL(NULL)} itself. Pinned because the bail is easy to mistake for an oversight.
     */
    public void testTypedNullLiteralIsNotExpanded() {
        RelDataType emptyStruct = typeFactory.createTypeWithNullability(typeFactory.createStructType(List.of(), List.of()), true);
        LogicalFilter filter = LogicalFilter.create(
            input,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rexBuilder.makeNullLiteral(emptyStruct))
        );

        assertTrue("a typed NULL literal is not a make_struct to expand", ObjectNullPredicateExpander.rewrite(filter).isEmpty());
    }

    /** The rewrite is not filter-specific — an object null test in a projection expands too. */
    public void testExpandsInsideAProjection() {
        LogicalProject project = (LogicalProject) LogicalProject.create(
            input,
            List.of(),
            List.of(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, struct())),
            List.of("populated"),
            java.util.Set.of()
        );

        LogicalProject rewritten = (LogicalProject) ObjectNullPredicateExpander.rewrite(project).orElseThrow();
        String shape = rewritten.getProjects().toString();

        assertFalse("struct call must be gone, got: " + shape, shape.contains("make_struct"));
        assertTrue("expected the disjunction, got: " + shape, shape.contains("OR"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    /** {@code make_struct('a', $0, 'b', $1)} over the two VARCHAR leaves. */
    private RexNode struct() {
        return MakeStructFunction.makeCall(
            rexBuilder,
            typeFactory.createStructType(List.of(field(0).getType(), field(1).getType()), List.of("a", "b")),
            List.of("a", "b"),
            List.of(field(0), field(1))
        );
    }

    private RexNode field(int index) {
        return rexBuilder.makeInputRef(input, index);
    }

    /** Rewrites a filter on {@code condition} and returns the resulting condition's text. */
    private String expandFilter(RexNode condition) {
        LogicalFilter filter = LogicalFilter.create(input, condition);
        Optional<RelNode> rewritten = ObjectNullPredicateExpander.rewrite(filter);
        assertTrue("expected the pass to report a change", rewritten.isPresent());
        return ((LogicalFilter) rewritten.get()).getCondition().toString();
    }
}
