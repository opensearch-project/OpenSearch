/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.plan.operators.BackendTagged;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;
import org.opensearch.analytics.plan.operators.OpenSearchFilter;
import org.opensearch.analytics.plan.operators.OpenSearchProject;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;
import org.opensearch.analytics.plan.rules.OperatorWrapperVisitor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Property-based tests for {@link OperatorWrapperVisitor}.
 *
 * <p>Uses OpenSearch's randomized testing utilities to simulate property-based testing
 * across many random inputs.
 */
public class OperatorWrapperVisitorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Builds a row type with {@code fieldCount} VARCHAR fields. */
    private RelDataType buildRowType(int fieldCount) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add("field_" + i, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    /** Stub RelNode that carries a fixed row type — used as a scan/input placeholder. */
    private static class StubRelNode extends AbstractRelNode {
        StubRelNode(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.rowType = rowType;
        }
    }

    /** Creates a mock {@link RelOptTable} whose {@code getRowType()} returns the given type. */
    private RelOptTable mockTable(RelDataType rowType) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowType()).thenReturn(rowType);
        when(table.getQualifiedName()).thenReturn(List.of("test_table"));
        return table;
    }

    /** Builds a {@link LogicalTableScan} over a mock table with {@code fieldCount} fields. */
    private LogicalTableScan buildScan(int fieldCount) {
        RelDataType rowType = buildRowType(fieldCount);
        RelOptTable table = mockTable(rowType);
        return new LogicalTableScan(cluster, cluster.traitSet(), Collections.emptyList(), table);
    }

    /** Builds a {@link LogicalFilter} with a trivially-true condition over the given input. */
    private LogicalFilter buildFilter(RelNode input) {
        // TRUE literal as the condition
        RexNode condition = rexBuilder.makeLiteral(true);
        return LogicalFilter.create(input, condition);
    }

    /** Builds a {@link LogicalAggregate} with COUNT(*) over the given input. */
    private LogicalAggregate buildAggregate(RelNode input) {
        // Use the simplest non-deprecated overload: (fn, distinct, argList, groupCount, input, type, name)
        AggregateCall countStar = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,                    // distinct
            Collections.emptyList(),  // argList (no args = COUNT(*))
            0,                        // groupCount
            input,
            null,                     // type (derived)
            "cnt"                     // name
        );
        return LogicalAggregate.create(
            input,
            Collections.emptyList(),  // hints
            ImmutableBitSet.of(),     // groupSet (no grouping keys)
            null,                     // groupSets
            List.of(countStar)
        );
    }

    /** Builds a {@link LogicalProject} with identity projections over the given input. */
    private LogicalProject buildProject(RelNode input) {
        RelDataType inputRowType = input.getRowType();
        List<RexNode> projects = new java.util.ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); i++) {
            projects.add(rexBuilder.makeInputRef(input, i));
        }
        return LogicalProject.create(input, Collections.emptyList(), projects, inputRowType);
    }

    // -----------------------------------------------------------------------
    // Property 5: Wrapping sets Backend_Tag to "unresolved"
    // -----------------------------------------------------------------------

    /**
     * Property 5: Wrapping sets Backend_Tag to "unresolved"
     *
     * <p>For any LogicalTableScan, LogicalFilter, LogicalAggregate, or LogicalProject,
     * after OperatorWrapperVisitor processes it, the resulting OpenSearch* operator
     * SHALL have backendTag equal to "unresolved".
     *
     * <p>Validates: Requirements 3.1, 3.2, 3.3, 3.4
     *
     * // Feature: analytics-query-planner, Property 5: Wrapping sets Backend_Tag to "unresolved"
     */
    public void testWrappingSetTagUnresolved() {
        for (int iteration = 0; iteration < 100; iteration++) {
            int fieldCount = randomIntBetween(1, 8);
            OperatorWrapperVisitor visitor = new OperatorWrapperVisitor();

            // LogicalTableScan → OpenSearchTableScan
            LogicalTableScan scan = buildScan(fieldCount);
            RelNode wrappedScan = scan.accept(visitor);
            assertInstanceOf(OpenSearchTableScan.class, wrappedScan,
                "iteration " + iteration + ": scan should be wrapped as OpenSearchTableScan");
            assertEquals("iteration " + iteration + ": scan backendTag must be 'unresolved'",
                "unresolved", ((BackendTagged) wrappedScan).getBackendTag());

            // LogicalFilter → OpenSearchFilter
            LogicalFilter filter = buildFilter(scan);
            RelNode wrappedFilter = filter.accept(visitor);
            assertInstanceOf(OpenSearchFilter.class, wrappedFilter,
                "iteration " + iteration + ": filter should be wrapped as OpenSearchFilter");
            assertEquals("iteration " + iteration + ": filter backendTag must be 'unresolved'",
                "unresolved", ((BackendTagged) wrappedFilter).getBackendTag());

            // LogicalAggregate → OpenSearchAggregate
            LogicalAggregate agg = buildAggregate(scan);
            RelNode wrappedAgg = agg.accept(visitor);
            assertInstanceOf(OpenSearchAggregate.class, wrappedAgg,
                "iteration " + iteration + ": agg should be wrapped as OpenSearchAggregate");
            assertEquals("iteration " + iteration + ": agg backendTag must be 'unresolved'",
                "unresolved", ((BackendTagged) wrappedAgg).getBackendTag());

            // LogicalProject → OpenSearchProject
            LogicalProject project = buildProject(scan);
            RelNode wrappedProject = project.accept(visitor);
            assertInstanceOf(OpenSearchProject.class, wrappedProject,
                "iteration " + iteration + ": project should be wrapped as OpenSearchProject");
            assertEquals("iteration " + iteration + ": project backendTag must be 'unresolved'",
                "unresolved", ((BackendTagged) wrappedProject).getBackendTag());
        }
    }

    // -----------------------------------------------------------------------
    // Property 6: Row type preservation during wrapping
    // -----------------------------------------------------------------------

    /**
     * Property 6: Row type preservation during wrapping
     *
     * <p>The rowType (field names, field types) of the wrapped OpenSearch* operator
     * SHALL be structurally equal to the rowType of the original operator.
     *
     * <p>Validates: Requirements 3.5
     *
     * // Feature: analytics-query-planner, Property 6: Row type preservation during wrapping
     */
    public void testRowTypePreserved() {
        for (int iteration = 0; iteration < 100; iteration++) {
            int fieldCount = randomIntBetween(1, 8);
            OperatorWrapperVisitor visitor = new OperatorWrapperVisitor();

            // LogicalTableScan row type preserved
            LogicalTableScan scan = buildScan(fieldCount);
            RelNode wrappedScan = scan.accept(visitor);
            assertRowTypesEqual("scan (iteration " + iteration + ")",
                scan.getRowType(), wrappedScan.getRowType());

            // LogicalFilter row type preserved (same as input)
            LogicalFilter filter = buildFilter(scan);
            RelNode wrappedFilter = filter.accept(visitor);
            assertRowTypesEqual("filter (iteration " + iteration + ")",
                filter.getRowType(), wrappedFilter.getRowType());

            // LogicalAggregate row type preserved
            LogicalAggregate agg = buildAggregate(scan);
            RelNode wrappedAgg = agg.accept(visitor);
            assertRowTypesEqual("agg (iteration " + iteration + ")",
                agg.getRowType(), wrappedAgg.getRowType());

            // LogicalProject row type preserved
            LogicalProject project = buildProject(scan);
            RelNode wrappedProject = project.accept(visitor);
            assertRowTypesEqual("project (iteration " + iteration + ")",
                project.getRowType(), wrappedProject.getRowType());
        }
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private static void assertInstanceOf(Class<?> expectedType, Object actual, String message) {
        assertTrue(message + ": expected " + expectedType.getSimpleName()
            + " but got " + actual.getClass().getSimpleName(),
            expectedType.isInstance(actual));
    }

    private static void assertRowTypesEqual(String context, RelDataType expected, RelDataType actual) {
        assertEquals(context + ": field count mismatch",
            expected.getFieldCount(), actual.getFieldCount());
        for (int i = 0; i < expected.getFieldCount(); i++) {
            assertEquals(context + ": field[" + i + "] name mismatch",
                expected.getFieldList().get(i).getName(),
                actual.getFieldList().get(i).getName());
            assertEquals(context + ": field[" + i + "] type mismatch",
                expected.getFieldList().get(i).getType().getSqlTypeName(),
                actual.getFieldList().get(i).getType().getSqlTypeName());
        }
    }
}
