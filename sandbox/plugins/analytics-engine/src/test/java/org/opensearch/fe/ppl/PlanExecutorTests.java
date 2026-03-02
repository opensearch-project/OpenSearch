/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link PlanExecutor}.
 */
public class PlanExecutorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Test that execute() returns an iterable and does not throw for a valid fragment.
     */
    public void testExecuteReturnsIterableForValidFragment() {
        PlanExecutor service = new PlanExecutor();

        RelNode fragment = createRelNodeWithFieldCount(3);
        Object context = new Object();

        Iterable<Object[]> result = service.execute(fragment, context);
        assertNotNull("execute() should return a non-null Iterable", result);
    }

    /**
     * Test that returned rows have correct length matching the fragment's field count.
     */
    public void testReturnedRowsMatchFieldCount() {
        PlanExecutor service = new PlanExecutor();

        int fieldCount = 5;
        RelNode fragment = createRelNodeWithFieldCount(fieldCount);
        Object context = new Object();

        Iterable<Object[]> result = service.execute(fragment, context);
        for (Object[] row : result) {
            assertEquals("Each row should have length equal to fragment field count", fieldCount, row.length);
        }
    }

    /**
     * Test that execute() works with a single-field fragment.
     */
    public void testExecuteWithSingleFieldFragment() {
        PlanExecutor service = new PlanExecutor();

        RelNode fragment = createRelNodeWithFieldCount(1);
        Object context = new Object();

        Iterable<Object[]> result = service.execute(fragment, context);
        assertNotNull(result);
        for (Object[] row : result) {
            assertEquals(1, row.length);
        }
    }

    /**
     * Test that execute() casts the logicalFragment to RelNode.
     * Passing a non-RelNode should throw ClassCastException.
     */
    public void testExecuteThrowsOnInvalidFragmentType() {
        PlanExecutor service = new PlanExecutor();

        expectThrows(ClassCastException.class, () -> { service.execute("not a RelNode", new Object()); });
    }

    private RelNode createRelNodeWithFieldCount(int fieldCount) {
        RelDataType rowType = buildRowType(fieldCount);
        return new StubRelNode(cluster, cluster.traitSet(), rowType);
    }

    private RelDataType buildRowType(int fieldCount) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add("field_" + i, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    /**
     * Minimal concrete RelNode for testing. Extends AbstractRelNode
     * which provides default implementations for all RelNode methods.
     */
    private static class StubRelNode extends AbstractRelNode {
        StubRelNode(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.rowType = rowType;
        }
    }
}
