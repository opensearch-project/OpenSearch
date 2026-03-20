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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link DefaultPlanExecutor}.
 */
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

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
     * Test that execute() does not throw for a valid fragment.
     */
    public void testExecuteDoesNotThrowForValidFragment() {
        DefaultPlanExecutor service = new DefaultPlanExecutor(List.of(), null, null);

        RelNode fragment = createRelNodeWithFieldCount(3);
        Object context = new Object();

        Object result = service.execute(fragment, context);
        assertNotNull("execute() stub should return non-null", result);
    }

    /**
     * Test that execute() works with a multi-field fragment.
     */
    public void testExecuteWithMultiFieldFragment() {
        DefaultPlanExecutor service = new DefaultPlanExecutor(List.of(), null, null);

        int fieldCount = 5;
        RelNode fragment = createRelNodeWithFieldCount(fieldCount);
        Object context = new Object();

        Object result = service.execute(fragment, context);
        assertNotNull("execute() stub should return non-null", result);
    }

    /**
     * Test that execute() works with a single-field fragment.
     */
    public void testExecuteWithSingleFieldFragment() {
        DefaultPlanExecutor service = new DefaultPlanExecutor(List.of(), null, null);

        RelNode fragment = createRelNodeWithFieldCount(1);
        Object context = new Object();

        Object result = service.execute(fragment, context);
        assertNotNull("execute() stub should return non-null", result);
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
