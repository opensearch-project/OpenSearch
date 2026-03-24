/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
     * extractTableName returns the table name from a TableScan node.
     */
    public void testExtractTableNameFromTableScan() {
        RelDataType rowType = buildRowType(3);
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("schema", "my_index"));
        when(table.getRowType()).thenReturn(rowType);

        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);
        assertEquals("my_index", DefaultPlanExecutor.extractTableName(scan));
    }

    /**
     * extractTableName throws when no TableScan is found.
     */
    public void testExtractTableNameThrowsForNonTableScan() {
        RelNode stub = new StubRelNode(cluster, cluster.traitSet(), buildRowType(1));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> DefaultPlanExecutor.extractTableName(stub));
        assertTrue(ex.getMessage().contains("No TableScan found"));
    }

    /**
     * execute() throws NPE when clusterService is null (current TODO state).
     */
    public void testExecuteThrowsWhenClusterServiceNull() {
        DefaultPlanExecutor executor = new DefaultPlanExecutor(List.of(), null, null);

        RelDataType rowType = buildRowType(1);
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("my_index"));
        when(table.getRowType()).thenReturn(rowType);
        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);

        expectThrows(NullPointerException.class, () -> executor.execute(scan, new Object()));
    }

    private RelDataType buildRowType(int fieldCount) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add("field_" + i, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    private static class StubRelNode extends AbstractRelNode {
        StubRelNode(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.rowType = rowType;
        }
    }

    private static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }
}
