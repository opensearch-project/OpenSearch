/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Tests for {@link PushDownPlanner}.
 */
public class PushDownPlannerTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private RelOptTable table;
    private QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;
    private JavaTypeFactoryImpl typeFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);

        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        cluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();
        schemaPlus.add("test_table", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("id", tf.createSqlType(SqlTypeName.INTEGER))
                    .add("name", tf.createSqlType(SqlTypeName.VARCHAR))
                    .add("value", tf.createSqlType(SqlTypeName.DOUBLE))
                    .build();
            }
        });

        Properties props = new Properties();
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(""), typeFactory, config);
        table = catalogReader.getTable(List.of("test_table"));
        assertNotNull("Table should be found in catalog", table);

        planExecutor = (fragment, ctx) -> Collections.emptyList();
    }

    /**
     * Test scan-only query: the boundary node should absorb just the scan.
     */
    public void testScanOnlyQueryProducesBoundaryNodeWithScanFragment() {
        SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
        PushDownPlanner planner = new PushDownPlanner(operatorTable, planExecutor);

        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        RelNode result = planner.plan(scan);

        assertTrue("Result should be an OpenSearchBoundaryTableScan", result instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) result;
        assertEquals("Convention should be BINDABLE", EnumerableConvention.INSTANCE, boundary.getConvention());

        RelNode fragment = boundary.getLogicalFragment();
        assertTrue("Logical fragment should be a LogicalTableScan", fragment instanceof LogicalTableScan);
    }

    /**
     * Test scan+filter query: the boundary node should absorb both scan and filter.
     */
    public void testScanFilterQueryProducesBoundaryNodeWithFilterFragment() {
        SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
        PushDownPlanner planner = new PushDownPlanner(operatorTable, planExecutor);

        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        // Build: value > 10 (supported condition)
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, valueRef, literal10);
        LogicalFilter filter = LogicalFilter.create(scan, condition);

        RelNode result = planner.plan(filter);

        assertTrue("Result should be an OpenSearchBoundaryTableScan", result instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) result;
        assertEquals("Convention should be BINDABLE", EnumerableConvention.INSTANCE, boundary.getConvention());

        RelNode fragment = boundary.getLogicalFragment();
        assertTrue("Logical fragment should be a LogicalFilter (scan+filter absorbed)", fragment instanceof LogicalFilter);
        LogicalFilter absorbedFilter = (LogicalFilter) fragment;
        assertTrue("Absorbed filter's input should be a LogicalTableScan", absorbedFilter.getInput() instanceof LogicalTableScan);
    }

    /**
     * Test mixed query: scan+filter are absorbed, unsupported project stays above.
     *
     * Uses a restricted operator table that does NOT include PLUS, so the project
     * containing value + 1 cannot be absorbed and remains above the boundary.
     */
    public void testMixedQueryKeepsUnsupportedProjectAboveBoundary() {
        // Restricted operator table: supports comparison but NOT PLUS
        List<SqlOperator> ops = List.of(
            SqlStdOperatorTable.EQUALS,
            SqlStdOperatorTable.GREATER_THAN,
            SqlStdOperatorTable.LESS_THAN,
            SqlStdOperatorTable.AND,
            SqlStdOperatorTable.OR
        );
        SqlOperatorTable operatorTable = new ListSqlOperatorTable(ops);
        PushDownPlanner planner = new PushDownPlanner(operatorTable, planExecutor);

        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        // Build filter: value > 10 (supported)
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, valueRef, literal10);
        LogicalFilter filter = LogicalFilter.create(scan, condition);

        // Build project: value + 1 (PLUS is unsupported in restricted table)
        RexNode filterValueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal1 = rexBuilder.makeLiteral(1.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode plusExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, filterValueRef, literal1);
        LogicalProject project = LogicalProject.create(filter, List.of(), List.of(plusExpr), List.of("result"));

        RelNode result = planner.plan(project);

        // The top-level node should NOT be a boundary node — the project stays above
        assertFalse("Top-level result should NOT be an OpenSearchBoundaryTableScan", result instanceof OpenSearchBoundaryTableScan);

        // Find the boundary node in the tree (should be the input of the project)
        RelNode child = result.getInput(0);
        assertTrue("Child of the project should be an OpenSearchBoundaryTableScan", child instanceof OpenSearchBoundaryTableScan);

        OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) child;
        RelNode fragment = boundary.getLogicalFragment();
        assertTrue("Boundary's logical fragment should be a LogicalFilter (scan+filter absorbed)", fragment instanceof LogicalFilter);
        LogicalFilter absorbedFilter = (LogicalFilter) fragment;
        assertTrue("Absorbed filter's input should be a LogicalTableScan", absorbedFilter.getInput() instanceof LogicalTableScan);
    }

}
