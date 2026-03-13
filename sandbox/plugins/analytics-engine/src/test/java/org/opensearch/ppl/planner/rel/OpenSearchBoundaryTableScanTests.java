/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner.rel;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Tests for {@link OpenSearchBoundaryTableScan}.
 */
public class OpenSearchBoundaryTableScanTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private RelOptTable table;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);

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
    }

    // --- Inheritance tests ---

    public void testExtendsTableScanNotLogicalTableScan() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        assertTrue("Should extend TableScan", TableScan.class.isAssignableFrom(OpenSearchBoundaryTableScan.class));
        assertFalse("Should NOT extend LogicalTableScan", LogicalTableScan.class.isAssignableFrom(OpenSearchBoundaryTableScan.class));
    }

    public void testImplementsEnumerableRel() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        assertTrue("Should implement EnumerableRel", boundary instanceof EnumerableRel);
    }

    // --- bind() tests ---

    public void testBindCallsEngineExecutorWithLogicalFragment() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);

        // Track what the executor receives
        final RelNode[] capturedFragment = new RelNode[1];
        final Object[] capturedContext = new Object[1];
        Object[][] rows = { new Object[] { 1, "a", 1.0 } };
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> {
            capturedFragment[0] = fragment;
            capturedContext[0] = ctx;
            return Linq4j.asEnumerable(rows);
        };

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        Enumerable<Object[]> result = boundary.bind(null);

        assertSame("bind() should pass the logical fragment to the executor", scan, capturedFragment[0]);
        assertNull("bind() should pass the DataContext to the executor", capturedContext[0]);
        assertNotNull("bind() should return a non-null Enumerable", result);
    }

    public void testBindPassesFilterFragmentToExecutor() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RexNode condition = rexBuilder.makeLiteral(true);
        LogicalFilter filter = LogicalFilter.create(scan, condition);
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);

        final RelNode[] capturedFragment = new RelNode[1];
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> {
            capturedFragment[0] = fragment;
            return Linq4j.emptyEnumerable();
        };

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, filter, executor);

        boundary.bind(null);

        assertSame("bind() should pass the filter fragment to the executor", filter, capturedFragment[0]);
    }

    // --- copy() tests ---

    public void testCopyPreservesLogicalFragment() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        RelNode copied = boundary.copy(traitSet, List.of());

        assertTrue("copy() should return an OpenSearchBoundaryTableScan", copied instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan copiedBoundary = (OpenSearchBoundaryTableScan) copied;
        assertSame("copy() should preserve the logical fragment", scan, copiedBoundary.getLogicalFragment());
    }

    public void testCopyPreservesTable() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        RelNode copied = boundary.copy(traitSet, List.of());
        OpenSearchBoundaryTableScan copiedBoundary = (OpenSearchBoundaryTableScan) copied;

        assertSame("copy() should preserve the table reference", table, copiedBoundary.getTable());
    }

    // --- getLogicalFragment() tests ---

    public void testGetLogicalFragmentReturnsScanSubtree() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, executor);

        assertSame("getLogicalFragment() should return the absorbed subtree", scan, boundary.getLogicalFragment());
    }

    public void testGetLogicalFragmentReturnsFilterSubtree() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RexNode condition = rexBuilder.makeLiteral(true);
        LogicalFilter filter = LogicalFilter.create(scan, condition);
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        QueryPlanExecutor<RelNode, Enumerable<Object[]>> executor = (fragment, ctx) -> Linq4j.emptyEnumerable();

        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, filter, executor);

        assertSame("getLogicalFragment() should return the filter subtree", filter, boundary.getLogicalFragment());
    }
}
