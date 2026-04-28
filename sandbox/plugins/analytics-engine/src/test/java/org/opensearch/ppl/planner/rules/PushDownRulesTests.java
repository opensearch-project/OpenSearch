/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
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
 * Tests for push-down rules: {@link BoundaryTableScanRule}, {@link AbsorbFilterRule}.
 */
public class PushDownRulesTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private RelOptTable table;
    private QueryPlanExecutor<RelNode, Enumerable<Object[]>> planExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
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

        planExecutor = (fragment, ctx, l) -> l.onResponse(Linq4j.emptyEnumerable());
    }

    // --- BoundaryTableScanRule tests (ConverterRule, uses VolcanoPlanner) ---

    public void testBoundaryTableScanRuleConvertsLogicalTableScan() {
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        LogicalTableScan scan = LogicalTableScan.create(volcanoCluster, table, List.of());

        volcanoPlanner.addRule(BoundaryTableScanRule.create(planExecutor));
        volcanoPlanner.setRoot(volcanoPlanner.changeTraits(scan, scan.getTraitSet().replace(EnumerableConvention.INSTANCE)));

        RelNode result = volcanoPlanner.findBestExp();

        assertTrue("Result should be an OpenSearchBoundaryTableScan", result instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) result;
        assertEquals("Convention should be BINDABLE", EnumerableConvention.INSTANCE, boundary.getConvention());
    }

    public void testBoundaryTableScanRulePreservesLogicalFragmentAsScan() {
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptCluster volcanoCluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        LogicalTableScan scan = LogicalTableScan.create(volcanoCluster, table, List.of());

        volcanoPlanner.addRule(BoundaryTableScanRule.create(planExecutor));
        volcanoPlanner.setRoot(volcanoPlanner.changeTraits(scan, scan.getTraitSet().replace(EnumerableConvention.INSTANCE)));

        RelNode result = volcanoPlanner.findBestExp();

        assertTrue("Result should be an OpenSearchBoundaryTableScan", result instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan boundary = (OpenSearchBoundaryTableScan) result;
        RelNode fragment = boundary.getLogicalFragment();
        assertTrue("Logical fragment should be a LogicalTableScan", fragment instanceof LogicalTableScan);
    }

    // --- AbsorbFilterRule tests (RelOptRule, uses HepPlanner for rule application) ---

    /**
     * Tests that AbsorbFilterRule absorbs a supported filter into the boundary node.
     */
    public void testAbsorbFilterRuleAbsorbsSupportedFilter() {
        SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        // Create a boundary node wrapping the scan
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, planExecutor);

        // Build: value > 10 (supported condition)
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, valueRef, literal10);
        LogicalFilter filter = LogicalFilter.create(boundary, condition);

        // Run AbsorbFilterRule via HepPlanner
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(AbsorbFilterRule.create(operatorTable));
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(filter);
        RelNode result = hepPlanner.findBestExp();

        // The filter should be absorbed: result is a new boundary node with filter in fragment
        assertTrue("Result should be an OpenSearchBoundaryTableScan (filter absorbed)", result instanceof OpenSearchBoundaryTableScan);
        OpenSearchBoundaryTableScan resultBoundary = (OpenSearchBoundaryTableScan) result;

        RelNode fragment = resultBoundary.getLogicalFragment();
        assertTrue("Logical fragment should be a LogicalFilter (absorbed)", fragment instanceof LogicalFilter);
        LogicalFilter absorbedFilter = (LogicalFilter) fragment;
        assertTrue("Absorbed filter's input should be a LogicalTableScan", absorbedFilter.getInput() instanceof LogicalTableScan);
    }

    /**
     * Tests that AbsorbFilterRule does NOT absorb a filter when the condition
     * contains unsupported functions (e.g. PLUS).
     */
    public void testAbsorbFilterRuleDoesNotAbsorbUnsupportedFunctions() {
        // Use restricted operator table where PLUS is not supported
        List<SqlOperator> ops = List.of(SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.GREATER_THAN);
        SqlOperatorTable operatorTable = new ListSqlOperatorTable(ops);

        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        // Create a boundary node wrapping the scan
        RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        OpenSearchBoundaryTableScan boundary = new OpenSearchBoundaryTableScan(cluster, traitSet, table, scan, planExecutor);

        // Build: (value + 1) > 10 — PLUS is not in the restricted operator table
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexNode valueRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
        RexNode literal1 = rexBuilder.makeLiteral(1.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode literal10 = rexBuilder.makeLiteral(10.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, valueRef, literal1);
        RexNode unsupportedCondition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, plus, literal10);
        LogicalFilter filter = LogicalFilter.create(boundary, unsupportedCondition);

        // Run AbsorbFilterRule via HepPlanner
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(AbsorbFilterRule.create(operatorTable));
        HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
        hepPlanner.setRoot(filter);
        RelNode result = hepPlanner.findBestExp();

        // The filter should NOT be absorbed — result should still be a LogicalFilter
        assertTrue("Result should still be a LogicalFilter (not absorbed)", result instanceof LogicalFilter);
        LogicalFilter resultFilter = (LogicalFilter) result;
        assertTrue(
            "Filter's input should still be an OpenSearchBoundaryTableScan",
            resultFilter.getInput() instanceof OpenSearchBoundaryTableScan
        );

        // The boundary node's fragment should still be just the scan
        OpenSearchBoundaryTableScan resultBoundary = (OpenSearchBoundaryTableScan) resultFilter.getInput();
        assertTrue(
            "Boundary fragment should still be LogicalTableScan (filter not absorbed)",
            resultBoundary.getLogicalFragment() instanceof LogicalTableScan
        );
    }
}
