/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl.planner;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.fe.ppl.planner.EngineCapabilities;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Tests for {@link EngineCapabilities}.
 */
public class EngineCapabilitiesTests extends OpenSearchTestCase {

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

    // --- supportsOperator tests ---

    public void testSupportsOperatorReturnsTrueForSupportedRelNode() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        assertTrue("LogicalTableScan should be supported", capabilities.supportsOperator(scan));
    }

    public void testSupportsOperatorReturnsTrueForSupportedFilter() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RexNode condition = rexBuilder.makeLiteral(true);
        LogicalFilter filter = LogicalFilter.create(scan, condition);

        assertTrue("LogicalFilter should be supported", capabilities.supportsOperator(filter));
    }

    public void testSupportsOperatorReturnsFalseForUnsupportedRelNode() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        // LogicalProject is not in the default supported operators
        RexNode idRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(idRef), List.of("id"));

        assertFalse("LogicalProject should not be supported", capabilities.supportsOperator(project));
    }

    public void testSupportsOperatorWithCustomCapabilities() {
        EngineCapabilities capabilities = new EngineCapabilities(Set.of(LogicalProject.class), Set.of());
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        assertFalse("LogicalTableScan should not be supported with custom capabilities", capabilities.supportsOperator(scan));
    }

    // --- supportsAllFunctions tests ---

    public void testSupportsAllFunctionsReturnsTrueForSupportedOperators() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // Build: id > 5
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal5 = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode greaterThan = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, idRef, literal5);

        assertTrue("GREATER_THAN should be supported", capabilities.supportsAllFunctions(greaterThan));
    }

    public void testSupportsAllFunctionsReturnsTrueForNestedSupportedOperators() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // Build: id > 5 AND id < 100
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal5 = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode literal100 = rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, idRef, literal5);
        RexNode lt = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, idRef, literal100);
        RexNode andExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, gt, lt);

        assertTrue("AND(GREATER_THAN, LESS_THAN) should all be supported", capabilities.supportsAllFunctions(andExpr));
    }

    public void testSupportsAllFunctionsReturnsFalseWhenAnyUnsupported() {
        // Use a restricted capabilities set so PLUS is not supported
        EngineCapabilities capabilities = new EngineCapabilities(
            Set.of(),
            Set.of(SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.GREATER_THAN)
        );
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // Build: id + 5 (PLUS is not in the restricted set)
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal5 = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, idRef, literal5);

        assertFalse("PLUS should not be supported", capabilities.supportsAllFunctions(plus));
    }

    public void testSupportsAllFunctionsReturnsFalseForNestedUnsupported() {
        // Use a restricted capabilities set so PLUS is not supported
        EngineCapabilities capabilities = new EngineCapabilities(
            Set.of(),
            Set.of(SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.GREATER_THAN, SqlStdOperatorTable.LESS_THAN, SqlStdOperatorTable.AND)
        );
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // Build: id > 5 AND (id + 1) < 100
        // AND and GREATER_THAN are supported, but PLUS is not
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal5 = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode literal1 = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode literal100 = rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, idRef, literal5);
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, idRef, literal1);
        RexNode lt = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, plus, literal100);
        RexNode andExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, gt, lt);

        assertFalse("Expression with nested PLUS should not be fully supported", capabilities.supportsAllFunctions(andExpr));
    }

    public void testSupportsAllFunctionsReturnsTrueForNull() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();

        assertTrue("null expression should be considered supported", capabilities.supportsAllFunctions(null));
    }

    // --- defaultCapabilities tests ---

    public void testDefaultCapabilitiesIncludesExpectedOperators() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RexNode condition = rexBuilder.makeLiteral(true);
        LogicalFilter filter = LogicalFilter.create(scan, condition);

        assertTrue("Default capabilities should support LogicalTableScan", capabilities.supportsOperator(scan));
        assertTrue("Default capabilities should support LogicalFilter", capabilities.supportsOperator(filter));
    }

    public void testDefaultCapabilitiesIncludesExpectedFunctions() {
        EngineCapabilities capabilities = EngineCapabilities.defaultCapabilities();
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexNode idRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true);

        // Test each expected function individually
        Set<SqlOperator> expectedFunctions = Set.of(
            SqlStdOperatorTable.EQUALS,
            SqlStdOperatorTable.GREATER_THAN,
            SqlStdOperatorTable.LESS_THAN,
            SqlStdOperatorTable.AND,
            SqlStdOperatorTable.OR
        );

        for (SqlOperator op : expectedFunctions) {
            RexNode call;
            if (op == SqlStdOperatorTable.AND || op == SqlStdOperatorTable.OR) {
                // AND/OR need boolean operands
                RexNode boolLeft = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal);
                RexNode boolRight = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, idRef, literal);
                call = rexBuilder.makeCall(op, boolLeft, boolRight);
            } else {
                call = rexBuilder.makeCall(op, idRef, literal);
            }
            assertTrue("Default capabilities should support " + op.getName(), capabilities.supportsAllFunctions(call));
        }
    }
}
