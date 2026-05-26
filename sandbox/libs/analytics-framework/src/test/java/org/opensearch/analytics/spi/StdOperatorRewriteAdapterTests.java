/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link StdOperatorRewriteAdapter} — verifies that PPL-emitted UDF calls
 * (e.g. a {@code SqlFunction} named "DIVIDE") are rewritten to the matching
 * {@link SqlStdOperatorTable} operator so Isthmus's {@code FunctionMappings.SCALAR_SIGS}
 * can map them to the Substrait default extension catalog.
 */
public class StdOperatorRewriteAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private SqlFunction pplUdf(String name) {
        return new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE,
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    public void testRewritesPplDivideToSqlStdDivide() {
        SqlFunction pplDivide = pplUdf("DIVIDE");
        RexNode a = rexBuilder.makeLiteral(2L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexNode b = rexBuilder.makeLiteral(4L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDivide, List.of(a, b));

        StdOperatorRewriteAdapter adapter = new StdOperatorRewriteAdapter("DIVIDE", SqlStdOperatorTable.DIVIDE);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("Adapter should return a RexCall", adapted instanceof RexCall);
        RexCall rewrite = (RexCall) adapted;
        assertSame("Operator should be SqlStdOperatorTable.DIVIDE", SqlStdOperatorTable.DIVIDE, rewrite.getOperator());
        assertEquals("Operand count preserved", 2, rewrite.getOperands().size());
        assertEquals("First operand preserved", 2L, ((RexLiteral) rewrite.getOperands().get(0)).getValueAs(Long.class).longValue());
        assertEquals("Second operand preserved", 4L, ((RexLiteral) rewrite.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testRewritesPplModToSqlStdMod() {
        SqlFunction pplMod = pplUdf("MOD");
        RexNode a = rexBuilder.makeLiteral(10L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexNode b = rexBuilder.makeLiteral(3L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexCall original = (RexCall) rexBuilder.makeCall(pplMod, List.of(a, b));

        StdOperatorRewriteAdapter adapter = new StdOperatorRewriteAdapter("MOD", SqlStdOperatorTable.MOD);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("Adapter should return a RexCall", adapted instanceof RexCall);
        assertSame("Operator should be SqlStdOperatorTable.MOD", SqlStdOperatorTable.MOD, ((RexCall) adapted).getOperator());
    }

    public void testNoRewriteWhenAlreadyStdOperator() {
        RexNode a = rexBuilder.makeLiteral(2L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexNode b = rexBuilder.makeLiteral(4L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, List.of(a, b));

        StdOperatorRewriteAdapter adapter = new StdOperatorRewriteAdapter("DIVIDE", SqlStdOperatorTable.DIVIDE);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Already-std call should be returned unchanged", original, adapted);
    }

    public void testNoRewriteWhenOperatorNameMismatches() {
        // Adapter registered for DIVIDE; call is for a differently-named UDF.
        SqlFunction other = pplUdf("SOMETHING_ELSE");
        RexNode a = rexBuilder.makeLiteral(2L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexNode b = rexBuilder.makeLiteral(4L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexCall original = (RexCall) rexBuilder.makeCall(other, List.of(a, b));

        SqlOperator target = SqlStdOperatorTable.DIVIDE;
        StdOperatorRewriteAdapter adapter = new StdOperatorRewriteAdapter("DIVIDE", target);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Non-matching names should be returned unchanged", original, adapted);
    }
}
