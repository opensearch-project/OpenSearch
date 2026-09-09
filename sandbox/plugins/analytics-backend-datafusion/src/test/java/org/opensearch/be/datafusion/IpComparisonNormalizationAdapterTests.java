/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
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
 * Unit tests for {@link IpComparisonNormalizationAdapter}: PPL IP-comparison UDFs over VARBINARY
 * operands are rewritten to native {@code SqlStdOperatorTable} comparators, native/non-VARBINARY
 * comparisons pass through untouched, and the wrapped {@link ComparisonTemporalCoercionAdapter}
 * is still invoked.
 */
public class IpComparisonNormalizationAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    private final IpComparisonNormalizationAdapter adapter = new IpComparisonNormalizationAdapter(new ComparisonTemporalCoercionAdapter());

    /**
     * Stand-in for an SQL-plugin IP-comparison UDF (e.g. {@code EQUALS_IP}): a UDF-category operator
     * carrying a comparison {@link SqlKind}, distinct from the {@code SqlStdOperatorTable} singleton.
     */
    private static SqlOperator ipCompareUdf(String name, SqlKind kind) {
        return new SqlFunction(
            name,
            kind,
            ReturnTypes.BOOLEAN_FORCE_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    private RexNode field(int index, SqlTypeName name) {
        return rexBuilder.makeInputRef(typeFactory.createSqlType(name), index);
    }

    private RexCall call(SqlOperator op, RexNode left, RexNode right) {
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        return (RexCall) rexBuilder.makeCall(boolType, op, List.of(left, right));
    }

    /** EQUALS_IP over two VARBINARY operands → native SqlStdOperatorTable.EQUALS, operands + type preserved. */
    public void testEqualsIpOverVarbinaryRewritesToNativeEquals() {
        RexNode left = field(0, SqlTypeName.VARBINARY);
        RexNode right = field(1, SqlTypeName.VARBINARY);
        RexCall original = call(ipCompareUdf("EQUALS_IP", SqlKind.EQUALS), left, right);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame(SqlStdOperatorTable.EQUALS, adapted.getOperator());
        assertEquals(List.of(left, right), adapted.getOperands());
        assertEquals("result type must be preserved", original.getType(), adapted.getType());
    }

    /** All six IP-comparison UDFs map to the matching native comparator. */
    public void testAllSixIpComparatorsRewrite() {
        assertRewritesTo("EQUALS_IP", SqlKind.EQUALS, SqlStdOperatorTable.EQUALS);
        assertRewritesTo("NOT_EQUALS_IP", SqlKind.NOT_EQUALS, SqlStdOperatorTable.NOT_EQUALS);
        assertRewritesTo("LESS_IP", SqlKind.LESS_THAN, SqlStdOperatorTable.LESS_THAN);
        assertRewritesTo("LTE_IP", SqlKind.LESS_THAN_OR_EQUAL, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        assertRewritesTo("GREATER_IP", SqlKind.GREATER_THAN, SqlStdOperatorTable.GREATER_THAN);
        assertRewritesTo("GTE_IP", SqlKind.GREATER_THAN_OR_EQUAL, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    }

    private void assertRewritesTo(String udfName, SqlKind kind, SqlOperator expectedNative) {
        RexCall original = call(ipCompareUdf(udfName, kind), field(0, SqlTypeName.VARBINARY), field(1, SqlTypeName.VARBINARY));
        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);
        assertSame(udfName + " must rewrite to its native comparator", expectedNative, adapted.getOperator());
    }

    /** A plain native comparator over VARBINARY (the post-BinaryFunctionAdapter shape) is left as-is. */
    public void testNativeEqualsOverVarbinaryPassesThrough() {
        RexCall original = call(SqlStdOperatorTable.EQUALS, field(0, SqlTypeName.VARBINARY), field(1, SqlTypeName.VARBINARY));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }

    /**
     * The VARBINARY guard: a UDF-form comparison whose operands are not both VARBINARY is not
     * rewritten (the IP-UDF path only ever produces VARBINARY operands on the analytics route).
     */
    public void testUdfComparisonWithNonVarbinaryOperandNotRewritten() {
        RexCall original = call(ipCompareUdf("EQUALS_IP", SqlKind.EQUALS), field(0, SqlTypeName.VARBINARY), field(1, SqlTypeName.VARCHAR));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        // Operator preserved (not swapped to a native comparator).
        assertSame(original.getOperator(), ((RexCall) adapted).getOperator());
    }

    /**
     * Delegation is preserved: the wrapped {@link ComparisonTemporalCoercionAdapter} still coerces a
     * char-vs-timestamp comparison (the shared comparison slots must keep their temporal behavior).
     */
    public void testTemporalCoercionStillDelegated() {
        RexNode varchar = field(0, SqlTypeName.VARCHAR);
        RexNode ts = field(1, SqlTypeName.TIMESTAMP);
        RexCall original = call(SqlStdOperatorTable.EQUALS, varchar, ts);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertNotSame("temporal coercion must still fire through the wrapper", original, adapted);
        assertSame(SqlTypeName.TIMESTAMP, adapted.getOperands().get(0).getType().getSqlTypeName());
        assertSame(ts, adapted.getOperands().get(1));
    }

    /** Numeric native comparison — neither rewritten nor temporally coerced; unchanged. */
    public void testNumericComparisonPassesThrough() {
        RexCall original = call(SqlStdOperatorTable.LESS_THAN, field(0, SqlTypeName.INTEGER), field(1, SqlTypeName.INTEGER));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }
}
