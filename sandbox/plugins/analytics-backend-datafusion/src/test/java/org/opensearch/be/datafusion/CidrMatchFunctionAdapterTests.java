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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Verifies the three-tier dispatch in {@link CidrMatchFunctionAdapter}:
 *
 * <ol>
 *   <li>both args literal &rarr; const-fold to a Boolean {@link RexLiteral}
 *   <li>VARBINARY column + cidr literal &rarr; byte-range AND comparison
 *   <li>else &rarr; return original unchanged (DataFusion will fail loudly)
 * </ol>
 */
public class CidrMatchFunctionAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private CidrMatchFunctionAdapter adapter;

    /** Stand-in for PPL's CIDRMATCH function — only operand shape + return type matter. */
    private static final SqlFunction CIDRMATCH_OP = new SqlFunction(
        "CIDRMATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        adapter = new CidrMatchFunctionAdapter();
    }

    public void testBothLiteralsInRangeFoldsToTrue() {
        RexCall call = makeCall(varcharLiteral("1.2.3.4"), varcharLiteral("1.2.3.0/24"));
        RexNode result = adapter.adapt(call, List.of(), cluster);
        assertTrue(result instanceof RexLiteral);
        assertEquals(SqlTypeName.BOOLEAN, result.getType().getSqlTypeName());
        assertEquals(Boolean.TRUE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testBothLiteralsOutOfRangeFoldsToFalse() {
        RexCall call = makeCall(varcharLiteral("9.9.9.9"), varcharLiteral("1.2.3.0/24"));
        RexNode result = adapter.adapt(call, List.of(), cluster);
        assertTrue(result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIpv6LiteralFoldsCorrectly() {
        // Pure IPv6 literal in pure IPv6 range.
        RexCall call = makeCall(varcharLiteral("2003:0db8::"), varcharLiteral("2003:db8::/32"));
        RexNode result = adapter.adapt(call, List.of(), cluster);
        assertTrue(result instanceof RexLiteral);
        assertEquals(Boolean.TRUE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testVarbinaryColumnWithCidrLiteralRewritesToByteRange() {
        RelDataType varbinary = typeFactory.createSqlType(SqlTypeName.VARBINARY);
        RexNode column = new RexInputRef(0, varbinary);
        RexCall call = makeCall(column, varcharLiteral("1.2.3.0/24"));

        RexNode result = adapter.adapt(call, List.of(), cluster);

        assertTrue("expected RexCall (AND of two comparisons), got " + result.getClass(), result instanceof RexCall);
        RexCall andCall = (RexCall) result;
        assertSame(SqlStdOperatorTable.AND, andCall.getOperator());
        assertEquals(2, andCall.getOperands().size());
        assertSame(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ((RexCall) andCall.getOperands().get(0)).getOperator());
        assertSame(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ((RexCall) andCall.getOperands().get(1)).getOperator());
    }

    public void testDynamicCidrFallsThrough() {
        // Non-literal cidr (a column reference) — should not be folded or rewritten.
        RelDataType varbinary = typeFactory.createSqlType(SqlTypeName.VARBINARY);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode column = new RexInputRef(0, varbinary);
        RexNode dynamicCidr = new RexInputRef(1, varchar);
        RexCall call = makeCall(column, dynamicCidr);

        RexNode result = adapter.adapt(call, List.of(), cluster);
        assertSame("dynamic cidr must fall through unchanged", call, result);
    }

    public void testUnparseableIpFallsThrough() {
        // Garbage input on either side: don't claim to know better than DataFusion.
        RexCall call = makeCall(varcharLiteral("not-an-ip"), varcharLiteral("1.2.3.0/24"));
        RexNode result = adapter.adapt(call, List.of(), cluster);
        assertSame(call, result);
    }

    private RexCall makeCall(RexNode ip, RexNode cidr) {
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        return (RexCall) rexBuilder.makeCall(boolType, CIDRMATCH_OP, List.of(ip, cidr));
    }

    private RexLiteral varcharLiteral(String value) {
        return (RexLiteral) rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    }
}
