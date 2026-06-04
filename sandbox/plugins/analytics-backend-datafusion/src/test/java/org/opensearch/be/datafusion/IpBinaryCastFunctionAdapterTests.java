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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Verifies the conditional dispatch in {@link IpBinaryCastFunctionAdapter}:
 *
 * <ul>
 *   <li>{@code CAST(<IpType> AS VARCHAR)} &rarr; {@code IP_TO_STRING_OP(col)} call.
 *   <li>{@code CAST(<BinaryType> AS VARCHAR)} &rarr; {@code BINARY_TO_BASE64_OP(col)} call.
 *   <li>{@code SAFE_CAST(<IpType>)} (PPL emits SAFE_CAST for {@code cast(... AS STRING)})
 *       &rarr; same rewrite as CAST.
 *   <li>Anything else (non-VARCHAR target, non-UDT source) &rarr; {@code original} unchanged.
 * </ul>
 */
public class IpBinaryCastFunctionAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private IpBinaryCastFunctionAdapter adapter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        adapter = new IpBinaryCastFunctionAdapter();
    }

    public void testIpCastIsRewrittenToIpToString() {
        RelDataType ipType = new IpType(true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RexNode column = new RexInputRef(0, ipType);
        RexCall cast = (RexCall) rexBuilder.makeCast(varcharType, column);

        RexNode result = adapter.adapt(cast, List.of(), cluster);

        assertTrue("Expected RexCall after rewrite, got " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertSame("Operator must be IP_TO_STRING_OP", IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, call.getOperator());
        assertEquals("Result type must remain VARCHAR", varcharType, call.getType());
        assertEquals("Operand must be the original source column", column.toString(), call.getOperands().get(0).toString());
    }

    public void testBinaryCastIsRewrittenToBinaryToBase64() {
        RelDataType binType = new BinaryType(true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RexNode column = new RexInputRef(0, binType);
        RexCall cast = (RexCall) rexBuilder.makeCast(varcharType, column);

        RexNode result = adapter.adapt(cast, List.of(), cluster);

        RexCall call = (RexCall) result;
        assertSame(IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, call.getOperator());
        assertEquals(varcharType, call.getType());
        assertEquals(column.toString(), call.getOperands().get(0).toString());
    }

    /**
     * PPL's {@code cast(host AS STRING)} lowers via
     * {@code rexBuilder.makeCast(varchar, expr, true, true)} — the {@code safe=true}
     * flag produces a {@link org.apache.calcite.sql.SqlKind#SAFE_CAST} call. The
     * adapter is registered for both CAST and SAFE_CAST in
     * {@link DataFusionAnalyticsBackendPlugin}; this test exercises the SAFE_CAST
     * path explicitly.
     */
    public void testSafeCastIpToVarcharIsRewritten() {
        RelDataType ipType = new IpType(true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RexNode column = new RexInputRef(0, ipType);
        // matchNullability=true, safe=true → SAFE_CAST RexCall.
        RexCall safeCast = (RexCall) rexBuilder.makeCast(varcharType, column, true, true);

        RexNode result = adapter.adapt(safeCast, List.of(), cluster);

        RexCall call = (RexCall) result;
        assertSame("SAFE_CAST(<IpType>) must rewrite to IP_TO_STRING_OP", IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, call.getOperator());
    }

    /** Casts whose target is not VARCHAR (e.g. IpType → BIGINT) pass through. */
    public void testNonVarcharTargetCastIsUntouched() {
        RelDataType ipType = new IpType(true);
        RelDataType bigintType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);

        RexNode column = new RexInputRef(0, ipType);
        RexCall cast = (RexCall) rexBuilder.makeCast(bigintType, column);

        RexNode result = adapter.adapt(cast, List.of(), cluster);
        assertSame("Non-VARCHAR target must round-trip identical", cast, result);
    }

    /**
     * Plain VARBINARY (no UDT) source casts pass through — the adapter is
     * source-type-discriminating, not target-type-blanket.
     */
    public void testPlainVarbinaryCastIsUntouched() {
        RelDataType varbinaryType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RexNode column = new RexInputRef(0, varbinaryType);
        RexCall cast = (RexCall) rexBuilder.makeCast(varcharType, column);

        RexNode result = adapter.adapt(cast, List.of(), cluster);
        assertSame("Plain VARBINARY (no UDT) cast must round-trip identical", cast, result);
    }

    /** Plain INTEGER source → VARCHAR cast passes through (DataFusion handles it natively). */
    public void testIntegerCastIsUntouched() {
        RelDataType intType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RexNode column = new RexInputRef(0, intType);
        RexCall cast = (RexCall) rexBuilder.makeCast(varcharType, column);

        RexNode result = adapter.adapt(cast, List.of(), cluster);
        assertSame("INTEGER → VARCHAR cast must round-trip identical", cast, result);
    }
}
