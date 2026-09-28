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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

public class ToNumberFunctionAdapterTests extends OpenSearchTestCase {

    /** Synthetic tonumber operator used to build input RexCalls */
    private static final SqlFunction TONUMBER = new SqlFunction(
        "tonumber",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final ToNumberFunctionAdapter adapter = new ToNumberFunctionAdapter();

    /** {@code tonumber(x)} rewrites to {@code CAST(x AS DOUBLE)}. */
    public void testSingleArgRewritesToDoubleCast() {
        Cluster cluster = newCluster();
        RexNode input = cluster.stringLiteral("4598.678");
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TONUMBER, input);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("kind=SAFE_CAST", SqlKind.SAFE_CAST, out.getKind());
        assertEquals("result type is DOUBLE", SqlTypeName.DOUBLE, out.getType().getSqlTypeName());
        RexCall castCall = (RexCall) out;
        assertEquals("single operand", 1, castCall.getOperands().size());
        assertSame("operand preserved by identity", input, castCall.getOperands().get(0));
    }

    /**
     * {@code tonumber(x, base)} stays a {@code tonumber}
     */
    public void testTwoArgKeepsTonumberCallAndNormalizesOperands() {
        Cluster cluster = newCluster();
        RexNode input = cluster.stringLiteral("FA34");
        RexNode base = cluster.intLiteral(16);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TONUMBER, input, base);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTonumberCall(out);
        assertEquals("two operands — value + base", 2, outCall.getOperands().size());
        RexNode valueArg = outCall.getOperands().get(0);
        RexNode baseArg = outCall.getOperands().get(1);
        assertEquals("value arg normalized to VARCHAR", SqlTypeName.VARCHAR, valueArg.getType().getSqlTypeName());
        assertEquals("base arg normalized to INTEGER", SqlTypeName.INTEGER, baseArg.getType().getSqlTypeName());
    }

    /** {@code tonumber(VARCHAR, INTEGER)} — already-normalized operands don't get redundant CASTs. */
    public void testTwoArgOnMatchingTypesSkipsRedundantCast() {
        Cluster cluster = newCluster();
        RexNode input = cluster.varcharInputRef(0);
        RexNode base = cluster.intLiteral(2);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TONUMBER, input, base);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTonumberCall(out);
        assertSame("VARCHAR operand kept as-is", input, outCall.getOperands().get(0));
        assertSame("INTEGER literal kept as-is", base, outCall.getOperands().get(1));
    }

    /** Zero-operand {@code tonumber} is degenerate; adapter should pass it through unchanged. */
    public void testZeroArgPassesThrough() {
        Cluster cluster = newCluster();
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TONUMBER);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertSame(call, out);
    }

    /** Arities above 2 aren't declared in the PPL spec — pass through so planning fails loudly. */
    public void testThreeArgPassesThrough() {
        Cluster cluster = newCluster();
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(
            TONUMBER,
            cluster.stringLiteral("10"),
            cluster.intLiteral(10),
            cluster.intLiteral(0)
        );

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertSame(call, out);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static RexCall assertTonumberCall(RexNode out) {
        assertTrue("expected a RexCall, got " + out.getClass(), out instanceof RexCall);
        RexCall outCall = (RexCall) out;
        assertSame(
            "operator is the synthetic `tonumber` that resolves to the Rust UDF",
            ToNumberFunctionAdapter.TONUMBER,
            outCall.getOperator()
        );
        return outCall;
    }

    private static Cluster newCluster() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        return new Cluster(cluster, typeFactory, rexBuilder);
    }

    private static final class Cluster {
        final RelOptCluster cluster;
        final RelDataTypeFactory typeFactory;
        final RexBuilder rexBuilder;

        Cluster(RelOptCluster cluster, RelDataTypeFactory typeFactory, RexBuilder rexBuilder) {
            this.cluster = cluster;
            this.typeFactory = typeFactory;
            this.rexBuilder = rexBuilder;
        }

        RexNode intLiteral(int value) {
            RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), intType);
        }

        RexNode stringLiteral(String value) {
            return rexBuilder.makeLiteral(value);
        }

        RexNode varcharInputRef(int index) {
            RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
            return rexBuilder.makeInputRef(varcharType, index);
        }
    }
}
