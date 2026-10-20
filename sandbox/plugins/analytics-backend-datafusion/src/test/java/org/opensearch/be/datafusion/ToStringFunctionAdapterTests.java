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
import org.apache.calcite.rex.RexLiteral;
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

public class ToStringFunctionAdapterTests extends OpenSearchTestCase {

    private final ToStringFunctionAdapter adapter = new ToStringFunctionAdapter();

    /** Synthetic tostring operator used to build input RexCalls. */
    private static final SqlFunction TOSTRING = new SqlFunction(
        "tostring",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** {@code tostring(x)} rewrites to {@code CAST(x AS VARCHAR)}. */
    public void testSingleArgRewritesToVarcharCast() {
        Cluster cluster = newCluster();
        RexNode input = cluster.intLiteral(39225);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, input);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("kind=CAST", SqlKind.CAST, out.getKind());
        assertEquals("result type is VARCHAR", SqlTypeName.VARCHAR, out.getType().getSqlTypeName());
        RexCall castCall = (RexCall) out;
        assertEquals("single operand", 1, castCall.getOperands().size());
        assertSame("operand preserved by identity", input, castCall.getOperands().get(0));
    }

    /**
     * {@code tostring(x, 'hex')} stays a {@code tostring} call (operator rebound to the
     * name the Rust UDF registers under) with the numeric argument widened to BIGINT.
     */
    public void testHexFormatKeepsTostringCallAndWidensToBigint() {
        Cluster cluster = newCluster();
        RexNode intInput = cluster.intLiteral(255);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, intInput, cluster.stringLiteral("hex"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTostringCall(out);
        assertEquals("two operands — value + format literal", 2, outCall.getOperands().size());
        RexNode operand = outCall.getOperands().get(0);
        assertEquals("integer widened to BIGINT to match the UDF signature", SqlTypeName.BIGINT, operand.getType().getSqlTypeName());
    }

    /** {@code tostring(bigint, 'binary')} — no CAST needed because the operand is already BIGINT. */
    public void testBinaryFormatOnBigintDoesNotReinsertCast() {
        Cluster cluster = newCluster();
        RexNode bigintInput = cluster.rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(100L),
            cluster.typeFactory.createSqlType(SqlTypeName.BIGINT)
        );
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, bigintInput, cluster.stringLiteral("binary"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTostringCall(out);
        assertSame("bigint operand is used directly — no redundant CAST", bigintInput, outCall.getOperands().get(0));
    }

    /** {@code tostring(double, 'commas')} preserves fractional precision by routing through DOUBLE. */
    public void testCommasFormatOnDoublePreservesFractionalPrecision() {
        Cluster cluster = newCluster();
        RexNode doubleInput = cluster.rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(12.5),
            cluster.typeFactory.createSqlType(SqlTypeName.DOUBLE)
        );
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, doubleInput, cluster.stringLiteral("commas"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTostringCall(out);
        RexNode operand = outCall.getOperands().get(0);
        assertEquals(
            "double kept as DOUBLE — 2-decimal rounding happens inside the UDF",
            SqlTypeName.DOUBLE,
            operand.getType().getSqlTypeName()
        );
    }

    /** {@code tostring(int, 'commas')} widens integer sources to BIGINT, same as every other mode. */
    public void testCommasFormatOnIntegerWidensToBigint() {
        Cluster cluster = newCluster();
        RexNode intInput = cluster.intLiteral(12345);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, intInput, cluster.stringLiteral("commas"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        RexCall outCall = assertTostringCall(out);
        assertEquals(SqlTypeName.BIGINT, outCall.getOperands().get(0).getType().getSqlTypeName());
    }

    /** {@code tostring(x, 'xyzzy')} is an unsupported format; the call is returned unchanged. */
    public void testUnsupportedFormatPassesThrough() {
        Cluster cluster = newCluster();
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, cluster.intLiteral(42), cluster.stringLiteral("xyzzy"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertSame("unknown format mode should leave the RexCall untouched so downstream planning fails loudly", call, out);
    }

    /**
     * {@code tostring(BOOLEAN)} lowers to a {@code CASE} that emits the uppercase
     * {@code 'TRUE'} / {@code 'FALSE'}
     */
    public void testBooleanOneArgLowersToCase() {
        Cluster cluster = newCluster();
        RexNode boolInput = cluster.booleanLiteral(true);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, boolInput);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("boolean tostring lowers to CASE", SqlKind.CASE, out.getKind());
        assertEquals("CASE returns VARCHAR", SqlTypeName.VARCHAR, out.getType().getSqlTypeName());
        RexCall caseCall = (RexCall) out;
        // CASE shape: WHEN value THEN 'TRUE' WHEN NOT value THEN 'FALSE' ELSE NULL.
        assertEquals("CASE has two WHEN branches plus ELSE — 5 operands total", 5, caseCall.getOperands().size());
        assertEquals("first THEN literal is uppercase TRUE", "TRUE", ((RexLiteral) caseCall.getOperands().get(1)).getValueAs(String.class));
        assertEquals(
            "second THEN literal is uppercase FALSE",
            "FALSE",
            ((RexLiteral) caseCall.getOperands().get(3)).getValueAs(String.class)
        );
    }

    /**
     * {@code tostring(BOOLEAN, '<any_format>')} ignores the format
     */
    public void testBooleanTwoArgIgnoresFormat() {
        Cluster cluster = newCluster();
        RexNode boolInput = cluster.nullableBooleanInputRef(0);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(TOSTRING, boolInput, cluster.stringLiteral("hex"));

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("boolean tostring(x, fmt) lowers to CASE regardless of format", SqlKind.CASE, out.getKind());
        RexCall caseCall = (RexCall) out;
        assertEquals("TRUE", ((RexLiteral) caseCall.getOperands().get(1)).getValueAs(String.class));
        assertEquals("FALSE", ((RexLiteral) caseCall.getOperands().get(3)).getValueAs(String.class));
    }

    // ── NUMBER_TO_STRING: PPL's intercepted numeric-to-varchar cast ───────────

    /**
     * PPL's {@code ExtendedRexBuilder.makeCast} rewrites {@code CAST(num AS VARCHAR)} into a
     * {@code NUMBER_TO_STRING(num)} call. That PPL-plugin UDF isn't in any Substrait catalog,
     * so the adapter must lower it back to a plain VARCHAR cast for DataFusion — DataFusion's
     * native numeric-to-string formatting is used in place of Java's {@code Number.toString}.
     */
    public void testNumberToStringLowersToVarcharCast() {
        Cluster cluster = newCluster();
        RexNode doubleInput = cluster.rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(12.3),
            cluster.typeFactory.createSqlType(SqlTypeName.DOUBLE)
        );
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(NUMBER_TO_STRING, doubleInput);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("NUMBER_TO_STRING lowers to CAST", SqlKind.CAST, out.getKind());
        assertEquals("result type is VARCHAR", SqlTypeName.VARCHAR, out.getType().getSqlTypeName());
        RexCall castCall = (RexCall) out;
        assertEquals("single operand", 1, castCall.getOperands().size());
        assertSame("numeric operand preserved by identity", doubleInput, castCall.getOperands().get(0));
    }

    /**
     * {@code NUMBER_TO_STRING} over a DECIMAL source — still lowers to a VARCHAR cast. The
     * adapter branches on operator name, not operand type, so decimal and approximate-numeric
     * paths both route identically.
     */
    public void testNumberToStringOnDecimalLowersToVarcharCast() {
        Cluster cluster = newCluster();
        RelDataType decimalType = cluster.typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        RexNode decimalInput = cluster.rexBuilder.makeExactLiteral(BigDecimal.valueOf(12.3), decimalType);
        RexCall call = (RexCall) cluster.rexBuilder.makeCall(NUMBER_TO_STRING, decimalInput);

        RexNode out = adapter.adapt(call, List.of(), cluster.cluster);

        assertEquals("decimal NUMBER_TO_STRING also lowers to CAST", SqlKind.CAST, out.getKind());
        assertEquals(SqlTypeName.VARCHAR, out.getType().getSqlTypeName());
        RexCall castCall = (RexCall) out;
        assertSame(decimalInput, castCall.getOperands().get(0));
    }

    /** Synthetic {@code NUMBER_TO_STRING} operator — the PPL plugin's
     *  {@code PPLBuiltinOperators.NUMBER_TO_STRING} isn't reachable from this module, so we
     *  declare a same-named clone that the adapter will match by
     *  {@link org.apache.calcite.sql.SqlOperator#getName()}. */
    private static final SqlFunction NUMBER_TO_STRING = new SqlFunction(
        "NUMBER_TO_STRING",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Assert that the rewrite produced a {@code tostring(...)} call routed through
     * {@link ToStringFunctionAdapter#TOSTRING}. Returns the RexCall for further assertions.
     */
    private static RexCall assertTostringCall(RexNode out) {
        assertTrue("expected a RexCall, got " + out.getClass(), out instanceof RexCall);
        RexCall outCall = (RexCall) out;
        assertSame(
            "operator is the synthetic `tostring` that resolves to the Rust UDF",
            ToStringFunctionAdapter.TOSTRING,
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

        RexNode booleanLiteral(boolean value) {
            return rexBuilder.makeLiteral(value);
        }

        RexNode nullableBooleanInputRef(int index) {
            RelDataType boolType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
            return rexBuilder.makeInputRef(boolType, index);
        }
    }
}
