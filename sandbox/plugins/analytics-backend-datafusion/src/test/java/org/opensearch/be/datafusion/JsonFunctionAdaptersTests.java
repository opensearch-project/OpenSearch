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

import java.util.List;

/**
 * Unit tests for the JSON-function adapter inner classes in
 * {@link JsonFunctionAdapters}. Each inner adapter gets its own test method
 * (shape + {@code testAdaptedCallPreservesOriginalReturnType} regression
 * guard). See {@link UnixTimestampAdapterTests} for the regression-guard rationale.
 */
public class JsonFunctionAdaptersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    // ── JsonArrayLengthAdapter ────────────────────────────────────────────

    public void testJsonArrayLengthRewritesToLocalOp() {
        // Synthesize JSON_ARRAY_LENGTH(value) with a single VARCHAR operand.
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType integerNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        SqlFunction pplJsonArrayLengthOp = new SqlFunction(
            "JSON_ARRAY_LENGTH",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(integerNullable),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        RexNode valueRef = rexBuilder.makeInputRef(varcharNullable, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplJsonArrayLengthOp, List.of(valueRef));

        RexNode adapted = new JsonFunctionAdapters.JsonArrayLengthAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target LOCAL_JSON_ARRAY_LENGTH_OP",
            JsonFunctionAdapters.JsonArrayLengthAdapter.LOCAL_JSON_ARRAY_LENGTH_OP,
            call.getOperator()
        );
        assertEquals("json_array_length is unary — no prepend / append", 1, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", valueRef, call.getOperands().get(0));
    }

    /**
     * The adapter MUST preserve the Calcite {@link RelDataType} of the original call.
     * PPL declares {@code JSON_ARRAY_LENGTH} with INTEGER_FORCE_NULLABLE; the
     * locally-declared {@code LOCAL_JSON_ARRAY_LENGTH_OP} uses
     * {@code ReturnTypes.INTEGER_NULLABLE} which would infer a different
     * typeFactory type instance and trip {@code Project.isValid}'s
     * {@code compatibleTypes} check during fragment conversion. See
     * {@link UnixTimestampAdapterTests#testAdaptedCallPreservesOriginalReturnType()}
     * for the original incident.
     */
    public void testJsonArrayLengthPreservesOriginalReturnType() {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        // Pick a type that specifically differs from what LOCAL_JSON_ARRAY_LENGTH_OP's
        // ReturnTypes.INTEGER_NULLABLE would compute — BIGINT here — so the
        // regression assertion actually distinguishes "preserve" from "infer".
        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        SqlFunction pplJsonArrayLengthOp = new SqlFunction(
            "JSON_ARRAY_LENGTH",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(bigintNullable),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        RexNode valueRef = rexBuilder.makeInputRef(varcharNullable, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplJsonArrayLengthOp, List.of(valueRef));
        assertEquals(bigintNullable, original.getType());

        RexNode adapted = new JsonFunctionAdapters.JsonArrayLengthAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original call's return type, "
                + "otherwise the enclosing Project.rowType assertion fails in fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }

    // ── JsonValidAdapter ──────────────────────────────────────────────────

    public void testJsonValidRewritesToLocalOp() {
        // Synthesize JSON_VALID(value). In production the source operator is
        // SqlStdOperatorTable.IS_JSON_VALUE (a SqlPostfixOperator) but the
        // adapter's contract is purely shape-based — any single-VARCHAR-operand
        // RexCall must rewrite to LOCAL_JSON_VALID_OP. Using a SqlFunction stand-in
        // exercises the same code path and keeps the test self-contained.
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType booleanNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
        SqlFunction pplJsonValidOp = new SqlFunction(
            "JSON_VALID",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(booleanNullable),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        RexNode valueRef = rexBuilder.makeInputRef(varcharNullable, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplJsonValidOp, List.of(valueRef));

        RexNode adapted = new JsonFunctionAdapters.JsonValidAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target LOCAL_JSON_VALID_OP",
            JsonFunctionAdapters.JsonValidAdapter.LOCAL_JSON_VALID_OP,
            call.getOperator()
        );
        assertEquals("json_valid is unary — no prepend / append", 1, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", valueRef, call.getOperands().get(0));
    }

    /**
     * Same regression guard as {@link #testJsonArrayLengthPreservesOriginalReturnType}:
     * the adapted call must keep the original call's {@link RelDataType} instance so
     * the cached {@code Project.rowType} matches post-adaptation. Production
     * {@code IS_JSON_VALUE} returns {@code BOOLEAN_NULLABLE}, same as
     * {@code LOCAL_JSON_VALID_OP}, so a naive {@code rexBuilder.makeCall(op, args)}
     * would happen to produce the right type — pick a differently-nullable BOOLEAN
     * here to make the assertion actually distinguish "preserve" from "infer".
     */
    public void testJsonValidPreservesOriginalReturnType() {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType booleanNotNull = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
        SqlFunction pplJsonValidOp = new SqlFunction(
            "JSON_VALID",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(booleanNotNull),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        RexNode valueRef = rexBuilder.makeInputRef(varcharNullable, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplJsonValidOp, List.of(valueRef));
        assertEquals(booleanNotNull, original.getType());

        RexNode adapted = new JsonFunctionAdapters.JsonValidAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original call's return type, "
                + "otherwise the enclosing Project.rowType assertion fails in fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }

    // ── JsonAdapter ───────────────────────────────────────────────────────

    public void testJsonRewritesToLocalOp() {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction pplJsonOp = new SqlFunction(
            "JSON",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        RexNode valueRef = rexBuilder.makeInputRef(varcharNullable, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplJsonOp, List.of(valueRef));

        RexNode adapted = new JsonFunctionAdapters.JsonAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame("adapted call must target LOCAL_JSON_OP", JsonFunctionAdapters.JsonAdapter.LOCAL_JSON_OP, call.getOperator());
        assertEquals("json is unary — no prepend / append", 1, call.getOperands().size());
        assertSame(valueRef, call.getOperands().get(0));
    }

    // ── JsonObjectAdapter ─────────────────────────────────────────────────

    /**
     * Strips the leading SqlJsonConstructorNullClause flag and inserts a
     * per-value type tag between each (key, value) pair so the Rust UDF can
     * emit numerics unquoted vs strings quoted.
     */
    public void testJsonObjectStripsFlagAndInsertsTypeTags() {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType integerNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        // Calcite's JSON_OBJECT prepends a SYMBOL-typed flag operand. Synthesize one with makeFlag.
        RexNode flag = rexBuilder.makeFlag(org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL);
        RexNode keyLiteral = rexBuilder.makeLiteral("name");
        RexNode intLiteral = rexBuilder.makeLiteral(5, integerNullable);
        SqlFunction stdJsonObjectOp = new SqlFunction(
            "JSON_OBJECT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );
        RexCall original = (RexCall) rexBuilder.makeCall(stdJsonObjectOp, List.of(flag, keyLiteral, intLiteral));

        RexNode adapted = new JsonFunctionAdapters.JsonObjectAdapter().adapt(original, List.of(), cluster);

        RexCall call = (RexCall) adapted;
        assertSame(JsonFunctionAdapters.JsonObjectAdapter.LOCAL_JSON_OBJECT_OP, call.getOperator());
        // Expected operand layout: [key, "n", castInt]. Flag stripped; tag "n" inserted before integer.
        assertEquals("triple per pair", 3, call.getOperands().size());
        RexNode tag = call.getOperands().get(1);
        assertEquals("numeric value gets tag 'n'", "n", ((org.apache.calcite.rex.RexLiteral) tag).getValueAs(String.class));
    }

    // ── JsonArrayAdapter ──────────────────────────────────────────────────

    /** Strips the flag and inserts one tag per value. Mixed-type input → mixed tags. */
    public void testJsonArrayStripsFlagAndTagsHeterogeneousValues() {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RelDataType integerNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode flag = rexBuilder.makeFlag(org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL);
        RexNode intLit = rexBuilder.makeLiteral(1, integerNullable);
        RexNode strLit = rexBuilder.makeLiteral("Tom");
        SqlFunction stdJsonArrayOp = new SqlFunction(
            "JSON_ARRAY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING
        );
        RexCall original = (RexCall) rexBuilder.makeCall(stdJsonArrayOp, List.of(flag, intLit, strLit));

        RexNode adapted = new JsonFunctionAdapters.JsonArrayAdapter().adapt(original, List.of(), cluster);

        RexCall call = (RexCall) adapted;
        assertSame(JsonFunctionAdapters.JsonArrayAdapter.LOCAL_JSON_ARRAY_OP, call.getOperator());
        // Expected operand layout: [tag1, val1, tag2, val2]. 4 ops total.
        assertEquals("pair per value", 4, call.getOperands().size());
        assertEquals("n", ((org.apache.calcite.rex.RexLiteral) call.getOperands().get(0)).getValueAs(String.class));
        assertEquals("s", ((org.apache.calcite.rex.RexLiteral) call.getOperands().get(2)).getValueAs(String.class));
    }
}
