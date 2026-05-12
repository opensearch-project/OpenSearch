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
 * guard). See {@link YearAdapterTests} for the regression-guard rationale.
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
     * {@link YearAdapterTests#testAdaptedCallPreservesOriginalReturnType()} for
     * the original incident.
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
}
